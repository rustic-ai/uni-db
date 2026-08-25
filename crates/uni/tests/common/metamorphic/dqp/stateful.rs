//! The batch-per-state driver for Tier-1 levers.
//!
//! # Why the loop is inverted
//!
//! A Tier-2 lever holds two execution paths open *simultaneously* — primary and
//! a fork, live and pinned — so [`super::driver::drive_prepared`] can run case
//! after case, comparing both sides each time.
//!
//! A Tier-1 transition is **global and irreversible**. There is no "unflush", no
//! un-create-index, no un-compact. Once the instance has flushed, every
//! subsequent query sees the post-flush world, so the obvious loop
//!
//! ```text
//! for case in cases { side_a(case); transition(); side_b(case) }
//! ```
//!
//! compares A against B on the first case and **B against B on every case
//! after** — passing vacuously forever while looking like it did 20 000
//! comparisons. This is the same failure the activation witness exists to catch,
//! reached by a different route, and it is why the driver is a separate function
//! rather than a flag on the existing one.
//!
//! So the loop inverts: generate a batch of `k` cases, run **all** of them
//! against state A, apply the transition **once**, replay **the same** `k` cases
//! against state B, compare pairwise. Then throw the instance away and build the
//! next batch. The activation witness still guards each pair, so an inverted loop
//! that silently stopped transitioning would fail on the floor rather than pass.
//!
//! # Why reproduction is by seed and not by shrinking
//!
//! Proptest's shrinker re-runs a failing case against the *current* state to
//! decide whether a smaller input still fails. After a Tier-1 transition side A's
//! state no longer exists, so every shrink replay would run against side B and
//! the shrinker would conclude the failure is not reproducible.
//!
//! Rather than let it draw that wrong conclusion, this driver does not use it. It
//! generates cases from an explicit `TestRng`, whose seed proptest guarantees
//! reproduces "the same sequence of values on all systems and all supporting
//! versions", and prints the exact `(seed, batch, case)` coordinates on failure.
//! [`replay_stateful`] rebuilds that one batch and re-runs that one case.
//!
//! The cost is real and worth stating: a failure reports the *generated* query,
//! not a minimized one. Reducing it is manual. The alternative was a shrinker
//! that reports "not reproducible" on every genuine bug.

use std::cell::Cell;

use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

use super::driver::{Budgets, CaseKind, Db, PrepareFut};
use super::lever::{Observed, Witness};
use super::seed::{Fixture, Tier, build_dqp_seed_for};
use crate::diff::{BagDiff, RowBag, bag_eq};
use crate::querygen::{Case, render::render};

/// Cases per batch — one fixture build and one transition per batch.
///
/// From the Phase-0A measurement: the tiny fixture builds in 0.163 s, so 20 000
/// soak cases at `k = 500` cost 40 rebuilds ≈ 6.5 s of fixture construction. On
/// smoke (0.788 s) a 2 000-case run costs 4 rebuilds ≈ 3 s. Both negligible.
///
/// The **large** tier is forbidden here entirely — see [`drive_stateful`].
pub const BATCH: u32 = 500;

/// Default RNG seed, overridable with `DQP_SEED`.
///
/// Fixed by default so a PR-lane failure is reproducible without having to
/// recover the seed from a log that may have rotated. Nightly runs may set
/// `DQP_SEED` to vary coverage; the seed in use is printed at the start of every
/// run either way, so a varying run is still reproducible from its own output.
const DEFAULT_SEED: u64 = 0x00D1_FFED_5EED_0004;

/// The run seed: `DQP_SEED`, or [`DEFAULT_SEED`].
pub fn run_seed() -> u64 {
    std::env::var("DQP_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SEED)
}

/// Expands `(seed, batch)` into the 32 bytes `RngAlgorithm::ChaCha` requires.
///
/// SplitMix64, so the two inputs are diffused across the whole seed rather than
/// leaving 20 of the 32 bytes zero — adjacent batch numbers should not produce
/// correlated case sequences.
fn seed_bytes(seed: u64, batch: u32) -> [u8; 32] {
    let mut state = seed ^ (u64::from(batch).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut out = [0u8; 32];
    for chunk in out.chunks_exact_mut(8) {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        chunk.copy_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    out
}

/// Generates the `n` cases belonging to one batch.
///
/// A pure function of `(kind, tier, seed, batch)`, which is what makes
/// [`replay_stateful`] possible: the same coordinates always yield the same
/// queries in the same order.
fn batch_cases(kind: CaseKind, f: impl Into<Fixture>, seed: u64, batch: u32, n: u32) -> Vec<Case> {
    let strategy = super::driver::strategy_for(kind, f);
    let mut runner = TestRunner::new_with_rng(
        Config {
            failure_persistence: None,
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes(seed, batch)),
    );
    (0..n)
        .map(|_| {
            strategy
                .new_tree(&mut runner)
                .expect("strategy produced no value")
                .current()
        })
        .collect()
}

/// One execution path that a global, irreversible transition moves between.
///
/// The contrast with [`super::lever::Lever`] is the whole point: a `Lever` has a
/// `side_a` and a `side_b` that can be called in any order, any number of times.
/// A `StatefulLever` has **one** observation method whose meaning depends on when
/// it is called, and a [`Self::transition`] that may be called exactly once.
pub trait StatefulLever {
    /// Short name, used in failure messages and the activation report.
    fn name(&self) -> &'static str;

    /// Runs `q` against whatever state the lever is in right now.
    async fn observe_now(&self, q: &uni_cypher::ast::Query) -> anyhow::Result<Observed>;

    /// Applies the transition. Called **exactly once**, between the two passes.
    ///
    /// Takes `&mut self` to make the one-shot nature structural rather than a
    /// comment: a lever cannot hand this out to be called from a shared context.
    async fn transition(&mut self) -> anyhow::Result<()>;

    /// Did the two states genuinely exercise different machinery?
    ///
    /// `a` is the witness from before the transition, `b` from after. As on
    /// [`super::lever::Lever::activated`] this has **no default body**.
    /// See [`super::lever::Lever::activated`] for why this takes the whole
    /// [`Observed`] rather than just the [`Witness`].
    fn activated(&self, a: &Observed, b: &Observed) -> bool;

    /// The law relating a pass-1 observation to its pass-2 counterpart.
    ///
    /// Defaults to equality, which is what every physical-state lever wants:
    /// the same query over the same data, executed differently, must return the
    /// same bag.
    ///
    /// A lever whose transition changes the *data* needs a weaker law, because
    /// the two sides then differ legitimately. Deletion is the case in hand:
    /// removing rows can only take answers away, so the law is containment, and
    /// a row present afterwards that was absent before is the defect.
    fn compare(&self, before: &RowBag, after: &RowBag) -> Result<(), BagDiff> {
        bag_eq(before, after)
    }

    /// Anything else that must hold for the comparison to mean something.
    /// Called before the first pass and after the second.
    ///
    /// # Errors
    ///
    /// Returns an error describing the violated invariant.
    async fn check_invariants(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Runs a Tier-1 lever over `cases` generated queries, in batches of [`BATCH`].
///
/// # Panics
///
/// Panics (failing the test) on a bag inequality, a broken invariant, or a
/// breach of the activation floor or row budget.
///
/// The **large tier is rejected outright**, and that is a measurement rather
/// than a preference: Phase 0A timed the large fixture at **621.73 s** to build.
/// At `k = 500` over 50 000 cases that is 100 rebuilds — 17.3 hours of fixture
/// construction before a single comparison runs. The large tier is Tier-2 only,
/// where the fixture is built exactly once per run.
pub fn drive_stateful<L, P>(cases: u32, f: impl Into<Fixture>, kind: CaseKind, prepare: P)
where
    L: StatefulLever,
    P: for<'a> Fn(&'a Db) -> PrepareFut<'a, L>,
{
    let f = f.into();
    drive_stateful_with(
        cases,
        f,
        kind,
        BATCH,
        Budgets::for_tier(f.tier, cases),
        prepare,
    );
}

/// [`drive_stateful`] with an explicit batch size and budgets, so the guards can
/// be tested.
///
/// # Panics
///
/// As [`drive_stateful`].
pub fn drive_stateful_with<L, P>(
    cases: u32,
    f: impl Into<Fixture>,
    kind: CaseKind,
    batch_size: u32,
    budgets: Budgets,
    prepare: P,
) where
    L: StatefulLever,
    P: for<'a> Fn(&'a Db) -> PrepareFut<'a, L>,
{
    let f = f.into();
    assert!(
        f.tier != Tier::Large,
        "the large tier is forbidden to drive_stateful: it measured 621.73 s per \
         fixture build in Phase 0A, and this driver rebuilds once per batch. Use \
         drive_prepared, which builds exactly once per run."
    );
    assert!(batch_size > 0, "batch size must be positive");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let seed = run_seed();
    let batches = cases.div_ceil(batch_size);
    let mut name = "unprepared";
    let total = Cell::new(0u64);
    let activated = Cell::new(0u64);
    let rows = Cell::new(0usize);

    for batch in 0..batches {
        let n = batch_size.min(cases - batch * batch_size);
        let queries: Vec<_> = batch_cases(kind, f, seed, batch, n)
            .into_iter()
            .map(|case| {
                if let Some(why) = kind.inadmissible(&case) {
                    panic!(
                        "[dqp] generated an inadmissible case for the {} kind: {why}.\n  {}",
                        kind.name(),
                        render(&case.base_query()),
                    );
                }
                case.base_query()
            })
            .collect();

        let db: Db =
            std::sync::Arc::new(rt.block_on(build_dqp_seed_for(f)).expect("build dqp seed"));
        let mut lever = rt.block_on(prepare(&db)).expect("prepare lever");
        name = lever.name();
        rt.block_on(lever.check_invariants())
            .expect("lever invariants violated before the batch");

        // Pass 1: every case against state A, before anything changes.
        let side_a: Vec<Observed> = queries
            .iter()
            .map(|q| rt.block_on(lever.observe_now(q)).expect("side A"))
            .collect();

        rt.block_on(lever.transition()).expect("transition");
        rt.block_on(lever.check_invariants())
            .expect("lever invariants violated by the transition");

        // Pass 2: the same cases, same order, against state B.
        for (index, q) in queries.iter().enumerate() {
            let b = rt.block_on(lever.observe_now(q)).expect("side B");
            let a = &side_a[index];

            total.set(total.get() + 1);
            if lever.activated(a, &b) {
                activated.set(activated.get() + 1);
            }
            let case_rows = a.bag.total + b.bag.total;
            rows.set(rows.get() + case_rows);

            assert!(
                case_rows <= budgets.per_case,
                "[{name}] one case returned {case_rows} rows against a per-case \
                 ceiling of {}. Offending query:\n  {}",
                budgets.per_case,
                render(q),
            );

            if let Err(d) = lever.compare(&a.bag, &b.bag) {
                panic!(
                    "[{name}] the two states disagree.\n\
                     query: {}\n\
                     before: {:?}\n\
                     after:  {:?}\n\
                     {d}\n\n\
                     repro: DQP_SEED={seed} DQP_BATCH={batch} DQP_CASE={index} \
                     cargo nextest run -p uni-db --test integration --no-capture \
                     -E 'test({name_replay})'",
                    render(q),
                    a.witness,
                    b.witness,
                    name_replay = replay_test_hint(name),
                );
            }
        }
    }

    let rate = if total.get() == 0 {
        0.0
    } else {
        activated.get() as f64 / total.get() as f64
    };
    eprintln!(
        "[dqp:{name}] seed={seed} batches={batches} cases={} activated={} ({:.1}%) rows={}",
        total.get(),
        activated.get(),
        rate * 100.0,
        rows.get(),
    );

    assert!(
        total.get() > 0,
        "[{name}] ran zero cases — the generator produced nothing, so this run \
         proves nothing"
    );
    assert!(
        rate >= budgets.min_activation,
        "[{name}] activation rate {:.1}% is below the {:.0}% floor: {} of {} cases \
         saw no difference between the two states. Either the witness has drifted, \
         or — the failure this driver's inverted loop exists to prevent — the \
         transition is no longer taking effect between the passes.",
        rate * 100.0,
        budgets.min_activation * 100.0,
        total.get() - activated.get(),
        total.get(),
    );
    assert!(
        rows.get() <= budgets.run_total,
        "[{name}] run row budget exceeded: {} rows across {} cases against a ceiling \
         of {}. The generator has drifted toward less selective queries, or the \
         fixture grew.",
        rows.get(),
        total.get(),
        budgets.run_total,
    );
}

/// Best-effort mapping from a lever name to its replay test, for the repro line.
fn replay_test_hint(lever: &str) -> String {
    format!("{}_replay", lever.replace('-', "_"))
}

/// Re-runs exactly one case from a failed [`drive_stateful`] run.
///
/// This is the reproduction path the seed-based design buys. Given the
/// coordinates printed in the failure, it rebuilds that batch's fixture, replays
/// the same generated cases in the same order, and compares that one case —
/// printing both witnesses and both bags whether it passes or fails, because
/// "the failure did not reproduce" is itself a result worth seeing plainly.
///
/// # Panics
///
/// Panics if the case index is out of range for the batch.
pub fn replay_stateful<L, P>(
    seed: u64,
    batch: u32,
    index: u32,
    f: impl Into<Fixture>,
    kind: CaseKind,
    batch_size: u32,
    prepare: P,
) where
    L: StatefulLever,
    P: for<'a> Fn(&'a Db) -> PrepareFut<'a, L>,
{
    let f = f.into();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let cases = batch_cases(kind, f, seed, batch, batch_size);
    let case = cases
        .get(index as usize)
        .unwrap_or_else(|| panic!("case {index} is out of range for a batch of {batch_size}"));
    let q = case.base_query();

    let db: Db = std::sync::Arc::new(rt.block_on(build_dqp_seed_for(f)).expect("build dqp seed"));
    let mut lever = rt.block_on(prepare(&db)).expect("prepare lever");
    let name = lever.name();

    println!("[dqp:replay {name}] seed={seed} batch={batch} case={index}");
    println!("  query: {}", render(&q));

    // The whole batch is replayed up to `index`, not just the one case: the
    // transition is one-shot, so a case's state includes every read that
    // preceded it. Skipping them would replay a different execution.
    let mut side_a = None;
    for (i, c) in cases.iter().enumerate().take(index as usize + 1) {
        let o = rt.block_on(lever.observe_now(&c.base_query())).expect("A");
        if i == index as usize {
            side_a = Some(o);
        }
    }
    let a = side_a.expect("side A observed");

    rt.block_on(lever.transition()).expect("transition");

    let mut side_b = None;
    for (i, c) in cases.iter().enumerate().take(index as usize + 1) {
        let o = rt.block_on(lever.observe_now(&c.base_query())).expect("B");
        if i == index as usize {
            side_b = Some(o);
        }
    }
    let b = side_b.expect("side B observed");

    println!("  before: {:?}", a.witness);
    println!("  after:  {:?}", b.witness);
    println!("  activated: {}", lever.activated(&a, &b));
    match lever.compare(&a.bag, &b.bag) {
        Ok(()) => println!("  bags AGREE — the failure did not reproduce at these coordinates"),
        Err(d) => panic!("  bags DISAGREE, as reported:\n{d}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{BATCH, batch_cases, seed_bytes};
    use crate::metamorphic::dqp::driver::CaseKind;
    use crate::metamorphic::dqp::seed::Tier;
    use crate::querygen::render::render;

    /// Reproduction is the entire reason this driver generates its own cases, so
    /// the guarantee it rests on gets a test: the same coordinates must yield the
    /// same queries, in the same order.
    #[test]
    fn batch_generation_is_reproducible_from_its_coordinates() {
        let first = batch_cases(CaseKind::Plain, Tier::Tiny, 42, 3, 16);
        let again = batch_cases(CaseKind::Plain, Tier::Tiny, 42, 3, 16);
        let rendered = |cs: &[crate::querygen::Case]| -> Vec<String> {
            cs.iter().map(|c| render(&c.base_query())).collect()
        };
        assert_eq!(
            rendered(&first),
            rendered(&again),
            "the same (seed, batch) produced different cases — the repro line \
             printed on failure would be worthless"
        );
    }

    /// And different coordinates must differ, or every batch would test the same
    /// queries and the case count would be a lie.
    #[test]
    fn different_coordinates_produce_different_cases() {
        let rendered = |seed: u64, batch: u32| -> Vec<String> {
            batch_cases(CaseKind::Plain, Tier::Tiny, seed, batch, 32)
                .iter()
                .map(|c| render(&c.base_query()))
                .collect()
        };
        assert_ne!(
            rendered(42, 0),
            rendered(42, 1),
            "adjacent batches generated identical cases"
        );
        assert_ne!(
            rendered(42, 0),
            rendered(43, 0),
            "adjacent seeds generated identical cases"
        );
    }

    /// The seed expansion must diffuse both inputs across all 32 bytes. A naive
    /// `seed.to_le_bytes()` into a zeroed array would leave 24 bytes constant and
    /// adjacent batches highly correlated.
    #[test]
    fn seed_expansion_diffuses_both_inputs() {
        let a = seed_bytes(1, 0);
        let b = seed_bytes(1, 1);
        let c = seed_bytes(2, 0);
        assert_ne!(a, b);
        assert_ne!(a, c);
        let differing = a.iter().zip(b.iter()).filter(|(x, y)| x != y).count();
        assert!(
            differing > 16,
            "a one-batch change altered only {differing} of 32 seed bytes"
        );
        assert!(
            a.iter().any(|&x| x != 0),
            "seed expansion produced all zeros"
        );
    }

    /// **The exit criterion: a failure reproduces from its printed coordinates
    /// alone.**
    ///
    /// This driver gave up proptest's shrinker in exchange for seed-based
    /// replay, and an untested replay path would make that a bad trade made
    /// silently — the repro line would be printed on every failure and work on
    /// none of them.
    ///
    /// So a fault is injected at a known case, the driver is run, and the
    /// coordinates it prints are fed back to [`super::replay_stateful`]. All
    /// three parts are asserted, and the third is what makes it a proof rather
    /// than a demonstration: replay at a *different* case must **not** fail. A
    /// replay that panicked wherever it was pointed would satisfy the first two.
    #[test]
    fn a_failure_reproduces_from_its_printed_coordinates() {
        use super::{drive_stateful_with, replay_stateful, run_seed};
        use crate::metamorphic::dqp::driver::Budgets;
        use crate::metamorphic::dqp::flush_lever::FlushLever;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        const BATCH_N: u32 = 6;
        const FAIL_AT: u32 = 3;

        let budgets = Budgets {
            per_case: usize::MAX,
            run_total: usize::MAX,
            min_activation: 0.0,
        };

        let message = catch_unwind(AssertUnwindSafe(|| {
            drive_stateful_with(
                BATCH_N,
                Tier::Tiny,
                CaseKind::Plain,
                BATCH_N,
                budgets,
                |db| Box::pin(FaultyLever::prepare(db, FAIL_AT)),
            );
        }))
        .expect_err("the injected fault did not fail the run");
        let message = panic_text(&message);

        assert!(
            message.contains("the two states disagree"),
            "the driver failed for some other reason:\n{message}"
        );
        assert!(
            message.contains(&format!("DQP_BATCH=0 DQP_CASE={FAIL_AT}")),
            "the repro line does not name the case that failed:\n{message}"
        );

        // The coordinates the driver printed, replayed.
        let replay = |index: u32| {
            catch_unwind(AssertUnwindSafe(|| {
                replay_stateful(
                    run_seed(),
                    0,
                    index,
                    Tier::Tiny,
                    CaseKind::Plain,
                    BATCH_N,
                    |db| Box::pin(FaultyLever::prepare(db, FAIL_AT)),
                );
            }))
        };

        let reproduced = replay(FAIL_AT).expect_err("replay did not reproduce the failure");
        assert!(
            panic_text(&reproduced).contains("bags DISAGREE"),
            "replay failed, but not with the bag difference it was pointed at:\n{}",
            panic_text(&reproduced)
        );

        // And it is genuinely pointing at that case, not failing wherever aimed.
        assert!(
            replay(FAIL_AT + 1).is_ok(),
            "replay reported a failure at a case that did not fail — it is not \
             reproducing the reported bug, it is failing unconditionally"
        );
    }

    /// Extracts a panic payload as text.
    fn panic_text(payload: &Box<dyn std::any::Any + Send>) -> String {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .unwrap_or_else(|| "<non-string panic payload>".to_string())
    }

    /// A [`super::StatefulLever`] that corrupts one known case after the
    /// transition — a stand-in for the bug this oracle is built to catch.
    ///
    /// It wraps the real flush lever rather than faking one, so the replay path
    /// is exercised against a lever that really does build a fixture, really does
    /// transition, and really does read rows. Only the one corrupted bag is
    /// synthetic.
    struct FaultyLever {
        inner: crate::metamorphic::dqp::flush_lever::FlushLever,
        transitioned: std::cell::Cell<bool>,
        calls: std::cell::Cell<u32>,
        fail_at: u32,
    }

    impl FaultyLever {
        async fn prepare(
            db: &crate::metamorphic::dqp::driver::Db,
            fail_at: u32,
        ) -> anyhow::Result<Self> {
            Ok(Self {
                inner: crate::metamorphic::dqp::flush_lever::FlushLever::prepare(db).await?,
                transitioned: std::cell::Cell::new(false),
                calls: std::cell::Cell::new(0),
                fail_at,
            })
        }
    }

    impl super::StatefulLever for FaultyLever {
        fn name(&self) -> &'static str {
            "faulty-for-replay-testing"
        }

        async fn observe_now(
            &self,
            q: &uni_cypher::ast::Query,
        ) -> anyhow::Result<crate::metamorphic::dqp::lever::Observed> {
            let index = self.calls.get();
            self.calls.set(index + 1);
            let mut o = self.inner.observe_now(q).await?;

            // The call counter resets at the transition, so `index` is the case
            // index within whichever pass is running — which is what makes the
            // fault land on the same case under both the driver (n calls per
            // pass) and replay (index+1 calls per pass).
            if self.transitioned.get() && index == self.fail_at {
                // Drop one row if there is one, invent one if there is not, so
                // the fault fires regardless of the generated case's selectivity.
                let key = o.bag.counts.keys().next().cloned();
                match key {
                    Some(k) => {
                        let c = o.bag.counts.get_mut(&k).expect("key just read");
                        *c -= 1;
                        if *c == 0 {
                            o.bag.counts.remove(&k);
                        }
                        o.bag.total -= 1;
                    }
                    None => {
                        o.bag
                            .counts
                            .insert(crate::diff::CanonRow(vec![uni_query::Value::Int(-1)]), 1);
                        o.bag.total += 1;
                    }
                }
            }
            Ok(o)
        }

        async fn transition(&mut self) -> anyhow::Result<()> {
            self.inner.transition().await?;
            self.transitioned.set(true);
            self.calls.set(0);
            Ok(())
        }

        fn activated(&self, a: &super::Observed, b: &super::Observed) -> bool {
            self.inner.activated(a, b)
        }
    }

    /// The batch constant is what keeps fixture rebuild cost negligible; a
    /// change to it should be deliberate.
    #[test]
    fn batch_size_matches_the_measured_budget() {
        assert_eq!(
            BATCH, 500,
            "BATCH drives the fixture rebuild count: at the Phase-0A tiny-tier \
             build cost of 0.163 s, 20 000 cases at k=500 is 40 rebuilds ≈ 6.5 s. \
             Halving it doubles that."
        );
    }
}
