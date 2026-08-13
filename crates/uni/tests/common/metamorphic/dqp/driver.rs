//! The prepared-state driver for Tier-2 levers.
//!
//! # Why this is not `metamorphic::drive`
//!
//! `drive` builds one `Uni`, shares it read-only across every case, and opens a
//! fresh session per query. That is exactly right for TLP and NoREC, which
//! rewrite one query and run both forms under the same configuration.
//!
//! Tier-2 DQP levers cannot use it, for a reason an earlier revision of the
//! proposal got wrong. It claimed these levers were *stateless* and could reuse
//! `drive` unchanged. They are not: creating a fork flushes the parent's L0 —
//! deliberately, as the fix for #97 — and then persists a registry entry, an
//! allocator and one Lance branch per dataset. Pinning likewise needs a snapshot,
//! and `create_snapshot` also flushes. Both mutate the instance `drive` assumes
//! is immutable, and both are far too expensive to pay per case.
//!
//! The resolution is *prepared state*: perform the flush and the fork creation
//! **once, before any case runs**, then hold both sides open read-only for the
//! whole run. That preserves the property that actually mattered — no per-case
//! state change — without the false claim that no state change occurs at all.
//!
//! # Shrinking still works here
//!
//! Because the case loop is read-only once prepared, proptest's in-place
//! shrinker remains valid: a failing case can be re-run against the identical
//! state. That is *not* true of the Tier-1 levers (flush, index creation,
//! compaction), whose transitions are global and irreversible — those need the
//! batch-per-state driver, and are out of scope here.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;

use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::lever::Lever;
use super::seed::{Tier, build_dqp_seed};
use crate::diff::bag_eq;
use crate::querygen::{Case, arb_case};

/// Default case count for the PR lane.
///
/// Measured at ~32 ms per case per side on the tiny fixture
/// (`docs/perf/dqp-feasibility-2026-08-12.md`), so ~32 s for both sides.
const SMOKE_CASES: u32 = 500;
/// Default case count for the nightly soak — ~21 min at the measured rate.
const SOAK_CASES: u32 = 20_000;

/// Fraction of cases that must genuinely activate the lever before a run counts.
///
/// Not 100%: some generated cases legitimately fail to engage a path (a filter
/// that matches nothing may short-circuit before a scan runs). But a lever
/// drifting toward vacuity should fail loudly rather than decay into silence,
/// and 80% is far enough below the ~100% a healthy lever reports to avoid
/// flapping while still catching real drift.
const MIN_ACTIVATION_RATE: f64 = 0.80;

/// Smoke-tier case count: `DQP_CASES` or [`SMOKE_CASES`].
///
/// Deliberately **not** `METAMORPHIC_CASES`: the nightly job pins that to 50 000
/// for the other oracles, which would put this lever 2.5× over the budget
/// measured for it.
pub fn smoke_cases() -> u32 {
    env_cases(SMOKE_CASES)
}

/// Soak-tier case count: `DQP_CASES` or [`SOAK_CASES`].
pub fn soak_cases() -> u32 {
    env_cases(SOAK_CASES)
}

fn env_cases(default: u32) -> u32 {
    std::env::var("DQP_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Mean rows returned per case per **side**, measured in Phase 0A.
fn mean_rows_per_side(tier: Tier) -> usize {
    match tier {
        Tier::Tiny => 1_959,
        Tier::Smoke => 19_517,
        Tier::Large => 97_559,
    }
}

/// Worst-case rows a single case may return per **side** — the measured maximum,
/// which is the fixture's edge count.
fn max_rows_per_side(tier: Tier) -> usize {
    match tier {
        Tier::Tiny => 4_000,
        Tier::Smoke => 40_000,
        Tier::Large => 200_000,
    }
}

/// Per-case ceiling: one case may not return more than the fixture's worst case,
/// doubled for the two sides, with headroom.
///
/// Split out from the run total on purpose. The run total accumulates across
/// proptest's **shrink replays**, so enforcing only a running sum inside the case
/// loop lets a long shrink sequence trip the budget and replace a genuine
/// divergence report with a budget message — the harness hiding the very bug it
/// just found. A per-case ceiling cannot be inflated that way, so it is the one
/// that fires mid-run.
fn per_case_budget(tier: Tier) -> usize {
    max_rows_per_side(tier) * 2 + max_rows_per_side(tier) // 3x max-per-side
}

/// Ceiling on rows materialized into `RowBag`s across a whole run.
///
/// Checked **after** the run and only when it otherwise passed, for the reason
/// above. Set from the Phase-0A mean with a 1.5x allowance, doubled because the
/// driver runs two sides per case — a factor the first version of this function
/// omitted, which made the budget fire on a healthy run.
///
/// Enforced over rows **returned**, not rows scanned: the proposal specified the
/// latter, but Phase 0A measured `rows_scanned` reading zero on every path at
/// the time, so a budget over it could never have fired.
fn run_budget(tier: Tier, cases: u32) -> usize {
    (mean_rows_per_side(tier) * 2 * cases as usize * 3) / 2
}

/// A boxed future borrowing the db for `'a` — the same shape
/// `metamorphic::drive` uses, for the same reason.
pub type PrepareFut<'a, L> = Pin<Box<dyn Future<Output = anyhow::Result<L>> + 'a>>;

/// The run-level floors a driven lever is held to.
///
/// Extracted as a parameter so the guards can be tested. A guard nobody has ever
/// seen fire is indistinguishable from one that cannot fire, which is the exact
/// class of defect this whole oracle exists to catch — so the tests at the
/// bottom of this file drive real levers against deliberately impossible
/// budgets.
#[derive(Clone, Copy, Debug)]
pub struct Budgets {
    /// Maximum rows one case may return across both sides.
    pub per_case: usize,
    /// Maximum rows the whole run may return across both sides.
    pub run_total: usize,
    /// Minimum fraction of cases that must activate the lever.
    pub min_activation: f64,
}

impl Budgets {
    /// The defaults for a tier, from the Phase-0A measurements.
    pub fn for_tier(tier: Tier, cases: u32) -> Self {
        Self {
            per_case: per_case_budget(tier),
            run_total: run_budget(tier, cases),
            min_activation: MIN_ACTIVATION_RATE,
        }
    }
}

/// Runs a Tier-2 lever over `cases` generated queries against a fixture at
/// `tier`.
///
/// Builds the seed and prepares the lever **once**, then compares both sides per
/// case. Fails the test on the first bag inequality, on any case where the lever
/// did not activate, or — after the run — if the activation rate or row budget
/// was breached.
///
/// `prepare` follows the boxed-free-async-fn shape the existing `drive` callers
/// use: pass `|db| Box::pin(MyLever::prepare(db))`, not an inline
/// `Box::pin(async move { … })`, which fails HRTB inference here.
///
/// # Panics
///
/// Panics (failing the test) if the seed cannot be built, the lever cannot be
/// prepared, any case violates the law, or the run-level floors are breached.
pub fn drive_prepared<L, P>(cases: u32, tier: Tier, prepare: P)
where
    L: Lever,
    P: for<'a> FnOnce(&'a uni_db::Uni) -> PrepareFut<'a, L>,
{
    drive_prepared_with(cases, tier, Budgets::for_tier(tier, cases), prepare);
}

/// [`drive_prepared`] with explicit budgets.
///
/// # Panics
///
/// As [`drive_prepared`].
pub fn drive_prepared_with<L, P>(cases: u32, tier: Tier, budgets: Budgets, prepare: P)
where
    L: Lever,
    P: for<'a> FnOnce(&'a uni_db::Uni) -> PrepareFut<'a, L>,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let db = rt.block_on(build_dqp_seed(tier)).expect("build dqp seed");
    // Explicit, though `create_fork_2pc` flushes too: doing it here makes the
    // fork point a stated precondition of the run rather than a side effect of
    // the lever, so a future change to the fork path cannot silently move it.
    rt.block_on(db.flush()).expect("flush seed");

    let lever = rt.block_on(prepare(&db)).expect("prepare lever");
    let name = lever.name();

    let total = Cell::new(0u64);
    let activated = Cell::new(0u64);
    let rows = Cell::new(0usize);
    let per_case_ceiling = budgets.per_case;
    let run_ceiling = budgets.run_total;

    let mut runner = TestRunner::new(Config {
        cases,
        // The generated cases are reproducible from the proptest seed printed on
        // failure, and this module has no source path proptest can resolve, so
        // it otherwise warns on every run about being unable to persist them.
        failure_persistence: None,
        ..Config::default()
    });

    let outcome = runner.run(&arb_case(), |case: Case| {
        let q = case.base_query();
        let (a, b) = rt
            .block_on(async {
                let a = lever.side_a(&q).await?;
                let b = lever.side_b(&q).await?;
                anyhow::Ok((a, b))
            })
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        total.set(total.get() + 1);
        if lever.activated(&a.witness, &b.witness) {
            activated.set(activated.get() + 1);
        }
        let case_rows = a.bag.total + b.bag.total;
        rows.set(rows.get() + case_rows);

        if case_rows > per_case_ceiling {
            return Err(TestCaseError::fail(format!(
                "[{name}] one case returned {case_rows} rows against a per-case \
                 ceiling of {per_case_ceiling}. Offending query:\n  {}",
                crate::querygen::render::render(&q),
            )));
        }

        bag_eq(&a.bag, &b.bag).map_err(|d| {
            TestCaseError::fail(format!(
                "[{name}] the two execution paths disagree.\n\
                 query: {}\n\
                 side A witness: {:?}\n\
                 side B witness: {:?}\n{d}",
                crate::querygen::render::render(&q),
                a.witness,
                b.witness,
            ))
        })?;
        Ok(())
    });

    // Report before asserting, so a failing run still shows its activation rate.
    let rate = if total.get() == 0 {
        0.0
    } else {
        activated.get() as f64 / total.get() as f64
    };
    // Counts include proptest's shrink replays, so on a failing run they read
    // higher than the configured case count. That is why the run budget is
    // asserted below rather than inside the loop.
    eprintln!(
        "[dqp:{name}] cases={} activated={} ({:.1}%) rows={} run_ceiling={run_ceiling}",
        total.get(),
        activated.get(),
        rate * 100.0,
        rows.get(),
    );

    outcome.expect("DQP lever found a divergence");

    assert!(
        total.get() > 0,
        "[{name}] ran zero cases — the generator produced nothing, so this run \
         proves nothing"
    );
    assert!(
        rate >= budgets.min_activation,
        "[{name}] activation rate {:.1}% is below the {:.0}% floor: {} of {} \
         cases exercised the same machinery on both sides, so those comparisons \
         were vacuous. Either the witness has drifted from what the lever \
         actually does, or the lever has stopped engaging its path.",
        rate * 100.0,
        budgets.min_activation * 100.0,
        total.get() - activated.get(),
        total.get(),
    );
    assert!(
        rows.get() <= run_ceiling,
        "[{name}] run row budget exceeded: {} rows materialized across {} cases \
         against a ceiling of {run_ceiling}. The generator has drifted toward \
         less selective queries, or the fixture grew.",
        rows.get(),
        total.get(),
    );
}

#[cfg(test)]
mod tests {
    use super::{Budgets, PrepareFut, drive_prepared_with};
    use crate::metamorphic::dqp::fork_lever::ForkLever;
    use crate::metamorphic::dqp::lever::{Lever, Observed, Witness, observe};
    use crate::metamorphic::dqp::seed::Tier;
    use uni_cypher::ast::Query;
    use uni_db::{Session, Uni};

    /// A lever whose two sides are the *same* session and which admits it never
    /// activates — the shape of a vacuous differential test.
    struct StubLever {
        session: Session,
    }

    impl StubLever {
        fn prepared(db: &Uni) -> PrepareFut<'_, Self> {
            let session = db.session();
            Box::pin(async move { Ok(Self { session }) })
        }
    }

    impl Lever for StubLever {
        fn name(&self) -> &'static str {
            "stub-never-activates"
        }
        async fn side_a(&self, q: &Query) -> anyhow::Result<Observed> {
            observe(&self.session, q).await
        }
        async fn side_b(&self, q: &Query) -> anyhow::Result<Observed> {
            observe(&self.session, q).await
        }
        fn activated(&self, _a: &Witness, _b: &Witness) -> bool {
            false
        }
    }

    const GENEROUS: Budgets = Budgets {
        per_case: usize::MAX,
        run_total: usize::MAX,
        min_activation: 0.80,
    };

    /// The activation floor must reject a lever that never activates, even
    /// though every one of its comparisons *passes* — both sides are identical,
    /// so `bag_eq` is trivially satisfied.
    ///
    /// This is the single most important guard in the driver. Without it a lever
    /// comparing a path against itself reports a clean green run forever.
    #[test]
    #[should_panic(expected = "activation rate")]
    fn activation_floor_rejects_a_vacuous_lever() {
        drive_prepared_with(8, Tier::Tiny, GENEROUS, StubLever::prepared);
    }

    /// The per-case ceiling must fire mid-run, naming the offending query.
    #[test]
    #[should_panic(expected = "per-case ceiling")]
    fn per_case_row_budget_fires() {
        let budgets = Budgets {
            per_case: 1,
            ..GENEROUS
        };
        drive_prepared_with(8, Tier::Tiny, budgets, ForkLever::prepared);
    }

    /// The run total must fire after a run that was otherwise clean — a healthy
    /// lever at 100% activation whose volume nonetheless blew the budget.
    #[test]
    #[should_panic(expected = "run row budget exceeded")]
    fn run_row_budget_fires() {
        let budgets = Budgets {
            run_total: 1,
            ..GENEROUS
        };
        drive_prepared_with(8, Tier::Tiny, budgets, ForkLever::prepared);
    }
}
