//! Lever 4 — a freshly planned query vs the same query from a warm plan cache.
//!
//! # This lever is Tier-2, and that is a measured correction
//!
//! The implementation plan listed plan-cache cold-vs-warm among the **Tier-1**
//! levers, alongside flush and compaction, on the assumption that warming the
//! cache is a transition on the database. It is not. The cache is owned by the
//! `Session` (`session.rs:202`, constructed at `:257`), and the probe in
//! [`super::transition_probe`] measured a fresh `db.session()` reading
//! `hits=0 misses=0 size=0`.
//!
//! So cold and warm are **two sessions**, not two points in time, and the lever
//! needs no new driver: it holds both open at once, exactly like the fork and
//! pinned levers, and runs under `drive_prepared` unchanged.
//!
//! # The law
//!
//! A cached logical plan must produce the same rows as one planned fresh. That is
//! not a tautology — the cache is keyed on **query text alone**
//! (`plan_cache_key`, `session.rs:2146`), so a cached entry outlives every change
//! that does not change the text: newly written rows, a flush, a schema addition,
//! an index appearing. Anything the planner folded in at plan time and the cache
//! then preserved past its validity shows up here as a bag difference.
//!
//! # Side B runs the query twice, and has to
//!
//! Cases are generated, so each one's text is new to the session: its first
//! execution is necessarily a miss. Side B therefore runs the query once to warm
//! the entry and observes the **second** execution. Three executions per case in
//! total, which is why this lever's PR budget is larger than the other two.

use uni_cypher::ast::Query;
use uni_db::Session;

use super::driver::{CaseKind, Db, PrepareFut, drive_prepared, smoke_cases, soak_cases};
use super::lever::{Lever, Observed, Witness, observe, observe_cached};
use super::seed::Tier;

/// A cold-planned read vs a plan-cache hit.
pub struct PlanCacheLever {
    db: Db,
    warm: Session,
}

impl PlanCacheLever {
    /// Captures the db and the session that will accumulate cached plans.
    ///
    /// # Errors
    ///
    /// Infallible today; fallible for symmetry with the other levers' `prepare`.
    pub async fn prepare(db: &Db) -> anyhow::Result<Self> {
        Ok(Self {
            db: db.clone(),
            warm: db.session(),
        })
    }

    /// Boxed constructor in the shape [`drive_prepared`] expects.
    pub fn prepared(db: &Db) -> PrepareFut<'_, Self> {
        Box::pin(Self::prepare(db))
    }
}

impl Lever for PlanCacheLever {
    fn name(&self) -> &'static str {
        "cold-plan-vs-cached-plan"
    }

    /// A session that has never seen this query — or any other.
    ///
    /// A brand-new session per case rather than one long-lived "cold" session,
    /// because a shared one would warm up as the run proceeded and side A would
    /// quietly become a second warm side. `Session` creation is sync and does no
    /// I/O, so the cost is a struct allocation.
    async fn side_a(&self, q: &Query) -> anyhow::Result<Observed> {
        observe_cached(&self.db.session(), q).await
    }

    /// The persistent session, warmed on this exact text first.
    async fn side_b(&self, q: &Query) -> anyhow::Result<Observed> {
        // Warm the entry. Its result is discarded — it is side A's execution
        // repeated, and comparing it would compare a cold plan against a cold
        // plan.
        let _ = observe(&self.warm, q).await?;
        observe_cached(&self.warm, q).await
    }

    /// Side B hit the cache and side A did not.
    ///
    /// Both halves matter, as ever. Without `a.plan_cache_hits == 0` a change
    /// that made the cache shared across sessions would turn side A warm too, and
    /// the lever would compare a cached plan against a cached plan while still
    /// reporting full activation.
    ///
    /// The counter is a `SessionMetrics` delta, not `QueryResult::plan_cache_hit`
    /// — that field is assigned at exactly one site, on the write path, and is
    /// permanently `false` for a read. `feasibility.rs` pins that fact, and
    /// `observe_cached` exists because of it.
    fn activated(&self, a: &Witness, b: &Witness) -> bool {
        b.plan_cache_hits > 0 && a.plan_cache_hits == 0
    }
}

/// PR lane: 500 cases. Three executions per case rather than the usual two.
#[test]
fn plan_cache_smoke() {
    drive_prepared(
        smoke_cases(),
        Tier::Tiny,
        CaseKind::Plain,
        PlanCacheLever::prepared,
    );
}

/// Nightly volume over plain projections.
#[test]
#[ignore = "soak: DQP cold plan vs cached plan at nightly volume"]
fn plan_cache_soak() {
    drive_prepared(
        soak_cases(),
        Tier::Tiny,
        CaseKind::Plain,
        PlanCacheLever::prepared,
    );
}

/// Nightly: aggregates, whose plans carry accumulator state the cache must not
/// be sharing between executions.
#[test]
#[ignore = "soak: DQP cold plan vs cached plan over integer aggregates"]
fn plan_cache_agg_soak() {
    drive_prepared(
        soak_cases(),
        Tier::Tiny,
        CaseKind::IntAggregate,
        PlanCacheLever::prepared,
    );
}

#[cfg(test)]
mod tests {
    use super::{PlanCacheLever, Tier};
    use crate::metamorphic::dqp::lever::{Lever, observe_cached};
    use crate::metamorphic::dqp::seed::build_dqp_seed;
    use crate::querygen::render::render;
    use crate::querygen::{Case, arb_case};
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use std::sync::Arc;

    fn one_case() -> Case {
        arb_case()
            .new_tree(&mut TestRunner::deterministic())
            .expect("strategy produced no value")
            .current()
    }

    /// The witness must discriminate on a real query: cold reads zero hits, warm
    /// reads one. A `plan_cache_hits` that were always zero — the state
    /// `witness_of` leaves it in — would make `activated` permanently false, and
    /// the activation floor would catch it; a value that were always non-zero
    /// would make it permanently true, and nothing would.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_cache_witness_discriminates() -> anyhow::Result<()> {
        let db = Arc::new(build_dqp_seed(Tier::Tiny).await?);
        let q = one_case().base_query();

        let cold = db.session();
        let first = observe_cached(&cold, &q).await?;
        assert_eq!(
            first.witness.plan_cache_hits,
            0,
            "a session's first execution of {} registered a cache hit",
            render(&q)
        );

        let second = observe_cached(&cold, &q).await?;
        assert_eq!(
            second.witness.plan_cache_hits, 1,
            "re-running the same text on the same session registered no cache hit \
             — either the cache is not keyed on text, or the metric moved"
        );

        // And a genuinely fresh session is cold again, which is the property that
        // makes side A repeatable across 20 000 cases.
        let fresh = db.session();
        let elsewhere = observe_cached(&fresh, &q).await?;
        assert_eq!(
            elsewhere.witness.plan_cache_hits, 0,
            "a fresh session inherited a warm cache — the plan cache is no longer \
             per-session and this lever compares a cached plan against itself"
        );
        Ok(())
    }

    /// End to end through the lever's own sides, which is what the driver calls.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_lever_activates_on_its_own_sides() -> anyhow::Result<()> {
        let db = Arc::new(build_dqp_seed(Tier::Tiny).await?);
        let lever = PlanCacheLever::prepare(&db).await?;
        let q = one_case().base_query();

        let a = lever.side_a(&q).await?;
        let b = lever.side_b(&q).await?;
        assert!(
            lever.activated(&a.witness, &b.witness),
            "the lever did not activate on its own sides: a={:?} b={:?}",
            a.witness,
            b.witness
        );
        Ok(())
    }
}
