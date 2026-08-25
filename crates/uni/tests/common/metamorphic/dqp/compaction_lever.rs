// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Lever 6 — the same query over many fragments vs the same data compacted.
//!
//! Tier-1: compaction mutates the database irreversibly, so this runs under
//! [`drive_stateful`]'s inverted loop — every case is observed against the
//! fragmented state, compaction runs once, and the same cases are replayed.
//!
//! # Why this lever was deferred twice, and what changed
//!
//! Phase 4B specified it and deferred it because no counter moved when a
//! compaction ran. #172 then made `CompactionStats` report measured work, and
//! **that still did not unblock it**: [`StatefulLever::activated`] takes two
//! per-*query* [`Witness`]es while `CompactionStats` is per-*run*, so a run-level
//! number has nothing to say about an individual case. The only run-level hook,
//! `check_invariants`, returns `Err` — "reject this run" — which is the right
//! home for "did the transition actually merge anything", and the wrong one for
//! "did this case activate".
//!
//! What unblocked it was a per-query counter: `QueryMetrics::lance_iops`, from
//! the `iops` field of Lance's execution-stats callback, which
//! `attach_scan_stats` had been receiving and discarding all along.
//!
//! # The measured shape
//!
//! Measured before this lever was written, in
//! `uni-store/src/backend/lance.rs::probe_compaction_moves_lance_io_counts`:
//!
//! | fragments merged | iops before | iops after | drop |
//! |---|---|---|---|
//! | 5 → 1 | 25 | 5 | 20 |
//! | 2 → 1 | 10 | 5 | 5 |
//!
//! Unanimous across two scan shapes and three repeats each. Roughly five iops
//! per fragment, so the counter reports not just *that* a compaction happened
//! but *how much* — which is what separates a witness from a `> 0` constant.
//!
//! # Two things this lever must not do
//!
//! **It must not use `CompactionStats` as its activation signal.** It uses it as
//! a precondition instead, in [`CompactionLever::check_invariants`]: a batch
//! whose transition merged nothing is rejected outright rather than compared,
//! because the comparison would be vacuous and the activation rate would be the
//! only thing to notice.
//!
//! **It must not compact only once.** Compaction runs one flush behind — the
//! pass immediately after a flush reports no work and the next one does the
//! merge, with nothing happening in between. `transition` therefore compacts to
//! a fixpoint, which is deterministic; single-pass completeness is not.

use uni_cypher::ast::Query;

use super::driver::{CaseKind, Db, PrepareFut};
use super::lever::{Observed, Witness, observe};
use super::seed::{Fixture, Tier};
use super::stateful::{StatefulLever, drive_stateful, drive_stateful_with};

/// Separate flushes performed during `prepare`, each leaving a fragment.
///
/// Five, matching the shape the probe measured, so the expected `iops` drop is
/// the large one rather than the marginal 2 → 1 case. Fewer would still
/// activate, but with less headroom above the noise floor of zero.
const FRAGMENT_ROUNDS: usize = 5;

/// Rows added per round. Small: the point is the number of *fragments*, not the
/// volume, and every extra row is charged against the tier's row budget on both
/// sides of every case.
const ROWS_PER_ROUND: usize = 50;

/// A fragmented read vs the same data compacted.
pub struct CompactionLever {
    db: Db,
    /// Fragments merged by the transition, recorded so `check_invariants` can
    /// reject a batch whose compaction did nothing.
    merged: usize,
    /// Whether [`StatefulLever::transition`] has run.
    ///
    /// `check_invariants` fires both before pass 1 and after the transition, and
    /// "nothing merged" is only a fault in the second position — before the
    /// transition, zero is correct by construction.
    transitioned: bool,
}

impl CompactionLever {
    /// Writes and flushes several times, leaving one fragment per round.
    ///
    /// Each round is a separate `flush`, which is what produces separate
    /// fragments; one write of the same total size would produce one fragment
    /// and leave the transition with nothing to merge.
    async fn prepare(db: &Db) -> anyhow::Result<Self> {
        for round in 0..FRAGMENT_ROUNDS {
            let rows: Vec<std::collections::HashMap<String, uni_db::Value>> = (0..ROWS_PER_ROUND)
                .map(|i| {
                    let mut p = std::collections::HashMap::new();
                    p.insert("name".to_string(), uni_db::unival!(format!("c{round}_{i}")));
                    // Drawn from the fixture's own domains so the delta does not
                    // shift predicate selectivity — the property the tiers were
                    // calibrated against.
                    p.insert(
                        "age".to_string(),
                        uni_db::unival!(
                            super::seed::AGE_DOMAIN
                                [(round * ROWS_PER_ROUND + i) % super::seed::AGE_DOMAIN.len()]
                        ),
                    );
                    p.insert(
                        "city".to_string(),
                        uni_db::unival!(["NYC", "SF", "LA"][i % 3].to_string()),
                    );
                    p
                })
                .collect();
            let tx = db.session().tx().await?;
            tx.bulk_insert_vertices("Person", rows).await?;
            tx.commit().await?;
            db.flush().await?;
        }
        Ok(Self {
            db: db.clone(),
            merged: 0,
            transitioned: false,
        })
    }

    /// Boxed constructor in the shape both drivers take.
    pub fn prepared(db: &Db) -> PrepareFut<'_, Self> {
        Box::pin(Self::prepare(db))
    }

    /// A lever that skips the fragment-building rounds, so its transition has
    /// nothing to merge.
    ///
    /// Exists only to prove [`StatefulLever::check_invariants`] fires. A guard
    /// that has never been observed to reject anything is indistinguishable
    /// from one that cannot.
    #[cfg(test)]
    fn prepared_without_fragments(db: &Db) -> PrepareFut<'_, Self> {
        let db = db.clone();
        Box::pin(async move {
            Ok(Self {
                db,
                merged: 0,
                transitioned: false,
            })
        })
    }
}

impl StatefulLever for CompactionLever {
    fn name(&self) -> &'static str {
        "fragmented-vs-compacted"
    }

    /// A fresh session per call, so plan-cache warming never leaks into this
    /// lever's comparison.
    async fn observe_now(&self, q: &Query) -> anyhow::Result<Observed> {
        observe(&self.db.session(), q).await
    }

    /// Compacts to a fixpoint and records how much was merged.
    ///
    /// Two consecutive quiet passes, not one. Because compaction runs a flush
    /// behind, the first pass after `prepare`'s final flush reports no work —
    /// stopping there would return having merged nothing and leave side B
    /// identical to side A, which is the vacuous comparison this whole oracle
    /// exists to prevent.
    async fn transition(&mut self) -> anyhow::Result<()> {
        let mut quiet = 0;
        for _ in 0..10 {
            let stats = self.db.compaction().compact("Person").await?;
            self.db.compaction().wait().await?;
            if stats.fragments_removed == 0 {
                quiet += 1;
                if quiet == 2 {
                    self.transitioned = true;
                    return Ok(());
                }
            } else {
                quiet = 0;
                self.merged += stats.fragments_removed;
            }
        }
        anyhow::bail!("compaction did not reach a fixpoint in 10 passes")
    }

    /// Before: many fragments, so many physical reads. After: one fragment, so
    /// fewer.
    ///
    /// Both clauses are load-bearing.
    ///
    /// `a.lance_iops > 0` is the denominator. Without it a case whose scan
    /// reported no I/O at all — a path that builds no scanner, or a stream
    /// dropped before it drains — would satisfy the comparison below by
    /// arithmetic rather than by evidence.
    ///
    /// `b.lance_iops < a.lance_iops` is the transition taking effect. Strictly
    /// lower, not merely different: compaction can only reduce the number of
    /// files a scan opens, so an increase means something other than this lever
    /// moved the counter and the run should fail rather than count it.
    ///
    /// Deliberately **no** `rows_scanned` clause. Compaction changes how rows
    /// are found, never which, so the row counts are expected to be identical on
    /// both sides — asserting on them would add nothing, and asserting they
    /// *differ* would be wrong.
    fn activated(&self, a: &Observed, b: &Observed) -> bool {
        a.witness.lance_iops > 0 && b.witness.lance_iops < a.witness.lance_iops
    }

    /// Rejects a batch whose transition merged nothing.
    ///
    /// This is where a run-level number belongs. `CompactionStats` cannot be an
    /// activation signal — `activated` is per-query — but it answers exactly the
    /// question a precondition should ask: did the state actually change? A
    /// batch that compacted nothing would compare a fragmented state against
    /// itself, and the only symptom would be a low activation rate, which reads
    /// as witness drift rather than as a transition that never fired.
    ///
    /// It runs before pass 1 too, when `merged` is still 0 by construction, so
    /// the check is conditional on the transition having been attempted.
    async fn check_invariants(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.transitioned || self.merged > 0,
            "the transition merged no fragments, so side B reads the same \
             physical layout as side A. Comparing them would be vacuous, and \
             the only symptom would be a low activation rate — which reads as \
             witness drift rather than as a transition that never fired."
        );
        Ok(())
    }
}

/// PR lane: 500 cases over a fragmented fixture.
///
/// **No soak is registered**, matching the index lever and for the same reason.
/// The nightly `dqp` job already runs ten concurrent soaks inside a 60-minute
/// budget, last measured at 29.9 minutes wall-clock. This lever's smoke run is
/// 51 s for 500 cases, so a 10 000-case soak extrapolates to roughly 17 minutes
/// — plausibly fine, but the workflow's own comment asks for the set to be
/// re-measured rather than extrapolated before it grows, and an eleventh heavy
/// test is exactly the change that comment is about. Adding one is a follow-up
/// with a measurement attached, not a line of code.
#[test]
fn compaction_smoke() {
    drive_stateful(
        super::driver::smoke_cases(),
        Fixture::TINY,
        CaseKind::Plain,
        CompactionLever::prepared,
    );
}

/// Reproduces a single reported failure from its printed coordinates.
///
/// The name is load-bearing: `replay_test_hint` prints
/// `name().replace('-', "_") + "_replay"` as the repro command, so a mismatch
/// would point a failing run at a test that does not exist.
#[test]
#[ignore = "repro: replays one DQP compaction case from DQP_SEED/DQP_BATCH/DQP_CASE"]
fn fragmented_vs_compacted_replay() {
    let env = |k: &str, d: u64| -> u64 {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    super::stateful::replay_stateful(
        super::stateful::run_seed(),
        env("DQP_BATCH", 0) as u32,
        env("DQP_CASE", 0) as u32,
        Fixture::TINY,
        CaseKind::Plain,
        super::stateful::BATCH,
        CompactionLever::prepared,
    );
}

#[cfg(test)]
mod tests {
    use super::{CaseKind, CompactionLever, Fixture, drive_stateful_with};
    use crate::metamorphic::dqp::driver::Budgets;

    const GENEROUS: Budgets = Budgets {
        per_case: usize::MAX,
        run_total: usize::MAX,
        min_activation: 0.80,
    };

    /// Every case in a batch must activate, not merely 80% of them.
    ///
    /// Unlike the index lever, this one has no data dependence: compaction
    /// changes the physical layout every scan reads, so it should reach every
    /// case regardless of the predicate drawn. A rate below 100% means the
    /// witness is intermittent, which is worth knowing before trusting the
    /// 500-case number.
    #[test]
    fn every_case_in_a_batch_activates() {
        drive_stateful_with(
            12,
            Fixture::TINY,
            CaseKind::Plain,
            12,
            Budgets {
                min_activation: 1.0,
                ..GENEROUS
            },
            CompactionLever::prepared,
        );
    }

    /// The precondition rejects a batch whose transition merged nothing.
    ///
    /// Without the fragment-building rounds the fixture holds a single fragment,
    /// so compaction reaches its fixpoint having merged nothing and side B reads
    /// the identical physical layout. The comparisons would all *pass* — the
    /// bags are equal, because compaction never changes which rows exist — and
    /// the run would look green while proving nothing. That is precisely the
    /// vacuous-lever failure this oracle exists to prevent, so it must fail
    /// loudly, and this is the test that says it does.
    #[test]
    #[should_panic(expected = "merged no fragments")]
    fn a_transition_that_merges_nothing_is_rejected() {
        drive_stateful_with(
            4,
            Fixture::TINY,
            CaseKind::Plain,
            4,
            GENEROUS,
            CompactionLever::prepared_without_fragments,
        );
    }

    /// The transition runs once per batch, so a multi-batch run must still
    /// activate every case — a lever that compacted only on the first batch
    /// would pass a single-batch test and fail here.
    #[test]
    fn multiple_batches_each_transition() {
        drive_stateful_with(
            10,
            Fixture::TINY,
            CaseKind::Plain,
            4,
            Budgets {
                min_activation: 1.0,
                ..GENEROUS
            },
            CompactionLever::prepared,
        );
    }
}
