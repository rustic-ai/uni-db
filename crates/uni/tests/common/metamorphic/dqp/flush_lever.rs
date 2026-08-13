//! Lever 3 — a mixed L0+L1 read vs the same data after a flush.
//!
//! The first genuinely Tier-1 lever: flushing is global and irreversible, so it
//! runs under [`drive_stateful`]'s inverted loop rather than `drive_prepared`.
//!
//! # Why this lever is worth its runtime
//!
//! A flush moves rows from the in-memory L0 buffer into Lance, and the read path
//! is supposed to be indifferent: before the flush a query unions L0 with L1,
//! after it reads L1 alone, and the rows must be identical. That union is where
//! a documented class of bug lives — **#135** is exactly this shape, where
//! `expand_batch` read properties L0-only and every one of them came back NULL
//! once the data had been flushed. **#97** and **#116** sit adjacent to it.
//!
//! Those bugs are invisible to TLP and NoREC by construction: both rewrite a
//! query and run both forms under the *same* storage state, so an L0/L1 union
//! defect corrupts both sides identically and the oracle passes.
//!
//! # The fixture is deliberately mixed, not L0-only
//!
//! [`super::seed::build_dqp_seed`] flushes at the end, so the lever adds a
//! **delta** of rows on top and leaves those unflushed. Side A therefore reads a
//! genuine union — measured at `l0=200, storage=1000` — rather than an all-L0
//! table. An all-L0 side A would still activate, but it would never exercise the
//! union, which is the part that historically broke.
//!
//! # Every observation opens a fresh session
//!
//! Not incidental. Reusing one session would warm its plan cache during pass 1,
//! so pass 2 would run against a cached plan — folding a second, unrelated
//! transition into the comparison and making a failure impossible to attribute.
//! `Session` creation is sync and does no I/O (`session.rs` docs), so this is
//! cheap.

use std::collections::HashMap;

use uni_cypher::ast::Query;
use uni_db::{Value, unival};

use super::driver::{CaseKind, Db, PrepareFut};
use super::lever::{Observed, Witness, observe};
use super::seed::Tier;
use super::stateful::{StatefulLever, drive_stateful, drive_stateful_with};

/// Extra `Person` rows the lever leaves in L0.
///
/// Large enough that `l0_reads` is unambiguous, small enough that it barely
/// shifts the row budget the tier was calibrated against: 200 on a 1 000-person
/// fixture is +20% on `Person`, and nothing on the 4 000 edges that dominate the
/// row counts.
const DELTA_ROWS: usize = 200;

/// A mixed L0+L1 read vs the same data after `flush()`.
pub struct FlushLever {
    db: Db,
}

impl FlushLever {
    /// Adds the unflushed delta and captures the handle.
    ///
    /// # Errors
    ///
    /// Returns any error from the delta insert.
    pub async fn prepare(db: &Db) -> anyhow::Result<Self> {
        // Values come from the domains `seed` uses, which `arb_literal` hardcodes
        // its predicates against. Widening them here would make generated filters
        // miss the delta rows entirely, and side A would read them into `l0_reads`
        // while no predicate ever selected one — activation would look healthy
        // while the delta contributed nothing to any comparison.
        let rows: Vec<HashMap<String, Value>> = (0..DELTA_ROWS)
            .map(|i| {
                let mut p = HashMap::new();
                p.insert("name".to_string(), unival!(format!("d{i}")));
                // Every 7th row leaves `age` unset, mirroring the fixture's NULL
                // density so three-valued logic still has something to catch.
                if i % 7 != 0 {
                    p.insert("age".to_string(), unival!([18i64, 25, 30, 40, 65][i % 5]));
                }
                if i % 5 != 0 {
                    p.insert(
                        "city".to_string(),
                        unival!(["NYC", "SF", "LA"][i % 3].to_string()),
                    );
                }
                p
            })
            .collect();
        let tx = db.session().tx().await?;
        tx.bulk_insert_vertices("Person", rows).await?;
        tx.commit().await?;
        Ok(Self { db: db.clone() })
    }

    /// Boxed constructor in the shape the drivers expect.
    pub fn prepared(db: &Db) -> PrepareFut<'_, Self> {
        Box::pin(Self::prepare(db))
    }
}

impl StatefulLever for FlushLever {
    fn name(&self) -> &'static str {
        "l0-union-vs-flushed"
    }

    async fn observe_now(&self, q: &Query) -> anyhow::Result<Observed> {
        observe(&self.db.session(), q).await
    }

    async fn transition(&mut self) -> anyhow::Result<()> {
        self.db.flush().await?;
        Ok(())
    }

    /// Before: some rows came from L0. After: none did, and storage served more.
    ///
    /// All three clauses are load-bearing.
    ///
    /// `a.l0_reads > 0` proves the delta was actually unflushed when pass 1 ran —
    /// without it, an auto-flush firing between prepare and pass 1 would leave
    /// both sides identical and the lever would compare L1 against L1.
    ///
    /// `b.l0_reads == 0` proves the transition took effect. This is the clause
    /// that catches the inverted loop failing: if `transition` silently became a
    /// no-op, pass 2 would still report L0 reads and every case would fall here.
    ///
    /// `b.storage_reads > a.storage_reads` proves the rows *moved* rather than
    /// merely disappearing from the L0 accounting — the measured shape is
    /// `l0 200→0, storage 1000→1200`. Without it, a bug that dropped the delta
    /// entirely on the post-flush path would still look activated; it would be
    /// caught by `bag_eq`, but the witness would be lying in the meantime.
    fn activated(&self, a: &Witness, b: &Witness) -> bool {
        a.l0_reads > 0 && b.l0_reads == 0 && b.storage_reads > a.storage_reads
    }
}

/// PR lane: 500 cases, one fixture rebuild.
#[test]
fn flush_smoke() {
    drive_stateful(
        super::driver::smoke_cases(),
        Tier::Tiny,
        CaseKind::Plain,
        FlushLever::prepared,
    );
}

/// Nightly volume over plain projections.
#[test]
#[ignore = "soak: DQP L0-union vs flushed at nightly volume"]
fn flush_soak() {
    drive_stateful(
        super::driver::soak_cases(),
        Tier::Tiny,
        CaseKind::Plain,
        FlushLever::prepared,
    );
}

/// Nightly: the same transition over integer aggregates, which accumulate over
/// the L0/L1 union rather than projecting from it — a different operator set.
#[test]
#[ignore = "soak: DQP L0-union vs flushed over integer aggregates"]
fn flush_agg_soak() {
    drive_stateful(
        super::driver::soak_cases(),
        Tier::Tiny,
        CaseKind::IntAggregate,
        FlushLever::prepared,
    );
}

/// The repro entry point named by [`drive_stateful`]'s failure message.
///
/// Reads its coordinates from `DQP_SEED` / `DQP_BATCH` / `DQP_CASE` and re-runs
/// that single case. Ignored by default because with no coordinates set there is
/// nothing to reproduce.
#[test]
#[ignore = "repro: set DQP_SEED, DQP_BATCH and DQP_CASE from a failure message"]
fn l0_union_vs_flushed_replay() {
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
        Tier::Tiny,
        CaseKind::Plain,
        super::stateful::BATCH,
        FlushLever::prepared,
    );
}

#[cfg(test)]
mod tests {
    use super::{CaseKind, FlushLever, Tier, drive_stateful_with};
    use crate::metamorphic::dqp::driver::Budgets;

    const GENEROUS: Budgets = Budgets {
        per_case: usize::MAX,
        run_total: usize::MAX,
        min_activation: 0.80,
    };

    /// **The inverted loop has teeth.** With a batch size of 1 the driver
    /// transitions once per case, which is the correct-but-degenerate case; with
    /// a larger batch it must still activate on *every* case, including the last
    /// one in the batch.
    ///
    /// A driver that transitioned per case instead of per batch would pass the
    /// first comparison and then compare B against B, so this asserts the
    /// property that distinguishes them: full activation across a multi-case
    /// batch.
    #[test]
    fn every_case_in_a_batch_activates() {
        drive_stateful_with(
            12,
            Tier::Tiny,
            CaseKind::Plain,
            12,
            Budgets {
                min_activation: 1.0,
                ..GENEROUS
            },
            FlushLever::prepared,
        );
    }

    /// More cases than fit in one batch, so the multi-batch path runs: two
    /// fixtures, two transitions, and the activation floor still met.
    #[test]
    fn multiple_batches_each_transition() {
        drive_stateful_with(
            10,
            Tier::Tiny,
            CaseKind::Plain,
            4,
            Budgets {
                min_activation: 1.0,
                ..GENEROUS
            },
            FlushLever::prepared,
        );
    }

    /// The large tier must be refused, with the measurement as the reason.
    #[test]
    #[should_panic(expected = "621.73 s")]
    fn large_tier_is_refused() {
        drive_stateful_with(
            1,
            Tier::Large,
            CaseKind::Plain,
            1,
            GENEROUS,
            FlushLever::prepared,
        );
    }
}
