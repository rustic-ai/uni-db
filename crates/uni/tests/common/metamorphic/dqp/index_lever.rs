//! Lever 5 — the same query with and without a scalar index on the column it
//! filters.
//!
//! Tier-1: creating an index mutates the database, so this runs under
//! [`drive_stateful`]'s inverted loop — every case is observed against the
//! un-indexed state, the index is created once, and the same cases are replayed.
//!
//! # Why this lever was deferred, and what changed
//!
//! Phase 4B specified it and then deferred it, because **no counter moved when
//! an index was created**. A lever whose two sides move no witness cannot state
//! what it exercised: its `activated` predicate has nothing true to say and the
//! 80% floor fails every run, or — written without a witness — it passes forever
//! while comparing two identical execution paths. That is the vacuous test this
//! oracle exists to prevent, so the honest response was to not build it.
//!
//! #175 supplied the witness. `QueryMetrics::index_scans` comes from Lance's
//! execution-stats callback, which walks the metrics of the plan that actually
//! ran — so it reports execution rather than the fact that an index was
//! declared. `Witness::index_scans` was added in that change specifically for
//! this lever.
//!
//! # Why it must run under `CaseKind::Pushdown`
//!
//! A scalar index is consulted only for an `=`/`IN` on a Hash-indexed column:
//! `build_indexed_property_pushdown` collects nothing else, so a range predicate
//! hands Lance no predicate an index could serve. Measured over the generator's
//! case kinds, the fraction of cases carrying such a predicate is ~7% for
//! `Plain` and ~22% for the selective variants — both far below the floor.
//! `CaseKind::Pushdown` routes to `arb_case_pushdown`, which narrows targets to
//! [`INDEXED_PROPERTY`] and always emits a same-column conjunction, so it is
//! **100% by construction**.
//!
//! A `Plain` variant of this lever would therefore fail its activation floor for
//! a reason that has nothing to do with indexing. It is deliberately not
//! registered.
//!
//! # The measured shape
//!
//! Spiked before this lever was written, on `Tier::Tiny` (1 000 `Person` rows):
//!
//! | predicate | `index_scans` | rows scanned |
//! |---|---|---|
//! | `age = 30` | 1 | 103 |
//! | `age = 30 AND age >= 22 AND age <= 40` | 1 | 103 |
//! | `age IN [30] AND age >= 22 AND age <= 40` | 1 | 103 |
//! | `age >= 22 AND age <= 40` | **0** | 1 000 |
//!
//! and every one of them zero without the index. The middle two are the shape
//! `arb_same_column_conj` emits, which is what made this lever buildable; the
//! last is why it cannot run on `Plain`.

use uni_cypher::ast::Query;
use uni_db::api::schema::{IndexType, ScalarType};

use super::driver::{CaseKind, Db, PrepareFut};
use super::lever::{Observed, Witness, observe};
use super::seed::{Fixture, INDEXED_PROPERTY};
use super::stateful::{StatefulLever, drive_stateful, drive_stateful_with};

/// The same query against an un-indexed then an indexed column.
pub struct IndexLever {
    db: Db,
}

impl IndexLever {
    /// Captures the handle. The fixture needs no preparation.
    ///
    /// Unlike the flush lever, which seeds an unflushed delta, side A here is
    /// the seed exactly as built — `Fixture::TINY` carries no index, and
    /// `build_dqp_seed` has already flushed, so the Lance table exists and the
    /// transition's index build happens immediately rather than being deferred
    /// to a later flush.
    ///
    /// # Errors
    ///
    /// Infallible today; the signature matches the driver's `PrepareFut`.
    pub async fn prepare(db: &Db) -> anyhow::Result<Self> {
        Ok(Self { db: db.clone() })
    }

    /// Boxed constructor in the shape the drivers expect.
    pub fn prepared(db: &Db) -> PrepareFut<'_, Self> {
        Box::pin(Self::prepare(db))
    }
}

impl StatefulLever for IndexLever {
    fn name(&self) -> &'static str {
        "index-absent-vs-present"
    }

    /// A fresh session per observation, for the reason the flush lever
    /// documents: reusing one would warm its plan cache during pass 1 and fold
    /// a second, unrelated transition into the comparison.
    async fn observe_now(&self, q: &Query) -> anyhow::Result<Observed> {
        observe(&self.db.session(), q).await
    }

    /// Declares a **Hash** index on the column the generator filters.
    ///
    /// The kind matters as much as the column: only `=`/`IN` against a
    /// Hash-indexed column populates `hash_index_columns`, so a `BTree` here
    /// would leave every case unable to reach pushdown and the lever would
    /// report 0% activation while looking correct.
    ///
    /// [`INDEXED_PROPERTY`] is a literal alias of the generator's
    /// `PUSHDOWN_PROPERTY`, so the indexed column and the filtered column cannot
    /// drift apart. Indexing anything else produces a run that passes and proves
    /// nothing.
    async fn transition(&mut self) -> anyhow::Result<()> {
        self.db
            .schema()
            .label("Person")
            .index(INDEXED_PROPERTY, IndexType::Scalar(ScalarType::Hash))
            .apply()
            .await?;
        Ok(())
    }

    /// Before: no index existed to consult. After: one was consulted.
    ///
    /// Both clauses are load-bearing and neither is redundant.
    ///
    /// `a.index_scans == 0` proves side A genuinely had no index. Without it, an
    /// index left over from a previous batch — or a fixture that quietly gained
    /// one — would make both sides identical while the second clause still held.
    ///
    /// `b.index_scans > 0` proves the transition took effect *and* that the
    /// generated predicate actually reached the pushdown path. This is the
    /// clause that catches a `transition` that silently became a no-op, and the
    /// one that would collapse if the lever were ever run on a case kind whose
    /// predicates cannot consult an index.
    ///
    /// Deliberately **no** `rows_scanned` clause. An index that narrows the scan
    /// is the expected case and was measured at 1 000 → 103, but it is not
    /// guaranteed: a predicate matching most of the table would legitimately
    /// scan nearly all of it while still consulting the index. A third clause
    /// would fail runs for a reason unrelated to what this lever measures.
    fn activated(&self, a: &Witness, b: &Witness) -> bool {
        a.index_scans == 0 && b.index_scans > 0
    }
}

/// PR lane: 500 cases over pushdown-reaching predicates.
#[test]
fn index_pushdown_smoke() {
    drive_stateful(
        super::driver::smoke_cases(),
        Fixture::TINY,
        CaseKind::Pushdown,
        IndexLever::prepared,
    );
}

/// Reproduces a single reported failure from its printed coordinates.
///
/// The name is load-bearing: `replay_test_hint` prints
/// `name().replace('-', "_") + "_replay"` as the repro command, so a mismatch
/// would point a failing run at a test that does not exist.
#[test]
#[ignore = "repro: replays one DQP index case from DQP_SEED/DQP_BATCH/DQP_CASE"]
fn index_absent_vs_present_replay() {
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
        CaseKind::Pushdown,
        super::stateful::BATCH,
        IndexLever::prepared,
    );
}

#[cfg(test)]
mod tests {
    use super::{CaseKind, Fixture, IndexLever, drive_stateful_with};
    use crate::metamorphic::dqp::driver::Budgets;

    const GENEROUS: Budgets = Budgets {
        per_case: usize::MAX,
        run_total: usize::MAX,
        min_activation: 0.80,
    };

    /// Every case in a batch must activate, not merely 80% of them.
    ///
    /// The production floor is 80% because a lever can legitimately draw a case
    /// its transition does not reach. This lever should have no such cases:
    /// `CaseKind::Pushdown` guarantees an `=`/`IN` on the indexed column, so a
    /// rate below 100% here means either the guarantee broke or the witness is
    /// intermittent — both worth knowing before trusting the 500-case number.
    #[test]
    fn every_case_in_a_batch_activates() {
        drive_stateful_with(
            12,
            Fixture::TINY,
            CaseKind::Pushdown,
            12,
            Budgets {
                min_activation: 1.0,
                ..GENEROUS
            },
            IndexLever::prepared,
        );
    }

    /// The transition is applied once per batch, so a multi-batch run must still
    /// activate every case — a lever that indexed only on the first batch would
    /// pass a single-batch test and fail here.
    #[test]
    fn multiple_batches_each_transition() {
        drive_stateful_with(
            10,
            Fixture::TINY,
            CaseKind::Pushdown,
            4,
            Budgets {
                min_activation: 1.0,
                ..GENEROUS
            },
            IndexLever::prepared,
        );
    }
}
