// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Lever 7 — the same query before and after a deletion.
//!
//! Tier-1, and the first lever whose transition changes *data* rather than
//! physical state. That difference drives everything below.
//!
//! # The law is containment, not equality
//!
//! Every other lever asserts `bag(A) == bag(B)`: the same query over the same
//! data, executed differently, must return the same rows. A deletion breaks
//! that premise — the data is not the same afterwards, and the two sides
//! *should* differ.
//!
//! What survives is one-sided. Removing rows can only take answers away, so
//!
//! ```text
//! bag(after) ⊆ bag(before)
//! ```
//!
//! and a row present afterwards that was absent before is a defect. That is
//! exactly the shape of a resurrected row, or of an edge left dangling with a
//! null endpoint: neither was in the "before" bag, so containment fails.
//!
//! Expressed through [`StatefulLever::compare`], which defaults to equality —
//! the driver is unchanged and the six existing levers are untouched.
//!
//! # Activation is measured on the bag, because no counter moves
//!
//! `transition_probe::probe_delete_transition` measured it: deleting 124
//! vertices and 539 edges moved **zero** of the seven `Witness` fields across
//! four probe queries. `rows_scanned` and `storage_reads` both stayed at
//! exactly 1000 on either side, because a tombstoned row is still scanned and
//! then filtered.
//!
//! So there is no counter to witness on, and a lever that returned a constant
//! from `activated` would be the vacuous test this oracle exists to prevent.
//! The witness is instead the bag itself: this case's result shrank, therefore
//! the deletion reached it. That is the same signal CERT calls its narrowing
//! rate, and it is why `activated` takes the whole [`Observed`].
//!
//! # What this reaches, and what it does not
//!
//! It reaches deletion defects on the **typed** scan and traversal paths, which
//! nothing else in this oracle covered.
//!
//! It does **not** reach the #181 family. That defect lives on the schemaless
//! traversal (`TraverseMainByType` → `find_edges_by_type_names`), and these
//! fixtures declare `WORKS_AT`, so they plan a typed `Traverse` instead —
//! `seed.rs`'s `typed_fixture_plans_a_typed_traverse` asserts precisely that and
//! says no amount of generator widening changes it. Reaching that path needs a
//! schemaless fixture, which Phase 5 deferred on cost. Stated here so the
//! coverage is not mistaken for more than it is.

use uni_cypher::ast::Query;

use super::driver::{CaseKind, Db, PrepareFut};
use super::lever::{Observed, observe};
use super::seed::{AGE_DOMAIN, Fixture};
use super::stateful::{StatefulLever, drive_stateful_with};
use crate::diff::{BagDiff, RowBag, bag_is_subset};

/// The `age` value whose rows the transition removes.
///
/// A fixed member of [`AGE_DOMAIN`] rather than a drawn one: `replay_stateful`
/// reproduces a failure from its printed seed, so a transition that varied run
/// to run would print coordinates that reproduce nothing. It is also the value
/// the fixture's NULL-injection modulus leaves densest, so the slice is large
/// enough to move most generated queries.
const DELETED_AGE: i64 = AGE_DOMAIN[0];

/// A read before a deletion vs the same read after it.
pub struct DeleteLever {
    db: Db,
    /// Vertices removed by the transition. Zero after it has run means the
    /// batch proved nothing.
    deleted: usize,
    /// Whether [`StatefulLever::transition`] has run. `check_invariants` fires
    /// both before pass 1 and after the transition, and "deleted nothing" is a
    /// fault only in the second position.
    transitioned: bool,
}

impl DeleteLever {
    async fn prepare(db: &Db) -> anyhow::Result<Self> {
        Ok(Self {
            db: db.clone(),
            deleted: 0,
            transitioned: false,
        })
    }

    /// Boxed constructor in the shape both drivers take.
    pub fn prepared(db: &Db) -> PrepareFut<'_, Self> {
        Box::pin(Self::prepare(db))
    }
}

impl StatefulLever for DeleteLever {
    fn name(&self) -> &'static str {
        "before-vs-after-delete"
    }

    /// A fresh session per call, so plan-cache warming never enters the
    /// comparison.
    async fn observe_now(&self, q: &Query) -> anyhow::Result<Observed> {
        observe(&self.db.session(), q).await
    }

    /// `DETACH DELETE` every `Person` at [`DELETED_AGE`].
    ///
    /// `DETACH` rather than a bare `DELETE`: a bare one errors when the vertex
    /// still has edges, and the fixture wires most `Person` rows to a `Company`.
    /// The cascade is also the interesting part — it removes incident
    /// `WORKS_AT` edges, so the edge-shaped generated queries narrow too, and a
    /// stranded edge would show up as a row that containment rejects.
    async fn transition(&mut self) -> anyhow::Result<()> {
        let tx = self.db.session().tx().await?;
        let r = tx
            .execute(&format!(
                "MATCH (a:Person) WHERE a.age = {DELETED_AGE} DETACH DELETE a"
            ))
            .await?;
        tx.commit().await?;
        self.deleted = r.nodes_deleted();
        self.transitioned = true;
        Ok(())
    }

    /// This case's result shrank, so the deletion reached it.
    ///
    /// The bag, not a counter — see the module docs: a deletion moves no
    /// `Witness` field, because a tombstoned row is still scanned and then
    /// filtered.
    ///
    /// Strictly smaller, not merely different. Containment is checked
    /// separately by [`Self::compare`]; what this asks is whether the
    /// transition had any effect on *this* query, and a case whose result is
    /// unchanged proves nothing about deletion however green it is.
    fn activated(&self, a: &Observed, b: &Observed) -> bool {
        b.bag.total < a.bag.total
    }

    /// Containment, not equality. See the module docs.
    fn compare(&self, before: &RowBag, after: &RowBag) -> Result<(), BagDiff> {
        bag_is_subset(after, before)
    }

    /// Reject a batch whose transition removed nothing.
    ///
    /// Containment is trivially true when nothing was deleted, so such a batch
    /// is green while proving nothing — and the only symptom would be a low
    /// activation rate, which reads as witness drift rather than as a
    /// transition that never fired. Same precondition shape as
    /// `CompactionLever`, and for the same reason.
    ///
    /// Runs before pass 1 too, when `deleted` is still 0 by construction, so
    /// the check is conditional on the transition having been attempted.
    async fn check_invariants(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.transitioned || self.deleted > 0,
            "the transition deleted no rows, so side B reads exactly what side A \
             did. Containment would hold trivially and the run would be green \
             while proving nothing about deletion."
        );
        Ok(())
    }
}

/// PR lane: the default case count over plain projections.
///
/// A lower activation floor than the physical-state levers, and deliberately
/// so. Those transition the whole database, so every case sees the change; a
/// deletion only reaches a case whose predicate selected some of the removed
/// rows. Two thirds of generated cases carry no `WHERE` at all and so always
/// narrow, but a filtered case may legitimately miss the slice entirely. The
/// floor is the same kind of number CERT uses for its narrowing rate, not the
/// 0.80 an all-cases-activate lever can promise.
#[test]
fn delete_smoke() {
    drive_stateful_with(
        super::driver::smoke_cases(),
        Fixture::TINY,
        CaseKind::Plain,
        super::stateful::BATCH,
        super::driver::Budgets {
            min_activation: 0.30,
            ..super::driver::Budgets::for_tier(Fixture::TINY.tier, super::driver::smoke_cases())
        },
        DeleteLever::prepared,
    );
}

/// Reproduces a single reported failure from its printed coordinates.
///
/// The name is load-bearing: `replay_test_hint` prints
/// `name().replace('-', "_") + "_replay"` as the repro command.
#[test]
#[ignore = "repro: replays one DQP delete case from DQP_SEED/DQP_BATCH/DQP_CASE"]
fn before_vs_after_delete_replay() {
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
        DeleteLever::prepared,
    );
}

#[cfg(test)]
mod tests {
    use super::{
        CaseKind, DeleteLever, Fixture, Observed, Query, StatefulLever, drive_stateful_with,
    };
    use crate::diff::{BagDiff, RowBag};
    use crate::metamorphic::dqp::driver::{Budgets, Db, PrepareFut};

    const GENEROUS: Budgets = Budgets {
        per_case: usize::MAX,
        run_total: usize::MAX,
        min_activation: 0.30,
    };

    /// The deletion reaches most cases, not a handful.
    #[test]
    fn most_cases_in_a_batch_activate() {
        drive_stateful_with(
            12,
            Fixture::TINY,
            CaseKind::Plain,
            12,
            GENEROUS,
            DeleteLever::prepared,
        );
    }

    /// The transition runs once per batch, so a multi-batch run must still
    /// activate — a lever that deleted only on the first batch would pass a
    /// single-batch test and fail here.
    #[test]
    fn multiple_batches_each_transition() {
        drive_stateful_with(
            10,
            Fixture::TINY,
            CaseKind::Plain,
            4,
            GENEROUS,
            DeleteLever::prepared,
        );
    }

    /// A lever that *inserts* instead of deleting. Containment must reject it.
    ///
    /// A one-sided law is only worth having if it is seen to fail. Equality
    /// would catch this trivially — the bags differ — but containment is the
    /// weaker claim, and the whole question is whether the weaker claim still
    /// has teeth. It does: a row present afterwards that was absent before is
    /// exactly what an insertion adds, and exactly what a resurrected row looks
    /// like.
    struct InsertingLever(DeleteLever);

    impl InsertingLever {
        fn prepared(db: &Db) -> PrepareFut<'_, Self> {
            Box::pin(async move { Ok(Self(DeleteLever::prepare(db).await?)) })
        }
    }

    impl StatefulLever for InsertingLever {
        fn name(&self) -> &'static str {
            "inserting-stub"
        }
        async fn observe_now(&self, q: &Query) -> anyhow::Result<Observed> {
            self.0.observe_now(q).await
        }
        async fn transition(&mut self) -> anyhow::Result<()> {
            let tx = self.0.db.session().tx().await?;
            tx.execute(
                "CREATE (:Person {name: 'inserted-by-the-teeth-test', age: 41, city: 'NYC'})",
            )
            .await?;
            tx.commit().await?;
            self.0.deleted = 1;
            self.0.transitioned = true;
            Ok(())
        }
        fn activated(&self, a: &Observed, b: &Observed) -> bool {
            a.bag.total != b.bag.total
        }
        fn compare(&self, before: &RowBag, after: &RowBag) -> Result<(), BagDiff> {
            self.0.compare(before, after)
        }
    }

    #[test]
    #[should_panic(expected = "the two states disagree")]
    fn containment_rejects_a_transition_that_adds_rows() {
        drive_stateful_with(
            8,
            Fixture::TINY,
            CaseKind::Plain,
            8,
            GENEROUS,
            InsertingLever::prepared,
        );
    }

    /// A transition that deletes nothing must reject the batch.
    ///
    /// Containment holds trivially when the two states are identical, so
    /// without this precondition the run would be green while proving nothing —
    /// and the only symptom would be a low activation rate, which reads as
    /// witness drift rather than as a transition that never fired.
    struct NoopDeleteLever(DeleteLever);

    impl NoopDeleteLever {
        fn prepared(db: &Db) -> PrepareFut<'_, Self> {
            Box::pin(async move { Ok(Self(DeleteLever::prepare(db).await?)) })
        }
    }

    impl StatefulLever for NoopDeleteLever {
        fn name(&self) -> &'static str {
            "noop-delete-stub"
        }
        async fn observe_now(&self, q: &Query) -> anyhow::Result<Observed> {
            self.0.observe_now(q).await
        }
        async fn transition(&mut self) -> anyhow::Result<()> {
            // An age no fixture row carries, so the DETACH DELETE matches
            // nothing.
            let tx = self.0.db.session().tx().await?;
            let r = tx
                .execute("MATCH (a:Person) WHERE a.age = 9999 DETACH DELETE a")
                .await?;
            tx.commit().await?;
            self.0.deleted = r.nodes_deleted();
            self.0.transitioned = true;
            Ok(())
        }
        fn activated(&self, a: &Observed, b: &Observed) -> bool {
            self.0.activated(a, b)
        }
        fn compare(&self, before: &RowBag, after: &RowBag) -> Result<(), BagDiff> {
            self.0.compare(before, after)
        }
        async fn check_invariants(&self) -> anyhow::Result<()> {
            self.0.check_invariants().await
        }
    }

    #[test]
    #[should_panic(expected = "deleted no rows")]
    fn a_transition_that_deletes_nothing_is_rejected() {
        drive_stateful_with(
            4,
            Fixture::TINY,
            CaseKind::Plain,
            4,
            GENEROUS,
            NoopDeleteLever::prepared,
        );
    }
}
