//! Lever 1 — primary vs a pristine fork.
//!
//! The highest-value lever in the DQP set, because the fork read path differs
//! from primary in four independently documented, supposedly *result-neutral*
//! ways:
//!
//! - `use_scalar_index(false)` is hardcoded on the branch scan (the #106 fix), so
//!   this lever is a scalar-index-on/off differential for free.
//! - Multivector search on a fork takes the brute-force branch scan, skipping
//!   both the MUVERA/FDE fast path and the candidate truncation.
//! - `rewrite_for_fork_fusion` can emit a `FusedIndexScan` when a fork-local
//!   index is registered.
//! - Fork sessions start with a cold plan cache.
//!
//! Per the fork contract, a pristine fork disagreeing with its parent is
//! unambiguously a bug — the fork is supposed to be a transparent view of the
//! parent until something is written to it. Issues #99, #103, #97, #110 and #135
//! all lived in exactly this gap.
//!
//! The fork here writes **nothing**. That is not a limitation: `create_fork_2pc`
//! materializes one Lance branch per dataset at fork time, so a pristine fork
//! already reads through the branch path — confirmed by the `branch_scans`
//! witness, which fires on every case.

use uni_cypher::ast::Query;
use uni_db::Session;

use super::driver::{CaseKind, Db, PrepareFut, drive_prepared, smoke_cases, soak_cases};
use super::lever::{Lever, Observed, Witness, observe};
use super::seed::Tier;

/// Fork name used by the lever. Fixed, because the driver creates exactly one.
const FORK_NAME: &str = "dqp_primary_vs_fork";

/// Primary vs a pristine fork of it.
pub struct ForkLever {
    primary: Session,
    fork: Session,
}

impl ForkLever {
    /// Creates the fork and captures both sessions.
    ///
    /// Called once by [`drive_prepared`] before any case runs. The sessions are
    /// owned — `Session` holds an `Arc<UniInner>` — so the lever carries no
    /// lifetime and keeps both the db and the fork alive for the whole run.
    ///
    /// # Errors
    ///
    /// Returns any error from fork creation.
    pub async fn prepare(db: &Db) -> anyhow::Result<Self> {
        let primary = db.session();
        let fork = db.session().fork(FORK_NAME).await?;
        Ok(Self { primary, fork })
    }

    /// Boxed constructor in the shape [`drive_prepared`] expects.
    pub fn prepared(db: &Db) -> PrepareFut<'_, Self> {
        Box::pin(Self::prepare(db))
    }
}

impl Lever for ForkLever {
    fn name(&self) -> &'static str {
        "primary-vs-pristine-fork"
    }

    async fn side_a(&self, q: &Query) -> anyhow::Result<Observed> {
        observe(&self.primary, q).await
    }

    async fn side_b(&self, q: &Query) -> anyhow::Result<Observed> {
        observe(&self.fork, q).await
    }

    /// Side B must have executed a branch scan and side A must not have.
    ///
    /// Both halves matter. Without the first, the fork may have fallen back to
    /// primary — `BranchedBackend` resolves a branch per table and falls back for
    /// tables the fork has no branch for — and the comparison would be primary
    /// against primary. Without the second, a change that started routing
    /// ordinary sessions through a branch would go unnoticed and the lever would
    /// again be comparing one path against itself.
    fn activated(&self, a: &Witness, b: &Witness) -> bool {
        b.branch_scans > 0 && a.branch_scans == 0
    }
}

/// PR lane: 500 cases on the tiny fixture.
///
/// Measured at 48 s, not the ~32 s projected from Phase 0A's single-side figure
/// — the fork side is slower than primary, which is what
/// `use_scalar_index(false)` on the branch scan buys.
#[test]
fn fork_smoke() {
    drive_prepared(
        smoke_cases(),
        Tier::Tiny,
        CaseKind::Plain,
        ForkLever::prepared,
    );
}

/// Nightly volume, ~32 min. Runs in its own job (see `nightly.yml`) because a
/// single lever at this volume takes roughly as long as the heaviest existing
/// metamorphic soak, and needs its own per-test ceiling — see the
/// `test(fork_soak)` override in `.config/nextest.toml`.
#[test]
#[ignore = "soak: DQP primary-vs-fork at nightly volume"]
fn fork_soak() {
    drive_prepared(
        soak_cases(),
        Tier::Tiny,
        CaseKind::Plain,
        ForkLever::prepared,
    );
}

/// Nightly: the same lever over integer aggregate projections.
///
/// Aggregation is a different execution path from a plain projection — a
/// different set of physical operators, different accumulation — so it is worth
/// a differential run of its own. Integer-only: see [`CaseKind`].
#[test]
#[ignore = "soak: DQP primary-vs-fork over integer aggregates"]
fn fork_agg_soak() {
    drive_prepared(
        soak_cases(),
        Tier::Tiny,
        CaseKind::IntAggregate,
        ForkLever::prepared,
    );
}
