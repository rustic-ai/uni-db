//! Lever 2 — a live read vs a pinned time-travel read of the same version.
//!
//! Pinning takes a different read path from a live session: the pinned view is
//! deliberately **L0-free** (`at_snapshot` passes no writer and no L0 manager, so
//! post-snapshot rows stay invisible), it conjoins a `_version <= hwm` ceiling
//! onto every scan, and it carries its own `AdjacencyManager` for snapshot
//! isolation. None of that is supposed to change *what a query returns* when the
//! snapshot is the current state — which is exactly the claim a differential
//! oracle can check.
//!
//! # The soundness condition, and why it is checked rather than assumed
//!
//! This lever is only valid while the pinned version *is* the live version. If a
//! write lands mid-run the live side legitimately moves ahead, and then a
//! matching bag is luck while a differing one is a false alarm. So the run must
//! be **rejected, not reported**, if that happens.
//!
//! An earlier revision of the plan had the witness assert the two versions
//! *differ*. That inverts the oracle: two sides reading different versions
//! should return different data, so a bag inequality would be correct behaviour
//! and the test would be unsound rather than merely unexercised.
//!
//! Checking it takes a detour, because **there is no public way to read the live
//! version**. `DatabaseMetrics` has no such field, `Session` exposes no
//! `pinned_version()`, and on an unpinned `StorageManager`
//! `version_high_water_mark()` returns `None` by construction — it is `Some` only
//! when a pin is in force. The route that does work: `Uni::create_snapshot`
//! flushes before recording its manifest, so **the manifest's
//! `version_high_water_mark` is the live version at that instant**, and
//! `Uni::list_snapshots()` is public. Taking one snapshot at prepare and another
//! after the run turns "did a write move the live version?" into a comparison of
//! two public numbers.

use uni_cypher::ast::Query;
use uni_db::Session;

use super::driver::{CaseKind, Db, PrepareFut, drive_prepared, smoke_cases, soak_cases};
use super::lever::{Lever, Observed, Witness, observe};
use super::seed::Tier;

/// Name for the snapshot the pinned side reads.
const SNAPSHOT_NAME: &str = "dqp_pinned_vs_live";
/// Name for the probe snapshot taken to re-read the live version.
const PROBE_PREFIX: &str = "dqp_live_probe";

/// A live session vs one pinned to a snapshot of the same state.
pub struct PinnedLever {
    db: Db,
    live: Session,
    pinned: Session,
    /// The live version at prepare time, read off the snapshot's manifest.
    hwm_at_prepare: u64,
    /// Counter so each invariant probe gets a distinct snapshot name.
    probe: std::sync::atomic::AtomicUsize,
}

impl PinnedLever {
    /// Snapshots the current state, pins a session to it, and records the
    /// version that snapshot represents.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be created, the session cannot be
    /// pinned, or the manifest cannot be found afterwards.
    pub async fn prepare(db: &Db) -> anyhow::Result<Self> {
        let snapshot_id = db.create_snapshot(SNAPSHOT_NAME).await?;
        let hwm_at_prepare = hwm_of(db, &snapshot_id).await?;

        let live = db.session();
        let mut pinned = db.session();
        pinned.pin_to_version(&snapshot_id).await?;

        Ok(Self {
            db: std::sync::Arc::clone(db),
            live,
            pinned,
            hwm_at_prepare,
            probe: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Boxed constructor in the shape `drive_prepared` expects.
    pub fn prepared(db: &Db) -> PrepareFut<'_, Self> {
        Box::pin(Self::prepare(db))
    }
}

/// The `version_high_water_mark` recorded on `snapshot_id`'s manifest.
async fn hwm_of(db: &Db, snapshot_id: &str) -> anyhow::Result<u64> {
    let manifests = db.list_snapshots().await?;
    manifests
        .iter()
        .find(|m| m.snapshot_id == snapshot_id)
        .map(|m| m.version_high_water_mark)
        .ok_or_else(|| anyhow::anyhow!("snapshot {snapshot_id} is not in list_snapshots()"))
}

impl Lever for PinnedLever {
    fn name(&self) -> &'static str {
        "live-vs-pinned-snapshot"
    }

    async fn side_a(&self, q: &Query) -> anyhow::Result<Observed> {
        observe(&self.live, q).await
    }

    async fn side_b(&self, q: &Query) -> anyhow::Result<Observed> {
        observe(&self.pinned, q).await
    }

    /// Side B must have applied a snapshot version ceiling and side A must not.
    ///
    /// Deliberately not `Session::is_pinned()`, which reports *configuration*: a
    /// manager can be pinned and still issue reads that apply no ceiling. The
    /// counter increments where the ceiling is actually conjoined onto a scan,
    /// and counts manifest pins only — an ordinary transaction's internal version
    /// pin does not qualify, or every transactional read would look like a
    /// time-travel read and the witness would mean nothing.
    fn activated(&self, a: &Witness, b: &Witness) -> bool {
        b.snapshot_reads > 0 && a.snapshot_reads == 0
    }

    /// The live version must not have moved since prepare.
    ///
    /// Taking a fresh snapshot is the only public way to observe it: the
    /// manifest's high-water mark is the live version at the moment the snapshot
    /// flushed. If it has advanced, some writer touched the database mid-run and
    /// the two sides were no longer reading the same state — so the run is
    /// rejected rather than reported either way.
    async fn check_invariants(&self) -> anyhow::Result<()> {
        let n = self
            .probe
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = self
            .db
            .create_snapshot(&format!("{PROBE_PREFIX}_{n}"))
            .await?;
        let now = hwm_of(&self.db, &id).await?;
        anyhow::ensure!(
            now == self.hwm_at_prepare,
            "the live version moved from {} to {now} during the run, so the live \
             and pinned sides stopped reading the same state — results discarded \
             rather than reported",
            self.hwm_at_prepare,
        );
        Ok(())
    }
}

/// PR lane, plain projections.
#[test]
fn pinned_smoke() {
    drive_prepared(
        smoke_cases(),
        Tier::Tiny,
        CaseKind::Plain,
        PinnedLever::prepared,
    );
}

/// Nightly volume, plain projections.
#[test]
#[ignore = "soak: DQP live-vs-pinned at nightly volume"]
fn pinned_soak() {
    drive_prepared(
        soak_cases(),
        Tier::Tiny,
        CaseKind::Plain,
        PinnedLever::prepared,
    );
}

/// Nightly volume, integer aggregate projections.
#[test]
#[ignore = "soak: DQP live-vs-pinned over integer aggregates"]
fn pinned_agg_soak() {
    drive_prepared(
        soak_cases(),
        Tier::Tiny,
        CaseKind::IntAggregate,
        PinnedLever::prepared,
    );
}

#[cfg(test)]
mod tests {
    use super::{PinnedLever, hwm_of};
    use crate::metamorphic::dqp::driver::Db;
    use crate::metamorphic::dqp::lever::Lever;
    use crate::metamorphic::dqp::seed::{Tier, build_dqp_seed};
    use std::sync::Arc;

    async fn prepared() -> anyhow::Result<(Db, PinnedLever)> {
        let db: Db = Arc::new(build_dqp_seed(Tier::Tiny).await?);
        let lever = PinnedLever::prepare(&db).await?;
        Ok((db, lever))
    }

    /// A quiet database must pass its own invariant check — otherwise the lever
    /// could never run at all and the rejection test below would prove nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn invariants_hold_on_a_quiet_database() -> anyhow::Result<()> {
        let (_db, lever) = prepared().await?;
        lever.check_invariants().await?;
        // Twice, because each call takes a fresh probe snapshot and must not
        // itself be what moves the version.
        lever.check_invariants().await?;
        Ok(())
    }

    /// **The rejection case.** A write during the run advances the live version,
    /// so the two sides stop reading the same state and the run must be
    /// discarded rather than reported — a matching bag would be luck and a
    /// differing one a false alarm.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_during_the_run_is_detected_and_rejects_the_run() -> anyhow::Result<()> {
        let (db, lever) = prepared().await?;
        lever.check_invariants().await?;

        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Person {name: 'intruder', age: 41})")
            .await?;
        tx.commit().await?;
        db.flush().await?;

        let err = lever
            .check_invariants()
            .await
            .expect_err("a write must be detected as version drift");
        let msg = err.to_string();
        assert!(
            msg.contains("live version moved"),
            "the rejection must name the cause; got: {msg}"
        );
        Ok(())
    }

    /// The high-water mark this lever reasons about must actually advance when
    /// the database is written to — otherwise the check above would pass for the
    /// wrong reason and could never detect real drift.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_high_water_mark_moves_when_the_database_does() -> anyhow::Result<()> {
        let db: Db = Arc::new(build_dqp_seed(Tier::Tiny).await?);
        let before = hwm_of(&db, &db.create_snapshot("hwm_before").await?).await?;

        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Person {name: 'mover', age: 22})")
            .await?;
        tx.commit().await?;
        db.flush().await?;

        let after = hwm_of(&db, &db.create_snapshot("hwm_after").await?).await?;
        assert!(
            after > before,
            "the snapshot high-water mark must advance after a write \
             (before={before}, after={after}); if it does not, the pinned \
             lever's drift check is inert"
        );
        Ok(())
    }
}
