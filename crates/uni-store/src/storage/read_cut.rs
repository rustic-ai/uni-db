// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The read cut: the table versions readers read.
//!
//! Every Lance commit creates a new table version, readable as soon as it
//! lands. The store's manifest is the commit point instead: it names the
//! version of every table and index dataset (`SnapshotManifest::tables`),
//! and reads see those versions. A flush becomes visible all at once, when
//! its manifest is published, and a table version no manifest names -- a
//! crashed flush's partial commits, an append a stale writer landed after
//! losing the store -- is never read.
//!
//! A [`ReadCut`] tracks, per table:
//!
//! - `published`: the versions the latest published manifest names. `None`
//!   while the store's manifest is one an older binary wrote (it names no
//!   tables): reads then see every table's newest version, as before.
//! - commits of a **unit** -- a flush, named by the snapshot id of the
//!   manifest it will publish. They stay unread until that manifest is
//!   published, and that manifest names them.
//! - **loose** commits, made outside any unit: compaction, index builds,
//!   schema widening, recovery. Each is complete when it lands, so this
//!   handle reads them at once; other handles see them once a manifest names
//!   them, at the next publish.
//! - `committed`: the newest version of each table this handle wrote or
//!   adopted, whichever way.
//!
//! A commit that finds a table at a version this handle did not write (and no
//! other commit of this handle on that table running) has met a foreign
//! commit: the handle trips its write fence and stops writing. Its own
//! unpublished commits are still in the WAL, so nothing is lost; the next
//! writable open rolls every table back to the published cut.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use parking_lot::Mutex;

tokio::task_local! {
    /// The unit the running task's commits belong to.
    static UNIT: String;
}

/// Runs `fut` with its commits counted to unit `id`.
pub async fn in_unit<F: std::future::Future>(id: String, fut: F) -> F::Output {
    UNIT.scope(id, fut).await
}

fn current_unit() -> Option<String> {
    UNIT.try_with(Clone::clone).ok()
}

/// Whether the running task belongs to a unit.
#[must_use]
pub fn in_any_unit() -> bool {
    UNIT.try_with(|_| ()).is_ok()
}

/// A set of table versions reads see.
#[derive(Clone, Debug)]
pub struct Published {
    /// The manifest the versions come from.
    pub snapshot_id: String,
    /// Table (or `indexes/...` dataset) to version.
    pub tables: Arc<BTreeMap<String, u64>>,
}

#[derive(Default)]
struct TableOps {
    /// Commits of this handle on the table that have started and not
    /// recorded their result.
    inflight: usize,
    /// Bumped by every commit that starts.
    epoch: u64,
    /// The epoch of the latest commit that started while another was
    /// running: it and those running then cannot tell each other's versions
    /// from a foreign one.
    contended: u64,
    /// A commit on the table failed: it may have landed (a Lance call that
    /// commits and then fails, a write acknowledged too late), so the next
    /// commit adopts the version it reads instead of judging it.
    uncertain: bool,
}

#[derive(Default)]
struct State {
    published: Option<Published>,
    published_id: Option<String>,
    /// What a manifest is stamped on when nothing is published: every table
    /// at the version a writable open found it (an older binary's store).
    baseline: BTreeMap<String, u64>,
    committed: BTreeMap<String, u64>,
    loose: BTreeMap<String, u64>,
    units: HashMap<String, BTreeMap<String, u64>>,
    /// Tables dropped since the last publish.
    dropped: BTreeSet<String>,
    ops: HashMap<String, TableOps>,
    /// What this handle reads: `published` with the loose commits on top.
    view: Option<Published>,
}

/// `top` over `base`, newer versions winning; a table in `dropped` has only
/// `top`'s version (it was created again), or none.
fn overlay_dropping(
    base: &mut BTreeMap<String, u64>,
    top: &BTreeMap<String, u64>,
    dropped: &BTreeSet<String>,
) {
    for table in dropped {
        base.remove(table);
    }
    for (table, &version) in top {
        let slot = base.entry(table.clone()).or_insert(version);
        *slot = (*slot).max(version);
    }
}

impl State {
    fn base(&self) -> BTreeMap<String, u64> {
        match &self.published {
            Some(p) => (*p.tables).clone(),
            None => self.baseline.clone(),
        }
    }

    fn refresh_view(&mut self) {
        self.view = match &self.published {
            None if self.loose.is_empty() => None,
            published => {
                let mut tables = self.base();
                overlay_dropping(&mut tables, &self.loose, &self.dropped);
                Some(Published {
                    snapshot_id: published
                        .as_ref()
                        .map_or_else(String::new, |p| p.snapshot_id.clone()),
                    tables: Arc::new(tables),
                })
            }
        };
    }
}

/// The read cut of one primary store, shared by its storage manager, its
/// backends and its snapshot manager. See the module docs.
pub struct ReadCut {
    state: Mutex<State>,
    /// Whether this handle records its commits: a writable primary handle.
    /// A read-only handle only reads at the cut.
    recording: std::sync::atomic::AtomicBool,
    /// Whether commits are checked for foreign versions: from the end of a
    /// writable open's reconciliation. Before it, the open's recoveries
    /// complete commits an earlier process left above the cut.
    checking: std::sync::atomic::AtomicBool,
    units_closed: tokio::sync::Notify,
    fence: std::sync::OnceLock<Arc<crate::lease::WriteFence>>,
    last_refresh: Mutex<Option<Instant>>,
    /// Serializes standalone publishes.
    publishing: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for ReadCut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("ReadCut")
            .field(
                "published",
                &state.published.as_ref().map(|p| &p.snapshot_id),
            )
            .field("recording", &self.is_recording())
            .finish()
    }
}

impl Default for ReadCut {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadCut {
    /// A cut that reads every table's newest version until one is published.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            recording: std::sync::atomic::AtomicBool::new(false),
            checking: std::sync::atomic::AtomicBool::new(false),
            units_closed: tokio::sync::Notify::new(),
            fence: std::sync::OnceLock::new(),
            last_refresh: Mutex::new(None),
            publishing: tokio::sync::Mutex::new(()),
        }
    }

    /// Starts recording this handle's commits (a writable primary handle).
    pub fn start_recording(&self, fence: Arc<crate::lease::WriteFence>) {
        let _ = self.fence.set(fence);
        self.recording
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Whether this handle records its commits.
    #[must_use]
    pub fn is_recording(&self) -> bool {
        self.recording.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Starts checking commits for foreign versions; see the field.
    pub fn start_checking(&self) {
        self.checking
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn is_checking(&self) -> bool {
        self.checking.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The versions the latest published manifest names; `None` reads every
    /// table's newest version.
    #[must_use]
    pub fn published(&self) -> Option<Published> {
        self.state.lock().published.clone()
    }

    /// The manifest the published cut came from, also when it names no
    /// tables (an older binary's).
    #[must_use]
    pub fn published_id(&self) -> Option<String> {
        self.state.lock().published_id.clone()
    }

    /// What this handle reads: the published versions with its own loose
    /// commits on top. `None` reads every table's newest version.
    #[must_use]
    pub fn view(&self) -> Option<Published> {
        self.state.lock().view.clone()
    }

    /// Makes `tables`, named by manifest `snapshot_id`, the published cut,
    /// and closes the unit of that id. An empty map is a manifest from an
    /// older binary: reads go back to every table's newest version.
    pub fn publish(&self, snapshot_id: &str, tables: BTreeMap<String, u64>) {
        {
            let mut state = self.state.lock();
            state.units.remove(snapshot_id);
            state
                .loose
                .retain(|t, v| tables.get(t).is_none_or(|named| *v > *named));
            // Still dropped: named here, and not created again since.
            let committed = &state.committed;
            let still: BTreeSet<String> = state
                .dropped
                .iter()
                .filter(|t| tables.contains_key(*t) && !committed.contains_key(*t))
                .cloned()
                .collect();
            state.dropped = still;
            state.published_id = Some(snapshot_id.to_string());
            state.published = (!tables.is_empty()).then(|| Published {
                snapshot_id: snapshot_id.to_string(),
                tables: Arc::new(tables),
            });
            state.refresh_view();
        }
        self.units_closed.notify_waiters();
    }

    /// Takes `tables` as both what this handle holds and what is published.
    pub fn adopt(&self, snapshot_id: &str, tables: BTreeMap<String, u64>) {
        self.state.lock().committed = tables.clone();
        self.publish(snapshot_id, tables);
    }

    /// Replaces what this handle holds with `tables`, the newest version of
    /// every table. With nothing published, a manifest saved before the
    /// first publish names them.
    pub fn hold(&self, tables: BTreeMap<String, u64>) {
        let mut state = self.state.lock();
        if state.published.is_none() {
            state.baseline = tables.clone();
        }
        state.committed = tables;
    }

    /// Records a loose commit, or one of the running task's unit: `table` now
    /// holds `version`.
    pub fn record(&self, table: &str, version: u64) {
        let unit = current_unit();
        let mut state = self.state.lock();
        let slot = state.committed.entry(table.to_string()).or_insert(0);
        *slot = (*slot).max(version);
        let target = match unit
            .as_ref()
            .and_then(|u| state.units.contains_key(u).then_some(u))
        {
            Some(unit) => state.units.get_mut(unit).expect("checked"),
            None => &mut state.loose,
        };
        let slot = target.entry(table.to_string()).or_insert(0);
        *slot = (*slot).max(version);
        state.refresh_view();
    }

    /// Forgets `table`: it was dropped, so a table created under its name
    /// starts its versions again.
    pub fn forget(&self, table: &str) {
        let mut state = self.state.lock();
        state.committed.remove(table);
        state.loose.remove(table);
        state.baseline.remove(table);
        for unit in state.units.values_mut() {
            unit.remove(table);
        }
        state.dropped.insert(table.to_string());
        state.refresh_view();
    }

    /// The version of `table` this handle holds.
    #[must_use]
    pub fn committed_version(&self, table: &str) -> Option<u64> {
        self.state.lock().committed.get(table).copied()
    }

    /// Opens unit `id` (a flush, named by its manifest's snapshot id).
    /// Closed by publishing that manifest, or by dropping the guard.
    #[must_use]
    pub fn open_unit(self: &Arc<Self>, id: &str) -> UnitGuard {
        self.state
            .lock()
            .units
            .insert(id.to_string(), BTreeMap::new());
        UnitGuard {
            cut: Some(Arc::clone(self)),
            id: id.to_string(),
        }
    }

    /// A guard that closes unit `id` when dropped, for the code that ends a
    /// unit opened elsewhere.
    #[must_use]
    pub fn closing_unit(self: &Arc<Self>, id: &str) -> UnitGuard {
        UnitGuard {
            cut: Some(Arc::clone(self)),
            id: id.to_string(),
        }
    }

    /// The versions a manifest with `snapshot_id` names: the published cut,
    /// with the commits of that unit -- or, for a manifest no unit saves,
    /// the loose commits -- on top.
    #[must_use]
    pub fn stamp(&self, snapshot_id: &str) -> BTreeMap<String, u64> {
        let state = self.state.lock();
        let mut tables = state.base();
        // Loose commits on a table another open flush has written contain
        // that flush's rows; they wait for it.
        let busy: BTreeSet<&String> = state
            .units
            .iter()
            .filter(|(id, _)| id.as_str() != snapshot_id)
            .flat_map(|(_, commits)| commits.keys())
            .collect();
        let mut top: BTreeMap<String, u64> = state
            .loose
            .iter()
            .filter(|(table, _)| !busy.contains(table))
            .map(|(t, v)| (t.clone(), *v))
            .collect();
        if let Some(unit) = state.units.get(snapshot_id) {
            overlay_dropping(&mut top, unit, &BTreeSet::new());
        }
        overlay_dropping(&mut tables, &top, &state.dropped);
        tables
    }

    /// Whether loose commits are waiting for a publish.
    #[must_use]
    pub fn has_loose(&self) -> bool {
        let state = self.state.lock();
        !state.loose.is_empty() || !state.dropped.is_empty()
    }

    /// Waits until every unit open now has closed: a loose commit made on
    /// a table one of them wrote contains that unit's rows, so it may only be
    /// published once that unit is.
    pub async fn wait_for_open_units(&self) {
        let open: Vec<String> = self.state.lock().units.keys().cloned().collect();
        loop {
            let notified = self.units_closed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !open
                .iter()
                .any(|id| self.state.lock().units.contains_key(id))
            {
                return;
            }
            notified.await;
        }
    }

    /// Serializes standalone publishes.
    pub async fn publishing(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.publishing.lock().await
    }

    /// Starts a commit on `table`. The ticket records the commit's result,
    /// and judges whether a version it meets is this handle's.
    #[must_use]
    pub fn begin(self: &Arc<Self>, table: &str) -> CommitTicket {
        if !self.is_recording() {
            return CommitTicket {
                cut: None,
                table: table.to_string(),
                epoch: 0,
            };
        }
        let mut state = self.state.lock();
        let ops = state.ops.entry(table.to_string()).or_default();
        ops.epoch += 1;
        if ops.inflight > 0 {
            ops.contended = ops.epoch;
        }
        ops.inflight += 1;
        let epoch = ops.epoch;
        CommitTicket {
            cut: Some(Arc::clone(self)),
            table: table.to_string(),
            epoch,
        }
    }

    /// Whether the read cut is due for a refresh, at most once per `every`;
    /// marks it refreshed when it is.
    #[must_use]
    pub fn refresh_due(&self, every: Duration) -> bool {
        let mut last = self.last_refresh.lock();
        let now = Instant::now();
        match *last {
            Some(at) if now.duration_since(at) < every => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }

    fn foreign(&self, table: &str, found: u64, expected: u64) -> anyhow::Error {
        let reason = format!(
            "table '{table}' is at version {found}, which this handle did not write \
             (it holds version {expected}): another writer committed to it"
        );
        match self.fence.get() {
            Some(fence) => {
                fence.trip(reason.clone());
                fence
                    .check()
                    .err()
                    .unwrap_or_else(|| anyhow::anyhow!("{reason}"))
            }
            None => anyhow::anyhow!("{reason}; this handle has stopped writing"),
        }
    }
}

/// Closes a unit when dropped; see [`ReadCut::open_unit`].
pub struct UnitGuard {
    cut: Option<Arc<ReadCut>>,
    id: String,
}

impl UnitGuard {
    /// Leaves the unit open past this guard: a later
    /// [`ReadCut::closing_unit`] or publish closes it.
    pub fn keep_open(mut self) {
        self.cut = None;
    }
}

impl Drop for UnitGuard {
    fn drop(&mut self) {
        if let Some(cut) = self.cut.take() {
            let closed = cut.state.lock().units.remove(&self.id).is_some();
            if closed {
                cut.units_closed.notify_waiters();
            }
        }
    }
}

/// One commit of this handle on a table; see [`ReadCut::begin`].
pub struct CommitTicket {
    cut: Option<Arc<ReadCut>>,
    table: String,
    epoch: u64,
}

impl CommitTicket {
    /// Whether no other commit of this handle on the table ran at any point
    /// during this one: only then is a version this handle has not recorded
    /// someone else's.
    fn sole(cut: &ReadCut, table: &str, epoch: u64) -> bool {
        let state = cut.state.lock();
        state
            .ops
            .get(table)
            .is_some_and(|ops| ops.inflight == 1 && ops.epoch == epoch && ops.contended < epoch)
    }

    /// Checks the version the commit read the table at: one above what this
    /// handle holds, met while no other commit of this handle on the table
    /// runs, is a foreign commit.
    ///
    /// # Errors
    /// A foreign commit; the write fence is tripped.
    pub fn check_read(&self, read: u64) -> Result<()> {
        let Some(cut) = &self.cut else {
            return Ok(());
        };
        let Some(expected) = cut.committed_version(&self.table) else {
            return Ok(());
        };
        let uncertain = {
            let mut state = cut.state.lock();
            state
                .ops
                .get_mut(&self.table)
                .is_some_and(|ops| std::mem::take(&mut ops.uncertain))
        };
        if uncertain {
            cut.record(&self.table, read);
            return Ok(());
        }
        if cut.is_checking() && read > expected && Self::sole(cut, &self.table, self.epoch) {
            return Err(cut.foreign(&self.table, read, expected));
        }
        Ok(())
    }

    /// Records the version the commit produced. With `read`, the commit was a
    /// single Lance commit on that version: a result past `read + 1` means
    /// Lance folded a foreign commit in.
    ///
    /// # Errors
    /// A foreign commit; the write fence is tripped and nothing is recorded.
    pub fn finish(mut self, read: Option<u64>, version: u64) -> Result<()> {
        let Some(cut) = self.cut.take() else {
            return Ok(());
        };
        let folded = cut.is_checking()
            && read.is_some_and(|r| version > r + 1)
            && Self::sole(&cut, &self.table, self.epoch);
        Self::release(&cut, &self.table);
        if folded {
            return Err(cut.foreign(&self.table, version, read.unwrap_or_default()));
        }
        cut.record(&self.table, version);
        Ok(())
    }

    /// Records that the table is gone.
    pub fn finish_dropped(mut self) {
        if let Some(cut) = self.cut.take() {
            Self::release(&cut, &self.table);
            cut.forget(&self.table);
        }
    }

    fn release(cut: &ReadCut, table: &str) {
        let mut state = cut.state.lock();
        if let Some(ops) = state.ops.get_mut(table) {
            ops.inflight = ops.inflight.saturating_sub(1);
        }
    }
}

impl Drop for CommitTicket {
    fn drop(&mut self) {
        // A commit that failed. It may have landed all the same, so the next
        // commit on the table takes the version it finds as this handle's.
        if let Some(cut) = self.cut.take() {
            Self::release(&cut, &self.table);
            if let Some(ops) = cut.state.lock().ops.get_mut(&self.table) {
                ops.uncertain = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recording() -> Arc<ReadCut> {
        let cut = Arc::new(ReadCut::new());
        cut.start_recording(crate::lease::WriteFence::new());
        cut.start_checking();
        cut
    }

    fn map(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
        pairs.iter().map(|(t, v)| ((*t).to_string(), *v)).collect()
    }

    #[tokio::test]
    async fn a_units_commits_are_unread_until_its_manifest_is_published() {
        let cut = recording();
        cut.adopt("s0", map(&[("t", 3), ("u", 1)]));
        let unit = cut.open_unit("s1");
        in_unit("s1".to_string(), async {
            let ticket = cut.begin("t");
            ticket.check_read(3).unwrap();
            ticket.finish(Some(3), 4).unwrap();
        })
        .await;
        assert_eq!(
            cut.view().unwrap().tables["t"],
            3,
            "not read before publish"
        );
        assert_eq!(cut.stamp("s1"), map(&[("t", 4), ("u", 1)]));
        assert_eq!(cut.stamp("other"), map(&[("t", 3), ("u", 1)]));
        cut.publish("s1", cut.stamp("s1"));
        drop(unit);
        assert_eq!(cut.view().unwrap().tables["t"], 4);
    }

    #[test]
    fn a_loose_commit_is_read_at_once_and_stamped_by_a_standalone_publish() {
        let cut = recording();
        cut.adopt("s0", map(&[("t", 3)]));
        let ticket = cut.begin("t");
        ticket.finish(Some(3), 4).unwrap();
        assert_eq!(cut.view().unwrap().tables["t"], 4);
        assert_eq!(cut.published().unwrap().tables["t"], 3);
        assert!(cut.has_loose());
        cut.publish("s1", cut.stamp("s1"));
        assert!(!cut.has_loose());
    }

    #[test]
    fn a_version_this_handle_did_not_write_trips_the_fence() {
        let cut = recording();
        cut.adopt("s", map(&[("t", 3)]));
        let err = cut.begin("t").check_read(4).unwrap_err();
        assert!(format!("{err:#}").contains("did not write"), "{err:#}");
        assert!(cut.fence.get().unwrap().check().is_err());
    }

    #[test]
    fn a_folded_commit_trips_the_fence() {
        let cut = recording();
        cut.adopt("s", map(&[("t", 3)]));
        let ticket = cut.begin("t");
        ticket.check_read(3).unwrap();
        assert!(ticket.finish(Some(3), 5).is_err());
        assert_eq!(cut.committed_version("t"), Some(3));
    }

    /// Two commits of this handle on one table: neither can tell the other's
    /// version from a foreign one, so neither judges.
    #[test]
    fn concurrent_commits_of_one_handle_are_not_foreign() {
        let cut = recording();
        cut.adopt("s", map(&[("t", 3)]));
        let a = cut.begin("t");
        let b = cut.begin("t");
        a.check_read(3).unwrap();
        a.finish(Some(3), 4).unwrap();
        b.check_read(4).unwrap();
        b.finish(Some(4), 5).unwrap();
        // B began while A ran, and Lance rebased B onto A's commit: not
        // foreign, though B finished alone.
        let a = cut.begin("t");
        let b = cut.begin("t");
        a.finish(Some(5), 6).unwrap();
        b.check_read(5).unwrap();
        b.finish(Some(5), 7).unwrap();
        let c = cut.begin("t");
        c.check_read(7).unwrap();
        assert!(c.check_read(8).is_err());
    }

    #[test]
    fn a_failed_commit_makes_the_next_one_adopt_what_it_finds() {
        let cut = recording();
        cut.adopt("s", map(&[("t", 3)]));
        let failed = cut.begin("t");
        failed.check_read(3).unwrap();
        drop(failed); // Lance committed v4, then the call failed
        let next = cut.begin("t");
        next.check_read(4).unwrap();
        next.finish(Some(4), 5).unwrap();
        assert_eq!(cut.committed_version("t"), Some(5));
    }

    #[tokio::test]
    async fn a_flush_manifest_names_loose_commits_no_other_flush_shares() {
        let cut = recording();
        cut.adopt("s", map(&[("a", 1), ("b", 1), ("c", 1)]));
        let f1 = cut.open_unit("f1");
        let f2 = cut.open_unit("f2");
        in_unit("f2".to_string(), async {
            cut.begin("b").finish(Some(1), 2).unwrap();
        })
        .await;
        cut.begin("a").finish(Some(1), 2).unwrap(); // loose
        cut.begin("b").finish(Some(2), 3).unwrap(); // loose, over f2's rows
        assert_eq!(cut.stamp("f1"), map(&[("a", 2), ("b", 1), ("c", 1)]));
        assert_eq!(cut.stamp("f2"), map(&[("a", 2), ("b", 3), ("c", 1)]));
        drop((f1, f2));
    }

    #[test]
    fn a_table_dropped_and_created_again_is_read() {
        let cut = recording();
        cut.adopt("s", map(&[("t", 7)]));
        cut.begin("t").finish_dropped();
        cut.begin("t").finish(None, 1).unwrap();
        cut.publish("s2", cut.stamp("s2"));
        assert_eq!(cut.view().unwrap().tables["t"], 1);
        assert_eq!(cut.stamp("s3")["t"], 1);
    }

    #[test]
    fn a_read_only_cut_records_nothing() {
        let cut = Arc::new(ReadCut::new());
        let ticket = cut.begin("t");
        ticket.check_read(99).unwrap();
        ticket.finish(Some(1), 99).unwrap();
        assert_eq!(cut.committed_version("t"), None);
        assert!(cut.view().is_none());
    }

    #[test]
    fn a_dropped_table_leaves_the_cut() {
        let cut = recording();
        cut.adopt("s", map(&[("t", 7), ("u", 1)]));
        cut.begin("t").finish_dropped();
        assert_eq!(cut.committed_version("t"), None);
        assert!(!cut.view().unwrap().tables.contains_key("t"));
        assert_eq!(cut.stamp("x"), map(&[("u", 1)]));
        let ticket = cut.begin("t");
        ticket.finish(None, 1).unwrap();
        assert_eq!(cut.stamp("x"), map(&[("t", 1), ("u", 1)]));
    }

    #[test]
    fn an_older_stores_tables_are_the_baseline_until_a_publish() {
        let cut = recording();
        cut.hold(map(&[("t", 2), ("u", 5)]));
        assert!(cut.view().is_none(), "reads newest versions");
        cut.begin("t").finish(Some(2), 3).unwrap();
        assert_eq!(cut.stamp("x"), map(&[("t", 3), ("u", 5)]));
    }

    #[tokio::test]
    async fn a_standalone_publish_waits_for_the_units_open_when_it_started() {
        let cut = recording();
        cut.adopt("s", map(&[("t", 1)]));
        let unit = cut.open_unit("f");
        let waiter = {
            let cut = Arc::clone(&cut);
            tokio::spawn(async move { cut.wait_for_open_units().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        drop(unit);
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap();
    }
}
