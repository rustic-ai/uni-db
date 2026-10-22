// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::l0::L0Buffer;
use crate::runtime::wal::WriteAheadLog;
use parking_lot::RwLock;
use std::sync::Arc;

/// Per-generation pin marker for snapshot isolation (Component C1).
///
/// Held by exactly two classes: the [`L0Manager`] keeps one clone for the
/// current generation, and every live [`SnapshotView`] holds one. So
/// `Arc::strong_count` on the manager's clone is `1 + (live snapshots of the
/// current generation)`, which [`L0Manager::is_current_pinned`] uses to decide
/// whether a commit must freeze the generation aside before mutating it. The
/// private field stops any other code from minting a token and breaking that
/// invariant.
///
/// Always compiled (so the inert threading types exist in every build); it is
/// only ever *minted* by [`L0Manager::pin_snapshot`], which a transaction calls
/// only when `UniConfig::ssi_enabled` is `true`.
#[derive(Debug)]
pub struct PinToken(());

/// An isolated, reference-counted view of the L0 tier captured at a point in time.
///
/// Reads built from a `SnapshotView` see the L0 generation(s) that were visible
/// at capture, not later commits: while any view of a generation is alive a
/// commit that would mutate it first freezes it aside
/// ([`L0Manager::freeze_current_for_snapshot`]), so the buffers behind `main`
/// and `extra` are never mutated after capture. Dropping the view releases its
/// pin; `Arc` reference counting reclaims a frozen generation once no view holds
/// it. `started_at_version` is captured for the future C2 base-pinning hook and
/// is not yet consulted.
///
/// Always compiled so it can thread through the executor as an inert
/// `Option<SnapshotView>` in every build; it is only ever *constructed* by
/// [`L0Manager::pin_snapshot`], which a transaction calls only when
/// `UniConfig::ssi_enabled` is `true`, so with SSI off the threaded option is
/// always `None`.
#[derive(Clone)]
pub struct SnapshotView {
    /// The pinned main L0 generation at capture time.
    pub main: Arc<RwLock<L0Buffer>>,
    /// Generations being flushed at capture time, read after `main` (oldest visible state).
    pub extra: Vec<Arc<RwLock<L0Buffer>>>,
    /// Pin marker keeping the captured generation freeze-on-commit.
    pin: Arc<PinToken>,
    /// Main-L0 version at capture (the C2 hwm fed into `pinned_storage`).
    pub started_at_version: u64,
    /// Bulk-load versions at or below `started_at_version` not visible at
    /// capture; the transaction's L1 reads exclude them for good
    /// ([`L0Manager::reserve_bulk_version`]).
    pub unpublished_bulk: Vec<u64>,
    /// C2: a `StorageManager` clone pinned to `started_at_version`
    /// (`StorageManager::pinned_at_version`), so L1 scans filter to
    /// `_version <= started_at_version` and an L0→L1 flush completing
    /// mid-transaction cannot leak post-snapshot rows. Installed by the
    /// transaction at begin (one per transaction — the pinned manager
    /// carries a fresh `AdjacencyManager`); `None` for snapshots taken
    /// without a storage pin.
    pub pinned_storage: Option<Arc<crate::storage::manager::StorageManager>>,
    /// The reading transaction's private, uncommitted L0 buffer, newest of all
    /// layers.
    ///
    /// Set only on a view pinned from a manager built by
    /// [`L0Manager::from_view`] for a read made inside a transaction (an
    /// algorithm projection, say); a transaction's own snapshot leaves it
    /// `None`, since the executor threads its buffer separately. Unlike `main`
    /// and `extra` it is live: the transaction keeps writing into it, and its
    /// reads must see those writes.
    pub transaction: Option<Arc<RwLock<L0Buffer>>>,
}

impl SnapshotView {
    /// A [`QueryContext`] reading exactly this view's layers, newest first:
    /// [`Self::transaction`], then `main`, then `extra`.
    ///
    /// [`QueryContext`]: crate::runtime::QueryContext
    #[must_use]
    pub fn query_context(&self) -> crate::runtime::QueryContext {
        crate::runtime::QueryContext::new_with_pending(
            self.main.clone(),
            self.transaction.clone(),
            self.extra.clone(),
        )
    }
}

impl std::fmt::Debug for SnapshotView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Avoid requiring `L0Buffer: Debug` and dumping buffer contents.
        f.debug_struct("SnapshotView")
            .field("extra_generations", &self.extra.len())
            .field("pins", &Arc::strong_count(&self.pin))
            .field("started_at_version", &self.started_at_version)
            .finish_non_exhaustive()
    }
}

pub struct L0Manager {
    // The current active L0 buffer.
    // Outer RwLock protects the Arc (swapping L0s).
    // Inner RwLock protects the L0Buffer content (concurrent reads/writes).
    current: RwLock<Arc<RwLock<L0Buffer>>>,
    // L0 buffers currently being flushed to L1.
    // These remain visible to reads until flush completes successfully.
    // This prevents data loss if L1 writes fail after rotation.
    pending_flush: RwLock<Vec<Arc<RwLock<L0Buffer>>>>,
    // Snapshot-isolation pin token for the current generation (Component C1).
    // Reset on every rotate so a fresh generation starts unpinned. Read/cloned
    // only under the `current` lock so a snapshot captures a buffer and token
    // from the same generation. See `PinToken`.
    current_pin: RwLock<Arc<PinToken>>,
    // Plugin registry for registry-dispatched CRDT merges, stamped onto every
    // buffer this manager mints (initial buffer via `set_plugin_registry`,
    // rotated buffers via `rotate`). `None` preserves native `try_merge`.
    plugin_registry: RwLock<Option<Arc<uni_plugin::PluginRegistry>>>,
    // The reading transaction's private L0, for a read-only view built by
    // `from_view`; `None` for every manager that owns an L0 lifecycle.
    transaction: Option<Arc<RwLock<L0Buffer>>>,
    // Versions reserved by bulk loads that are not visible yet; see
    // `reserve_bulk_version`. Locked only inside a buffer's lock, as a leaf.
    bulk_reserved: parking_lot::Mutex<std::collections::BTreeSet<u64>>,
}

/// A bulk load's version, reserved by [`L0Manager::reserve_bulk_version`]
/// and held until the load is visible or discarded. Dropping it releases the
/// version.
#[derive(Debug)]
pub struct BulkVersionReservation {
    manager: Arc<L0Manager>,
    version: u64,
}

impl BulkVersionReservation {
    /// The reserved version.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }
}

impl Drop for BulkVersionReservation {
    fn drop(&mut self) {
        self.manager.bulk_reserved.lock().remove(&self.version);
    }
}

impl std::fmt::Debug for L0Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L0Manager").finish_non_exhaustive()
    }
}

impl L0Manager {
    pub fn new(start_version: u64, wal: Option<Arc<WriteAheadLog>>) -> Self {
        let l0 = L0Buffer::new(start_version, wal);
        Self {
            current: RwLock::new(Arc::new(RwLock::new(l0))),
            pending_flush: RwLock::new(Vec::new()),
            current_pin: RwLock::new(Arc::new(PinToken(()))),
            plugin_registry: RwLock::new(None),
            transaction: None,
            bulk_reserved: parking_lot::Mutex::new(std::collections::BTreeSet::new()),
        }
    }

    /// Install the plugin registry for registry-dispatched CRDT merges.
    ///
    /// Stamps the current buffer immediately and every buffer minted by a
    /// later [`Self::rotate`], so commit-time property merges route custom
    /// CRDT kinds through a registered provider. Called once at writer
    /// construction from the owning `StorageManager`'s registry. A `None`
    /// registry (never installed) preserves native `try_merge` behavior.
    pub fn set_plugin_registry(&self, registry: Arc<uni_plugin::PluginRegistry>) {
        *self.plugin_registry.write() = Some(registry.clone());
        // Stamp the buffer that already exists (minted by `new` before the
        // registry was known).
        self.current.read().write().set_plugin_registry(registry);
    }

    /// Create a read-only snapshot L0Manager from existing buffers.
    ///
    /// Used by the algorithm execution path to provide L0 visibility
    /// without owning the actual L0 lifecycle (rotation, flush, WAL).
    pub fn from_snapshot(
        current: Arc<RwLock<L0Buffer>>,
        pending_flush: Vec<Arc<RwLock<L0Buffer>>>,
    ) -> Self {
        Self {
            current: RwLock::new(current),
            pending_flush: RwLock::new(pending_flush),
            current_pin: RwLock::new(Arc::new(PinToken(()))),
            // Read-only snapshot manager: buffers already carry their registry
            // and this manager never rotates, so no stamping is needed.
            plugin_registry: RwLock::new(None),
            transaction: None,
            bulk_reserved: parking_lot::Mutex::new(std::collections::BTreeSet::new()),
        }
    }

    /// A read-only manager over a query's L0 visibility, including the
    /// reading transaction's private buffer.
    ///
    /// [`Self::from_snapshot`] has no place for a transaction buffer, and
    /// listing it among the pending-flush generations makes it the *oldest*
    /// layer, so the transaction's own updates lose to committed values.
    /// Snapshots pinned from this manager carry the buffer as
    /// [`SnapshotView::transaction`], the newest layer.
    #[must_use]
    pub fn from_view(
        current: Arc<RwLock<L0Buffer>>,
        pending_flush: Vec<Arc<RwLock<L0Buffer>>>,
        transaction: Option<Arc<RwLock<L0Buffer>>>,
    ) -> Self {
        Self {
            transaction,
            ..Self::from_snapshot(current, pending_flush)
        }
    }

    /// The transaction buffer of a manager built by [`Self::from_view`].
    #[must_use]
    pub fn transaction_l0(&self) -> Option<&Arc<RwLock<L0Buffer>>> {
        self.transaction.as_ref()
    }

    /// Reserves a version for a bulk load: the next version of the counter,
    /// recorded as not visible until the returned reservation drops.
    ///
    /// The counter is advanced and the version recorded under the current
    /// buffer's lock, which is the lock every version-ceiling capture reads
    /// the counter under ([`Self::pin_snapshot`], [`Self::version_and_unpublished_bulk`]).
    /// So a ceiling that covers the version always sees it recorded, and a
    /// reader captured before the load is visible keeps excluding it. Held
    /// under `current`'s read lock, so a rotation cannot leave the version
    /// on a superseded buffer.
    pub fn reserve_bulk_version(self: &Arc<Self>) -> BulkVersionReservation {
        let current = self.current.read();
        let mut buffer = current.write();
        buffer.current_version += 1;
        let version = buffer.current_version;
        self.bulk_reserved.lock().insert(version);
        drop(buffer);
        drop(current);
        BulkVersionReservation {
            manager: Arc::clone(self),
            version,
        }
    }

    /// The current version and the bulk-load versions at or below it that
    /// are not visible yet, read together.
    ///
    /// For a reader's version ceiling: rows of those versions must stay
    /// hidden from it for good, since the loads become visible only later.
    #[must_use]
    pub fn version_and_unpublished_bulk(&self) -> (u64, Vec<u64>) {
        let current = self.current.read();
        let buffer = current.read();
        let version = buffer.current_version;
        let unpublished = self.unpublished_bulk_at_most(version);
        (version, unpublished)
    }

    /// The bulk-load versions at or below `version` that are not visible
    /// yet. Call it under the lock of the buffer `version` was read from.
    #[must_use]
    pub fn unpublished_bulk_at_most(&self, version: u64) -> Vec<u64> {
        self.bulk_reserved
            .lock()
            .range(..=version)
            .copied()
            .collect()
    }

    /// Get the current L0 buffer.
    pub fn get_current(&self) -> Arc<RwLock<L0Buffer>> {
        self.current.read().clone()
    }

    /// Get all L0 buffers that should be visible to reads.
    /// This includes the current L0 plus any L0s being flushed.
    pub fn get_all_readable(&self) -> Vec<Arc<RwLock<L0Buffer>>> {
        let current = self.get_current();
        let pending = self.pending_flush.read().clone();
        let mut all = vec![current];
        all.extend(pending);
        all
    }

    /// Get L0 buffers currently being flushed (for QueryContext).
    pub fn get_pending_flush(&self) -> Vec<Arc<RwLock<L0Buffer>>> {
        self.pending_flush.read().clone()
    }

    /// Rotate L0. Returns the OLD L0 buffer.
    /// The new L0 is initialized with `next_version` and `new_wal`.
    pub fn rotate(
        &self,
        next_version: u64,
        new_wal: Option<Arc<WriteAheadLog>>,
    ) -> Arc<RwLock<L0Buffer>> {
        let mut guard = self.current.write();
        let old_l0 = guard.clone();

        // Never below the old buffer's counter: a version reserved from the
        // new buffer before the caller copies the counter over would
        // otherwise repeat one already used.
        let next_version = next_version.max(old_l0.read().current_version);
        let mut new_l0 = L0Buffer::new(next_version, new_wal);
        // Carry the registry onto the fresh generation so its commit-time
        // merges route through any registered CRDT provider.
        if let Some(reg) = self.plugin_registry.read().as_ref() {
            new_l0.set_plugin_registry(reg.clone());
        }
        *guard = Arc::new(RwLock::new(new_l0));

        // A fresh generation starts unpinned. Reset the pin token while still
        // holding the `current` write guard: `pin_snapshot` clones the buffer
        // and token under `current.read()`, so this serializes against it and a
        // snapshot can never capture a buffer/token from different generations.
        *self.current_pin.write() = Arc::new(PinToken(()));

        old_l0
    }

    /// Begin flush: rotate L0 and add old L0 to pending flush list.
    /// The old L0 remains visible to reads until `complete_flush` is called.
    /// Returns the old L0 buffer to be flushed.
    pub fn begin_flush(
        &self,
        next_version: u64,
        new_wal: Option<Arc<WriteAheadLog>>,
    ) -> Arc<RwLock<L0Buffer>> {
        let old_l0 = self.rotate(next_version, new_wal);
        self.pending_flush.write().push(old_l0.clone());
        old_l0
    }

    /// Complete flush: remove the flushed L0 from pending list.
    /// Call this only after L1 writes have succeeded.
    pub fn complete_flush(&self, l0: &Arc<RwLock<L0Buffer>>) {
        let mut pending = self.pending_flush.write();
        pending.retain(|x| !Arc::ptr_eq(x, l0));
    }

    /// Captures an isolated snapshot of the current L0 (strategy D).
    ///
    /// Freezes the current buffer by rotating it aside — writers re-fetch
    /// `get_current()` at write time, so they move to the fresh buffer and can
    /// never mutate the frozen one — and keeps it readable via the pending
    /// list. Returns the `(frozen_main, pending)` pair used to build a
    /// [`QueryContext`] whose reads are isolated from later writes. Capture is
    /// O(1): one empty-buffer allocation and an `Arc` move, with no deep copy.
    ///
    /// The caller must coordinate with the commit path (e.g. hold the writer's
    /// `flush_lock`) so the rotation does not race an in-flight merge into the
    /// current buffer. The frozen generation currently rides the pending-flush
    /// list; a dedicated generation list with reader-count GC is the production
    /// follow-up (see the proposal's open questions).
    ///
    /// [`QueryContext`]: crate::runtime::QueryContext
    pub fn snapshot_isolated(
        &self,
        next_version: u64,
        new_wal: Option<Arc<WriteAheadLog>>,
    ) -> (Arc<RwLock<L0Buffer>>, Vec<Arc<RwLock<L0Buffer>>>) {
        // Capture pending before freezing so the frozen buffer becomes the
        // snapshot's main view rather than one of its pending peers.
        let pending = self.pending_flush.read().clone();
        let frozen = self.rotate(next_version, new_wal);
        // Keep the frozen generation visible to latest (non-snapshot) reads.
        self.pending_flush.write().push(frozen.clone());
        (frozen, pending)
    }

    /// Pins an isolated view of the current L0 tier for a transaction.
    ///
    /// O(1): clones the current buffer handle, the pending-flush set, and the
    /// generation's pin token. No freeze happens here — the current buffer keeps
    /// taking writes; it is frozen aside lazily, and only if still pinned, when a
    /// commit would next mutate it (see [`Self::freeze_current_for_snapshot`] and
    /// [`Self::is_current_pinned`]). Holds the `current` read lock across the
    /// buffer and token clones so both come from the same generation even if a
    /// rotate races. Does not require the writer's `flush_lock`.
    ///
    /// # Examples
    /// ```ignore
    /// let snap = writer.l0_manager().pin_snapshot();
    /// // build a QueryContext from `snap.main` + `snap.extra`
    /// ```
    pub fn pin_snapshot(&self) -> SnapshotView {
        // Hold `current` read across both clones: a concurrent `rotate` needs
        // `current.write()` and resets the pin token under it, so it cannot
        // interleave and split the buffer/token across generations.
        let current_guard = self.current.read();
        let main = current_guard.clone();
        let pin = self.current_pin.read().clone();
        let (started_at_version, unpublished_bulk) = {
            let buffer = main.read();
            let version = buffer.current_version;
            (version, self.unpublished_bulk_at_most(version))
        };
        let extra = self.pending_flush.read().clone();
        drop(current_guard);
        SnapshotView {
            main,
            extra,
            pin,
            started_at_version,
            unpublished_bulk,
            pinned_storage: None,
            transaction: self.transaction.clone(),
        }
    }

    /// Returns `true` if any live [`SnapshotView`] pins the current generation.
    ///
    /// `strong_count > 1` means a snapshot besides the manager holds the token.
    /// Call under the writer's `flush_lock` at commit so the decision and any
    /// resulting freeze are atomic with respect to the merge.
    pub fn is_current_pinned(&self) -> bool {
        Arc::strong_count(&self.current_pin.read()) > 1
    }

    /// Clones the current (pinned) generation aside so a commit can mutate a
    /// fresh buffer without the pinning snapshots observing the write — lazy
    /// copy-on-write, performed only when [`Self::is_current_pinned`] holds.
    ///
    /// The outgoing buffer — which the pinning [`SnapshotView`]s hold via `main`
    /// — becomes immutable: a deep copy carrying the same data is installed as
    /// the new current, the commit merges into that copy, and the original is
    /// never mutated again. `L0Buffer::clone` drops the WAL handle, so the
    /// original's WAL (already flushed at this commit's WAL step) is handed to
    /// the copy; the frozen original keeps none, as it takes no more writes. The
    /// original is **not** placed on the pending-flush list — it is reclaimed by
    /// `Arc` refcount once the last snapshot drops, so nothing leaks. The new
    /// generation starts unpinned (the pin token is reset). Must be called under
    /// the writer's `flush_lock`, since it swaps the current buffer.
    pub fn freeze_current_for_snapshot(&self) {
        let mut guard = self.current.write();
        let frozen = guard.clone();
        let mut new_buf = frozen.read().clone();
        // Hand the WAL from the now-frozen original to the writable copy.
        new_buf.wal = frozen.write().wal.take();
        *guard = Arc::new(RwLock::new(new_buf));
        // The fresh generation starts unpinned; reset under the `current` write
        // guard (consistent with `rotate`, which a non-clone path would use).
        *self.current_pin.write() = Arc::new(PinToken(()));
    }

    /// Minimum `wal_lsn_at_start` among pending-flush L0s other than `except`.
    ///
    /// This is the floor below which every WAL entry is durable in L1: a pending
    /// flush — one still streaming, or one whose flush FAILED and left the buffer
    /// in `pending_flush` — holds committed WAL entries strictly above its start
    /// that are not yet in L1. WAL truncation and the published
    /// `wal_high_water_mark` must not advance past this floor, or that buffer's
    /// committed-but-unflushed data is silently dropped by the next (e.g.
    /// shutdown) flush. Using the high watermark (`wal_lsn_at_flush`) here was the
    /// lost-commit bug: it truncated / checkpointed past the pending buffer's own
    /// entries.
    ///
    /// `except` is the buffer the caller is itself flushing — its data IS entering
    /// the new snapshot, so it must not constrain the floor. At truncation time it
    /// has already been removed via `complete_flush`, so passing it is a harmless
    /// no-op; during the stream phase it is still pending and the exclusion is
    /// load-bearing.
    ///
    /// Returns `None` when no other pending flush exists.
    pub fn min_pending_wal_lsn_start(&self, except: &Arc<RwLock<L0Buffer>>) -> Option<u64> {
        self.pending_flush
            .read()
            .iter()
            .filter(|l0_arc| !Arc::ptr_eq(l0_arc, except))
            .map(|l0_arc| l0_arc.read().wal_lsn_at_start)
            .min()
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use crate::runtime::QueryContext;
    use crate::runtime::l0_visibility::lookup_vertex_prop;
    use uni_common::core::id::Vid;
    use uni_common::{Properties, Value};

    fn named(name: &str) -> Properties {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::String(name.to_string()));
        props
    }

    fn name_of(vid: Vid, ctx: &QueryContext) -> Option<String> {
        match lookup_vertex_prop(vid, "name", Some(ctx)) {
            Some(Value::String(s)) => Some(s),
            _ => None,
        }
    }

    /// A strategy-D snapshot must not observe writes that land after capture,
    /// while a fresh latest view must, and frozen data must stay visible.
    #[test]
    fn snapshot_isolated_from_later_writes() {
        let mgr = L0Manager::new(0, None);
        let alice = Vid::from(1_u64);
        let bob = Vid::from(2_u64);
        let labels = ["Node".to_string()];

        // Pre-snapshot state.
        {
            let current = mgr.get_current();
            let mut guard = current.write();
            guard.insert_vertex_with_labels(alice, named("alice"), &labels);
            guard.insert_vertex_with_labels(bob, named("bob"), &labels);
        }

        // Freeze-rotate snapshot.
        let (frozen, pending) = mgr.snapshot_isolated(1, None);
        let snap = QueryContext::new_with_pending(frozen, None, pending);

        // Post-snapshot write into the fresh current buffer.
        mgr.get_current()
            .write()
            .insert_vertex_with_labels(alice, named("alice2"), &labels);

        // The snapshot is isolated: it still sees the pre-write value.
        assert_eq!(name_of(alice, &snap).as_deref(), Some("alice"));

        // A fresh latest view sees the new value...
        let latest =
            QueryContext::new_with_pending(mgr.get_current(), None, mgr.get_pending_flush());
        assert_eq!(name_of(alice, &latest).as_deref(), Some("alice2"));

        // ...and the untouched vertex remains visible via the frozen generation.
        assert_eq!(name_of(bob, &latest).as_deref(), Some("bob"));
    }

    /// A pin marks the current generation; dropping the snapshot releases it.
    #[test]
    fn pin_marks_current_generation() {
        let mgr = L0Manager::new(0, None);
        assert!(!mgr.is_current_pinned());
        let snap = mgr.pin_snapshot();
        assert!(mgr.is_current_pinned());
        drop(snap);
        assert!(
            !mgr.is_current_pinned(),
            "dropping the snapshot releases the pin"
        );
    }

    /// Clone-on-freeze: after a pinned generation is frozen aside, the snapshot
    /// still observes its captured state while the new generation takes writes,
    /// and the new generation starts unpinned.
    #[test]
    fn clone_freeze_isolates_pinned_snapshot() {
        let mgr = L0Manager::new(0, None);
        let alice = Vid::from(1_u64);
        let labels = ["Node".to_string()];
        mgr.get_current()
            .write()
            .insert_vertex_with_labels(alice, named("alice"), &labels);

        let snap = mgr.pin_snapshot();
        assert!(mgr.is_current_pinned());

        // Commit-equivalent: freeze the pinned generation aside, then mutate the
        // fresh current (where a real commit's merge would land).
        mgr.freeze_current_for_snapshot();
        assert!(
            !mgr.is_current_pinned(),
            "the fresh generation starts unpinned"
        );
        mgr.get_current()
            .write()
            .insert_vertex_with_labels(alice, named("alice2"), &labels);

        // The snapshot still sees the pre-freeze value (isolated).
        let snap_ctx = QueryContext::new_with_pending(snap.main.clone(), None, snap.extra.clone());
        assert_eq!(name_of(alice, &snap_ctx).as_deref(), Some("alice"));

        // A fresh latest view sees the post-freeze value.
        let latest =
            QueryContext::new_with_pending(mgr.get_current(), None, mgr.get_pending_flush());
        assert_eq!(name_of(alice, &latest).as_deref(), Some("alice2"));

        // Dropping the snapshot releases its hold on the frozen generation.
        drop(snap);
    }
}
