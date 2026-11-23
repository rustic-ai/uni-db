// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Database-level fork administration.
//!
//! The `Uni` half of the fork surface: listing and inspecting forks,
//! dropping them (single and cascading), diffing fork against fork or
//! fork against primary, promoting fork content onto primary, and the
//! Lance-tag retention helpers. Fork *creation* and the forked-session
//! surface live in [`crate::api::fork`]; the registry state machine lives
//! in `uni-store`.

use std::sync::atomic::Ordering;

use uni_common::core::fork::ForkId;
use uni_common::{Result, UniError};

use crate::api::{Uni, fork_diff};

impl Uni {
    /// List every active fork on this database.
    ///
    /// Returns metadata snapshots — see [`uni_common::core::fork::ForkInfo`].
    /// Pending or Tombstoned entries are omitted; recovery resumes them
    /// on the next [`Uni::open`].
    ///
    /// # Scope: forks created through *this* handle
    ///
    /// The fork registry is cached in memory per handle and refreshed only by
    /// that handle's own fork-lifecycle calls. A fork created through a
    /// *different* [`Uni`] handle on the same store — even one that has since
    /// been shut down — is **not** reflected here, and the omission is silent:
    /// you get a shorter list, never an error. If a stale list would be acted
    /// on (creating a duplicate, sweeping by TTL, deciding a store is fork-free),
    /// re-open the handle rather than trusting this call across handles.
    ///
    /// Note this differs from schema and property reads, which *do* observe a
    /// second handle's writes. The divergence is deliberate rather than
    /// overlooked: every registry mutation goes through `ForkRegistryHandle`'s
    /// 2PC state machine as the single writer, so refreshing the cache from
    /// disk on a read has to be reconciled with that protocol. Multi-handle
    /// fork administration on one store is outside the supported model.
    /// Pinned by `second_handle_observes_fork_created_elsewhere`
    /// (crates/uni/tests/common/bugs/repro_issue_168_family_stale_second_handle.rs).
    pub async fn list_forks(&self) -> Vec<uni_common::core::fork::ForkInfo> {
        self.inner.fork_registry.list_active().await
    }

    /// Look up a fork by name.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::ForkNotFound`] when no fork has this name.
    pub async fn fork_info(&self, name: &str) -> Result<uni_common::core::fork::ForkInfo> {
        self.inner.fork_registry.get(name).await
    }

    /// Wait (bounded) for a fork's `holder_count` to drain to zero,
    /// returning the final count.
    ///
    /// Wait for a fork's holder count to drain to zero, returning the final
    /// count. (L1)
    ///
    /// Under async-flush a fork's `FlushCoordinator` finalizer is an orphan
    /// tokio task that transitively pins the fork's `ForkHolderGuard`, so
    /// `holder_count_for` can sit briefly above zero after the last session
    /// drops. This awaits the registry's deterministic drain `Notify` —
    /// fired the instant the last guard drops — bounded by
    /// `drop_fork_drain_timeout`, rather than a fixed-budget poll that could
    /// expire before the finalizer is scheduled under CPU starvation (the
    /// #99 `ForkInUse` flake). Shared by `drop_fork` (ignores the count) and
    /// `drop_fork_cascade` (uses it to build the blocker message).
    async fn wait_for_holders_drained(&self, fork_id: ForkId) -> usize {
        self.inner
            .fork_registry
            .wait_holders_drained(fork_id, self.inner.config.drop_fork_drain_timeout)
            .await
    }

    /// Drop a fork by name (Phase 1: read-only forks only).
    ///
    /// Runs the full drop 2PC: tombstone → delete branches → clear
    /// registry → delete tombstone + schema overlay. Recovery resumes
    /// from any in-progress state if the process dies mid-drop.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when the name is unknown.
    /// - [`UniError::ForkInUse`] when forked sessions are still live
    ///   on this fork. Drop again after they're released.
    /// - [`UniError::ForkHasChildren`] when the fork has nested children.
    /// - [`UniError::ForkTagged`] when the fork has tags. Nothing is
    ///   tombstoned; untag it with [`Self::untag_fork`] and drop again.
    ///
    /// # Examples
    ///
    /// ```
    /// # use uni_db::Uni;
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// let session = db.session();
    /// let forked = session.fork("ephemeral").await?;
    /// drop(forked);
    /// db.drop_fork("ephemeral").await?;
    /// # db.shutdown().await
    /// # }
    /// ```
    pub async fn drop_fork(&self, name: &str) -> Result<()> {
        if self.inner.is_read_only() {
            return Err(UniError::ReadOnly {
                operation: format!("drop fork '{name}' from a read-only handle"),
            });
        }
        // Hold the per-name lock for the whole drop sequence. `fork(name).build()`
        // holds the same lock for its entire open-or-create flow (api/fork.rs),
        // so create/open and drop are mutually exclusive per name: a concurrent
        // builder can never observe `Active` + register a holder while we are
        // tombstoning and force-deleting the Lance branches (review H2/M9). The
        // cascade path (`drop_fork_cascade`) drops each node through here, so it
        // inherits the same per-node serialization.
        let name_lock = self.inner.fork_registry.name_lock(name).await;
        let _name_guard = name_lock.lock().await;

        // Phase 2 Day 11: surface in-flight transactions before the
        // registry transitions to Tombstoned. The `ForkInUse` check in
        // `begin_drop` catches *session* holders; this catches the
        // case where a session is alive AND has at least one alive
        // `Transaction` on the fork's UniInner. We track this via an
        // `inflight_tx_count` AtomicUsize that `Transaction::new`
        // increments and `Transaction::drop` decrements unconditionally
        // (so commit/rollback/silent-drop all converge to zero).
        let preview = self.inner.fork_registry.get(name).await?;

        // Phase 3: refuse to drop a parent that still has children.
        // Callers should use `drop_fork_cascade` to remove the subtree.
        let children = self.inner.fork_registry.list_children(preview.id).await;
        if !children.is_empty() {
            return Err(UniError::ForkHasChildren {
                name: name.to_string(),
                children: children.into_iter().map(|c| c.name).collect(),
            });
        }
        // A tag pins the fork's branch and Lance refuses to delete a tagged
        // branch, so refuse here, before anything is tombstoned.
        self.refuse_if_tagged(&preview).await?;

        if let Some(weak) = self
            .inner
            .fork_inners
            .get(&preview.id)
            .map(|e| e.value().clone())
            && let Some(inner) = weak.upgrade()
        {
            if inner.inflight_tx_count.load(Ordering::Acquire) > 0 {
                return Err(UniError::ForkInflightTx {
                    name: name.to_string(),
                });
            }
            // Drain any pending async flushes, THEN shut down the
            // coordinator so its finalizer task exits. Both steps are
            // required: drain waits for in-flight streams to finalize
            // (pending_count → 0), but the finalizer task itself stays
            // parked at submit_rx.recv() holding Arc<StorageManager>.
            // Storage pins Arc<ForkScope> (manager.rs:364), which holds
            // the ForkHolderGuard. Without the explicit shutdown, the
            // task lives until Writer/Coordinator drop transitively,
            // which never happens before drop_fork's holder-count check.
            // See async-flush plan §3.9 / L8.
            if let Some(writer) = inner.writer.as_ref()
                && let Some(coord) = writer.flush_coordinator()
            {
                if coord
                    .drain(self.inner.config.drop_fork_drain_timeout)
                    .await
                    .is_err()
                {
                    return Err(UniError::PendingFlushTimeout {
                        name: name.to_string(),
                    });
                }
                // Drop submit_tx + await finalizer task exit so
                // Arc<storage> (+ Arc<ForkScope>) drops on this writer.
                coord.shutdown().await;
            }
            // Drop our local Arc clone of `inner` so the only strong
            // ref to fork's UniInner is gone. ForkHolderGuard drops
            // when ForkScope drops, which happens once storage Arc → 0.
            drop(inner);
        }
        // Wait for the fork's holder_count to drop to zero. Under async-
        // flush, the fork's FlushCoordinator's finalizer task is an
        // orphan tokio task that holds Arc<StorageManager> via
        // SharedFlushCtx. Storage pins Arc<ForkScope> which holds the
        // ForkHolderGuard. When the fork's Session drops at scope-end,
        // UniInner drops (so the `weak.upgrade()` above returns None
        // and we never enter the drain/shutdown branch), but the orphan
        // finalizer task is STILL alive in tokio's queue holding the
        // chain that ultimately pins the holder counter at 1.
        //
        // The fix is to wait: the orphan task exits the moment its
        // mpsc receiver sees a closed channel, which happens when
        // FlushCoordinator drops submit_tx in its own Drop. That Drop
        // ran transitively when UniInner dropped, but the spawned
        // task's destructor may still be pending in the scheduler
        // queue. yield_now repeatedly lets the runtime work through
        // those destructors before we check holder_count.
        self.wait_for_holders_drained(preview.id).await;
        let info = self.inner.fork_registry.begin_drop(name).await?;
        // Crash seam: the tombstone and the Tombstoned registry entry are now
        // durable, and nothing else has happened. Recovery must finish the drop.
        fail::fail_point!("fork::drop-after-begin");
        // Phase 2 Day 8: evict the cached `Weak<UniInner>` (if any)
        // before deleting branches. The registry has already
        // transitioned the fork to Tombstoned, so concurrent
        // `fork(name)` calls now error out before reaching the cache;
        // this eviction is purely cleanup so the map doesn't accumulate
        // dead Weak entries across the lifetime of the database.
        self.inner.fork_inners.remove(&info.id);
        // Step 3: walk branches and force-delete each. Track failures: if any
        // branch delete fails we must NOT finish_drop, because finish_drop
        // deletes the recovery tombstone — the only anchor that lets boot-time
        // recovery retry the deletion. Dropping it would orphan the surviving
        // branches permanently (review M3). Leave the fork Tombstoned instead.
        let branching =
            self.inner
                .storage
                .backend()
                .branching()
                .ok_or_else(|| UniError::ForkLifecycle {
                    name: info.name.clone(),
                    stage: "drop",
                    source: anyhow::anyhow!("storage backend does not support fork branching")
                        .into(),
                })?;
        let mut delete_failure: Option<String> = None;
        for (dataset, branch) in &info.datasets {
            if let Err(e) = branching.delete_branch(dataset, branch).await {
                tracing::warn!(
                    dataset = %dataset,
                    branch = %branch,
                    "delete_branch during drop_fork failed: {e}"
                );
                delete_failure = Some(format!("{dataset}/{branch}: {e}"));
            }
            // Crash seam, per iteration. Arm it `1*off->panic` to crash after
            // one branch is already gone: that is the partially-deleted fork
            // the env-var fault injection cannot produce, because it fails
            // *every* delete rather than the Nth.
            fail::fail_point!("fork::drop-mid-delete-loop");
        }
        if let Some(detail) = delete_failure {
            // Tombstone + registry entry remain; `recover_forks` will retry
            // delete_all_branches + finish_drop on the next open.
            return Err(UniError::ForkLifecycle {
                name: name.to_string(),
                stage: "delete_branch",
                source: format!(
                    "branch delete failed; fork left Tombstoned for recovery ({detail})"
                )
                .into(),
            });
        }
        // Crash seam: every branch is gone but the tombstone is still the
        // anchor. Recovery must finish the drop and leave no residue.
        fail::fail_point!("fork::drop-before-finish");
        // Step 4: remove the fork's storage-side artifacts (WAL, id allocator,
        // fork-scoped snapshot manifests) so a dropped fork leaves no disk
        // residue (review H3). On the storage object store, not the registry's.
        //
        // **Before `finish_drop`, deliberately.** The Step-3 comment above
        // states the rule for branches — `finish_drop` deletes the recovery
        // tombstone, "the only anchor that lets boot-time recovery retry the
        // deletion" — and the same rule governs these artifacts. With the old
        // order a crash between the two left the WAL directory and the id
        // allocator on disk with no tombstone and no registry entry, so
        // `recover_forks` never saw them: a permanent leak, and the one drop
        // window that was not self-healing.
        //
        // This order has no such window. `delete_fork_artifacts` is best-effort
        // and idempotent (prefix list + delete, errors only warned), so a crash
        // between it and `finish_drop` leaves a Tombstoned fork whose artifacts
        // are already gone — and recovery simply re-runs the same idempotent
        // deletes.
        uni_store::fork::delete_fork_artifacts(&self.inner.storage.store(), &info.id).await;
        // Crash seam for exactly that window: artifacts gone, tombstone still
        // present. This is the seam that proves the reorder is safe.
        fail::fail_point!("fork::drop-after-artifacts");
        // Step 5 + 6: clear the registry entry, delete tombstone + schema
        // overlay files.
        self.inner.fork_registry.finish_drop(&info).await?;
        Ok(())
    }

    /// Drop a fork and every descendant in its subtree (Phase 3).
    ///
    /// Pre-validates the entire subtree before tombstoning anything:
    /// every node must pass the same `ForkInUse` + `ForkInflightTx`
    /// checks `drop_fork` applies for a single node. On any blocker
    /// the call errors with [`UniError::ForkSubtreeInUse`] and no
    /// branch is deleted. Once validation passes, the cascade drops
    /// each node deepest-first via the single-fork `drop_fork` path,
    /// so a crash mid-cascade resumes cleanly through existing
    /// tombstone recovery.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if `name` is unknown.
    /// - [`UniError::ForkSubtreeInUse`] if any node in the subtree has
    ///   live sessions or open transactions.
    /// - [`UniError::ForkTagged`] if any node in the subtree has tags,
    ///   naming the first tagged node. Nothing is tombstoned.
    pub async fn drop_fork_cascade(&self, name: &str) -> Result<()> {
        if self.inner.is_read_only() {
            return Err(UniError::ReadOnly {
                operation: format!("drop fork '{name}' from a read-only handle"),
            });
        }
        // 1. Resolve the root and walk descendants depth-first.
        let root = self.inner.fork_registry.get(name).await?;
        let mut order: Vec<uni_common::core::fork::ForkInfo> = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(node) = stack.pop() {
            let kids = self.inner.fork_registry.list_children(node.id).await;
            for k in &kids {
                stack.push(k.clone());
            }
            order.push(node);
        }
        // `order` is roots-first by construction. Reversing it yields
        // deepest-first, which is the order we drop in.
        order.reverse();

        // 2. Pre-validate every node. Aggregate blockers; refuse before
        // tombstoning if any node is held or has in-flight tx.
        //
        // Under async-flush, holder_count may transiently sit at 1 for a
        // brief window after the last session drops, while orphan
        // FlushCoordinator finalizer tasks finish exiting (they hold
        // Arc<storage> → Arc<ForkScope> → ForkHolderGuard). Apply the
        // same bounded wait we use in `drop_fork`.
        let mut blockers: Vec<String> = Vec::new();
        for node in &order {
            if let Some(weak) = self
                .inner
                .fork_inners
                .get(&node.id)
                .map(|e| e.value().clone())
                && let Some(inner) = weak.upgrade()
                && inner.inflight_tx_count.load(Ordering::Acquire) > 0
            {
                blockers.push(format!("{}: in-flight tx", node.name));
                continue;
            }
            // Wait briefly for orphan finalizer tasks to exit.
            let holders = self.wait_for_holders_drained(node.id).await;
            if holders > 0 {
                blockers.push(format!("{}: {} live session(s)", node.name, holders));
            }
        }
        if !blockers.is_empty() {
            return Err(UniError::ForkSubtreeInUse { blockers });
        }
        // Any tagged node would stop the cascade part-way (after its
        // descendants were already dropped), so check them all first.
        for node in &order {
            self.refuse_if_tagged(node).await?;
        }

        // 3. Drop deepest-first using the single-fork path. Each call
        // re-checks holders/inflight inside `drop_fork`, which is
        // belt-and-braces against a session opening between validation
        // and drop; that race surfaces as a normal ForkInUse error.
        for node in order {
            self.drop_fork(&node.name).await?;
        }
        Ok(())
    }

    /// Structural diff between two forks.
    ///
    /// Returns the delta that would turn `a` into `b`: `added` rows
    /// are present in `b` only, `deleted` in `a` only, and `changed`
    /// rows are on both sides with different properties. Identity is
    /// content-addressed UID (Phase 6b) for vertices and an
    /// edge-content UID (Phase 7d) for edges, so the diff is correct
    /// even between two unrelated forks that happen to have rolled
    /// the same VIDs.
    ///
    /// An edited row has a new UID, so two more pairings recover it as a
    /// `changed` row: vertices sharing `(label, ext_id)` always pair, and
    /// when one fork is the other's ancestor (on its parent chain),
    /// vertices and edges also pair by the VID / EID the descendant
    /// inherited. Ids never pair between unrelated forks (siblings, or
    /// forks of different parents), which allocate them independently.
    ///
    /// `diff(a, b).invert() == diff(b, a)` by construction — see
    /// [`fork_diff::ForkDiff::invert`].
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when either name is unknown.
    /// - Any error from opening a fork session on either side.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::{DataType, Uni};
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// db.schema().label("Person").property("name", DataType::String).apply().await?;
    /// let primary = db.session();
    /// {
    ///     let a = primary.fork("scenario_a").await?;
    ///     let tx = a.tx().await?;
    ///     tx.execute("CREATE (:Person {name: 'A-only'})").await?;
    ///     tx.commit().await?;
    /// }
    /// {
    ///     let b = primary.fork("scenario_b").await?;
    ///     let tx = b.tx().await?;
    ///     tx.execute("CREATE (:Person {name: 'B-only'})").await?;
    ///     tx.commit().await?;
    /// }
    /// let diff = db.diff_forks("scenario_a", "scenario_b").await?;
    /// assert_eq!(diff.vertices.added.len(), 1);   // B-only
    /// assert_eq!(diff.vertices.deleted.len(), 1); // A-only
    /// # db.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn diff_forks(&self, a: &str, b: &str) -> Result<fork_diff::ForkDiff> {
        // `fork(name)` opens or creates: resolve both names first so an
        // unknown one is `ForkNotFound`, not a new fork.
        let info_a = self.inner.fork_registry.get(a).await?;
        let info_b = self.inner.fork_registry.get(b).await?;
        let lineage = self
            .shared_lineage(Some(&info_a), &info_b)
            .await
            .or(self.shared_lineage(Some(&info_b), &info_a).await);
        let primary = self.session();
        let sess_a = primary.fork(a).await?;
        let sess_b = primary.fork(b).await?;
        fork_diff::compute_diff_with_lineage(&sess_a, &sess_b, lineage).await
    }

    /// The ids `descendant` shares with `ancestor` (`None` = primary), or
    /// `None` when `ancestor` is not on `descendant`'s parent chain.
    ///
    /// The floors are the fork-point high-water marks of the chain node
    /// directly below `ancestor`: everything that node, and so `descendant`,
    /// holds below them was inherited from `ancestor`. A fork created before
    /// the marks were recorded has zero floors, which pairs nothing by id.
    async fn shared_lineage(
        &self,
        ancestor: Option<&uni_common::core::fork::ForkInfo>,
        descendant: &uni_common::core::fork::ForkInfo,
    ) -> Option<fork_diff::SharedLineage> {
        let forks: std::collections::HashMap<ForkId, uni_common::core::fork::ForkInfo> = self
            .inner
            .fork_registry
            .snapshot()
            .await
            .forks
            .into_values()
            .map(|f| (f.id, f))
            .collect();
        let target = ancestor.map(|a| a.id);
        let mut node = descendant.clone();
        // Bounded by the registry size, so a corrupt cycle cannot spin.
        for _ in 0..=forks.len() {
            if node.parent_fork_id == target {
                return Some(fork_diff::SharedLineage {
                    vid_floor: node.fork_point_vid_hwm,
                    eid_floor: node.fork_point_eid_hwm,
                });
            }
            node = forks.get(&node.parent_fork_id?)?.clone();
        }
        None
    }

    /// Structural diff between a fork and primary.
    ///
    /// Equivalent to `diff(primary, fork)`: rows the fork has added
    /// since the fork point appear in `added`; rows it has dropped
    /// appear in `deleted`; rows it (or primary) edited appear in
    /// `changed`. Primary is every fork's ancestor, so besides content
    /// UID and `(label, ext_id)`, vertices and edges the fork inherited
    /// pair by VID / EID. See [`fork_diff::ForkDiff`] for the data
    /// model.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when the fork name is unknown.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::{DataType, Uni};
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// db.schema().label("Person").property("name", DataType::String).apply().await?;
    /// let primary = db.session();
    /// {
    ///     let fork = primary.fork("audit").await?;
    ///     let tx = fork.tx().await?;
    ///     tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    ///     tx.commit().await?;
    /// }
    /// let diff = db.diff_fork_primary("audit").await?;
    /// assert_eq!(diff.vertices.added.len(), 1); // Bob
    /// # db.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn diff_fork_primary(&self, fork_name: &str) -> Result<fork_diff::ForkDiff> {
        // `fork(name)` opens or creates: resolve the name first.
        let info = self.inner.fork_registry.get(fork_name).await?;
        // Every fork descends from primary.
        let lineage = self.shared_lineage(None, &info).await;
        let primary = self.session();
        let sess_fork = primary.fork(fork_name).await?;
        fork_diff::compute_diff_with_lineage(&primary, &sess_fork, lineage).await
    }

    /// Promote matched fork rows onto primary.
    ///
    /// For each [`fork_diff::PromotePattern`] in `patterns`:
    ///
    /// - **`PromotePattern::Vertex`** — scan the fork for vertices
    ///   with the given label, compute a content-derived UID for
    ///   each match, skip rows that already exist on primary by UID,
    ///   bulk-insert the rest.
    /// - **`PromotePattern::Edge`** — scan the fork for edges of the
    ///   given type, resolve endpoint UIDs against primary, skip
    ///   rows whose endpoints aren't on primary (counted in
    ///   [`fork_diff::PromoteReport::edges_skipped_no_endpoint`]),
    ///   dedup against existing parallel edges by content UID
    ///   (Phase 7d multi-edge identity), and bulk-insert the rest.
    ///
    /// All inserts run inside one primary-targeted transaction that
    /// commits on success. Mixing vertex and edge patterns in one
    /// call is supported — endpoints inserted by an earlier vertex
    /// pattern are visible to a subsequent edge pattern via an
    /// in-memory cache.
    ///
    /// A label or edge type declared on neither side (schemaless) is
    /// promoted schemalessly, as a Cypher `CREATE` on primary would
    /// write it.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when the fork name is unknown.
    /// - [`UniError::ReadOnly`] on a read-only handle.
    /// - [`UniError::LabelNotFound`] when a vertex pattern targets a
    ///   label the fork declared but primary does not.
    /// - [`UniError::EdgeTypeNotFound`] when an edge pattern targets
    ///   an edge type the fork declared but primary does not.
    /// - Any error from the primary write path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::{DataType, PromotePattern, Uni};
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// db.schema().label("Person").property("name", DataType::String).apply().await?;
    /// let primary = db.session();
    /// {
    ///     let fork = primary.fork("publish").await?;
    ///     let tx = fork.tx().await?;
    ///     tx.execute("CREATE (:Person {name: 'NewKid'})").await?;
    ///     tx.commit().await?;
    /// }
    /// let report = db.promote_from_fork(
    ///     "publish",
    ///     &[PromotePattern::label("Person")],
    /// ).await?;
    /// assert!(report.vertices_inserted >= 1);
    /// # db.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn promote_from_fork(
        &self,
        fork_name: &str,
        patterns: &[fork_diff::PromotePattern],
    ) -> Result<fork_diff::PromoteReport> {
        self.promote_from_fork_with_options(
            fork_name,
            patterns,
            &fork_diff::PromoteOptions::default(),
        )
        .await
    }

    /// Promote fork changes to primary with explicit merge [`options`].
    ///
    /// Same as [`Self::promote_from_fork`] but lets the caller enable
    /// ext_id-keyed upsert (`PromoteOptions::with_upsert`): a fork edit to
    /// a vertex that already exists on primary is applied in place instead
    /// of inserting a twin. The default options reproduce the insert-only
    /// behavior of `promote_from_fork`, so existing callers are unaffected.
    ///
    /// # Errors
    /// Returns [`UniError::ReadOnly`] on a read-only handle,
    /// [`UniError::LabelNotFound`] / [`UniError::EdgeTypeNotFound`]
    /// when a pattern targets a label or edge type the fork declared but
    /// primary does not, or
    /// any error from the underlying fork flush, transaction, or commit.
    ///
    /// [`options`]: fork_diff::PromoteOptions
    pub async fn promote_from_fork_with_options(
        &self,
        fork_name: &str,
        patterns: &[fork_diff::PromotePattern],
        options: &fork_diff::PromoteOptions,
    ) -> Result<fork_diff::PromoteReport> {
        // Promote commits to primary: a read-only handle writes nothing.
        if self.inner.is_read_only() {
            return Err(UniError::ReadOnly {
                operation: format!("promote from fork '{fork_name}' on a read-only handle"),
            });
        }
        // `fork(name)` opens or creates: resolve the name first so an unknown
        // fork is `ForkNotFound`, not a new, empty fork to promote from.
        self.inner.fork_registry.get(fork_name).await?;
        let primary = self.session();
        let fork = primary.fork(fork_name).await?;
        // Persist any pending tx commits on the fork to Lance so the
        // promote engine's reads see them. Without this, edges
        // committed via a now-dropped fork session may not be visible
        // to the fresh fork session we just opened.
        fork.flush().await?;
        // Every pattern's target (label or edge type) must be one primary can
        // take: declared on primary, or undeclared on both sides (schemaless,
        // #306), which the promote sink inserts schemalessly. A target the
        // fork declared in its overlay but primary lacks is refused before
        // the transaction opens: promoting it schemalessly would silently
        // drop the fork's declaration (types, constraints, indexes).
        let primary_schema = self.inner.schema.schema();
        let fork_schema = fork.db.schema.schema();
        for pat in patterns {
            if pat.is_edge() {
                let edge_type = pat.edge_type_name();
                if !primary_schema.edge_types.contains_key(edge_type)
                    && fork_schema.edge_types.contains_key(edge_type)
                {
                    return Err(UniError::EdgeTypeNotFound {
                        edge_type: edge_type.to_string(),
                    });
                }
            } else {
                let label = pat.label_name();
                if !primary_schema.labels.contains_key(label)
                    && fork_schema.labels.contains_key(label)
                {
                    return Err(UniError::LabelNotFound {
                        label: label.to_string(),
                    });
                }
            }
        }
        // Delete-promotion and conflict detection need the fork-point
        // baseline: primary's state as of the fork point, read by pinning a
        // session at the fork's parent snapshot. Built only for merge mode.
        let baseline = if options.delete_promotion {
            Some(self.build_promote_baseline(fork_name, patterns).await?)
        } else {
            None
        };

        let primary_tx = primary.tx().await?;
        let report = fork_diff::run_promote(
            &fork,
            &primary,
            &primary_tx,
            patterns,
            options,
            baseline.as_ref(),
        )
        .await?;
        primary_tx.commit().await?;
        Ok(report)
    }

    /// Read primary as of a fork's creation point into a [`PromoteBaseline`].
    ///
    /// Pins a session to the fork's `parent_snapshot_id` and scans each
    /// vertex-pattern label, keying rows by `ext_id` (and `ext_id`-less rows
    /// by content UID). Returns an empty baseline when the fork has no
    /// fork-point snapshot (an in-memory primary that never flushed before
    /// forking — there is no prior primary state to delete against).
    ///
    /// [`PromoteBaseline`]: fork_diff::PromoteBaseline
    async fn build_promote_baseline(
        &self,
        fork_name: &str,
        patterns: &[fork_diff::PromotePattern],
    ) -> Result<fork_diff::PromoteBaseline> {
        use uni_common::Value;
        use uni_fork::ForkQueryHost;
        use uni_store::storage::vertex::VertexDataset;

        let mut baseline = fork_diff::PromoteBaseline::default();
        let info = self.inner.fork_registry.get(fork_name).await?;
        if info.parent_snapshot_id == "uninitialized" {
            return Ok(baseline);
        }

        let mut base_session = self.session();
        base_session
            .pin_to_version(&info.parent_snapshot_id)
            .await?;
        let ext_ids = base_session
            .storage()
            .get_vertex_ext_ids()
            .await
            .unwrap_or_default();

        let mut labels: Vec<&str> = patterns
            .iter()
            .filter(|p| !p.is_edge())
            .map(|p| p.label_name())
            .collect();
        labels.sort_unstable();
        labels.dedup();

        for label in labels {
            let escaped = label.replace('`', "``");
            let cypher = format!("MATCH (n:`{escaped}`) RETURN n");
            let rs = base_session.query(&cypher).await?;
            for row in rs.rows() {
                if let Some(Value::Node(node)) = row.value("n") {
                    match ext_ids.get(&node.vid) {
                        Some(eid) if !eid.is_empty() => {
                            baseline
                                .ext
                                .entry(label.to_string())
                                .or_default()
                                .insert(eid.clone(), node.properties.clone());
                        }
                        _ => {
                            baseline
                                .no_ext
                                .entry(label.to_string())
                                .or_default()
                                .insert(VertexDataset::compute_vertex_uid(
                                    label,
                                    None,
                                    &node.properties,
                                ));
                        }
                    }
                }
            }
        }
        Ok(baseline)
    }

    /// Tag a fork with a Lance tag (Phase 4a).
    ///
    /// Creates one tag per dataset the fork has branched, named
    /// `fork_{fork_id}_{tag}_{dataset}`, so two forks can use the same tag
    /// name. Lance tags are GC-exempt — the tagged versions survive
    /// compaction's retention sweep. Useful for audit hold, regulatory
    /// snapshots, or named pre-publish checkpoints.
    ///
    /// The fork is flushed first, so rows still buffered in its L0 are in
    /// the tagged versions; the tag pins each branch's version at that
    /// point, and later fork writes do not move it.
    ///
    /// A tagged fork cannot be dropped: a tag pins its branch, and
    /// [`Self::drop_fork`] refuses it with [`UniError::ForkTagged`]. Untag
    /// it with [`Self::untag_fork`] first.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ReadOnly`] on a read-only handle.
    /// - [`UniError::InvalidArgument`] for an empty tag name.
    /// - [`UniError::ForkLifecycle`] (stage = `tag`) when the fork has no
    ///   branched datasets even after the flush (nothing to tag), when it
    ///   already has this tag, or on Lance failures (invalid tag
    ///   characters, IO).
    pub async fn tag_fork(&self, fork_name: &str, tag: &str) -> Result<()> {
        // A tag is a Lance ref written to the store: a read-only handle
        // writes none, and a fenced one no longer may.
        if self.inner.is_read_only() {
            return Err(UniError::ReadOnly {
                operation: format!("tag fork '{fork_name}' from a read-only handle"),
            });
        }
        if tag.is_empty() {
            return Err(UniError::InvalidArgument {
                arg: "tag".to_string(),
                message: "a fork tag name cannot be empty".to_string(),
            });
        }
        self.inner
            .storage
            .write_fence()
            .check()
            .map_err(crate::api::storage_error)?;
        // `fork(name)` opens or creates: resolve the name first so an unknown
        // fork is `ForkNotFound`, not a new fork.
        self.inner.fork_registry.get(fork_name).await?;
        // Flush the fork so its L0 rows are in the tagged versions. A fork
        // branches a dataset at its first flush to it, so this is also what
        // gives a fork that has only buffered writes something to tag.
        {
            let fork = self.session().fork(fork_name).await?;
            fork.flush().await?;
        }
        let info = self.inner.fork_registry.get(fork_name).await?;
        if info.datasets.is_empty() {
            return Err(UniError::ForkLifecycle {
                name: fork_name.to_string(),
                stage: "tag",
                source: "fork has no branched datasets: nothing to tag".into(),
            });
        }
        let branching = self.fork_branching(fork_name, "tag")?;

        // L9: a fork tag spans one backend tag per dataset, and `create_tag`
        // is not atomic across them. Pre-validate that this fork does not
        // already carry the tag (fail fast with no partial state on the
        // common "already tagged" case), then create while tracking what
        // THIS call created so a mid-loop failure rolls back only those —
        // never a pre-existing tag on another dataset.
        let existing = fork_tags_on_branches(branching.as_ref(), &info, "tag").await?;
        if let Some(found) = existing.iter().find(|t| t.tag == tag) {
            return Err(UniError::ForkLifecycle {
                name: fork_name.to_string(),
                stage: "tag",
                source: format!("tag '{tag}' already present on dataset '{}'", found.dataset)
                    .into(),
            });
        }

        let mut created: Vec<(String, String)> = Vec::new();
        for (dataset, branch) in &info.datasets {
            let lance_tag = fork_lance_tag_name(info.id, tag, dataset);
            if let Err(e) = branching.create_tag(dataset, &lance_tag, branch).await {
                for (rb_dataset, tag_name) in &created {
                    if let Err(rb) = branching.delete_tag(rb_dataset, tag_name).await {
                        tracing::warn!(
                            "tag_fork rollback: delete_tag '{tag_name}' on '{rb_dataset}' failed: {rb}"
                        );
                    }
                }
                return Err(UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "tag",
                    source: e.into(),
                });
            }
            created.push((dataset.clone(), lance_tag));
        }
        Ok(())
    }

    /// Remove a tag previously applied via [`Self::tag_fork`] (Phase 4a).
    ///
    /// Only this fork's tag of that name is removed: another fork's tag of
    /// the same name is left alone. Idempotent — a missing tag is treated
    /// as success so partial cleanup retries are safe. Tags written by
    /// earlier releases under `fork_{tag}_{dataset}` are removed too.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ReadOnly`] on a read-only handle.
    /// - [`UniError::ForkLifecycle`] (stage = `untag`) on Lance failures.
    pub async fn untag_fork(&self, fork_name: &str, tag: &str) -> Result<()> {
        // A tag is a Lance ref written to the store: a read-only handle
        // writes none, and a fenced one no longer may.
        if self.inner.is_read_only() {
            return Err(UniError::ReadOnly {
                operation: format!("untag fork '{fork_name}' from a read-only handle"),
            });
        }
        self.inner
            .storage
            .write_fence()
            .check()
            .map_err(crate::api::storage_error)?;
        let info = self.inner.fork_registry.get(fork_name).await?;
        if info.datasets.is_empty() {
            return Ok(());
        }
        let branching = self.fork_branching(fork_name, "untag")?;
        for found in fork_tags_on_branches(branching.as_ref(), &info, "untag").await? {
            if found.tag != tag {
                continue;
            }
            branching
                .delete_tag(&found.dataset, &found.lance_tag)
                .await
                .map_err(|e| UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "untag",
                    source: e.into(),
                })?;
        }
        Ok(())
    }

    /// List the unique tag names applied to this fork (Phase 4a).
    ///
    /// A fork's tag is stored as one Lance tag per dataset, on the fork's
    /// branch of that dataset. This method enumerates the distinct tag
    /// names pinned on at least one of the fork's branches — never another
    /// fork's tags, even of the same name.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ForkLifecycle`] (stage = `list_tags`) on Lance failures.
    pub async fn list_fork_tags(&self, fork_name: &str) -> Result<Vec<String>> {
        let info = self.inner.fork_registry.get(fork_name).await?;
        if info.datasets.is_empty() {
            return Ok(Vec::new());
        }
        let branching = self.fork_branching(fork_name, "list_tags")?;
        let tags: std::collections::BTreeSet<String> =
            fork_tags_on_branches(branching.as_ref(), &info, "list_tags")
                .await?
                .into_iter()
                .map(|t| t.tag)
                .collect();
        Ok(tags.into_iter().collect())
    }

    /// Refuse to drop `info` while any of its branches carries a tag.
    ///
    /// Lance refuses to delete a branch a tag references, even forced, so a
    /// drop that went ahead would tombstone the fork and then fail at
    /// `delete_branch`. Checked before `begin_drop`, so a refusal leaves the
    /// fork Active.
    async fn refuse_if_tagged(&self, info: &uni_common::core::fork::ForkInfo) -> Result<()> {
        if info.datasets.is_empty() {
            return Ok(());
        }
        let Some(branching) = self.inner.storage.backend().branching() else {
            return Ok(());
        };
        let tags: std::collections::BTreeSet<String> =
            fork_tags_on_branches(branching.as_ref(), info, "drop")
                .await?
                .into_iter()
                .map(|t| t.tag)
                .collect();
        if tags.is_empty() {
            return Ok(());
        }
        Err(UniError::ForkTagged {
            name: info.name.clone(),
            tags: tags.into_iter().collect(),
        })
    }

    /// The backend's branching surface, or a `stage` lifecycle error.
    fn fork_branching(
        &self,
        fork_name: &str,
        stage: &'static str,
    ) -> Result<std::sync::Arc<dyn uni_store::backend::ForkBranching>> {
        self.inner
            .storage
            .backend()
            .branching()
            .ok_or_else(|| UniError::ForkLifecycle {
                name: fork_name.to_string(),
                stage,
                source: anyhow::anyhow!("storage backend does not support fork branching").into(),
            })
    }
}

/// A tag found on one of a fork's branches.
struct ForkBranchTag {
    /// The dataset whose branch carries the tag.
    dataset: String,
    /// The Lance tag's own name.
    lance_tag: String,
    /// The fork-level tag name (`tag_fork`'s `tag`).
    tag: String,
}

/// The Lance tag name for fork tag `tag` on `dataset`.
///
/// Lance tags are table-wide, so the fork id is part of the name: two forks
/// tagging the same name on one dataset must not collide.
fn fork_lance_tag_name(fork_id: ForkId, tag: &str, dataset: &str) -> String {
    format!("fork_{fork_id}_{tag}_{dataset}")
}

/// The fork-level tag name of a Lance tag found on a fork's branch.
///
/// Reads `fork_{fork_id}_{tag}_{dataset}` and, for tags written before tag
/// names carried the fork id, `fork_{tag}_{dataset}`. Any other name on the
/// branch (a tag not written by `tag_fork`) is returned as is: it pins the
/// branch all the same.
fn fork_tag_from_lance_name(lance_tag: &str, fork_id: ForkId, dataset: &str) -> String {
    let scoped = format!("fork_{fork_id}_");
    let suffix = format!("_{dataset}");
    if let Some(tag) = lance_tag
        .strip_prefix(&scoped)
        .and_then(|rest| rest.strip_suffix(&suffix))
        .filter(|t| !t.is_empty())
    {
        return tag.to_string();
    }
    if let Some(tag) = lance_tag
        .strip_prefix("fork_")
        .and_then(|rest| rest.strip_suffix(&suffix))
        .filter(|t| !t.is_empty())
    {
        return tag.to_string();
    }
    lance_tag.to_string()
}

/// Every tag pinned on one of `info`'s branches.
///
/// Lance tags are table-wide, so the filter is the tag's branch: a tag on
/// another fork's branch of the same dataset is not this fork's.
async fn fork_tags_on_branches(
    branching: &dyn uni_store::backend::ForkBranching,
    info: &uni_common::core::fork::ForkInfo,
    stage: &'static str,
) -> Result<Vec<ForkBranchTag>> {
    let mut found = Vec::new();
    for (dataset, branch) in &info.datasets {
        let on_disk = branching
            .list_tags(dataset)
            .await
            .map_err(|e| UniError::ForkLifecycle {
                name: info.name.clone(),
                stage,
                source: e.into(),
            })?;
        for t in on_disk {
            if t.branch.as_deref() != Some(branch.as_str()) {
                continue;
            }
            found.push(ForkBranchTag {
                dataset: dataset.clone(),
                tag: fork_tag_from_lance_name(&t.name, info.id, dataset),
                lance_tag: t.name,
            });
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_tag_names_round_trip_in_both_forms() {
        let id = ForkId::new();
        let scoped = fork_lance_tag_name(id, "audit_q1", "vertices_Person");
        assert_eq!(
            fork_tag_from_lance_name(&scoped, id, "vertices_Person"),
            "audit_q1"
        );
        assert_eq!(
            fork_tag_from_lance_name("fork_audit_q1_vertices_Person", id, "vertices_Person"),
            "audit_q1"
        );
        assert_eq!(
            fork_tag_from_lance_name("pinned", id, "vertices_Person"),
            "pinned"
        );
    }
}
