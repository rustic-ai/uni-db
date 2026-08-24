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
    pub async fn drop_fork_cascade(&self, name: &str) -> Result<()> {
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
    /// are present in `b` only, `deleted` in `a` only. Identity is
    /// content-addressed UID (Phase 6b) for vertices and an
    /// edge-content UID (Phase 7d) for edges, so the diff is correct
    /// even between two unrelated forks that happen to have rolled
    /// the same VIDs.
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
        let primary = self.session();
        let sess_a = primary.fork(a).await?;
        let sess_b = primary.fork(b).await?;
        fork_diff::compute_diff(&sess_a, &sess_b).await
    }

    /// Structural diff between a fork and primary.
    ///
    /// Equivalent to `diff(primary, fork)`: rows the fork has added
    /// since the fork point appear in `added`; rows it has dropped
    /// appear in `deleted`. Identity is content-addressed UID
    /// (vertices) / edge-content UID (edges), so unrelated forks
    /// pair correctly. See [`fork_diff::ForkDiff`] for the data
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
        let primary = self.session();
        let sess_fork = primary.fork(fork_name).await?;
        fork_diff::compute_diff(&primary, &sess_fork).await
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
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] when the fork name is unknown.
    /// - [`UniError::LabelNotFound`] when a vertex pattern targets a
    ///   label that does not exist on primary.
    /// - [`UniError::EdgeTypeNotFound`] when an edge pattern targets
    ///   an edge type that does not exist on primary.
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
    /// Returns [`UniError::LabelNotFound`] / [`UniError::EdgeTypeNotFound`]
    /// when a pattern targets a label or edge type absent on primary, or
    /// any error from the underlying fork flush, transaction, or commit.
    ///
    /// [`options`]: fork_diff::PromoteOptions
    pub async fn promote_from_fork_with_options(
        &self,
        fork_name: &str,
        patterns: &[fork_diff::PromotePattern],
        options: &fork_diff::PromoteOptions,
    ) -> Result<fork_diff::PromoteReport> {
        let primary = self.session();
        let fork = primary.fork(fork_name).await?;
        // Persist any pending tx commits on the fork to Lance so the
        // promote engine's reads see them. Without this, edges
        // committed via a now-dropped fork session may not be visible
        // to the fresh fork session we just opened.
        fork.flush().await?;
        // Ensure every pattern's target (label or edge type) exists on
        // primary; surfacing a clear error is preferable to letting
        // bulk_insert_* fail mid-flight.
        let primary_schema = self.inner.schema.schema();
        for pat in patterns {
            if pat.is_edge() {
                let edge_type = pat.edge_type_name();
                if !primary_schema.edge_types.contains_key(edge_type) {
                    return Err(UniError::EdgeTypeNotFound {
                        edge_type: edge_type.to_string(),
                    });
                }
            } else {
                let label = pat.label_name();
                if !primary_schema.labels.contains_key(label) {
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
    /// `fork_{tag}_{dataset}`. Lance tags are GC-exempt — the tagged
    /// versions survive compaction's retention sweep — so a tagged
    /// fork's state is preserved on disk even after the fork itself
    /// is dropped (cascade or otherwise). Useful for audit hold,
    /// regulatory snapshots, or named pre-publish checkpoints.
    ///
    /// The tag pins the branch's *current* version: subsequent fork
    /// writes do not "follow" the tag.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ForkLifecycle`] (stage = `tag`) on Lance failures
    ///   (tag-name conflict, IO).
    pub async fn tag_fork(&self, fork_name: &str, tag: &str) -> Result<()> {
        let info = self.inner.fork_registry.get(fork_name).await?;
        let branching =
            self.inner
                .storage
                .backend()
                .branching()
                .ok_or_else(|| UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "tag",
                    source: anyhow::anyhow!("storage backend does not support fork branching")
                        .into(),
                })?;

        // L9: a fork tag spans one backend tag per dataset, and `create_tag`
        // is not atomic across them. Pre-validate that none of the target
        // tags already exist (fail fast with no partial state on the common
        // "already tagged" case), then create while tracking what THIS call
        // created so a mid-loop failure rolls back only those — never a
        // pre-existing tag on another dataset.
        for dataset in info.datasets.keys() {
            let lance_tag = format!("fork_{tag}_{dataset}");
            let existing =
                branching
                    .list_tags(dataset)
                    .await
                    .map_err(|e| UniError::ForkLifecycle {
                        name: fork_name.to_string(),
                        stage: "tag",
                        source: e.into(),
                    })?;
            if existing.iter().any(|(n, _)| n == &lance_tag) {
                return Err(UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "tag",
                    source: format!("tag '{tag}' already present on dataset '{dataset}'").into(),
                });
            }
        }

        let mut created: Vec<(String, String)> = Vec::new();
        for (dataset, branch) in &info.datasets {
            let lance_tag = format!("fork_{tag}_{dataset}");
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
    /// Idempotent per dataset — missing tags are treated as success so
    /// partial cleanup retries are safe.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ForkLifecycle`] (stage = `untag`) on Lance failures.
    pub async fn untag_fork(&self, fork_name: &str, tag: &str) -> Result<()> {
        let info = self.inner.fork_registry.get(fork_name).await?;
        let branching =
            self.inner
                .storage
                .backend()
                .branching()
                .ok_or_else(|| UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "untag",
                    source: anyhow::anyhow!("storage backend does not support fork branching")
                        .into(),
                })?;
        for dataset in info.datasets.keys() {
            let lance_tag = format!("fork_{tag}_{dataset}");
            branching
                .delete_tag(dataset, &lance_tag)
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
    /// A fork's tag is stored as one Lance tag per dataset under the
    /// namespace `fork_{tag}_{dataset}`. This method enumerates the
    /// distinct `tag` values present on at least one of the fork's
    /// branched datasets.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if the fork is unknown.
    /// - [`UniError::ForkLifecycle`] (stage = `list_tags`) on Lance failures.
    pub async fn list_fork_tags(&self, fork_name: &str) -> Result<Vec<String>> {
        let info = self.inner.fork_registry.get(fork_name).await?;
        let branching =
            self.inner
                .storage
                .backend()
                .branching()
                .ok_or_else(|| UniError::ForkLifecycle {
                    name: fork_name.to_string(),
                    stage: "list_tags",
                    source: anyhow::anyhow!("storage backend does not support fork branching")
                        .into(),
                })?;
        let mut tags: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for dataset in info.datasets.keys() {
            let suffix = format!("_{dataset}");
            let prefix = "fork_";
            let on_disk =
                branching
                    .list_tags(dataset)
                    .await
                    .map_err(|e| UniError::ForkLifecycle {
                        name: fork_name.to_string(),
                        stage: "list_tags",
                        source: e.into(),
                    })?;
            for (name, _) in on_disk {
                if let Some(rest) = name.strip_prefix(prefix)
                    && let Some(tag) = rest.strip_suffix(&suffix)
                {
                    tags.insert(tag.to_string());
                }
            }
        }
        Ok(tags.into_iter().collect())
    }
}
