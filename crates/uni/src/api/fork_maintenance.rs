// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni-db host implementation for the `uni_fork::maintenance` background tasks.
//!
//! The reusable scheduling/shutdown skeleton lives in
//! `uni_fork::maintenance`; this module supplies the per-tick bodies that
//! touch [`UniInner`] (the TTL sweep and the fork-local index build). The host
//! holds a `Weak<UniInner>` so the tasks do not extend the database's
//! lifetime — each tick attempts to upgrade and no-ops if the database has
//! been dropped.

use std::sync::Weak;

use tracing::{debug, warn};
use uni_common::core::schema::IndexDefinition;
use uni_store::fork::ForkLocalIndexKind;

use super::{Uni, UniInner};

/// `Weak<UniInner>`-backed host for the fork-maintenance tasks.
pub(crate) struct ForkMaintenanceHostImpl {
    weak: Weak<UniInner>,
}

impl ForkMaintenanceHostImpl {
    pub(crate) fn new(weak: Weak<UniInner>) -> Self {
        Self { weak }
    }
}

#[async_trait::async_trait]
impl uni_fork::ForkMaintenanceHost for ForkMaintenanceHostImpl {
    async fn sweep_expired_forks(&self) {
        if let Err(e) = sweep_tick(&self.weak).await {
            warn!("fork sweeper tick failed: {e}");
        }
    }

    async fn build_fork_local_indexes(&self, threshold: u64) {
        if let Err(e) = index_builder_tick(&self.weak, threshold).await {
            warn!("fork index builder tick failed: {e}");
        }
    }
}

/// One TTL-sweeper tick: drop every fork whose `ttl_expires_at` is past now.
async fn sweep_tick(weak: &Weak<UniInner>) -> anyhow::Result<()> {
    let Some(inner) = weak.upgrade() else {
        // Database dropped; the next tick observes the same and shuts down
        // via the broadcast channel instead of looping.
        return Ok(());
    };
    let now = chrono::Utc::now();
    let expired = inner.fork_registry.list_expired(now).await;
    if expired.is_empty() {
        return Ok(());
    }
    // Re-wrap into a Uni handle ONLY to reach the `drop_fork_cascade` API.
    //
    // `impl Drop for Uni` broadcasts `shutdown_blocking()` on the shared
    // `UniInner`, which would permanently stop every background task of the
    // still-running database (auto-flush, compaction, CDC, the scheduler, and
    // this sweeper itself) — silently, since queries keep working. A transient
    // wrapper over an upgraded `Weak<UniInner>` must NOT own that shutdown
    // responsibility, so wrap it in `ManuallyDrop` and reclaim the `Arc`
    // afterwards instead of letting `Uni::drop` run.
    let db = std::mem::ManuallyDrop::new(Uni { inner });
    for fork in expired {
        match db.drop_fork_cascade(&fork.name).await {
            Ok(()) => {
                debug!(fork = %fork.name, "swept expired fork");
            }
            Err(e) => {
                warn!(
                    fork = %fork.name,
                    "sweeper failed to drop expired fork (will retry next tick): {e}"
                );
            }
        }
    }
    // SAFETY: move the single `Arc<UniInner>` field out of the `ManuallyDrop`
    // without running `Uni::drop`. `db` is never touched again and
    // `ManuallyDrop` never drops its contents, so the `Arc`'s strong count is
    // decremented exactly once — here, as an ordinary `Arc` — with no leak, no
    // double-free, and no shutdown broadcast on the live database.
    let _inner: std::sync::Arc<UniInner> = unsafe { std::ptr::read(&db.inner) };
    Ok(())
}

/// One index-builder tick: for every active fork whose per-table fragment
/// count crosses `threshold`, build a fork-local `ScalarBtree` for any column
/// primary has a single-column scalar index on.
async fn index_builder_tick(weak: &Weak<UniInner>, threshold: u64) -> anyhow::Result<()> {
    let Some(inner) = weak.upgrade() else {
        return Ok(());
    };
    let active = inner.fork_registry.list_active().await;
    if active.is_empty() {
        return Ok(());
    }

    // Snapshot which (label, column, kind) targets primary has indexes on.
    // The schema is shared across forks, so we read it once per tick. M7
    // extends this beyond single-column scalar indexes to Vector and
    // FullText: fork rows written after branch-creation are otherwise
    // unindexed, and for FTS there is generally no brute-force fallback,
    // so fork-local matches would be silently omitted.
    let index_targets: Vec<(String, String, ForkLocalIndexKind)> = inner
        .schema
        .schema()
        .indexes
        .iter()
        .flat_map(|idx| match idx {
            IndexDefinition::Scalar(cfg) if cfg.properties.len() == 1 => {
                vec![(
                    cfg.label.clone(),
                    cfg.properties[0].clone(),
                    ForkLocalIndexKind::ScalarBtree,
                )]
            }
            IndexDefinition::Vector(cfg) => {
                vec![(
                    cfg.label.clone(),
                    cfg.property.clone(),
                    ForkLocalIndexKind::Vector,
                )]
            }
            IndexDefinition::FullText(cfg) => cfg
                .properties
                .iter()
                .map(|p| (cfg.label.clone(), p.clone(), ForkLocalIndexKind::FullText))
                .collect(),
            // Sparse registers a fork-local marker (issue #95 Task #4) so fork
            // `uni.sparse.query` reports the `SparseDot` fused plan; retrieval is
            // a brute-force branch scan, so the marker is observability, not a
            // built index file.
            IndexDefinition::Sparse(cfg) => {
                vec![(
                    cfg.label.clone(),
                    cfg.property.clone(),
                    ForkLocalIndexKind::Sparse,
                )]
            }
            _ => Vec::new(),
        })
        .collect();
    if index_targets.is_empty() {
        return Ok(());
    }

    for fork_info in active {
        // Only fire builds against forks that have a live UniInner — the build
        // path runs through the fork's storage manager.
        let Some(fork_inner_weak) = inner
            .fork_inners
            .get(&fork_info.id)
            .map(|e| e.value().clone())
        else {
            continue;
        };
        let Some(fork_inner) = fork_inner_weak.upgrade() else {
            continue;
        };
        let Some(scope) = fork_inner.storage.fork_scope().cloned() else {
            warn!(
                fork = %fork_info.name,
                "fork inner cache hit but storage has no fork scope; skipping",
            );
            continue;
        };
        // Optional flush so the build sees the latest committed rows. Errors
        // here are non-fatal — the build uses whatever's already on the branch
        // and the next tick can retry.
        if let Some(writer_lock) = &fork_inner.writer {
            let writer: &uni_store::Writer = writer_lock.as_ref();
            if let Err(e) = writer.flush_to_l1(None).await {
                warn!(
                    fork = %fork_info.name,
                    "pre-build flush failed; continuing with current branch tip: {e}"
                );
            }
        }
        let Some(branching) = fork_inner.storage.backend().branching() else {
            debug!(
                fork = %fork_info.name,
                "storage backend does not support fork branching; skipping index build",
            );
            continue;
        };
        for (label, column, kind) in &index_targets {
            let dataset_name = format!("vertices_{label}");
            let count = scope.fragment_count(&dataset_name);
            if count < threshold {
                continue;
            }
            // Skip only when a fork-local index of the SAME kind already
            // exists: a column can carry e.g. both a scalar and a
            // full-text index, so `fork_local_indexes` holds a set of
            // kinds per `(label, column)` and we probe for this one.
            if scope.has_fork_local_index(label, column, *kind) {
                continue;
            }
            // A branch index build commits a branch version without passing
            // the fork backend's fence check: a handle that lost the store
            // (or a read-only one) builds nothing.
            if let Err(e) = fork_inner.storage.write_fence().check() {
                debug!(fork = %fork_info.name, "skipping fork-local index builds: {e}");
                break;
            }
            match uni_store::fork::index_builder::build_fork_local_index(
                &scope,
                branching.as_ref(),
                label,
                column,
                *kind,
            )
            .await
            {
                Ok(()) => {
                    debug!(
                        fork = %fork_info.name,
                        label = %label,
                        column = %column,
                        kind = ?kind,
                        "auto-built fork-local index"
                    );
                }
                Err(e) => {
                    warn!(
                        fork = %fork_info.name,
                        label = %label,
                        column = %column,
                        kind = ?kind,
                        "fork-local index build failed (will retry next tick): {e}"
                    );
                }
            }
        }
    }
    Ok(())
}
