// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Announces, at open, the commits a crashed process made durable in the WAL
//! but never announced.
//!
//! A commit is durable once its WAL segment is flushed, and is announced (a
//! `CommitNotification` on the broadcaster, its after-triggers dispatched)
//! only afterwards. A process that dies in between leaves a commit that the
//! next open replays into L0 and nobody hears of: CDC never sees it and no
//! trigger fires. Each WAL segment records the highest LSN announced when it
//! was written (`WalSegment::announced_through`), and every segment above that
//! mark that holds a transaction's commit (`WalSegment::commits`) is announced
//! again here.
//!
//! [`replay_wal`] replays the WAL one segment at a time and, before applying
//! each unannounced commit, rebuilds its mutation events against the state it
//! committed on top of -- the same classification and pre-images the commit
//! path computes ([`crate::api::commit_events::extract`]). A flush never moves
//! an unannounced commit into L1 (`Writer::flush_l0_rotate` waits for its
//! announcement), so that state is exactly L1 plus the segments before it. The
//! commit's place in the announcement order is taken as it is replayed;
//! [`deliver_recovered_commits`] publishes it once the broadcaster and the
//! reactivated triggers exist.
//!
//! Delivery is at-least-once: a commit announced after the last segment was
//! written, by a process that then crashed, is announced again. A database
//! closed normally records the mark (`Writer::seal_wal`), so a clean reopen
//! announces nothing twice.

use std::sync::Arc;

use parking_lot::RwLock;
use uni_common::UniError;
use uni_plugin::PluginRegistry;
use uni_plugin::traits::trigger::TriggerContext;
use uni_store::Writer;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::publish_sequencer::PublishTicket;
use uni_store::storage::manager::StorageManager;

use crate::api::UniInner;
use crate::api::notifications::CommitNotification;
use crate::api::triggers::{MutationEvents, TriggerRouter, tx_id_to_u64};

/// The session id a recovered commit's announcement and trigger fires carry:
/// the session that made the commit is gone.
const RECOVERY_SESSION_ID: &str = "wal-recovery";

/// A commit recovery replayed and has not announced yet; see
/// [`deliver_recovered_commits`].
pub(crate) struct RecoveredCommit {
    /// The WAL segment the commit is durable in.
    lsn: u64,
    /// The commit's place in the announcement order.
    ticket: PublishTicket,
    /// The database version before and after the commit was replayed.
    causal_version: u64,
    version: u64,
    mutation_count: usize,
    labels_affected: Vec<String>,
    edge_types_affected: Vec<String>,
    /// The commit's events; `None` when no trigger or CDC output was
    /// registered to receive them.
    events: Option<MutationEvents>,
    /// Whether a CDC output was registered when the events were built.
    cdc_active: bool,
}

impl std::fmt::Debug for RecoveredCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveredCommit")
            .field("lsn", &self.lsn)
            .field("version", &self.version)
            .field("mutation_count", &self.mutation_count)
            .finish_non_exhaustive()
    }
}

/// Replay the WAL above `wal_high_water_mark` into L0, as
/// `Writer::replay_wal` does, and return the number of mutations replayed
/// together with the commits to announce.
///
/// With `announce` false (a read-only open, which has nothing to announce
/// with) the WAL is replayed and nothing is returned to announce.
///
/// # Errors
/// The WAL cannot be read or a mutation cannot be applied.
pub(crate) async fn replay_wal(
    writer: &Arc<Writer>,
    storage: &Arc<StorageManager>,
    properties: &PropertyManager,
    registry: &PluginRegistry,
    wal_high_water_mark: u64,
    announce: bool,
) -> Result<(usize, Vec<RecoveredCommit>), UniError> {
    let Some(recovery) = writer
        .begin_wal_replay(wal_high_water_mark)
        .await
        .map_err(UniError::Internal)?
    else {
        return Ok((0, Vec::new()));
    };
    // The consumers the events are built for: those registered now, which are
    // the ones the open reactivates (declared triggers) before this runs.
    let router = TriggerRouter::from_registry(registry).or_else(|e| {
        tracing::error!(
            error = %e,
            "WAL recovery: the trigger registry could not be read; no trigger fires for the \
             recovered commits",
        );
        // A router over no triggers, so CDC still gets the commits' rows.
        TriggerRouter::from_registry(&PluginRegistry::new())
    })?;
    let cdc_active = !registry.cdc_outputs_is_empty();
    let need_events = !router.is_empty() || cdc_active;

    let mut count = 0;
    let mut recovered = Vec::new();
    for segment in recovery.segments {
        count += segment.mutations.len();
        if !announce || segment.lsn <= recovery.announced_through || segment.commits.is_empty() {
            writer
                .apply_replayed_mutations(segment.mutations)
                .map_err(UniError::Internal)?;
            continue;
        }
        // Apply the segment in order, building each commit's events against
        // the state just before it.
        let lsn = segment.lsn;
        let mut spans = segment.commits;
        spans.sort_by_key(|span| span.start);
        let mut applied = 0;
        let mut rest = segment.mutations.into_iter();
        for span in spans {
            let Some(skip) = span.start.checked_sub(applied) else {
                return Err(UniError::Internal(anyhow::anyhow!(
                    "WAL segment {lsn} records overlapping commit spans"
                )));
            };
            let before: Vec<_> = rest.by_ref().take(skip).collect();
            let commit: Vec<_> = rest.by_ref().take(span.len).collect();
            if commit.len() != span.len || before.len() != skip {
                return Err(UniError::Internal(anyhow::anyhow!(
                    "WAL segment {lsn} records a commit span past its mutations"
                )));
            }
            applied = span.start + span.len;
            writer
                .apply_replayed_mutations(before)
                .map_err(UniError::Internal)?;
            recovered.push(
                recover_commit(
                    writer,
                    storage,
                    properties,
                    &router,
                    cdc_active,
                    need_events,
                    lsn,
                    commit,
                )
                .await?,
            );
        }
        writer
            .apply_replayed_mutations(rest.collect())
            .map_err(UniError::Internal)?;
    }
    if count > 0 {
        tracing::info!(
            mutations = count,
            wal_high_water_mark,
            unannounced_commits = recovered.len(),
            "WAL recovery: replayed mutations above the snapshot's mark",
        );
        writer.finish_wal_replay();
    }
    Ok((count, recovered))
}

/// Build the events of one unannounced commit, take its place in the
/// announcement order, then apply it.
#[expect(
    clippy::too_many_arguments,
    reason = "one call site; the arguments are the replay's shared context"
)]
async fn recover_commit(
    writer: &Arc<Writer>,
    storage: &Arc<StorageManager>,
    properties: &PropertyManager,
    router: &TriggerRouter,
    cdc_active: bool,
    need_events: bool,
    lsn: u64,
    commit: Vec<uni_store::runtime::wal::Mutation>,
) -> Result<RecoveredCommit, UniError> {
    let mutation_count = commit.len();
    // The commit's writes on their own, as its transaction's private L0
    // held them.
    let mut scratch = L0Buffer::new(0, None);
    scratch
        .replay_mutations(commit.clone())
        .map_err(UniError::Internal)?;
    let (labels_affected, edge_types_affected) = crate::api::commit_events::affected(&scratch);
    let scratch = RwLock::new(scratch);
    let events = if need_events {
        Some(
            crate::api::commit_events::extract(
                storage,
                properties,
                Some(&writer.l0_manager),
                &scratch,
                router,
                cdc_active,
            )
            .await,
        )
    } else {
        None
    };
    let ticket = writer.sequence_recovered_commit(lsn).await;
    let causal_version = writer.current_version();
    writer
        .apply_replayed_mutations(commit)
        .map_err(UniError::Internal)?;
    Ok(RecoveredCommit {
        lsn,
        ticket,
        causal_version,
        version: writer.current_version(),
        mutation_count,
        labels_affected,
        edge_types_affected,
        events,
        cdc_active,
    })
}

/// Announce the commits [`replay_wal`] recovered, in WAL order, and dispatch
/// their after-phase triggers.
///
/// Call once the commit broadcaster, the CDC runtime and the reactivated
/// declared triggers exist. Before-phase triggers are not run: the commits
/// are durable, so there is nothing left for one to refuse.
pub(crate) fn deliver_recovered_commits(inner: &Arc<UniInner>, commits: Vec<RecoveredCommit>) {
    if commits.is_empty() {
        return;
    }
    let triggers = inner.writer.as_ref().and_then(|writer| {
        match crate::api::bulk_events::router(inner) {
            Ok(router) if !router.is_empty() => {
                Some((router, crate::api::bulk_events::trigger_host(inner, writer)))
            }
            Ok(_) => None,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "WAL recovery: the trigger registry could not be read; no trigger fires for \
                     the recovered commits",
                );
                None
            }
        }
    });
    let runtime = tokio::runtime::Handle::current();
    for commit in commits {
        let RecoveredCommit {
            lsn,
            ticket,
            causal_version,
            version,
            mutation_count,
            labels_affected,
            edge_types_affected,
            events,
            cdc_active,
        } = commit;
        let tx_id = format!("wal-recovery-{lsn}");
        let mut mutations_failed = false;
        let mutations = if cdc_active {
            match events.as_ref().map(MutationEvents::materialize_all) {
                Some(Ok(batch)) => batch.map(Arc::new),
                Some(Err(e)) => {
                    tracing::error!(
                        error = %e,
                        lsn,
                        "WAL recovery: a recovered commit's CDC batch could not be built; CDC \
                         streams halt rather than checkpoint past it",
                    );
                    mutations_failed = true;
                    None
                }
                None => None,
            }
        } else {
            None
        };
        tracing::info!(
            lsn,
            version,
            "WAL recovery: announcing a commit the crashed process never announced"
        );
        let notif = Arc::new(CommitNotification {
            version,
            mutation_count,
            labels_affected,
            edge_types_affected,
            rules_promoted: 0,
            timestamp: chrono::Utc::now(),
            tx_id: tx_id.clone(),
            session_id: RECOVERY_SESSION_ID.to_owned(),
            causal_version,
            mutations,
            mutations_failed,
            cdc_active,
            mutation_stream: None,
            dropped_before: 0,
        });
        let commit_tx = inner.commit_tx.clone();
        ticket.publish(move || {
            let _ = commit_tx.send(notif);
        });
        if let (Some((router, host)), Some(events)) = (triggers.as_ref(), events.as_ref()) {
            let ctx = TriggerContext::new(RECOVERY_SESSION_ID, tx_id_to_u64(&tx_id))
                .with_host(Arc::clone(host));
            router.dispatch_after(ctx, events, &runtime);
        }
    }
}

// Rust guideline compliant
