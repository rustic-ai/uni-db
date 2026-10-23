// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The database's [`BulkEventSink`]: announces a visible bulk load on the
//! commit broadcaster, fires its create events at after-phase triggers, and
//! streams them to CDC. See `uni_bulk::events` for the load side.
//!
//! Also delivers, at open, the events of the loads recovery completed
//! ([`deliver_recovered_loads`]).

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use uni_bulk::{BulkEventSink, BulkWriteKind, EventDelivery, VisibleLoad};
use uni_plugin::traits::trigger::{TriggerContext, TriggerEventMask};

use crate::api::UniInner;
use crate::api::notifications::CommitNotification;
use crate::api::triggers::{TriggerRouter, tx_id_to_u64};

/// The session id a bulk load's announcement and trigger fires carry: a load
/// does not run in a session of its own.
const BULK_SESSION_ID: &str = "bulk-load";

/// [`BulkEventSink`] over a database inner. Holds it weakly: a bulk writer, or
/// the background re-drive of one, must not keep the database alive.
pub(crate) struct DbBulkEventSink {
    inner: Weak<UniInner>,
}

impl DbBulkEventSink {
    pub(crate) fn new(inner: &Arc<UniInner>) -> Self {
        Self {
            inner: Arc::downgrade(inner),
        }
    }
}

fn router(inner: &UniInner) -> Result<TriggerRouter, uni_common::UniError> {
    Ok(TriggerRouter::from_registry_with_queue(
        &inner.plugin_registry,
        Some(Arc::clone(&inner.defer_queue)),
        Arc::clone(&inner.ec_queue),
    )?
    .with_task_tracker(crate::shutdown::ShutdownHandle::clone(
        &inner.shutdown_handle,
    )))
}

impl BulkEventSink for DbBulkEventSink {
    fn blocking_trigger(&self, kind: BulkWriteKind, label_or_type: &str) -> Option<String> {
        let inner = self.inner.upgrade()?;
        let mask = match kind {
            BulkWriteKind::Vertex => TriggerEventMask::NODE_CREATE,
            BulkWriteKind::Edge => TriggerEventMask::EDGE_CREATE,
        };
        match router(&inner) {
            Ok(router) => router
                .synchronous_before_trigger_for(mask, label_or_type)
                .map(str::to_owned),
            // A trigger whose predicate does not compile fails every commit;
            // name it so the load is refused rather than loaded past it.
            Err(e) => Some(format!("<trigger registry: {e}>")),
        }
    }

    fn publish(
        &self,
        load: VisibleLoad,
    ) -> Pin<Box<dyn std::future::Future<Output = EventDelivery> + Send + '_>> {
        Box::pin(async move {
            let Some(inner) = self.inner.upgrade() else {
                // The database is gone; its next open delivers the events.
                return EventDelivery::Interrupted;
            };
            // The delivery holds no strong reference to the database: it
            // stops at the database's shutdown instead.
            let announced = announce(&inner, load).await;
            drop(inner);
            match announced {
                Some(announced) => announced.deliver().await,
                None => EventDelivery::Finished,
            }
        })
    }
}

/// Deliver the events of the loads recovery completed at open
/// (`uni_bulk::recover_interrupted_bulk_load`), whose delivery the crashed
/// process did not finish.
///
/// Announces every load now, in the order given, so the announcements
/// precede any commit made after the open returns; then streams the loads'
/// rows, one load after another, on a tracked background task, finishing
/// each load's marker once its events are delivered. A load nothing consumes
/// (no matching after-phase trigger, no CDC output) is finished here. A
/// shutdown part way leaves the undelivered loads' markers for the next open,
/// which delivers them again.
///
/// Only consumers registered before this runs see the events: the triggers
/// the open reactivates (declared triggers) and CDC outputs already
/// registered. Nothing on a read-only database, which has no writer to
/// announce with; its markers wait for a writable open.
pub(crate) async fn deliver_recovered_loads(
    inner: &Arc<UniInner>,
    loads: Vec<uni_bulk::RecoveredLoad>,
) {
    if loads.is_empty() || inner.writer.is_none() {
        return;
    }
    let mut streaming = Vec::new();
    for load in loads {
        let load_id = load.load_id.clone();
        let Some(announced) = announce(inner, load.into_visible(Arc::clone(&inner.storage))).await
        else {
            return;
        };
        if announced.has_consumers() {
            streaming.push((load_id, announced));
        } else {
            finish_recovered(&inner.storage, &load_id).await;
        }
    }
    if streaming.is_empty() {
        return;
    }
    let storage = Arc::clone(&inner.storage);
    let handle = tokio::spawn(async move {
        for (load_id, announced) in streaming {
            match announced.deliver().await {
                EventDelivery::Finished => finish_recovered(&storage, &load_id).await,
                // The loads after this one were announced too; dropping them
                // drops their CDC streams unfinished, and the next open
                // delivers them again.
                EventDelivery::Interrupted => return,
            }
        }
    });
    super::track_task_with_scratch_claim(&inner.shutdown_handle, inner._temp_dir.as_ref(), handle);
}

/// Finish a recovered load's marker once its events are delivered; on
/// failure the marker stays and the next open delivers them again.
async fn finish_recovered(storage: &uni_store::storage::manager::StorageManager, load_id: &str) {
    if let Err(e) = uni_bulk::finish_recovered_load_events(storage, load_id).await {
        tracing::warn!(
            load_id,
            error = %e,
            "bulk recovery: a recovered load's events were delivered but its marker could not \
             be finished; the next open delivers them again",
        );
    }
}

/// A visible load announced on the commit broadcaster, its events not yet
/// delivered; see [`announce`] and [`AnnouncedLoad::deliver`].
pub(crate) struct AnnouncedLoad {
    load: VisibleLoad,
    /// The triggers the load's events are dispatched to; `None` when none
    /// selects them.
    triggers: Option<(
        TriggerRouter,
        Arc<dyn uni_plugin::traits::procedure::ProcedureHost>,
    )>,
    props_referenced: HashSet<String>,
    cdc_stream: Option<uni_plugin_host::cdc_runtime::MutationStreamSender>,
    shutdown: Arc<crate::shutdown::ShutdownHandle>,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
}

impl AnnouncedLoad {
    /// Whether a trigger or a CDC stream receives the load's events.
    fn has_consumers(&self) -> bool {
        self.triggers.is_some() || self.cdc_stream.is_some()
    }
}

/// Announce `load` in commit order: take its place in the order, then open
/// its CDC stream and publish its notification. `None` on a database without
/// a writer.
///
/// The place is taken before the stream is opened: taking it awaits the
/// writer's commit lock, and a stream opened before that await and left
/// behind by a cancellation would be registered with CDC under no
/// announcement. From the place on nothing awaits until the notification is
/// published.
async fn announce(inner: &Arc<UniInner>, load: VisibleLoad) -> Option<AnnouncedLoad> {
    let writer = inner.writer.as_ref()?;
    // Subscribed before the shutdown check `deliver` starts with, so a
    // shutdown is seen whichever side of this it lands on.
    let shutdown = Arc::clone(&inner.shutdown_handle);
    let shutdown_rx = shutdown.subscribe();
    let router = match router(inner) {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::error!(
                load_id = %load.load_id,
                error = %e,
                "bulk load events: the trigger registry could not be read; no trigger fires for \
                 this load",
            );
            None
        }
    };
    // Only the triggers that would select a row of the load: the load's rows
    // are read back from storage to build its events, which is not worth
    // doing for a trigger that would discard every one of them.
    let router = router.filter(|r| {
        load.labels
            .iter()
            .any(|l| r.wants_after_event(TriggerEventMask::NODE_CREATE, l))
            || load
                .edge_types
                .iter()
                .any(|t| r.wants_after_event(TriggerEventMask::EDGE_CREATE, t))
    });

    // Test seam: the load is visible and its announcement not yet ordered.
    // No-op unless built with `--features failpoints`.
    fail::fail_point!("bulk::events::before-announce");
    // The load's place in the commit order, and the version it is announced
    // at, are taken together under the writer's commit lock.
    let (ticket, version) = writer.sequence_announcement().await;
    let cdc_stream = if inner.plugin_registry.cdc_outputs_is_empty() {
        None
    } else {
        inner
            .cdc_runtime
            .as_ref()
            .and_then(|cdc| cdc.open_mutation_stream())
    };
    let notif = CommitNotification {
        version,
        mutation_count: load.rows,
        labels_affected: load.labels.clone(),
        edge_types_affected: load.edge_types.clone(),
        rules_promoted: 0,
        timestamp: chrono::Utc::now(),
        tx_id: load.load_id.clone(),
        session_id: BULK_SESSION_ID.to_owned(),
        causal_version: load.write_version,
        mutations: None,
        mutations_failed: false,
        mutation_stream: cdc_stream.as_ref().map(|s| s.id()),
        dropped_before: 0,
    };
    let commit_tx = inner.commit_tx.clone();
    let notif = Arc::new(notif);
    ticket.publish(move || {
        let _ = commit_tx.send(notif);
    });

    let props_referenced = router
        .as_ref()
        .map(TriggerRouter::properties_referenced)
        .unwrap_or_default();
    let triggers = router.map(|r| (r, trigger_host(inner, writer)));
    Some(AnnouncedLoad {
        load,
        triggers,
        props_referenced,
        cdc_stream,
        shutdown,
        shutdown_rx,
    })
}

impl AnnouncedLoad {
    /// Stream the load's events to after-phase triggers and CDC, one bounded
    /// chunk at a time.
    ///
    /// Stops at the database's shutdown, between chunks or while CDC holds a
    /// chunk back, and returns [`EventDelivery::Interrupted`]: a large load
    /// must not hold the shutdown up until the bound runs out. CDC then sees
    /// the load's stream end unfinished and halts its streams, its checkpoint
    /// left before the load -- it is shutting down too, and its next start
    /// resumes from that checkpoint. The load's marker is kept, so the next
    /// open delivers the whole load again; consumers may see its first chunks
    /// twice.
    async fn deliver(self) -> EventDelivery {
        let Self {
            mut load,
            triggers,
            props_referenced,
            mut cdc_stream,
            shutdown,
            mut shutdown_rx,
        } = self;
        if triggers.is_none() && cdc_stream.is_none() {
            return EventDelivery::Finished;
        }
        let load_id = load.load_id.clone();
        let interrupted = || {
            tracing::info!(
                load_id = %load_id,
                "bulk load events: delivery stopped by shutdown; the next open delivers the \
                 load's events again",
            );
            EventDelivery::Interrupted
        };
        let tx_id = tx_id_to_u64(&load.load_id);
        let runtime = tokio::runtime::Handle::current();
        loop {
            if shutdown.is_signalled() {
                return interrupted();
            }
            let next = tokio::select! {
                biased;
                _ = shutdown_rx.recv() => return interrupted(),
                next = load.reader.next_chunk(&props_referenced) => next,
            };
            let events = match next {
                Ok(Some(events)) => events,
                Ok(None) => break,
                Err(e) => {
                    // Dropping the CDC stream unfinished halts CDC rather than
                    // checkpointing past the rows it never got.
                    tracing::error!(
                        load_id = %load.load_id,
                        error = %e,
                        "bulk load events: reading the load's rows back failed; triggers missed \
                         the rest of the load and CDC streams halt",
                    );
                    return EventDelivery::Finished;
                }
            };
            // Test seam: a chunk of the load is read back and not yet handed
            // to anyone. No-op unless built with `--features failpoints`.
            fail::fail_point!("bulk::events::before-chunk");
            if let Some((router, host)) = triggers.as_ref() {
                let ctx = TriggerContext::new(BULK_SESSION_ID, tx_id).with_host(Arc::clone(host));
                router.dispatch_after(ctx, &events, &runtime);
            }
            if let Some(stream) = cdc_stream.as_ref() {
                match events.materialize_all() {
                    Ok(Some(batch)) => {
                        let sent = tokio::select! {
                            biased;
                            _ = shutdown_rx.recv() => return interrupted(),
                            sent = stream.send(batch) => sent,
                        };
                        if !sent {
                            // CDC stopped reading (shutdown, or it fell behind
                            // and halted); nothing more to send it.
                            cdc_stream = None;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!(
                            load_id = %load.load_id,
                            error = %e,
                            "bulk load events: a chunk's CDC batch could not be built; CDC \
                             streams halt rather than checkpoint past it",
                        );
                        cdc_stream = None;
                    }
                }
            }
            if triggers.is_none() && cdc_stream.is_none() {
                return EventDelivery::Finished;
            }
        }
        if let Some(stream) = cdc_stream {
            stream.finish().await;
        }
        EventDelivery::Finished
    }
}

/// The write-enabled host a declared trigger's after-commit action runs
/// against, as a transaction commit builds it (WS-A): the load is in storage,
/// and the action's own writes land in main L0.
fn trigger_host(
    inner: &UniInner,
    writer: &Arc<uni_store::Writer>,
) -> Arc<dyn uni_plugin::traits::procedure::ProcedureHost> {
    let l0_ctx = uni_query::query::df_graph::L0Context {
        current_l0: Some(writer.l0_manager.get_current()),
        transaction_l0: None,
        pending_flush_l0s: writer.l0_manager.get_pending_flush(),
    };
    Arc::new(
        uni_query::query::executor::procedure_host::QueryProcedureHost::from_commit_parts(
            Arc::clone(&inner.storage),
            Arc::clone(&inner.properties),
            Some(Arc::clone(&inner.procedure_registry)),
            inner.xervo_runtime.clone(),
            l0_ctx,
            0,
        )
        .with_writer(Arc::clone(writer)),
    )
}
