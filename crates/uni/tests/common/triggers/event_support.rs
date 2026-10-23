// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared fixtures for the commit-event tests: a CDC provider that records
//! every batch it is handed, and a trigger that records every row it fires on.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use arrow_array::{Array, Int64Array, RecordBatch, UInt8Array};
use parking_lot::Mutex;
use smol_str::SmolStr;
use uni_db::Uni;
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

/// The batches a [`RecordingCdc`] stream was delivered, in delivery order.
pub type Delivered = Arc<Mutex<Vec<CdcBatch>>>;

struct RecordingStream {
    delivered: Delivered,
    last: CdcLsn,
    checkpoints: Arc<Mutex<Vec<CdcLsn>>>,
}

impl CdcStream for RecordingStream {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
        self.last = batch.lsn_end;
        self.delivered.lock().push(batch.clone());
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
        self.checkpoints.lock().push(self.last);
        Ok(self.last)
    }

    fn shutdown(&mut self) -> Result<(), FnError> {
        Ok(())
    }
}

/// A CDC provider recording each delivered batch and each checkpoint it
/// reported.
#[derive(Debug, Default)]
pub struct RecordingCdc {
    /// Batches in delivery order.
    pub delivered: Delivered,
    /// The LSN `checkpoint()` returned after each delivery, in order.
    pub checkpoints: Arc<Mutex<Vec<CdcLsn>>>,
}

impl CdcOutputProvider for RecordingCdc {
    fn name(&self) -> &str {
        "event-recorder"
    }

    fn start(&self, _ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
        Ok(Box::new(RecordingStream {
            delivered: Arc::clone(&self.delivered),
            last: CdcLsn(0),
            checkpoints: Arc::clone(&self.checkpoints),
        }))
    }
}

/// Register a [`RecordingCdc`] on `db` and commit one transaction so the CDC
/// runtime discovers it (providers registered after build are picked up on the
/// next commit). Returns the provider once its stream has seen that commit,
/// with the recorded batches cleared.
pub async fn attach_cdc(db: &Uni, warmup_cypher: &str) -> Result<Arc<RecordingCdc>> {
    let provider = Arc::new(RecordingCdc::default());
    let caps = CapabilitySet::from_iter_of([Capability::Cdc]);
    let mut r = PluginRegistrar::new(PluginId::new("event-recorder"), &caps, db.plugin_registry());
    r.cdc_output(Arc::clone(&provider) as Arc<dyn CdcOutputProvider>)?;
    r.commit_to_registry()?;
    let tx = db.session().tx().await?;
    tx.execute(warmup_cypher).await?;
    tx.commit().await?;
    wait_for_batches(&provider.delivered, 1).await;
    // The checkpoint follows the delivery; wait for it too, so it is not
    // recorded after the clear below.
    let deadline = Instant::now() + Duration::from_secs(10);
    while provider.checkpoints.lock().is_empty() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    provider.delivered.lock().clear();
    provider.checkpoints.lock().clear();
    Ok(provider)
}

/// Poll until `delivered` holds at least `n` batches, or 10 s pass.
pub async fn wait_for_batches(delivered: &Delivered, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while delivered.lock().len() < n && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The `event_kind` discriminants of every row in `batch`.
pub fn event_kinds(batch: &RecordBatch) -> Vec<u8> {
    let Some(col) = batch.column_by_name("event_kind") else {
        return Vec::new();
    };
    let kinds = col
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("event_kind is UInt8");
    (0..kinds.len()).map(|i| kinds.value(i)).collect()
}

/// The `vid_or_eid` of every row in `batch`.
pub fn event_ids(batch: &RecordBatch) -> Vec<i64> {
    let Some(col) = batch.column_by_name("vid_or_eid") else {
        return Vec::new();
    };
    let ids = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("vid_or_eid is Int64");
    (0..ids.len()).map(|i| ids.value(i)).collect()
}

/// A synchronous trigger recording the `event_kind` of every row it fires on.
pub struct RecordingTrigger {
    sub: TriggerSubscription,
    /// One entry per row fired on.
    pub kinds: Arc<Mutex<Vec<u8>>>,
    /// The ids (`vid_or_eid`) of the rows fired on.
    pub ids: Arc<Mutex<Vec<i64>>>,
    /// Number of `fire` calls.
    pub fires: AtomicUsize,
}

impl RecordingTrigger {
    /// A trigger in `phase` on `events`, optionally restricted to `labels`.
    pub fn new(
        phase: TriggerPhase,
        events: TriggerEventMask,
        labels: Option<Vec<SmolStr>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            sub: TriggerSubscription {
                phase,
                events,
                labels,
                edge_types: None,
                properties: None,
                predicate_source: None,
                fire_mode: FireMode::Synchronous,
                docs: "event recorder".to_owned(),
            },
            kinds: Arc::new(Mutex::new(Vec::new())),
            ids: Arc::new(Mutex::new(Vec::new())),
            fires: AtomicUsize::new(0),
        })
    }

    /// Rows fired on so far.
    pub fn rows(&self) -> usize {
        self.kinds.lock().len()
    }
}

impl TriggerPlugin for RecordingTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        self.fires.fetch_add(1, Ordering::SeqCst);
        self.kinds.lock().extend(event_kinds(&events.events));
        self.ids.lock().extend(event_ids(&events.events));
        Ok(TriggerOutcome::Continue)
    }
}

/// Register `trigger` on `db` under plugin id `id`.
pub fn register_trigger(db: &Uni, id: &str, trigger: Arc<dyn TriggerPlugin>) -> Result<()> {
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new(id), &caps, db.plugin_registry());
    r.trigger(trigger)?;
    r.commit_to_registry()?;
    Ok(())
}
