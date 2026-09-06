// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-plugin-host/src/cdc_runtime.rs:334
//
// A failed `CdcStream::deliver` is logged + `continue`d, but the runtime
// keeps delivering subsequent commits and checkpointing them, creating a
// permanent, undetectable gap in the CDC feed. Once a later commit's
// checkpoint advances the persisted LSN past the dropped commit, the
// dropped commit is never redelivered (no replay path exists).

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::broadcast;

use uni_plugin::errors::FnError;
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry};

use uni_plugin_host::cdc_runtime::CdcRuntime;
use uni_plugin_host::notifications::CommitNotification;
use uni_plugin_host::shutdown::ShutdownHandle;

/// Shared recorder observed by the test after the driver task runs.
#[derive(Default)]
struct Recorder {
    /// deliver() call count across the stream's life.
    deliver_calls: u64,
    /// lsn_end of every batch that was *successfully* delivered.
    delivered_lsns: Vec<u64>,
    /// The most-recently successfully-delivered lsn_end (what
    /// checkpoint() reports as durable progress).
    last_delivered: u64,
}

/// Test CDC provider: its stream fails `deliver` on the FIRST commit,
/// succeeds thereafter, and `checkpoint()` returns the last
/// successfully-delivered lsn_end. This is the natural contract of a
/// sink that transiently rejects one write then recovers.
struct FlakyProvider {
    rec: Arc<Mutex<Recorder>>,
}

impl CdcOutputProvider for FlakyProvider {
    fn name(&self) -> &str {
        "flaky"
    }

    fn start(&self, _ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
        Ok(Box::new(FlakyStream {
            rec: Arc::clone(&self.rec),
        }))
    }
}

struct FlakyStream {
    rec: Arc<Mutex<Recorder>>,
}

impl CdcStream for FlakyStream {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
        let mut rec = self.rec.lock();
        rec.deliver_calls += 1;
        if rec.deliver_calls == 1 {
            // Transient failure on the very first commit.
            return Err(FnError::new(0xBAD, "transient sink failure"));
        }
        rec.delivered_lsns.push(batch.lsn_end.0);
        rec.last_delivered = batch.lsn_end.0;
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
        Ok(CdcLsn(self.rec.lock().last_delivered))
    }

    fn shutdown(&mut self) -> Result<(), FnError> {
        Ok(())
    }
}

fn make_registry(rec: Arc<Mutex<Recorder>>) -> Arc<PluginRegistry> {
    let registry = Arc::new(PluginRegistry::new());
    let caps = CapabilitySet::from_iter_of([Capability::Cdc]);
    let mut r = PluginRegistrar::new(PluginId::new("test"), &caps, &registry);
    r.cdc_output(Arc::new(FlakyProvider {
        rec: Arc::clone(&rec),
    }))
    .expect("register cdc_output");
    r.commit_to_registry().expect("commit");
    registry
}

fn commit(version: u64) -> Arc<CommitNotification> {
    Arc::new(CommitNotification {
        version,
        mutation_count: 1,
        labels_affected: vec!["Widget".to_owned()],
        edge_types_affected: vec![],
        rules_promoted: 0,
        timestamp: chrono::Utc::now(),
        tx_id: format!("tx-{version}"),
        session_id: "s1".to_owned(),
        causal_version: version - 1,
        // None → runtime falls back to an empty event batch; the
        // deliver/checkpoint/gap machinery under test is unaffected.
        mutations: None,
        mutations_failed: false,
        dropped_before: 0,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_deliver_failure_creates_permanent_gap() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_path = tmp.path().to_path_buf();

    let rec = Arc::new(Mutex::new(Recorder::default()));
    let registry = make_registry(Arc::clone(&rec));

    let (tx, rx) = broadcast::channel::<Arc<CommitNotification>>(16);
    let shutdown = ShutdownHandle::new(Duration::from_secs(5));

    let runtime = CdcRuntime::spawn(&registry, rx, Some(data_path.clone()), &shutdown);

    // Commit N (=100) then N+1 (=101).
    tx.send(commit(100)).unwrap();
    tx.send(commit(101)).unwrap();

    // Let the driver process both commits. Poll until the stream is halted
    // (bounded) rather than sleeping a fixed amount.
    let sidecar = runtime.checkpoint_sidecar().expect("sidecar enabled");
    for _ in 0..100 {
        if runtime.halted_stream_count() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Scope the guard in a block so it is released before the later `await`
    // (clippy::await_holding_lock does not credit an explicit `drop`).
    {
        let rec = rec.lock();

        // FIXED: commit 100's deliver failed, so the stream is HALTED — commit 101 is
        // then skipped (never delivered, never checkpointed). The gap is now DETECTABLE
        // (a halted stream) instead of silently papered over by 101's checkpoint.
        assert_eq!(
            rec.deliver_calls, 1,
            "only commit 100 is attempted; 101 is skipped once the stream halts"
        );
        assert!(
            rec.delivered_lsns.is_empty(),
            "nothing was successfully delivered, so no lsn should be recorded: {:?}",
            rec.delivered_lsns
        );
    }

    assert_eq!(
        runtime.halted_stream_count(),
        1,
        "the flaky stream must be halted after its deliver failure"
    );
    // The durable checkpoint never advances past the undelivered commit — no
    // at-least-once violation.
    // `lookup` returns `Result` since #233: a read failure is no longer
    // reported as "no checkpoint", so unwrap first and keep asserting the
    // original intent — no checkpoint was recorded for the halted stream.
    assert!(
        sidecar
            .lookup("flaky")
            .expect("the sidecar itself is readable")
            .is_none(),
        "checkpoint must not advance past the undelivered commit; got {:?}",
        sidecar.lookup("flaky")
    );

    let _ = shutdown.shutdown_async().await;
}
