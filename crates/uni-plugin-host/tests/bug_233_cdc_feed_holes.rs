// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #233 Tier 1 — the two ways a CDC feed could lose commits silently.
//!
//! `CdcRuntime` already halts a stream when `deliver` fails, so the
//! checkpoint stays at the last contiguous commit and the gap is visible
//! (`bug_cdc_deliver_gap`). Two paths walked around that mechanism:
//!
//! 1. A commit whose mutation batch could not be materialized arrived as
//!    `mutations: None` — indistinguishable from "zero rows" — and was
//!    delivered as an EMPTY batch carrying the commit's real LSN range,
//!    after which the stream checkpointed past it.
//! 2. `CdcCheckpointSidecar::lookup` used `.ok()`, so an unreadable sidecar
//!    read as "never checkpointed" and a provider resumed from the wrong
//!    position.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::broadcast;

use uni_plugin::errors::FnError;
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry};
use uni_plugin_host::cdc_runtime::{CdcCheckpointSidecar, CdcRuntime, PersistedCheckpoint};
use uni_plugin_host::notifications::CommitNotification;
use uni_plugin_host::shutdown::ShutdownHandle;

/// A corrupt sidecar must not read as "this provider never checkpointed".
#[test]
fn an_unreadable_checkpoint_sidecar_is_an_error_not_a_missing_checkpoint() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let root: PathBuf = tmp.path().to_path_buf();
    let sidecar = CdcCheckpointSidecar::new(root.clone());

    // Control: a real checkpoint round-trips, so the harness is wired up.
    sidecar
        .write_all(&[PersistedCheckpoint {
            name: "kafka".to_owned(),
            last_lsn: 42,
        }])
        .expect("write checkpoint");
    assert_eq!(
        sidecar.lookup("kafka").expect("healthy lookup succeeds"),
        Some(CdcLsn(42)),
        "control: the persisted LSN is readable"
    );

    // Corrupt the sidecar in place. The file exists, so this is a read
    // failure rather than the legitimate "no sidecar yet" case.
    std::fs::write(sidecar.path(), b"{ this is not valid json").expect("corrupt the sidecar");

    let result = sidecar.lookup("kafka");
    assert!(
        result.is_err(),
        "an unreadable sidecar must surface as an error, not as Ok(None); \
         Ok(None) tells the provider it has never checkpointed, so it resumes \
         from the wrong position and either replays or skips commits. Got: {result:?}"
    );
}

/// A checkpoint that genuinely does not exist is still `Ok(None)`.
#[test]
fn an_absent_checkpoint_is_still_not_an_error() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let sidecar = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
    assert_eq!(
        sidecar
            .lookup("never-seen")
            .expect("absent is not a failure"),
        None,
        "a provider that has never checkpointed must read as Ok(None)"
    );
}

/// A provider that accepts everything, so any delivery is the runtime's choice.
#[derive(Default)]
struct Accepted {
    delivered_lsns: Vec<u64>,
    last: u64,
}

struct HealthyProvider {
    rec: Arc<Mutex<Accepted>>,
}

impl CdcOutputProvider for HealthyProvider {
    fn name(&self) -> &str {
        "healthy"
    }

    fn start(&self, _ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
        Ok(Box::new(HealthyStream {
            rec: Arc::clone(&self.rec),
        }))
    }
}

struct HealthyStream {
    rec: Arc<Mutex<Accepted>>,
}

impl CdcStream for HealthyStream {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
        let mut rec = self.rec.lock();
        rec.delivered_lsns.push(batch.lsn_end.0);
        rec.last = batch.lsn_end.0;
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
        Ok(CdcLsn(self.rec.lock().last))
    }

    fn shutdown(&mut self) -> Result<(), FnError> {
        Ok(())
    }
}

fn healthy_registry(rec: Arc<Mutex<Accepted>>) -> Arc<PluginRegistry> {
    let registry = Arc::new(PluginRegistry::new());
    let caps = CapabilitySet::from_iter_of([Capability::Cdc]);
    let mut r = PluginRegistrar::new(PluginId::new("test"), &caps, &registry);
    r.cdc_output(Arc::new(HealthyProvider { rec }))
        .expect("register cdc_output");
    r.commit_to_registry().expect("commit");
    registry
}

fn notification(version: u64, mutations_failed: bool) -> Arc<CommitNotification> {
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
        mutations: None,
        mutations_failed,
    })
}

/// A commit whose mutations could not be materialized must halt, not checkpoint.
///
/// #233 Tier 1. `mutations: None` meant both "zero rows / no CDC subscriber"
/// and "the batch could not be built". The runtime substituted an EMPTY batch
/// carrying the commit's real `lsn_start`/`lsn_end` and then checkpointed past
/// it — a silent hole, through the back door of the very mechanism (`halted`)
/// that exists to make such holes visible. The provider here accepts
/// everything, so delivering is the runtime's decision alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_mutation_batch_halts_instead_of_checkpointing_past_it() {
    let tmp = tempfile::TempDir::new().unwrap();
    let rec = Arc::new(Mutex::new(Accepted::default()));
    let registry = healthy_registry(Arc::clone(&rec));

    let (tx, rx) = broadcast::channel::<Arc<CommitNotification>>(16);
    let shutdown = ShutdownHandle::new(Duration::from_secs(5));
    let runtime = CdcRuntime::spawn(&registry, rx, Some(tmp.path().to_path_buf()), &shutdown);
    let sidecar = runtime.checkpoint_sidecar().expect("sidecar enabled");

    // Control: a healthy commit is delivered and checkpointed.
    tx.send(notification(100, false)).unwrap();
    for _ in 0..100 {
        if !rec.lock().delivered_lsns.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        rec.lock().delivered_lsns,
        vec![100],
        "control: a healthy commit reaches the provider"
    );

    // Commit 101's mutation batch could not be materialized.
    tx.send(notification(101, true)).unwrap();
    for _ in 0..100 {
        if runtime.halted_stream_count() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        runtime.halted_stream_count(),
        1,
        "a commit whose mutations could not be materialized must halt the stream"
    );
    assert_eq!(
        rec.lock().delivered_lsns,
        vec![100],
        "commit 101 must NOT be delivered as an empty batch carrying its real LSN range"
    );
    assert_eq!(
        sidecar
            .lookup("healthy")
            .expect("sidecar readable")
            .map(|l| l.0),
        Some(100),
        "the checkpoint must stay at the last contiguous commit, not advance past the gap"
    );

    let _ = shutdown.shutdown_async().await;
}
