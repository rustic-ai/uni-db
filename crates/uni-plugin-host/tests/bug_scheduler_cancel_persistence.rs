// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-plugin-host/src/scheduler.rs:148
//
// `SchedulerControl::cancel` (the host path taken by `uni.periodic.cancel`)
// only cancels the in-memory primitive Scheduler and never calls
// `self.persistence.cancel(id)`. This is asymmetric with
// `add_scheduled_job`, which persists via `record_scheduled` first. Result:
// the durable sidecar row survives a cancel, and on the next restart
// `load_all` re-registers the cancelled job — it resumes firing.
//
// We inject a *recording* durable-style `SchedulerPersistence` (a real
// backend impl, the designed injection point — not a mock of the code
// under test) and observe that host `cancel` never invokes its `cancel`,
// leaving the row present so `load_all` still returns it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;

use uni_plugin::PluginRegistry;
use uni_plugin::qname::QName;
use uni_plugin::scheduler::{
    SchedulerControl, SchedulerJobRecord, SchedulerPersistence, SchedulerPersistenceError,
};
use uni_plugin::traits::background::Schedule;

use uni_plugin_host::scheduler::SchedulerHost;
use uni_plugin_host::shutdown::ShutdownHandle;

/// A durable-style recording backend: `record_scheduled` upserts a row
/// (one per qname, exactly like the real system-label backend), `cancel`
/// DETACH-DELETEs it, and `load_all` reflects the stored rows. Also
/// counts how many times each method was called so the test can prove
/// host `cancel` never reaches persistence.
#[derive(Debug, Default)]
struct RecordingPersistence {
    rows: Mutex<Vec<SchedulerJobRecord>>,
    record_scheduled_calls: AtomicU64,
    cancel_calls: AtomicU64,
}

impl SchedulerPersistence for RecordingPersistence {
    fn record_scheduled(
        &self,
        id: &QName,
        schedule: &Schedule,
    ) -> Result<(), SchedulerPersistenceError> {
        self.record_scheduled_calls.fetch_add(1, Ordering::SeqCst);
        let mut rows = self.rows.lock();
        if let Some(r) = rows.iter_mut().find(|r| r.id == *id) {
            r.schedule = schedule.clone();
        } else {
            rows.push(SchedulerJobRecord::pending_with_schedule(
                id.clone(),
                schedule.clone(),
                SystemTime::now(),
            ));
        }
        Ok(())
    }

    fn record_started(
        &self,
        _id: &QName,
        _started_at: SystemTime,
    ) -> Result<(), SchedulerPersistenceError> {
        Ok(())
    }

    fn record_finished(
        &self,
        _id: &QName,
        _finished_at: SystemTime,
        _success: bool,
    ) -> Result<(), SchedulerPersistenceError> {
        Ok(())
    }

    fn cancel(&self, id: &QName) -> Result<(), SchedulerPersistenceError> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        self.rows.lock().retain(|r| r.id != *id);
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<SchedulerJobRecord>, SchedulerPersistenceError> {
        Ok(self.rows.lock().clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_cancel_does_not_persist_cancellation() {
    let registry = Arc::new(PluginRegistry::new());
    let persistence = Arc::new(RecordingPersistence::default());
    let shutdown = ShutdownHandle::new(Duration::from_secs(5));

    let host = SchedulerHost::spawn(
        Arc::clone(&registry) as Arc<PluginRegistry>,
        Arc::clone(&persistence) as Arc<dyn SchedulerPersistence>,
        &shutdown,
        Duration::from_millis(50),
    );

    let id = QName::new("uni", "ttl_sweep");

    // 1) Register via the host SchedulerControl path → persists the row.
    SchedulerControl::add_scheduled_job(
        &*host,
        id.clone(),
        Schedule::Periodic(Duration::from_millis(50)),
    )
    .expect("registration is durable");
    assert_eq!(
        persistence.record_scheduled_calls.load(Ordering::SeqCst),
        1,
        "add_scheduled_job persisted the schedule"
    );
    assert_eq!(persistence.load_all().unwrap().len(), 1, "row is durable");

    // 2) Cancel via the host SchedulerControl path (same path
    // `uni.periodic.cancel` takes).
    assert!(
        SchedulerControl::cancel(&*host, &id).expect("cancel is durable"),
        "in-memory cancel succeeds"
    );

    // FIXED: host cancel now also invokes persistence.cancel, so the durable
    // sidecar row is deleted and cannot resurrect the job on restart.
    assert_eq!(
        persistence.cancel_calls.load(Ordering::SeqCst),
        1,
        "host cancel must reach persistence.cancel"
    );
    let surviving = persistence.load_all().unwrap();
    assert!(
        surviving.is_empty(),
        "cancelled job's durable row must be removed, got {surviving:?}"
    );

    let _ = shutdown.shutdown_async().await;
}
