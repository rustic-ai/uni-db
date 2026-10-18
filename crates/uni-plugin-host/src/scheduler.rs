// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Host-side tokio-backed scheduler driver.
//!
//! Wraps the runtime-free [`uni_plugin::scheduler::Scheduler`]
//! primitive in a [`SchedulerHost`] that:
//!
//! - calls [`Scheduler::tick_at`](uni_plugin::scheduler::Scheduler::tick_at)
//!   every `tick_interval`,
//! - looks up the [`BackgroundJobProvider`] for each due job from
//!   the host's [`PluginRegistry`],
//! - dispatches each provider's
//!   [`execute`](uni_plugin::traits::background::BackgroundJobProvider::execute)
//!   on [`tokio::task::spawn_blocking`] (the trait method is sync;
//!   providers that need async work `Handle::current().block_on(...)`
//!   inside),
//! - reports lifecycle transitions to the configured
//!   [`SchedulerPersistence`] backend, and
//! - drains all in-flight runs on shutdown via the supplied
//!   `ShutdownHandle` broadcast.
//!
//! Per the M11 plan, the durable backend (writes through
//! `uni_system.background_jobs` via the write-enabled
//! `execute_inner_query`) lives downstream in `uni-query`; this module
//! consumes only the [`SchedulerPersistence`] trait.

// Rust guideline compliant

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::broadcast;

use uni_plugin::PluginRegistry;
use uni_plugin::circuit_breaker::{BreakerConfig, CircuitBreaker};
use uni_plugin::plugin::PluginId;
use uni_plugin::qname::QName;
use uni_plugin::scheduler::{MemoryPersistence, Scheduler, SchedulerPersistence};
use uni_plugin::traits::background::{BackgroundJobProvider, JobContext, JobHost};
use uni_store::storage::manager::StorageManager;

use crate::host::HostCypherExecutor;
use crate::shutdown::ShutdownHandle;

/// Default driver tick interval — chosen to match the existing
/// [`DeferralQueue`](crate::triggers::DeferralQueue) ticker so the
/// two background drivers cohabit on the same cadence.
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Host-side scheduler driver.
///
/// One per `Uni` instance. Constructed during
/// `Uni::build`; the constructor spawns the driving loop onto the
/// ambient tokio runtime and tracks the join handle through the supplied
/// `ShutdownHandle`.
#[derive(Debug)]
pub struct SchedulerHost {
    /// The primitive scheduler this host drives.
    scheduler: Arc<Scheduler>,
    /// Persistence backend for job state.
    persistence: Arc<dyn SchedulerPersistence>,
    /// Per-(plugin, qname) circuit breaker. Background jobs that fail
    /// `failure_threshold` consecutive times trip the breaker and are
    /// skipped on subsequent ticks until `cooldown` elapses (after
    /// which the breaker half-opens and admits one test call). This
    /// prevents a flapping job from monopolizing the spawn-blocking
    /// pool. See [`CircuitBreaker`].
    circuit_breaker: Arc<CircuitBreaker>,
    /// True when the persisted job set could not be read at startup.
    ///
    /// #233 Tier 1: a failed `load_all` starts the scheduler with no jobs,
    /// and `list()` then reports none — indistinguishable from a genuinely
    /// empty scheduler. Callers can consult
    /// [`SchedulerHost::persistence_degraded`] to tell the two apart.
    persistence_degraded: bool,
    /// Host services passed to each [`JobContext`] on dispatch.
    /// Optional so test fixtures that don't need storage / Uni access
    /// can construct a `SchedulerHost` without a full `Uni`.
    job_host: Option<Arc<SchedulerJobHost>>,
}

/// Concrete [`JobHost`] implementation that lets built-in background
/// jobs reach the storage manager and (after `Uni::build` finishes)
/// the host `UniInner` for write-mode Cypher execution.
///
/// Built by `Uni::build`. Held by [`SchedulerHost`] and passed by
/// reference into each [`JobContext`] in `dispatch_one_tick`.
pub struct SchedulerJobHost {
    storage: Arc<StorageManager>,
    /// Set after `Uni::build` returns — gives ttl/iterate-style jobs
    /// access to the host's write-mode Cypher executor. The executor
    /// itself holds a `Weak` to the host so the scheduler-host ↔ Uni
    /// cycle doesn't leak.
    host_executor: OnceLock<Arc<dyn HostCypherExecutor>>,
}

impl std::fmt::Debug for SchedulerJobHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerJobHost")
            .field("host_executor_wired", &self.host_executor.get().is_some())
            .finish_non_exhaustive()
    }
}

impl SchedulerJobHost {
    /// Construct with the storage manager only. The host Cypher
    /// executor is set later (after `Uni::build` constructs it) via
    /// [`Self::set_host_executor`].
    #[must_use]
    pub fn new(storage: Arc<StorageManager>) -> Self {
        Self {
            storage,
            host_executor: OnceLock::new(),
        }
    }

    /// Wire the host write-mode Cypher executor. Idempotent —
    /// subsequent calls after the first are no-ops.
    pub fn set_host_executor(&self, exec: Arc<dyn HostCypherExecutor>) {
        let _ = self.host_executor.set(exec);
    }

    /// Borrow the storage manager.
    #[must_use]
    pub fn storage(&self) -> &Arc<StorageManager> {
        &self.storage
    }
}

/// `SchedulerControl` impl for the host-side `SchedulerHost`.
///
/// Delegates list/add/cancel to the inner [`Scheduler`] and overrides
/// `submit_cypher` to dispatch through the attached
/// [`SchedulerJobHost`]'s `execute_write_cypher`. This is what makes
/// the `uni.periodic.submit` / `uni.periodic.iterate` procedures
/// actually run Cypher when invoked.
impl uni_plugin::scheduler::SchedulerControl for SchedulerHost {
    fn add_scheduled_job(
        &self,
        id: QName,
        schedule: uni_plugin::traits::background::Schedule,
    ) -> Result<(), uni_plugin::FnError> {
        // Persist the schedule kind before registering so a restart can
        // replay `Periodic` / `Cron` / `Once` jobs without downgrading them
        // to `Manual`. #233 Tier 1: a failure here used to be logged while
        // the caller was told the job was registered, so the job silently
        // vanished at the next restart. Register in memory anyway — the job
        // should still run for this process — but report the lost
        // durability rather than swallowing it.
        let persisted = self.persistence.record_scheduled(&id, &schedule);
        self.scheduler.add_scheduled_job(id.clone(), schedule);
        if let Err(e) = persisted {
            tracing::error!(
                qname = %id,
                error = %e,
                "SchedulerHost: record_scheduled failed; the job runs now but will not \
                 survive a restart",
            );
            return Err(uni_plugin::FnError::new(
                0x2330,
                format!("job {id} registered in memory but could not be persisted: {e}"),
            ));
        }
        Ok(())
    }

    fn cancel(&self, id: &QName) -> Result<bool, uni_plugin::FnError> {
        let cancelled = self.scheduler.cancel(id);
        // Also delete the persisted sidecar row, otherwise the cancelled job
        // resurrects on the next restart (the host replays persistence). Only
        // attempt it when the in-memory job actually existed. #233 Tier 1: a
        // persistence failure used to be logged while `true` was still
        // returned, so the caller was told the job was cancelled and it came
        // back at the next restart.
        if cancelled && let Err(e) = self.persistence.cancel(id) {
            tracing::error!(
                qname = %id,
                error = %e,
                "SchedulerHost: persistence.cancel failed; job cancelled in memory but its \
                 sidecar row survives and will resurrect on restart",
            );
            return Err(uni_plugin::FnError::new(
                0x2331,
                format!("job {id} cancelled in memory but its persisted row survives: {e}"),
            ));
        }
        Ok(cancelled)
    }

    fn list(&self) -> Vec<uni_plugin::scheduler::SchedulerJobRecord> {
        self.scheduler.list()
    }

    fn submit_cypher(&self, cypher: &str) -> Result<(), uni_plugin::FnError> {
        let Some(host) = self.job_host.as_ref() else {
            return Err(uni_plugin::FnError::new(
                0xD21,
                "submit_cypher: scheduler host has no JobHost wired",
            ));
        };
        host.execute_write_cypher(cypher)
    }

    fn flush_checkpoint(&self) -> Result<(), uni_plugin::FnError> {
        self.persistence
            .flush_checkpoint()
            .map_err(|e| uni_plugin::FnError::new(0xD22, format!("flush_checkpoint: {e}")))
    }
}

impl JobHost for SchedulerJobHost {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compact_storage(&self) -> Result<(), uni_plugin::FnError> {
        // `StorageManager::compact` is async; bridge sync→async via
        // `block_in_place` + `Handle::current().block_on(...)`. Safe
        // because the job's `execute()` already runs on
        // `spawn_blocking`, which is a multi-thread tokio worker.
        let storage = Arc::clone(&self.storage);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move { storage.compact().await })
        })
        .map(|_stats| ())
        .map_err(|e| uni_plugin::FnError::new(0xD11, format!("compact_storage: {e}")))
    }

    fn execute_write_cypher(&self, cypher: &str) -> Result<(), uni_plugin::FnError> {
        // The host executor is set after `Uni::build`. Absence means
        // the host isn't wired yet (or has been dropped — racing
        // shutdown); bail without error so the scheduler doesn't open
        // the circuit breaker on a graceful-shutdown race. The
        // executor owns the `block_in_place`/current-thread-runtime
        // guard and the Session/tx/commit body — see
        // `UniInnerCypherExecutor` in `uni-db`.
        let Some(exec) = self.host_executor.get() else {
            tracing::debug!("execute_write_cypher: host executor not wired (shutdown race?)",);
            return Ok(());
        };
        exec.execute_write_cypher(cypher)
            .map_err(|e| uni_plugin::FnError::new(0xD12, format!("execute_write_cypher: {e}")))
    }
}

impl SchedulerHost {
    /// Construct a scheduler host wired to the supplied
    /// [`PluginRegistry`] and `ShutdownHandle`, spawning the driving
    /// loop on the ambient tokio runtime.
    ///
    /// Reloads previously-persisted jobs via
    /// [`SchedulerPersistence::load_all`] and registers them with the
    /// primitive. Re-queues any orphaned `Running` runs via
    /// [`Scheduler::requeue_orphaned_runs`].
    ///
    /// # Panics
    ///
    /// Does not panic itself, but the spawned driver task panics if
    /// no tokio runtime is active. Callers must invoke this from
    /// inside a tokio context (which is the case during `Uni::build`).
    #[must_use]
    pub fn spawn(
        registry: Arc<PluginRegistry>,
        persistence: Arc<dyn SchedulerPersistence>,
        shutdown: &ShutdownHandle,
        tick_interval: Duration,
    ) -> Arc<Self> {
        Self::spawn_with_job_host(registry, persistence, shutdown, tick_interval, None)
    }

    /// Construct a scheduler host with an attached
    /// [`SchedulerJobHost`] (built by `Uni::build` and threaded into
    /// every dispatched [`JobContext`]).
    #[must_use]
    pub fn spawn_with_job_host(
        registry: Arc<PluginRegistry>,
        persistence: Arc<dyn SchedulerPersistence>,
        shutdown: &ShutdownHandle,
        tick_interval: Duration,
        job_host: Option<Arc<SchedulerJobHost>>,
    ) -> Arc<Self> {
        let scheduler = Arc::new(Scheduler::new());
        let mut persistence_degraded = false;

        // Replay persisted job records (if any) so jobs survive restart. A
        // read failure does not block startup, but it is recorded rather than
        // swallowed — see `persistence_degraded`.
        match persistence.load_all() {
            Ok(records) => {
                for record in records {
                    scheduler.add_scheduled_job(record.id.clone(), record.schedule);
                }
                // Anything previously stuck in `Running` is now
                // `Pending` and will fire on next tick.
                metrics::gauge!("uni_scheduler_persistence_degraded").set(0.0);
                let requeued = scheduler.requeue_orphaned_runs();
                if requeued > 0 {
                    tracing::info!(
                        requeued,
                        "scheduler: requeued orphaned runs from previous shutdown"
                    );
                }
            }
            Err(e) => {
                // #233 Tier 1: every persisted job silently vanishes here and
                // `list()` then reports none, so an operator cannot tell an
                // empty scheduler from an unreadable one. The sidecar is not
                // rewritten on this path, so the rows survive on disk for a
                // later restart; what is wrong is the silence. Recorded on the
                // host so the degraded state is observable.
                tracing::error!(
                    error = %e,
                    "scheduler: load_all failed; starting with NO persisted jobs. They are not \
                     lost on disk, but none will run in this process and list() will report none",
                );
                persistence_degraded = true;
                // #233 P2: `persistence_degraded` is readable only through an
                // accessor nothing outside this crate calls, so a scheduler
                // that silently lost its whole persisted job set looked
                // identical to an empty one. Emit it.
                metrics::gauge!("uni_scheduler_persistence_degraded").set(1.0);
            }
        }

        scheduler.resume();

        let circuit_breaker = Arc::new(CircuitBreaker::new(BreakerConfig::default()));

        let host = Arc::new(Self {
            scheduler: Arc::clone(&scheduler),
            persistence: Arc::clone(&persistence),
            circuit_breaker: Arc::clone(&circuit_breaker),
            persistence_degraded,
            job_host: job_host.clone(),
        });

        // Spawn the driver loop.
        let driver_scheduler = Arc::clone(&scheduler);
        let driver_persistence = Arc::clone(&persistence);
        let driver_registry = Arc::clone(&registry);
        let driver_breaker = Arc::clone(&circuit_breaker);
        let driver_job_host = job_host;
        let shutdown_rx = shutdown.subscribe();
        let handle = tokio::spawn(driver_loop(
            driver_scheduler,
            driver_persistence,
            driver_registry,
            driver_breaker,
            driver_job_host,
            shutdown_rx,
            shutdown.clone(),
            tick_interval,
        ));
        shutdown.track_task(handle);

        host
    }

    /// Borrow the attached [`SchedulerJobHost`] (if any).
    #[must_use]
    pub fn job_host(&self) -> Option<&Arc<SchedulerJobHost>> {
        self.job_host.as_ref()
    }

    /// Borrow the host's circuit breaker. Exposed for tests + the
    /// eventual `uni.system.circuit_breaker.*` introspection
    /// procedures.
    #[must_use]
    pub fn circuit_breaker(&self) -> &Arc<CircuitBreaker> {
        &self.circuit_breaker
    }

    /// Borrow the underlying primitive scheduler.
    #[must_use]
    pub fn scheduler(&self) -> &Arc<Scheduler> {
        &self.scheduler
    }

    /// True when persisted jobs could not be read at startup.
    ///
    /// When this is set, `list()` reporting no jobs means "the persisted set
    /// could not be read", not "no jobs are scheduled" (#233).
    #[must_use]
    pub fn persistence_degraded(&self) -> bool {
        self.persistence_degraded
    }

    /// Borrow the persistence backend.
    #[must_use]
    pub fn persistence(&self) -> &Arc<dyn SchedulerPersistence> {
        &self.persistence
    }
}

/// Convenience constructor returning a [`SchedulerHost`] backed by an
/// in-memory persistence — used by the default `Uni::build` path
/// before the host wires a durable backend.
#[must_use]
pub fn spawn_with_memory_persistence(
    registry: Arc<PluginRegistry>,
    shutdown: &ShutdownHandle,
) -> Arc<SchedulerHost> {
    SchedulerHost::spawn(
        registry,
        Arc::new(MemoryPersistence),
        shutdown,
        DEFAULT_TICK_INTERVAL,
    )
}

/// The driving loop. Calls
/// [`Scheduler::tick_at`](uni_plugin::scheduler::Scheduler::tick_at) on
/// every interval and dispatches each due job to its provider on a
/// blocking worker thread. Each run is a task tracked by `tracker`, so
/// shutdown waits for in-flight runs.
#[allow(clippy::too_many_arguments)]
async fn driver_loop(
    scheduler: Arc<Scheduler>,
    persistence: Arc<dyn SchedulerPersistence>,
    registry: Arc<PluginRegistry>,
    circuit_breaker: Arc<CircuitBreaker>,
    job_host: Option<Arc<SchedulerJobHost>>,
    mut shutdown_rx: broadcast::Receiver<()>,
    tracker: ShutdownHandle,
    tick_interval: Duration,
) {
    let mut ticker = tokio::time::interval(tick_interval);
    // Skip the immediate first tick so registration of jobs (which
    // may happen synchronously right after `spawn`) can settle.
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                dispatch_one_tick(
                    &scheduler,
                    &persistence,
                    &registry,
                    &circuit_breaker,
                    job_host.as_ref(),
                    &tracker,
                );
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("scheduler driver: shutdown received");
                break;
            }
        }
    }
}

/// Perform one tick: collect due jobs, look up their providers, and
/// start one tracked run task per job.
///
/// Everything that can block runs on the blocking pool, never on the
/// executor: the provider's `execute` and the persistence backend's
/// lifecycle writes (the durable backend fsyncs a sidecar and issues a
/// Cypher write through `block_in_place` for each one). Each run task
/// performs `record_started` → `execute` → `record_finished` in order,
/// so the backend sees a consistent pair per run.
fn dispatch_one_tick(
    scheduler: &Arc<Scheduler>,
    persistence: &Arc<dyn SchedulerPersistence>,
    registry: &Arc<PluginRegistry>,
    circuit_breaker: &Arc<CircuitBreaker>,
    job_host: Option<&Arc<SchedulerJobHost>>,
    tracker: &ShutdownHandle,
) {
    let due = scheduler.tick();
    if due.is_empty() {
        return;
    }
    let providers = registry.background_jobs();
    // All background-job invocations are attributed to the "uni"
    // plugin id for breaker bookkeeping; per-job qname distinguishes
    // them so a flapping `ttl_sweep` does not poison `compaction`.
    let plugin_id = PluginId::new("uni");
    for id in due {
        // Circuit-breaker gate: if a job has tripped, skip dispatch
        // until the cooldown elapses (half-open lets one through).
        if !circuit_breaker.allow(&plugin_id, &id) {
            tracing::debug!(
                job = %id,
                "scheduler: circuit breaker open; skipping this tick"
            );
            // Mark finished with success=false so the schedule recomputes a
            // next fire instead of leaving the job stuck Running. We
            // intentionally don't `record_failure` here — the breaker is
            // already open; recording would only add noise.
            //
            // #233 P3: `success=false` also increments `consecutive_failures`
            // for a run that never happened, so the in-memory counter drifts
            // above the number of real failures while the breaker is open. It
            // is reset by the next success, so this is drift rather than a
            // wrong answer — counted so the drift is attributable.
            metrics::counter!("uni_scheduler_breaker_skipped_total").increment(1);
            scheduler.mark_finished(&id, false);
            continue;
        }
        let Some(provider) = find_provider(&providers, &id) else {
            tracing::warn!(
                job = %id,
                "scheduler: no provider registered; marking finished with failure"
            );
            let now = std::time::SystemTime::now();
            scheduler.mark_finished(&id, false);
            circuit_breaker.record_failure(&plugin_id, &id);
            let persistence = Arc::clone(persistence);
            tracker.track_task(tokio::task::spawn_blocking(move || {
                if let Err(e) = persistence.record_finished(&id, now, false) {
                    tracing::warn!(job = %id, error = %e, "scheduler: record_finished failed");
                }
            }));
            continue;
        };
        let scheduler_clone = Arc::clone(scheduler);
        let persistence_clone = Arc::clone(persistence);
        let breaker_clone = Arc::clone(circuit_breaker);
        let plugin_id_clone = plugin_id.clone();
        let job_host_clone = job_host.cloned();
        // §1.2 / Phase 6: use the cancel token attached to the
        // scheduler's job record (rather than a fresh one) so that
        // `Scheduler::cancel(id)` and any other holder of the token can
        // signal in-flight runs cooperatively. Fall back to a fresh
        // token if the record vanished between `tick()` and here
        // (extremely unlikely; defensive).
        let cancel = scheduler.cancel_token_for(&id).unwrap_or_default();
        tracker.track_task(tokio::spawn(run_job(
            id,
            provider,
            cancel,
            job_host_clone,
            scheduler_clone,
            persistence_clone,
            breaker_clone,
            plugin_id_clone,
        )));
    }
}

/// One scheduled run: `record_started`, `execute`, `record_finished`.
///
/// The three blocking steps each run on the blocking pool. The run is
/// raced against the job's cancel token: on cancellation the lifecycle is
/// recorded at once (the scheduler does not wait for the body to poll
/// `is_cancelled()`), and the task then waits for the body to return, so
/// a still-running body stays counted and joinable by shutdown instead of
/// detaching.
#[allow(clippy::too_many_arguments)]
async fn run_job(
    id: QName,
    provider: Arc<dyn BackgroundJobProvider>,
    cancel: uni_plugin::traits::background::CancellationToken,
    job_host: Option<Arc<SchedulerJobHost>>,
    scheduler: Arc<Scheduler>,
    persistence: Arc<dyn SchedulerPersistence>,
    breaker: Arc<CircuitBreaker>,
    plugin_id: PluginId,
) {
    let started_at = std::time::SystemTime::now();
    let started = {
        let persistence = Arc::clone(&persistence);
        let id = id.clone();
        tokio::task::spawn_blocking(move || persistence.record_started(&id, started_at)).await
    };
    match started {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(
            job = %id,
            error = %e,
            "scheduler: record_started failed; continuing"
        ),
        Err(e) => tracing::warn!(
            job = %id,
            error = %e,
            "scheduler: record_started did not complete; continuing"
        ),
    }

    let cancel_for_select = cancel.clone();
    let mut blocking = tokio::task::spawn_blocking(move || {
        let mut ctx = JobContext::new(cancel, None);
        // The host Arc is held by `job_host` for the entire scope of
        // this closure, so the borrow stays valid for the synchronous
        // `provider.execute(ctx)` call, which consumes `ctx` before the
        // closure ends.
        if let Some(host) = job_host.as_deref() {
            ctx = ctx.with_host(host as &dyn JobHost);
        }
        provider.execute(ctx)
    });
    // §1.2 / Phase 6: if cancellation arrives mid-run, the scheduler
    // observes it immediately and marks the job finished without waiting
    // for the body to poll `is_cancelled()`. The body keeps running on the
    // blocking worker (synchronous Rust cannot be preempted), but its
    // result is dropped — the lifecycle records use the "cancelled"
    // outcome so the breaker / persistence stay consistent.
    let (success, body_running) = tokio::select! {
        joined = &mut blocking => {
            let success = match joined {
                Ok(outcome) => outcome.is_ok(),
                Err(join_err) => {
                    tracing::warn!(
                        job = %id,
                        error = %join_err,
                        "scheduler: blocking dispatch join failed"
                    );
                    false
                }
            };
            (success, false)
        }
        () = cancel_for_select.cancelled() => {
            tracing::info!(
                job = %id,
                "scheduler: cancellation observed before job completion"
            );
            (false, true)
        }
    };
    let finished_at = std::time::SystemTime::now();
    scheduler.mark_finished(&id, success);
    if success {
        breaker.record_success(&plugin_id, &id);
    } else {
        breaker.record_failure(&plugin_id, &id);
    }
    let finished = {
        let id = id.clone();
        tokio::task::spawn_blocking(move || persistence.record_finished(&id, finished_at, success))
            .await
    };
    match finished {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(job = %id, error = %e, "scheduler: record_finished failed"),
        Err(e) => tracing::warn!(
            job = %id,
            error = %e,
            "scheduler: record_finished did not complete"
        ),
    }
    if body_running {
        // The outcome is already recorded; this only keeps the task alive
        // (and counted) until the cancelled body actually returns.
        let _ = blocking.await;
    }
}

/// Linear search through the registry's background-job providers for
/// one whose [`JobDefinition::id`](
/// uni_plugin::traits::background::JobDefinition) matches.
///
/// Provider count is small (single-digit to low-double-digit in
/// practice — one per registered built-in / user job), so the linear
/// scan is cheaper than a hash lookup at this scale.
fn find_provider(
    providers: &Arc<Vec<Arc<dyn BackgroundJobProvider>>>,
    id: &QName,
) -> Option<Arc<dyn BackgroundJobProvider>> {
    providers
        .iter()
        .find(|p| &p.definition().id == id)
        .map(Arc::clone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;
    use uni_plugin::Capability;
    use uni_plugin::CapabilitySet;
    use uni_plugin::PluginRegistrar;
    use uni_plugin::errors::FnError;
    use uni_plugin::traits::background::{
        ConcurrencyLimit, JobDefinition, JobOutcome, RetryPolicy, Schedule,
    };

    /// Test fixture: a job that increments a shared counter on each fire.
    #[derive(Debug)]
    struct CountingJob {
        definition: JobDefinition,
        counter: Arc<AtomicU64>,
    }

    impl BackgroundJobProvider for CountingJob {
        fn definition(&self) -> &JobDefinition {
            &self.definition
        }

        fn execute(&self, _ctx: JobContext<'_>) -> Result<JobOutcome, FnError> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(JobOutcome::Done)
        }
    }

    /// Test fixture: a job that always fails. Used to drive the
    /// circuit breaker open.
    #[derive(Debug)]
    struct AlwaysFailJob {
        definition: JobDefinition,
        attempts: Arc<AtomicU64>,
    }

    impl BackgroundJobProvider for AlwaysFailJob {
        fn definition(&self) -> &JobDefinition {
            &self.definition
        }

        fn execute(&self, _ctx: JobContext<'_>) -> Result<JobOutcome, FnError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(FnError::new(0xC1F, "always fails"))
        }
    }

    fn make_registry_with_job(provider: Arc<dyn BackgroundJobProvider>) -> Arc<PluginRegistry> {
        let registry = Arc::new(PluginRegistry::new());
        let caps = CapabilitySet::from_iter_of([Capability::BackgroundJob { max_concurrent: 0 }]);
        let plugin_id = uni_plugin::PluginId::new("test");
        let mut r = PluginRegistrar::new(plugin_id, &caps, &registry);
        r.background_job(provider).expect("background_job register");
        r.commit_to_registry().expect("commit");
        registry
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_fires_periodic_job() {
        let counter = Arc::new(AtomicU64::new(0));
        let provider = Arc::new(CountingJob {
            definition: JobDefinition {
                id: QName::new("test", "ticker"),
                schedule: Schedule::Periodic(Duration::from_millis(50)),
                concurrency: ConcurrencyLimit::Exclusive,
                timeout: Duration::from_secs(1),
                retry: RetryPolicy::Never,
                docs: "test ticker".to_owned(),
            },
            counter: Arc::clone(&counter),
        });
        let registry = make_registry_with_job(provider);
        let shutdown = ShutdownHandle::new(Duration::from_secs(5));
        let host = SchedulerHost::spawn(
            registry,
            Arc::new(MemoryPersistence),
            &shutdown,
            Duration::from_millis(25),
        );
        host.scheduler().add_scheduled_job(
            QName::new("test", "ticker"),
            Schedule::Periodic(Duration::from_millis(50)),
        );

        // Let the driver run for ~400ms — should yield several fires.
        tokio::time::sleep(Duration::from_millis(400)).await;

        let fires = counter.load(Ordering::SeqCst);
        assert!(
            fires >= 2,
            "expected the periodic job to fire at least twice, got {fires}"
        );

        // Clean shutdown.
        let _ = shutdown.shutdown_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_halts_further_runs() {
        let counter = Arc::new(AtomicU64::new(0));
        let provider = Arc::new(CountingJob {
            definition: JobDefinition {
                id: QName::new("test", "cancelme"),
                schedule: Schedule::Periodic(Duration::from_millis(50)),
                concurrency: ConcurrencyLimit::Exclusive,
                timeout: Duration::from_secs(1),
                retry: RetryPolicy::Never,
                docs: "cancelme".to_owned(),
            },
            counter: Arc::clone(&counter),
        });
        let registry = make_registry_with_job(provider);
        let shutdown = ShutdownHandle::new(Duration::from_secs(5));
        let host = SchedulerHost::spawn(
            registry,
            Arc::new(MemoryPersistence),
            &shutdown,
            Duration::from_millis(25),
        );
        let job_id = QName::new("test", "cancelme");
        host.scheduler().add_scheduled_job(
            job_id.clone(),
            Schedule::Periodic(Duration::from_millis(50)),
        );

        // Let it fire at least once.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let pre_cancel = counter.load(Ordering::SeqCst);
        assert!(pre_cancel >= 1, "expected at least one pre-cancel fire");

        host.scheduler().cancel(&job_id);

        // Settle, then take a final count.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let post_cancel = counter.load(Ordering::SeqCst);

        // After cancel the counter should stop advancing. We allow up
        // to one extra fire in flight at the moment of cancel.
        assert!(
            post_cancel <= pre_cancel + 1,
            "expected cancel to halt firing; pre={pre_cancel} post={post_cancel}"
        );

        let _ = shutdown.shutdown_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn circuit_breaker_opens_after_threshold_failures() {
        let attempts = Arc::new(AtomicU64::new(0));
        let provider = Arc::new(AlwaysFailJob {
            definition: JobDefinition {
                id: QName::new("test", "flaky"),
                schedule: Schedule::Periodic(Duration::from_millis(20)),
                concurrency: ConcurrencyLimit::Exclusive,
                timeout: Duration::from_secs(1),
                retry: RetryPolicy::Never,
                docs: "flaky".to_owned(),
            },
            attempts: Arc::clone(&attempts),
        });
        let registry = make_registry_with_job(provider);
        let shutdown = ShutdownHandle::new(Duration::from_secs(5));
        let host = SchedulerHost::spawn(
            registry,
            Arc::new(MemoryPersistence),
            &shutdown,
            Duration::from_millis(10),
        );
        host.scheduler().add_scheduled_job(
            QName::new("test", "flaky"),
            Schedule::Periodic(Duration::from_millis(20)),
        );

        // Let the driver pile up failures. With a 10ms tick interval
        // and a 20ms periodic schedule, we expect ~25 dispatch
        // opportunities over 500ms — but the breaker opens after 10
        // failures, after which attempts plateau.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let total_attempts = attempts.load(Ordering::SeqCst);
        assert!(
            (10..=20).contains(&total_attempts),
            "expected the breaker to cap attempts around the failure_threshold (10); \
             got {total_attempts}"
        );

        let _ = shutdown.shutdown_async().await;
    }

    /// A job whose `execute` blocks its thread for `delay`.
    #[derive(Debug)]
    struct SlowJob {
        definition: JobDefinition,
        delay: Duration,
        started: Arc<AtomicU64>,
        finished: Arc<AtomicU64>,
    }

    impl BackgroundJobProvider for SlowJob {
        fn definition(&self) -> &JobDefinition {
            &self.definition
        }

        fn execute(&self, _ctx: JobContext<'_>) -> Result<JobOutcome, FnError> {
            self.started.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(self.delay);
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(JobOutcome::Done)
        }
    }

    /// Persistence whose `record_started` blocks for `delay`, as the durable
    /// backend's fsynced sidecar write plus Cypher mirror can.
    #[derive(Debug)]
    struct SlowPersistence {
        delay: Duration,
        started: Arc<AtomicU64>,
    }

    impl SchedulerPersistence for SlowPersistence {
        fn record_started(
            &self,
            _id: &QName,
            _started_at: std::time::SystemTime,
        ) -> Result<(), uni_plugin::scheduler::SchedulerPersistenceError> {
            self.started.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(self.delay);
            Ok(())
        }
        fn record_finished(
            &self,
            _id: &QName,
            _finished_at: std::time::SystemTime,
            _success: bool,
        ) -> Result<(), uni_plugin::scheduler::SchedulerPersistenceError> {
            Ok(())
        }
        fn cancel(
            &self,
            _id: &QName,
        ) -> Result<(), uni_plugin::scheduler::SchedulerPersistenceError> {
            Ok(())
        }
        fn load_all(
            &self,
        ) -> Result<
            Vec<uni_plugin::scheduler::SchedulerJobRecord>,
            uni_plugin::scheduler::SchedulerPersistenceError,
        > {
            Ok(Vec::new())
        }
    }

    fn slow_job(name: &str, delay: Duration) -> (Arc<SlowJob>, Arc<AtomicU64>, Arc<AtomicU64>) {
        let started = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicU64::new(0));
        let job = Arc::new(SlowJob {
            definition: JobDefinition {
                id: QName::new("test", name),
                schedule: Schedule::Periodic(Duration::from_secs(3600)),
                concurrency: ConcurrencyLimit::Exclusive,
                timeout: Duration::from_secs(30),
                retry: RetryPolicy::Never,
                docs: name.to_owned(),
            },
            delay,
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
        });
        (job, started, finished)
    }

    /// A slow lifecycle write must not stall other tasks on the runtime.
    ///
    /// `record_started` ran inline on the driver task, so on a
    /// `current_thread` runtime the durable backend's per-run sidecar fsync
    /// and Cypher mirror stalled every other task. An unrelated timer loop
    /// that spans the job's first due tick must finish promptly.
    #[tokio::test]
    async fn slow_lifecycle_write_does_not_stall_the_runtime() {
        const SLOW: Duration = Duration::from_secs(2);
        let (job, _, _) = slow_job("persisted", Duration::ZERO);
        let registry = make_registry_with_job(job);
        let started = Arc::new(AtomicU64::new(0));
        let shutdown = ShutdownHandle::new(Duration::from_secs(10));
        let host = SchedulerHost::spawn(
            registry,
            Arc::new(SlowPersistence {
                delay: SLOW,
                started: Arc::clone(&started),
            }),
            &shutdown,
            Duration::from_millis(10),
        );
        host.scheduler().add_scheduled_job(
            QName::new("test", "persisted"),
            Schedule::Once(std::time::SystemTime::now() + Duration::from_millis(50)),
        );

        let t0 = Instant::now();
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let elapsed = t0.elapsed();

        assert!(
            started.load(Ordering::SeqCst) >= 1,
            "control: a run must have started (and its record_started begun) in the window"
        );
        assert!(
            elapsed < SLOW / 2,
            "a timer loop took {elapsed:?} with a {SLOW:?} lifecycle write in flight: \
             the write is blocking the runtime"
        );
        shutdown.shutdown_async().await.unwrap();
    }

    /// A run is a tracked task: counted while the job executes, and shutdown
    /// waits for it rather than returning with the job still running.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scheduled_run_is_tracked_and_joined_by_shutdown() {
        let (job, started, finished) = slow_job("slow", Duration::from_secs(1));
        let registry = make_registry_with_job(job);
        let shutdown = ShutdownHandle::new(Duration::from_secs(30));
        let host = SchedulerHost::spawn(
            registry,
            Arc::new(MemoryPersistence),
            &shutdown,
            Duration::from_millis(10),
        );
        host.scheduler().add_scheduled_job(
            QName::new("test", "slow"),
            Schedule::Once(std::time::SystemTime::now() + Duration::from_millis(20)),
        );
        let t0 = Instant::now();
        while started.load(Ordering::SeqCst) == 0 {
            assert!(t0.elapsed() < Duration::from_secs(10), "the job never ran");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            shutdown.live_task_count() >= 2,
            "the in-flight run is not tracked (live: {})",
            shutdown.live_task_count()
        );

        shutdown.shutdown_async().await.unwrap();
        assert_eq!(
            finished.load(Ordering::SeqCst),
            1,
            "shutdown returned while a scheduled run was still executing"
        );
        assert_eq!(shutdown.live_task_count(), 0);
    }
}
