// Rust guideline compliant

//! Background-job scheduler skeleton.
//!
//! The host owns a single scheduler that drives every registered
//! [`crate::traits::background::BackgroundJobProvider`]. This module
//! ships the scheduler's public API + persistent state record + a
//! `SchedulerPersistence` trait. The host-side Tokio driver
//! (`crates/uni-plugin-host/src/scheduler.rs`) wraps a loop that calls
//! `tick_at(SystemTime::now())`, dispatches the returned jobs through
//! the plugin registry, and forwards lifecycle transitions to the
//! configured persistence backend.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use parking_lot::Mutex;
use thiserror::Error;

use crate::qname::QName;
use crate::traits::background::{CancellationToken, Schedule};

/// Lifecycle state of one scheduled job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchedulerJobStatus {
    /// Registered but not yet started.
    Pending,
    /// Currently running.
    Running,
    /// Last run finished successfully.
    Idle,
    /// Last run failed; retry-policy applies.
    FailedRetrying,
    /// Cancelled by `cancel()`.
    Cancelled,
}

/// Persistable record of a scheduled job's state.
///
/// Round-trips through `uni_system.background_jobs` in M11 cutover.
#[derive(Clone, Debug)]
pub struct SchedulerJobRecord {
    /// Job id.
    pub id: QName,
    /// Lifecycle status.
    pub status: SchedulerJobStatus,
    /// When the next fire of this job is due. `None` for `Manual`
    /// schedules until [`Scheduler::add_job`] marks the job `Pending`,
    /// at which point it is eligible immediately.
    pub next_fire_at: Option<SystemTime>,
    /// When the most-recent run started.
    pub last_started_at: Option<SystemTime>,
    /// When the most-recent run finished.
    pub last_finished_at: Option<SystemTime>,
    /// Number of consecutive failures since the last success.
    pub consecutive_failures: u32,
    /// Schedule describing when fires are eligible.
    pub schedule: Schedule,
    /// Cancellation token; flipped on `cancel()` or shutdown.
    pub cancel: CancellationToken,
}

impl SchedulerJobRecord {
    /// Construct a pending record with the legacy `Manual` schedule.
    ///
    /// Equivalent to `pending_with_schedule(id, Schedule::Manual,
    /// SystemTime::now())`.
    #[must_use]
    pub fn pending(id: QName) -> Self {
        Self::pending_with_schedule(id, Schedule::Manual, SystemTime::now())
    }

    /// Construct a pending record with an explicit schedule.
    ///
    /// `now` is used both as the initial registration instant and as
    /// the reference point for the first `next_fire_at` computation.
    #[must_use]
    pub fn pending_with_schedule(id: QName, schedule: Schedule, now: SystemTime) -> Self {
        let next_fire_at = schedule.next_after(now);
        Self {
            id,
            status: SchedulerJobStatus::Pending,
            next_fire_at,
            last_started_at: None,
            last_finished_at: None,
            consecutive_failures: 0,
            schedule,
            cancel: CancellationToken::new(),
        }
    }
}

/// Host-side scheduler skeleton.
///
/// One per Uni instance. M11 cutover wires `tokio::spawn` driving and
/// persistence into `uni_system.background_jobs`. Currently the
/// scheduler is paused — registered jobs are stored but not executed.
#[derive(Debug)]
pub struct Scheduler {
    records: Mutex<Vec<SchedulerJobRecord>>,
    paused: AtomicBool,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler {
    /// Construct a paused scheduler.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            paused: AtomicBool::new(true),
        }
    }

    /// Register a new job with the legacy `Manual` schedule.
    ///
    /// Equivalent to `add_scheduled_job(id, Schedule::Manual)`. The
    /// job becomes eligible immediately and fires on the next tick
    /// (no-op while paused).
    pub fn add_job(&self, id: QName) {
        self.add_scheduled_job(id, Schedule::Manual);
    }

    /// Register a new job with an explicit schedule.
    ///
    /// The job's `next_fire_at` is computed from the schedule plus
    /// the current `SystemTime`. The scheduler picks it up on the
    /// first [`Self::tick`] / [`Self::tick_at`] whose `now` is at or
    /// past `next_fire_at` (no-op while paused).
    pub fn add_scheduled_job(&self, id: QName, schedule: Schedule) {
        let now = SystemTime::now();
        let record = SchedulerJobRecord::pending_with_schedule(id.clone(), schedule, now);
        let mut records = self.records.lock();
        // Upsert by id: re-registering the same job REPLACES its record rather
        // than pushing a duplicate. Two records for one id would double-fire, and
        // a single `cancel` would leave the stale copy behind.
        if let Some(existing) = records.iter_mut().find(|r| r.id == id) {
            *existing = record;
        } else {
            records.push(record);
        }
    }

    /// Cancel a scheduled job by id.
    ///
    /// Returns `true` if the job was found and cancelled.
    pub fn cancel(&self, id: &QName) -> bool {
        let mut records = self.records.lock();
        let Some(r) = records.iter_mut().find(|r| &r.id == id) else {
            return false;
        };
        r.status = SchedulerJobStatus::Cancelled;
        r.cancel.cancel();
        true
    }

    /// List all known jobs and their statuses (snapshot).
    #[must_use]
    pub fn list(&self) -> Vec<SchedulerJobRecord> {
        self.records.lock().clone()
    }

    /// Look up the cancellation token associated with a registered job.
    ///
    /// Returns `None` if no job matches `id`. The returned clone shares
    /// state with the record's token, so callers can both await
    /// `cancelled().await` and observe the same cancel signal trip via
    /// [`Self::cancel`].
    ///
    /// Used by the host driver to wrap each dispatched
    /// `spawn_blocking` in a `tokio::select!` against `cancelled().await`,
    /// so shutdown / explicit cancel propagates without waiting for the
    /// job body to poll [`CancellationToken::is_cancelled`].
    #[must_use]
    pub fn cancel_token_for(&self, id: &QName) -> Option<CancellationToken> {
        self.records
            .lock()
            .iter()
            .find(|r| &r.id == id)
            .map(|r| r.cancel.clone())
    }

    /// Resume the scheduler (M11 cutover wires actual driving here).
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    /// Drive the scheduler with the current wall-clock time.
    ///
    /// Equivalent to `tick_at(SystemTime::now())`. See [`Self::tick_at`]
    /// for the full semantics.
    pub fn tick(&self) -> Vec<QName> {
        self.tick_at(SystemTime::now())
    }

    /// Pop every pending job whose schedule has fired at or before
    /// `now`, transition each to `Running`, and return their ids for
    /// the caller to dispatch.
    ///
    /// **M11 substantive driver primitive.** This is the synchronous,
    /// runtime-free heart of the scheduler — the eventual Tokio
    /// driver wraps a poll loop that calls `tick_at(SystemTime::now())`,
    /// dispatches the returned jobs (e.g., via `tokio::spawn` invoking
    /// each job's `BackgroundJobProvider::execute`), and calls
    /// [`Scheduler::mark_finished`] when each completes.
    ///
    /// Schedule semantics (delegated to
    /// [`crate::traits::background::Schedule::next_after`]):
    ///
    /// - A job is "due" iff `status == Pending`,
    ///   `next_fire_at.is_none()` or `next_fire_at <= now`, and the
    ///   cancel token is not already triggered.
    /// - `Manual` jobs have `next_fire_at = now` at registration and
    ///   so are immediately due (matching legacy `tick()` behavior).
    /// - `Once(at)` jobs become due only when `now >= at`.
    /// - `Periodic(every)` jobs become due `every` after each fire.
    /// - `Cron(expr)` jobs become due at the next cron instant
    ///   computed via the [`cron`] crate.
    ///
    /// Honors pause: returns empty when [`Self::is_paused`].
    /// Honors cancellation: skips jobs whose `cancel` token is
    /// already triggered (filtering them out of the return).
    pub fn tick_at(&self, now: SystemTime) -> Vec<QName> {
        if self.is_paused() {
            return Vec::new();
        }
        let mut records = self.records.lock();
        let mut due: Vec<QName> = Vec::new();
        for r in records.iter_mut() {
            if !matches!(r.status, SchedulerJobStatus::Pending) {
                continue;
            }
            if r.cancel.is_cancelled() {
                r.status = SchedulerJobStatus::Cancelled;
                continue;
            }
            // Time-gate.
            match r.next_fire_at {
                Some(fire_at) if fire_at > now => continue,
                Some(_) => {}
                None => {
                    // No computed fire time. For a `Cron` this means the
                    // expression FAILED TO PARSE (`next_after` logged + returned
                    // None) — the job must NEVER fire, so skip it rather than
                    // falling through and dispatching it once. For an overdue
                    // `Once`/`Manual` (whose single instant is already past)
                    // `next_after` also returns None, but that job SHOULD fire once
                    // — a finished one is later gated out by its non-`Pending`
                    // status, not by `next_fire_at`. So only a fire-time-less Cron
                    // is skipped.
                    if matches!(r.schedule, Schedule::Cron(_)) {
                        continue;
                    }
                }
            }
            r.status = SchedulerJobStatus::Running;
            r.last_started_at = Some(now);
            due.push(r.id.clone());
        }
        due
    }

    /// Number of jobs currently in `Running` state. Useful for
    /// observability (e.g., a metrics gauge).
    #[must_use]
    pub fn running_count(&self) -> usize {
        self.records
            .lock()
            .iter()
            .filter(|r| matches!(r.status, SchedulerJobStatus::Running))
            .count()
    }

    /// Number of pending jobs ready for the next `tick`.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.records
            .lock()
            .iter()
            .filter(|r| matches!(r.status, SchedulerJobStatus::Pending))
            .count()
    }

    /// Reset every `Running` job back to `Pending` — used by the
    /// driver to recover from a crash where jobs were started but not
    /// finished. The host restores the scheduler state from
    /// `uni_system.background_jobs` and calls this to make all
    /// previously-`Running` jobs eligible for re-dispatch.
    pub fn requeue_orphaned_runs(&self) -> usize {
        let mut records = self.records.lock();
        let mut count = 0;
        for r in records.iter_mut() {
            if matches!(r.status, SchedulerJobStatus::Running) {
                r.status = SchedulerJobStatus::Pending;
                count += 1;
            }
        }
        count
    }

    /// Pause the scheduler.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// Returns `true` if currently paused.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Mark a job as starting a new run.
    ///
    /// Used by tests + the M11 cutover driver. Updates the record's
    /// `status` to `Running` and stamps `last_started_at`.
    pub fn mark_started(&self, id: &QName) {
        let mut records = self.records.lock();
        if let Some(r) = records.iter_mut().find(|r| &r.id == id) {
            r.status = SchedulerJobStatus::Running;
            r.last_started_at = Some(SystemTime::now());
        }
    }

    /// Mark a job's run as finished (success or failure).
    ///
    /// Recomputes `next_fire_at` from the job's [`Schedule`] using
    /// `SystemTime::now()` as the reference point. If the schedule has
    /// another fire upcoming (Periodic, Cron, or a Once whose instant
    /// is still in the future — which shouldn't normally happen after
    /// it has just fired), the job transitions back to `Pending` so
    /// the next [`Self::tick_at`] can pick it up. Otherwise the job
    /// stays in its terminal state (`Idle` on success,
    /// `FailedRetrying` on failure).
    pub fn mark_finished(&self, id: &QName, success: bool) {
        let now = SystemTime::now();
        let mut records = self.records.lock();
        let Some(r) = records.iter_mut().find(|r| &r.id == id) else {
            return;
        };
        r.last_finished_at = Some(now);

        // Only Periodic / Cron reschedule; Once / Manual terminate after a run.
        let next = r.schedule.next_after(now);
        let has_next =
            matches!(r.schedule, Schedule::Periodic(_) | Schedule::Cron(_)) && next.is_some();

        // Periodic / Cron jobs keep firing on schedule even after a
        // failed run; the `consecutive_failures` counter and the
        // [`crate::circuit_breaker::CircuitBreaker`] decide when to
        // stop dispatching a flapping job. A `Once` job that failed
        // stays in `FailedRetrying` since `has_next` is false.
        if has_next {
            r.status = SchedulerJobStatus::Pending;
            r.next_fire_at = next;
        } else {
            r.status = if success {
                SchedulerJobStatus::Idle
            } else {
                SchedulerJobStatus::FailedRetrying
            };
            if success {
                r.next_fire_at = None;
            }
        }

        if success {
            r.consecutive_failures = 0;
        } else {
            r.consecutive_failures = r.consecutive_failures.saturating_add(1);
        }
    }
}

impl PartialEq for SchedulerJobRecord {
    /// Two records are equal iff all their persisted state matches.
    ///
    /// Previously this compared only `id` and `status`, so two records
    /// that differed in `schedule`, `next_fire_at`, or
    /// `consecutive_failures` (i.e. genuinely-different job states)
    /// would still compare equal. The `cancel` field is intentionally
    /// excluded because it is per-process identity (an `Arc<AtomicBool>`)
    /// and is not part of the persisted record.
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.status == other.status
            && self.next_fire_at == other.next_fire_at
            && self.last_started_at == other.last_started_at
            && self.last_finished_at == other.last_finished_at
            && self.consecutive_failures == other.consecutive_failures
            && self.schedule == other.schedule
    }
}

/// Errors raised by [`SchedulerPersistence`] backends.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchedulerPersistenceError {
    /// Backend-specific failure (I/O, Cypher execution, serialization).
    #[error("scheduler persistence: {0}")]
    Backend(String),
}

/// Persistence backend for [`Scheduler`] job state.
///
/// Mirrors the meta-plugin's `Persistence` trait in shape but scoped
/// to scheduler records. The Tokio driver
/// (`crates/uni-plugin-host/src/scheduler.rs`)
/// invokes `record_started` / `record_finished` / `cancel` on each
/// lifecycle transition; on startup the driver calls `load_all` and
/// re-registers persisted jobs (followed by
/// [`Scheduler::requeue_orphaned_runs`] for any that were `Running`
/// at the previous shutdown / crash).
///
/// Two impls ship in-tree:
///
/// - [`MemoryPersistence`] — no-op tests + as the default before the
///   host wires a system-label backend.
/// - `SystemLabelPersistence` (in `uni-query`, lands with the M9
///   cutover): round-trips through `uni_system.background_jobs` via
///   the write-enabled
///   `QueryProcedureHost::execute_inner_query`.
pub trait SchedulerPersistence: Send + Sync + std::fmt::Debug {
    /// Persist a job's schedule at registration time.
    ///
    /// Called by the host wrapper (e.g. `SchedulerHost`) whenever a
    /// caller invokes `add_scheduled_job`, so the schedule kind
    /// (`Periodic` / `Cron` / `Once` / `Manual`) survives restart and
    /// can be round-tripped through [`Self::load_all`]. The default
    /// no-op suits in-memory backends and pre-existing impls that do
    /// not need durability.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerPersistenceError`] on backend failure.
    fn record_scheduled(
        &self,
        _id: &QName,
        _schedule: &Schedule,
    ) -> Result<(), SchedulerPersistenceError> {
        Ok(())
    }

    /// Persist a job's transition into a new run.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerPersistenceError`] on backend failure.
    fn record_started(
        &self,
        id: &QName,
        started_at: SystemTime,
    ) -> Result<(), SchedulerPersistenceError>;

    /// Persist the outcome of a finished run.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerPersistenceError`] on backend failure.
    fn record_finished(
        &self,
        id: &QName,
        finished_at: SystemTime,
        success: bool,
    ) -> Result<(), SchedulerPersistenceError>;

    /// Persist a cancellation.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerPersistenceError`] on backend failure.
    fn cancel(&self, id: &QName) -> Result<(), SchedulerPersistenceError>;

    /// Reload all known job records (used on host startup to restore
    /// scheduler state across restart). Order is unspecified — the
    /// driver re-registers them in any order.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerPersistenceError`] on backend failure.
    fn load_all(&self) -> Result<Vec<SchedulerJobRecord>, SchedulerPersistenceError>;

    /// Force any in-memory buffers to durable storage.
    ///
    /// Invoked by `uni.periodic.commit` so operators can drive a
    /// synchronous checkpoint flush. Backends that write through on
    /// every event (the default for the system-label backend) leave
    /// this as the default no-op; buffered backends override.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerPersistenceError`] on backend failure.
    fn flush_checkpoint(&self) -> Result<(), SchedulerPersistenceError> {
        Ok(())
    }
}

/// In-memory [`SchedulerPersistence`] backend. Always returns an empty
/// `load_all`; every other call is a no-op.
///
/// Used by tests and as the default backend when the host has not yet
/// wired a durable backend (e.g., during early `Uni::build` before the
/// storage manager is available).
#[derive(Debug, Default)]
pub struct MemoryPersistence;

impl SchedulerPersistence for MemoryPersistence {
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

    fn cancel(&self, _id: &QName) -> Result<(), SchedulerPersistenceError> {
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<SchedulerJobRecord>, SchedulerPersistenceError> {
        Ok(Vec::new())
    }
}

/// Trait-object handle to a scheduler, for cross-crate callers that
/// can't depend on the concrete host-side `SchedulerHost` type.
///
/// The built-in `uni.periodic.*` procedures hold an `Arc<dyn
/// SchedulerControl>` so they can register / cancel / list jobs
/// without depending on `uni-db`. The host crate (`uni-db`) implements
/// this on its `SchedulerHost` and passes it down at registration
/// time.
pub trait SchedulerControl: Send + Sync + std::fmt::Debug {
    /// Register a job to fire on `schedule`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::FnError`] if the registration could not be made
    /// durable. #233 Tier 1: this returned `()`, so a failed persist was
    /// logged and the caller was told the job was registered — it then
    /// vanished at the next restart.
    fn add_scheduled_job(&self, id: QName, schedule: Schedule) -> Result<(), crate::FnError>;

    /// Cancel a job by id. Returns `true` if it existed.
    ///
    /// # Errors
    ///
    /// Returns [`crate::FnError`] if the cancellation could not be made
    /// durable. #233 Tier 1: this returned a bare `bool`, so a failed
    /// persist still reported `true` and the job resurrected on restart
    /// while the caller had been told it was cancelled.
    fn cancel(&self, id: &QName) -> Result<bool, crate::FnError>;

    /// Snapshot of every known job.
    fn list(&self) -> Vec<SchedulerJobRecord>;

    /// Submit an inline write-mode Cypher body for synchronous
    /// execution. The default impl returns an error so simple
    /// scheduler primitives (without a host) can still satisfy the
    /// trait shape; the `uni-db::scheduler::SchedulerHost` override
    /// dispatches through its [`crate::traits::background::JobHost`].
    ///
    /// Used by `uni.periodic.submit(...)` and as the inner-loop body
    /// of `uni.periodic.iterate(...)`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::FnError`] when the scheduler is not wired to a
    /// Cypher-execution host (default impl) or when the submitted
    /// statement fails.
    fn submit_cypher(&self, _cypher: &str) -> Result<(), crate::FnError> {
        Err(crate::FnError::new(
            0xD20,
            "scheduler: submit_cypher not supported by this control (no host wired)",
        ))
    }

    /// Drive the persistence backend to flush its checkpoint buffer.
    ///
    /// Default impl is a no-op so the bare [`Scheduler`] (with
    /// [`MemoryPersistence`]) and any control that has no durable
    /// backend keep working without an override. The host-side
    /// `SchedulerHost` override forwards to its
    /// [`SchedulerPersistence::flush_checkpoint`].
    ///
    /// # Errors
    ///
    /// Returns [`crate::FnError`] when the persistence backend reports
    /// a flush failure.
    fn flush_checkpoint(&self) -> Result<(), crate::FnError> {
        Ok(())
    }
}

impl SchedulerControl for Scheduler {
    // The in-memory primitive has no durability to lose, so both are
    // infallible here.
    fn add_scheduled_job(&self, id: QName, schedule: Schedule) -> Result<(), crate::FnError> {
        Self::add_scheduled_job(self, id, schedule);
        Ok(())
    }

    fn cancel(&self, id: &QName) -> Result<bool, crate::FnError> {
        Ok(Self::cancel(self, id))
    }

    fn list(&self) -> Vec<SchedulerJobRecord> {
        Self::list(self)
    }
}

/// Cooperative-cancel handle handed to job implementations.
#[derive(Clone, Debug)]
pub struct SchedulerHandle {
    inner: Arc<Scheduler>,
}

impl SchedulerHandle {
    /// Wrap a scheduler in a clonable handle.
    #[must_use]
    pub fn new(scheduler: Arc<Scheduler>) -> Self {
        Self { inner: scheduler }
    }

    /// Borrow the underlying scheduler.
    #[must_use]
    pub fn scheduler(&self) -> &Scheduler {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_default_is_paused() {
        let s = Scheduler::new();
        assert!(s.is_paused());
        assert!(s.list().is_empty());
    }

    #[test]
    fn scheduler_resume_pause_round_trip() {
        let s = Scheduler::new();
        s.resume();
        assert!(!s.is_paused());
        s.pause();
        assert!(s.is_paused());
    }

    #[test]
    fn add_job_and_cancel() {
        let s = Scheduler::new();
        s.add_job(QName::builtin("ttl_sweep"));
        assert_eq!(s.list().len(), 1);
        assert!(s.cancel(&QName::builtin("ttl_sweep")));
        let recs = s.list();
        assert_eq!(recs[0].status, SchedulerJobStatus::Cancelled);
        assert!(recs[0].cancel.is_cancelled());
    }

    #[test]
    fn cancel_unknown_job_returns_false() {
        let s = Scheduler::new();
        assert!(!s.cancel(&QName::builtin("nope")));
    }

    #[test]
    fn run_lifecycle_increments_failures_then_resets() {
        let s = Scheduler::new();
        let id = QName::builtin("flaky");
        s.add_job(id.clone());

        s.mark_started(&id);
        s.mark_finished(&id, false);
        s.mark_started(&id);
        s.mark_finished(&id, false);

        let recs = s.list();
        assert_eq!(recs[0].consecutive_failures, 2);
        assert_eq!(recs[0].status, SchedulerJobStatus::FailedRetrying);

        s.mark_started(&id);
        s.mark_finished(&id, true);

        let recs = s.list();
        assert_eq!(recs[0].consecutive_failures, 0);
        assert_eq!(recs[0].status, SchedulerJobStatus::Idle);
    }

    // ── tick / driver primitive tests ──────────────────────────────

    #[test]
    fn tick_returns_empty_when_paused() {
        let s = Scheduler::new();
        s.add_job(QName::builtin("job1"));
        // Scheduler defaults to paused.
        assert!(s.tick().is_empty());
    }

    #[test]
    fn tick_dispatches_pending_jobs_when_resumed() {
        let s = Scheduler::new();
        s.add_job(QName::builtin("job1"));
        s.add_job(QName::builtin("job2"));
        s.resume();
        let due = s.tick();
        assert_eq!(due.len(), 2);
        assert!(due.iter().any(|q| q.local() == "job1"));
        assert!(due.iter().any(|q| q.local() == "job2"));
        // Each ticked job is now Running.
        assert_eq!(s.running_count(), 2);
        assert_eq!(s.pending_count(), 0);
    }

    #[test]
    fn tick_skips_cancelled_jobs() {
        let s = Scheduler::new();
        s.add_job(QName::builtin("doomed"));
        s.cancel(&QName::builtin("doomed"));
        s.resume();
        let due = s.tick();
        assert!(due.is_empty(), "cancelled job should not be dispatched");
    }

    #[test]
    fn second_tick_returns_empty_until_jobs_marked_pending() {
        let s = Scheduler::new();
        s.add_job(QName::builtin("once"));
        s.resume();
        assert_eq!(s.tick().len(), 1);
        // Without mark_finished, the job stays Running; second tick
        // doesn't redispatch.
        assert!(s.tick().is_empty());
        s.mark_finished(&QName::builtin("once"), true);
        // Now Idle, not Pending — still won't redispatch (idempotent).
        assert!(s.tick().is_empty());
    }

    #[test]
    fn requeue_orphaned_runs_moves_running_back_to_pending() {
        let s = Scheduler::new();
        s.add_job(QName::builtin("orphan"));
        s.resume();
        s.tick();
        assert_eq!(s.running_count(), 1);
        let count = s.requeue_orphaned_runs();
        assert_eq!(count, 1);
        assert_eq!(s.running_count(), 0);
        assert_eq!(s.pending_count(), 1);
        // After requeue, next tick dispatches again.
        assert_eq!(s.tick().len(), 1);
    }

    // ── Schedule semantics tests ────────────────────────────────

    #[test]
    fn schedule_once_fires_only_after_instant() {
        use std::time::Duration;
        let s = Scheduler::new();
        s.resume();
        let future = SystemTime::now() + Duration::from_secs(60);
        s.add_scheduled_job(QName::builtin("once"), Schedule::Once(future));
        let due_now = s.tick_at(SystemTime::now());
        assert!(
            due_now.is_empty(),
            "Once job should not fire before its instant"
        );
        let due_after = s.tick_at(future + Duration::from_secs(1));
        assert_eq!(due_after.len(), 1);
        assert_eq!(due_after[0].local(), "once");
    }

    #[test]
    fn schedule_once_does_not_reschedule_after_finish() {
        use std::time::Duration;
        let s = Scheduler::new();
        s.resume();
        let past = SystemTime::now() - Duration::from_secs(1);
        s.add_scheduled_job(QName::builtin("once"), Schedule::Once(past));
        let due = s.tick_at(SystemTime::now());
        assert_eq!(due.len(), 1);
        s.mark_finished(&QName::builtin("once"), true);
        let recs = s.list();
        assert_eq!(recs[0].status, SchedulerJobStatus::Idle);
        assert!(recs[0].next_fire_at.is_none());
        assert!(
            s.tick_at(SystemTime::now() + Duration::from_secs(3600))
                .is_empty()
        );
    }

    #[test]
    fn schedule_periodic_reschedules_after_finish() {
        use std::time::Duration;
        let s = Scheduler::new();
        s.resume();
        let start = SystemTime::now();
        s.add_scheduled_job(
            QName::builtin("ticker"),
            Schedule::Periodic(Duration::from_secs(10)),
        );
        assert!(s.tick_at(start + Duration::from_secs(5)).is_empty());
        let due = s.tick_at(start + Duration::from_secs(11));
        assert_eq!(due.len(), 1);
        s.mark_finished(&QName::builtin("ticker"), true);
        let recs = s.list();
        assert_eq!(recs[0].status, SchedulerJobStatus::Pending);
        assert!(recs[0].next_fire_at.is_some());
    }

    #[test]
    fn schedule_cron_emits_future_fire() {
        use std::time::Duration;
        let s = Scheduler::new();
        s.resume();
        s.add_scheduled_job(
            QName::builtin("every_min"),
            Schedule::Cron(smol_str::SmolStr::new("0 * * * * *")),
        );
        let recs = s.list();
        let next = recs[0].next_fire_at.expect("cron must produce a next fire");
        assert!(next > SystemTime::now() - Duration::from_secs(1));
    }

    #[test]
    fn manual_schedule_is_immediately_due() {
        let s = Scheduler::new();
        s.resume();
        s.add_scheduled_job(QName::builtin("legacy"), Schedule::Manual);
        let due = s.tick();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].local(), "legacy");
    }

    #[test]
    fn pending_count_and_running_count_track_lifecycle() {
        let s = Scheduler::new();
        for n in 0..5 {
            s.add_job(QName::builtin(format!("job{n}")));
        }
        s.resume();
        assert_eq!(s.pending_count(), 5);
        assert_eq!(s.running_count(), 0);
        let due = s.tick();
        assert_eq!(due.len(), 5);
        assert_eq!(s.pending_count(), 0);
        assert_eq!(s.running_count(), 5);
        s.mark_finished(&QName::builtin("job0"), true);
        s.mark_finished(&QName::builtin("job1"), false);
        assert_eq!(s.running_count(), 3, "two have finished");
    }
}
