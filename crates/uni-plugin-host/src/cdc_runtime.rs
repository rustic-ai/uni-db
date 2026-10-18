// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M11 FU-4 — change-data-capture (CDC) runtime.
//!
//! Drives every registered [`uni_plugin::traits::cdc::CdcOutputProvider`]
//! by subscribing to the commit broadcaster and converting each
//! `crate::notifications::CommitNotification` into a
//! [`uni_plugin::traits::cdc::CdcBatch`] delivered to every active
//! stream.
//!
//! ## Lifecycle
//!
//! - At `Uni::build` time, [`CdcRuntime::spawn`] takes a snapshot of
//!   the registered CDC providers, loads each provider's last
//!   committed LSN from the JSON-sidecar
//!   `<data_path>/_system/cdc_checkpoints.json`, and calls
//!   `provider.start(CdcStartContext { from_lsn })` to obtain a live
//!   [`uni_plugin::traits::cdc::CdcStream`]. The runtime spawns a
//!   tokio task that subscribes to the commit broadcaster and
//!   forwards each commit as a `CdcBatch` to every stream.
//! - Per-commit, after every stream has accepted the batch, the
//!   runtime calls `checkpoint()` on each stream and hands the
//!   returned LSN to a background checkpoint writer, which persists it
//!   to the sidecar. On restart, providers resume from that LSN.
//! - On shutdown the runtime calls `shutdown()` on each stream, waits
//!   for the checkpoint writer to persist every LSN handed to it, and
//!   exits.
//!
//! ## Checkpoint guarantees
//!
//! The persisted LSN is what a restarted provider resumes from, so it
//! must never be AHEAD of what the provider actually acknowledged:
//!
//! - **Never past a gap.** Only an LSN returned by `checkpoint()` after a
//!   successful `deliver` is ever handed to the writer, and a halted
//!   stream hands it nothing more, so the persisted LSN stays at the last
//!   contiguous delivered commit.
//! - **Never reordered.** One writer task owns the sidecar and runs one
//!   write at a time. Per provider it keeps only the LATEST submitted LSN,
//!   and submissions arrive in commit order from the single driver task,
//!   so a stale checkpoint can never overwrite a newer one.
//! - **At-least-once, not exactly-once.** The persisted LSN may lag the
//!   acknowledged one: several commits that arrive while a write is in
//!   flight coalesce into one write of the newest LSN, and a crash (or a
//!   failed write) before it lands means those commits are redelivered
//!   after a restart. This was already the contract for a failed write;
//!   coalescing only widens the window, it never lets the checkpoint
//!   skip ahead. A failed write keeps its LSNs queued (unless superseded)
//!   so the next write retries them.
//!
//! The write itself (two `fsync`s) runs on tokio's blocking pool, never on
//! an executor thread. A slow disk therefore delays only the checkpoint,
//! not every other task on the caller's runtime — on a `current_thread`
//! runtime an inline `fsync` stalls the whole runtime for as long as the
//! disk takes.
//!
//! ## Plugin calls run off the executor
//!
//! `CdcStream::deliver` / `checkpoint` / `shutdown` and
//! `CdcOutputProvider::start` are synchronous plugin calls whose duration
//! the host does not control (a Kafka or webhook sink does network I/O in
//! `deliver`). The driver task therefore owns the streams outright and
//! hands them to the blocking pool for each commit; nothing on an executor
//! thread runs plugin code or waits on a lock that plugin code holds.
//! Stream counts for diagnostics are published as atomics after each step.
//!
//! Providers whose [`CdcOutputProvider::resumes_from_checkpoint`] is
//! `false` (they ignore `from_lsn`, e.g. the built-in in-memory provider)
//! are not persisted at all.
//!
//! [`CdcOutputProvider::resumes_from_checkpoint`]: uni_plugin::traits::cdc::CdcOutputProvider::resumes_from_checkpoint
//!
//! ## Mutation delivery
//!
//! `CdcBatch::mutations` carries the actual committed mutation rows:
//! the broadcaster pre-materializes the RecordBatch on `CommitNotification`
//! when at least one `CdcOutputProvider` is registered (FU-4), using the
//! same machinery as `crate::triggers::MutationEvents`. An empty batch
//! (matching the canonical event-row schema) is delivered only as a
//! fallback when a commit carried zero rows or a provider registered
//! mid-commit (picked up on the next commit).

// Rust guideline compliant

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, broadcast, watch};
use uni_plugin::PluginRegistry;
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcStartContext, CdcStream};

use crate::notifications::CommitNotification;
use crate::shutdown::ShutdownHandle;
use uni_sidecar::VecSidecar;

/// Per-provider checkpoint row written to the JSON sidecar.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedCheckpoint {
    /// Provider name (`CdcOutputProvider::name()`).
    pub name: String,
    /// Last successfully-acknowledged LSN.
    pub last_lsn: u64,
}

/// JSON-sidecar checkpoint store at
/// `<data_path>/_system/cdc_checkpoints.json`.
#[derive(Clone, Debug)]
pub struct CdcCheckpointSidecar {
    sidecar: VecSidecar<PersistedCheckpoint>,
    /// Test-only hook run at the start of every write, used to simulate a
    /// slow disk.
    #[cfg(test)]
    before_write: Option<tests::WriteHook>,
}

impl CdcCheckpointSidecar {
    /// Construct rooted at `<data_path>/_system/cdc_checkpoints.json`.
    #[must_use]
    pub fn new(data_path: PathBuf) -> Self {
        Self {
            sidecar: VecSidecar::new(data_path, "cdc_checkpoints.json"),
            #[cfg(test)]
            before_write: None,
        }
    }

    /// Borrow the sidecar path (for diagnostics).
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        self.sidecar.path()
    }

    /// Load all persisted checkpoints. Returns an empty vec if the
    /// sidecar doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on I/O or parse failure.
    pub fn load_all(&self) -> Result<Vec<PersistedCheckpoint>, String> {
        self.sidecar.load().map_err(|e| e.to_string())
    }

    /// Write the full checkpoint set atomically.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on I/O failure.
    pub fn write_all(&self, rows: &[PersistedCheckpoint]) -> Result<(), String> {
        #[cfg(test)]
        if let Some(hook) = &self.before_write {
            (hook.0)(rows);
        }
        self.sidecar.store(rows).map_err(|e| e.to_string())
    }

    /// Look up the persisted LSN for a single provider.
    ///
    /// `Ok(None)` means the provider has genuinely never checkpointed.
    ///
    /// # Errors
    ///
    /// Returns an error if the sidecar cannot be read. #233 Tier 1: this was
    /// `.ok()`, so an unreadable or corrupt sidecar was indistinguishable
    /// from "never checkpointed" and the provider silently resumed from the
    /// wrong point — replaying delivered commits, or skipping undelivered
    /// ones, with no signal either way.
    pub fn lookup(&self, name: &str) -> Result<Option<CdcLsn>, String> {
        Ok(self
            .load_all()?
            .into_iter()
            .find(|r| r.name == name)
            .map(|r| CdcLsn(r.last_lsn)))
    }

    /// Replace a single provider's LSN, leaving other providers
    /// unchanged. Reads-modify-writes the full sidecar atomically.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on I/O or parse failure.
    pub fn write_one(&self, name: &str, lsn: CdcLsn) -> Result<(), String> {
        self.write_many(std::iter::once((name, lsn)))
    }

    /// Replace several providers' LSNs in one atomic read-modify-write,
    /// leaving every other provider's row unchanged.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on I/O or parse failure; nothing is
    /// written in that case.
    pub fn write_many<'a>(
        &self,
        updates: impl IntoIterator<Item = (&'a str, CdcLsn)>,
    ) -> Result<(), String> {
        let mut rows = self.load_all()?;
        for (name, lsn) in updates {
            if let Some(row) = rows.iter_mut().find(|r| r.name == name) {
                row.last_lsn = lsn.0;
            } else {
                rows.push(PersistedCheckpoint {
                    name: name.to_owned(),
                    last_lsn: lsn.0,
                });
            }
        }
        self.write_all(&rows)
    }
}

/// Mutable state of the [`CheckpointWriter`], guarded by a lock that is only
/// ever held for map operations — never across I/O or an `.await`.
#[derive(Default)]
struct WriterState {
    /// Latest LSN submitted per provider and not yet persisted.
    pending: BTreeMap<String, CdcLsn>,
    /// Number of submissions so far; [`CdcRuntime::flush_checkpoints`] waits
    /// for the writer to cover the value it observes here.
    submitted: u64,
    /// Set once the driver exits; the writer drains `pending` and stops.
    closed: bool,
}

/// Persists CDC checkpoints off the async executor.
///
/// One writer per [`CdcRuntime`]. The driver task [`submit`](Self::submit)s
/// an LSN (a map insert — no I/O) and the writer task, looping in
/// [`run`](Self::run), writes the latest LSN per provider on the blocking
/// pool, one write at a time. See the module docs for the guarantees.
struct CheckpointWriter {
    sidecar: CdcCheckpointSidecar,
    state: Mutex<WriterState>,
    /// Wakes the writer task after a submission or on close. `Notify` keeps a
    /// permit when nobody is waiting, so a wake-up during a write is not lost.
    wake: Notify,
    /// Highest submission count whose write has been attempted; `u64::MAX`
    /// once the writer task has exited.
    attempted: watch::Sender<u64>,
}

impl CheckpointWriter {
    fn new(sidecar: CdcCheckpointSidecar) -> Self {
        Self {
            sidecar,
            state: Mutex::new(WriterState::default()),
            wake: Notify::new(),
            attempted: watch::Sender::new(0),
        }
    }

    /// Queue `lsn` as `name`'s newest checkpoint, superseding any queued one.
    fn submit(&self, name: &str, lsn: CdcLsn) {
        {
            let mut state = self.state.lock();
            state.pending.insert(name.to_owned(), lsn);
            state.submitted += 1;
        }
        self.wake.notify_one();
    }

    /// Stop accepting work: the writer persists what is queued, then exits.
    fn close(&self) {
        self.state.lock().closed = true;
        self.wake.notify_one();
    }

    /// Writer task body: persist queued checkpoints until closed.
    async fn run(self: Arc<Self>) {
        loop {
            let (batch, generation, closed) = {
                let mut state = self.state.lock();
                (
                    std::mem::take(&mut state.pending),
                    state.submitted,
                    state.closed,
                )
            };
            if !batch.is_empty() {
                self.write(batch).await;
            }
            self.attempted.send_replace(generation);
            if closed {
                break;
            }
            self.wake.notified().await;
        }
        self.attempted.send_replace(u64::MAX);
    }

    /// Write `batch` on the blocking pool; on failure, re-queue every entry
    /// that no newer submission has superseded.
    async fn write(&self, batch: BTreeMap<String, CdcLsn>) {
        let sidecar = self.sidecar.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let result = sidecar.write_many(batch.iter().map(|(name, lsn)| (name.as_str(), *lsn)));
            (batch, result)
        })
        .await;
        let (batch, error) = match outcome {
            Ok((_, Ok(()))) => return,
            Ok((batch, Err(e))) => (Some(batch), e),
            // The blocking task panicked or was cancelled; its batch is gone,
            // so those commits are redelivered after a restart.
            Err(e) => (None, e.to_string()),
        };
        // #233 P3: was `debug!`, i.e. below the default level, so a lost
        // checkpoint was invisible in a normal deployment. Not a gap — the
        // commits WERE delivered — but they will be redelivered after a
        // restart, which is at-least-once and worth saying.
        tracing::warn!(
            error = %error,
            "CdcRuntime: checkpoint write failed; the affected commits will be redelivered \
             after a restart (at-least-once) unless a later write succeeds",
        );
        if let Some(batch) = batch {
            let mut state = self.state.lock();
            for (name, lsn) in batch {
                state.pending.entry(name).or_insert(lsn);
            }
        }
    }

    /// Wait until every checkpoint submitted so far has had its write attempted.
    async fn flush(&self) {
        let target = self.state.lock().submitted;
        let mut rx = self.attempted.subscribe();
        // The sender lives as long as `self`, so `wait_for` cannot fail here;
        // a closed writer reports `u64::MAX`, which satisfies any target.
        let _ = rx.wait_for(|attempted| *attempted >= target).await;
    }
}

/// Wraps a live CDC stream with the provider's name and most-recent
/// committed LSN.
struct ActiveStream {
    name: String,
    stream: Box<dyn CdcStream>,
    /// Whether this stream's checkpoints are persisted — the provider's
    /// `resumes_from_checkpoint()`. Persisting a checkpoint nothing reads back
    /// would cost two `fsync`s per commit for no benefit.
    persist: bool,
    /// Set once a `deliver` fails, or the commit broadcaster drops commits
    /// (`Lagged`), for this stream. A halted stream is never delivered to or
    /// checkpointed again — its checkpoint therefore stays at the last
    /// CONTIGUOUS delivered commit rather than silently advancing past the gap.
    /// There is no redelivery path, so continuing would create a permanent,
    /// undetectable hole in the CDC feed.
    halted: bool,
}

/// Halt `active`, counting the transition so an operator can see it.
///
/// #233 P2: `halted` was a private `bool` whose only reader outside this
/// module was `halted_stream_count()`, which nothing outside the crate called.
/// A halted feed is correct-but-stopped and was therefore invisible. Counting
/// only the `false -> true` edge keeps the total meaningful when
/// `halt_all_streams` sweeps a list that is already partly halted.
fn halt(active: &mut ActiveStream, reason: &'static str) {
    if active.halted {
        return;
    }
    active.halted = true;
    metrics::counter!("uni_cdc_stream_halted_total").increment(1);
    tracing::error!(
        provider = %active.name,
        reason,
        "CdcRuntime: stream halted; its checkpoint stays at the last contiguous commit and it \
         will not resume without a restart",
    );
}

/// Resume `provider` from its persisted LSN and start its stream.
///
/// Returns the [`ActiveStream`] on success, or `None` (logged) on failure so
/// the caller skips it. Shared by [`CdcRuntime::spawn`] (`late = false`) and
/// [`CdcRuntime::discover_new_providers`] (`late = true`); the only difference
/// is the log wording.
fn start_stream(
    checkpoint: Option<&CdcCheckpointSidecar>,
    name: &str,
    provider: &Arc<dyn uni_plugin::traits::cdc::CdcOutputProvider>,
    late: bool,
) -> Option<ActiveStream> {
    // #233 Tier 1: a failed checkpoint read must not become `None`. `None`
    // tells the provider "you have never checkpointed", so it resumes from the
    // beginning (replaying delivered commits) or from the live tail (skipping
    // undelivered ones). Refusing to start is the safe answer: a provider that
    // is not started never checkpoints, so its persisted LSN stays put and a
    // later successful start resumes from the right place.
    let from_lsn = match checkpoint.map(|c| c.lookup(name)).transpose() {
        Ok(lsn) => lsn.flatten(),
        Err(e) => {
            tracing::error!(
                provider = %name,
                error = %e,
                "CdcRuntime: checkpoint lookup failed; refusing to start the provider rather \
                 than resuming from an unknown position",
            );
            return None;
        }
    };
    match provider.start(CdcStartContext::new(from_lsn)) {
        Ok(stream) => {
            let kind = if late {
                "late-registered provider"
            } else {
                "provider"
            };
            tracing::info!(provider = %name, from_lsn = ?from_lsn, "CdcRuntime: {kind} started");
            Some(ActiveStream {
                name: name.to_owned(),
                stream,
                persist: provider.resumes_from_checkpoint(),
                halted: false,
            })
        }
        Err(e) => {
            let detail = if late {
                "late-registered provider start failed"
            } else {
                "provider start failed; skipping"
            };
            tracing::warn!(provider = %name, error = %e, "CdcRuntime: {detail}");
            None
        }
    }
}

/// Host-side CDC runtime that drives every registered provider on
/// the commit broadcaster.
///
/// One per `Uni` instance. Constructed by [`Self::spawn`] in
/// `Uni::build`; the running background task exits when
/// `ShutdownHandle` signals shutdown.
pub struct CdcRuntime {
    /// Number of streams the driver holds, published after each step.
    ///
    /// The streams themselves are owned by the driver task (see the module
    /// docs), so diagnostics read these counts instead of a shared lock.
    active_count: AtomicUsize,
    /// Number of those streams that are halted, published with
    /// [`Self::active_count`].
    halted_count: AtomicUsize,
    /// Checkpoint sidecar (`None` when no local data path).
    checkpoint: Option<CdcCheckpointSidecar>,
    /// Background writer for `checkpoint` (`None` exactly when it is).
    writer: Option<Arc<CheckpointWriter>>,
    /// Shared plugin registry — consulted on every commit to discover
    /// providers registered *after* `Uni::build` returned (e.g., via
    /// `Uni::add_plugin`).
    registry: Arc<PluginRegistry>,
}

impl std::fmt::Debug for CdcRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcRuntime")
            .field("active_streams", &self.active_stream_count())
            .field(
                "checkpoint_path",
                &self.checkpoint.as_ref().map(|c| c.path().to_path_buf()),
            )
            .finish()
    }
}

impl CdcRuntime {
    /// Construct and spawn the CDC runtime.
    ///
    /// Snapshots every registered [`uni_plugin::traits::cdc::CdcOutputProvider`],
    /// resumes each from its last persisted LSN (via the sidecar at
    /// `<data_path>/_system/cdc_checkpoints.json`), and starts a tokio
    /// task that delivers each commit notification to every active
    /// stream.
    ///
    /// When no providers are registered, this is a no-op fast path —
    /// the background task is still spawned so dynamic
    /// `Uni::add_plugin` registrations land as a future improvement,
    /// but it currently subscribes once at startup.
    #[must_use]
    pub fn spawn(
        registry: &Arc<PluginRegistry>,
        commit_rx: broadcast::Receiver<Arc<CommitNotification>>,
        data_path: Option<PathBuf>,
        shutdown: &ShutdownHandle,
    ) -> Arc<Self> {
        Self::spawn_with_sidecar(
            registry,
            commit_rx,
            data_path.map(CdcCheckpointSidecar::new),
            shutdown,
        )
    }

    /// [`Self::spawn`] with an explicit checkpoint sidecar (tests inject a
    /// slow one).
    fn spawn_with_sidecar(
        registry: &Arc<PluginRegistry>,
        mut commit_rx: broadcast::Receiver<Arc<CommitNotification>>,
        checkpoint: Option<CdcCheckpointSidecar>,
        shutdown: &ShutdownHandle,
    ) -> Arc<Self> {
        let mut active: Vec<ActiveStream> = Vec::new();
        for (name, provider) in registry.cdc_outputs_snapshot() {
            if let Some(stream) = start_stream(checkpoint.as_ref(), name.as_str(), &provider, false)
            {
                active.push(stream);
            }
        }

        let writer = checkpoint
            .clone()
            .map(|sidecar| Arc::new(CheckpointWriter::new(sidecar)));
        let writer_handle = writer.as_ref().map(|w| tokio::spawn(Arc::clone(w).run()));
        let runtime = Arc::new(Self {
            active_count: AtomicUsize::new(0),
            halted_count: AtomicUsize::new(0),
            checkpoint,
            writer,
            registry: Arc::clone(registry),
        });
        runtime.publish_counts(&active);

        // Spawn the driver task. It owns the streams; every step that calls
        // into a plugin runs on the blocking pool via `off_executor`.
        let runtime_clone = Arc::clone(&runtime);
        let mut shutdown_rx = shutdown.subscribe();
        let handle = tokio::spawn(async move {
            let mut streams = active;
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => {
                        runtime_clone
                            .off_executor(streams, |rt, s| rt.shutdown_streams(s))
                            .await;
                        break;
                    }
                    next = commit_rx.recv() => match next {
                        Ok(notif) => {
                            let delivered = runtime_clone
                                .off_executor(streams, move |rt, s| rt.deliver_commit(s, &notif))
                                .await;
                            match delivered {
                                Some(s) => streams = s,
                                None => break,
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                lagged = n,
                                "CdcRuntime: commit broadcaster lagged; halting all streams \
                                 (the dropped commits cannot be redelivered, so continuing would \
                                 leave a silent gap in every feed)",
                            );
                            halt_all_streams(&mut streams);
                            runtime_clone.publish_counts(&streams);
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
            // No more submissions can arrive: let the writer persist what is
            // queued and wait for it, so a tracked shutdown covers the final
            // checkpoints too.
            if let Some(writer) = &runtime_clone.writer {
                writer.close();
            }
            if let Some(handle) = writer_handle
                && let Err(e) = handle.await
            {
                tracing::error!(error = %e, "CdcRuntime: checkpoint writer did not exit cleanly");
            }
        });
        shutdown.track_task(handle);

        runtime
    }

    /// Number of currently-active CDC streams (for diagnostics + tests).
    ///
    /// As of the driver's last completed step: a commit being delivered is
    /// reflected once its delivery returns.
    #[must_use]
    pub fn active_stream_count(&self) -> usize {
        self.active_count.load(Ordering::Acquire)
    }

    /// Number of streams currently halted after a deliver failure or a
    /// broadcaster lag (for diagnostics + tests). Same freshness as
    /// [`Self::active_stream_count`].
    #[must_use]
    pub fn halted_stream_count(&self) -> usize {
        self.halted_count.load(Ordering::Acquire)
    }

    /// Publish `streams`' counts for [`Self::active_stream_count`] and
    /// [`Self::halted_stream_count`].
    fn publish_counts(&self, streams: &[ActiveStream]) {
        let halted = streams.iter().filter(|s| s.halted).count();
        self.halted_count.store(halted, Ordering::Release);
        self.active_count.store(streams.len(), Ordering::Release);
    }

    /// Run `step` over the driver's streams on the blocking pool.
    ///
    /// Returns the streams back, or `None` if the blocking task did not
    /// complete (the runtime is shutting down, or `step` panicked outside a
    /// plugin call); the streams are then gone and the driver stops.
    async fn off_executor<F>(
        self: &Arc<Self>,
        mut streams: Vec<ActiveStream>,
        step: F,
    ) -> Option<Vec<ActiveStream>>
    where
        F: FnOnce(&Self, &mut Vec<ActiveStream>) + Send + 'static,
    {
        let rt = Arc::clone(self);
        let joined = tokio::task::spawn_blocking(move || {
            step(&rt, &mut streams);
            streams
        })
        .await;
        match joined {
            Ok(streams) => {
                self.publish_counts(&streams);
                Some(streams)
            }
            Err(e) => {
                metrics::counter!("uni_cdc_stream_halted_total")
                    .increment(self.active_stream_count() as u64);
                self.publish_counts(&[]);
                tracing::error!(
                    error = %e,
                    "CdcRuntime: a delivery step did not complete; CDC has stopped for every \
                     stream and will not resume without a restart",
                );
                None
            }
        }
    }

    /// Borrow the checkpoint sidecar, if local-disk persistence is
    /// enabled. Used by tests to assert on persisted LSN.
    #[must_use]
    pub fn checkpoint_sidecar(&self) -> Option<&CdcCheckpointSidecar> {
        self.checkpoint.as_ref()
    }

    /// Wait until every checkpoint handed to the background writer so far has
    /// been written (or its write attempted and logged as failed).
    ///
    /// Checkpoints are persisted asynchronously, off the commit path; callers
    /// that read [`Self::checkpoint_sidecar`] right after a commit was
    /// delivered await this first. Returns immediately when persistence is
    /// disabled or the runtime has shut down.
    pub async fn flush_checkpoints(&self) {
        if let Some(writer) = &self.writer {
            writer.flush().await;
        }
    }

    /// Discover any providers registered after `Uni::build` (e.g.,
    /// via `Uni::add_plugin`) and start a stream for each one. Called
    /// at the start of every `deliver_commit` so dynamic
    /// registrations don't miss any commits past the first.
    fn discover_new_providers(&self, streams: &mut Vec<ActiveStream>) {
        let snapshot = self.registry.cdc_outputs_snapshot();
        for (name, provider) in snapshot {
            if streams.iter().any(|s| s.name == name.as_str()) {
                continue;
            }
            if let Some(stream) =
                start_stream(self.checkpoint.as_ref(), name.as_str(), &provider, true)
            {
                streams.push(stream);
            }
        }
    }

    /// Convert a single [`CommitNotification`] into a [`CdcBatch`] and
    /// deliver it to every active stream, then checkpoint each
    /// stream and persist the new LSN to the sidecar.
    ///
    /// Runs on the blocking pool (see [`Self::off_executor`]): every call in
    /// here into a stream is a synchronous plugin call.
    fn deliver_commit(&self, streams: &mut Vec<ActiveStream>, notif: &CommitNotification) {
        self.discover_new_providers(streams);
        // FU-4: the broadcaster pre-materializes the mutation RecordBatch
        // when at least one `CdcOutputProvider` is registered (see
        // `Transaction::commit`). `None` here means either there were
        // zero rows or the broadcaster ran without CDC subscribers
        // (race: provider registered between the snapshot and now —
        // discover_new_providers above picks them up for the *next*
        // commit). Fall back to an empty batch matching the canonical
        // event-row schema so downstream filters see consistent
        // column types.
        // #233 Tier 1: a commit whose mutations could not be materialized must
        // not be delivered as an empty batch and checkpointed past — that is a
        // silent feed hole with the LSN range advanced over it. Halt instead,
        // which is the same contract `deliver` failure already follows: the
        // checkpoint stays at the last contiguous commit so the gap is visible.
        if notif.mutations_failed {
            for active in streams.iter_mut() {
                if active.halted {
                    continue;
                }
                tracing::error!(
                    provider = %active.name,
                    version = notif.version,
                    "CdcRuntime: commit mutations could not be materialized; halting stream to \
                     avoid a silent feed gap (its checkpoint stays at the last delivered commit)",
                );
                halt(active, "mutations could not be materialized");
            }
            return;
        }
        let mutations = notif.mutations.clone().unwrap_or_else(|| {
            Arc::new(arrow_array::RecordBatch::new_empty(
                crate::triggers::event_row_schema(),
            ))
        });
        let batch = CdcBatch {
            lsn_start: CdcLsn(notif.causal_version),
            lsn_end: CdcLsn(notif.version),
            mutations,
            commit_timestamp: SystemTime::now(),
        };

        for active in streams.iter_mut() {
            // Never deliver to (or checkpoint) a halted stream — its checkpoint
            // must stay at the last contiguous commit so the gap is visible, not
            // papered over by a later successful commit.
            if active.halted {
                continue;
            }
            // A panicking plugin halts its own stream rather than unwinding
            // through the driver and taking every other stream with it.
            let delivered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                active.stream.deliver(&batch)
            }));
            match delivered {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        provider = %active.name,
                        error = %e,
                        "CdcRuntime: deliver failed; halting stream to avoid a silent feed gap \
                         (its checkpoint stays at the last delivered commit)",
                    );
                    halt(active, "deliver failed");
                    continue;
                }
                Err(_) => {
                    halt(active, "deliver panicked");
                    continue;
                }
            }
            let checkpointed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                active.stream.checkpoint()
            }));
            let Ok(checkpointed) = checkpointed else {
                halt(active, "checkpoint panicked");
                continue;
            };
            match checkpointed {
                Ok(lsn) => {
                    // Queue only — the fsyncing write runs on the writer
                    // task's own blocking-pool call, serialized with the
                    // other checkpoint writes.
                    if active.persist
                        && let Some(writer) = &self.writer
                    {
                        writer.submit(&active.name, lsn);
                    }
                }
                Err(e) => tracing::warn!(
                    provider = %active.name,
                    error = %e,
                    "CdcRuntime: checkpoint failed",
                ),
            }
        }
    }

    /// Call `shutdown()` on every active stream and drop them.
    ///
    /// Runs on the blocking pool (see [`Self::off_executor`]).
    fn shutdown_streams(&self, streams: &mut Vec<ActiveStream>) {
        for active in streams.iter_mut() {
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| active.stream.shutdown()));
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(
                    provider = %active.name,
                    error = %e,
                    "CdcRuntime: shutdown failed",
                ),
                Err(_) => tracing::warn!(provider = %active.name, "CdcRuntime: shutdown panicked"),
            }
        }
        streams.clear();
    }
}

/// Halt every stream — used when the commit broadcaster lagged and dropped
/// commits that cannot be redelivered.
fn halt_all_streams(streams: &mut [ActiveStream]) {
    for active in streams.iter_mut() {
        halt(active, "broadcaster lagged");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use uni_plugin::errors::FnError;
    use uni_plugin::traits::cdc::CdcOutputProvider;
    use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

    /// Signature of a [`WriteHook`] callback.
    type WriteHookFn = dyn Fn(&[PersistedCheckpoint]) + Send + Sync;

    /// Callback run with the rows about to be written, before the write.
    #[derive(Clone)]
    pub(super) struct WriteHook(pub(super) Arc<WriteHookFn>);

    impl std::fmt::Debug for WriteHook {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("WriteHook")
        }
    }

    /// Provider whose stream acknowledges every delivered batch.
    struct AckProvider {
        name: &'static str,
        resumes: bool,
    }

    struct AckStream {
        last: CdcLsn,
    }

    impl CdcOutputProvider for AckProvider {
        fn name(&self) -> &str {
            self.name
        }
        fn start(&self, _ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
            Ok(Box::new(AckStream { last: CdcLsn(0) }))
        }
        fn resumes_from_checkpoint(&self) -> bool {
            self.resumes
        }
    }

    impl CdcStream for AckStream {
        fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
            self.last = batch.lsn_end;
            Ok(())
        }
        fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
            Ok(self.last)
        }
        fn shutdown(&mut self) -> Result<(), FnError> {
            Ok(())
        }
    }

    fn registry(providers: Vec<AckProvider>) -> Arc<PluginRegistry> {
        let registry = Arc::new(PluginRegistry::new());
        let caps = CapabilitySet::from_iter_of([Capability::Cdc]);
        let mut r = PluginRegistrar::new(PluginId::new("test"), &caps, &registry);
        for p in providers {
            r.cdc_output(Arc::new(p)).expect("register cdc_output");
        }
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
            mutations: None,
            mutations_failed: false,
            dropped_before: 0,
        })
    }

    /// A sidecar whose every write first runs `hook`.
    fn hooked_sidecar(
        root: &std::path::Path,
        hook: impl Fn(&[PersistedCheckpoint]) + Send + Sync + 'static,
    ) -> CdcCheckpointSidecar {
        let mut sidecar = CdcCheckpointSidecar::new(root.to_path_buf());
        sidecar.before_write = Some(WriteHook(Arc::new(hook)));
        sidecar
    }

    /// Yield until the driver has handed `n` checkpoints to the writer.
    async fn wait_for_submissions(runtime: &CdcRuntime, n: u64) {
        let writer = runtime.writer.as_ref().expect("persistence enabled");
        tokio::time::timeout(Duration::from_secs(10), async {
            while writer.state.lock().submitted < n {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the driver never submitted the expected checkpoints");
    }

    /// A slow checkpoint write must not stall other tasks on the runtime.
    ///
    /// `#[tokio::test]` is a `current_thread` runtime: one thread polls every
    /// task. The write used to run inline in the driver task, so a disk whose
    /// `fsync` took 13-30 s stalled every other task for that long (seen as a
    /// 30 s timeout on a ~10 ms query). Here the write sleeps for `SLOW` and an
    /// unrelated task must finish its short timer loop while the write is
    /// still in flight.
    #[tokio::test]
    async fn slow_checkpoint_write_does_not_stall_the_runtime() {
        const SLOW: Duration = Duration::from_secs(3);
        let tmp = TempDir::new().unwrap();
        let started = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let (s, f) = (Arc::clone(&started), Arc::clone(&finished));
        let sidecar = hooked_sidecar(tmp.path(), move |_| {
            s.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(SLOW);
            f.fetch_add(1, Ordering::SeqCst);
        });
        let registry = registry(vec![AckProvider {
            name: "durable",
            resumes: true,
        }]);
        let (tx, rx) = broadcast::channel(16);
        let shutdown = ShutdownHandle::new(Duration::from_secs(10));
        let runtime = CdcRuntime::spawn_with_sidecar(&registry, rx, Some(sidecar), &shutdown);

        tx.send(commit(100)).unwrap();
        let t0 = Instant::now();
        // Unrelated work on the same runtime: 20 short timer waits. Each wait
        // yields, so the CDC driver runs (and starts the write) in between.
        let unrelated = tokio::spawn(async {
            for _ in 0..20 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        unrelated.await.unwrap();
        let elapsed = t0.elapsed();

        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "control: the checkpoint write must have started during the window"
        );
        assert!(
            elapsed < SLOW / 2,
            "an unrelated task took {elapsed:?} with a {SLOW:?} checkpoint write in flight: \
             the write is blocking the runtime"
        );
        assert_eq!(
            finished.load(Ordering::SeqCst),
            0,
            "the unrelated task must finish while the write is still in flight"
        );

        runtime.flush_checkpoints().await;
        assert_eq!(
            runtime
                .checkpoint_sidecar()
                .unwrap()
                .lookup("durable")
                .unwrap(),
            Some(CdcLsn(100)),
            "the checkpoint is still persisted, just off the executor"
        );
        shutdown.shutdown_async().await.unwrap();
    }

    /// Provider whose stream's `deliver` blocks its thread for `delay`, as a
    /// network sink does.
    struct SlowDeliverProvider {
        delay: Duration,
        started: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
    }

    struct SlowDeliverStream {
        delay: Duration,
        started: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
        last: CdcLsn,
    }

    impl CdcOutputProvider for SlowDeliverProvider {
        fn name(&self) -> &str {
            "slow-sink"
        }
        fn start(&self, _ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
            Ok(Box::new(SlowDeliverStream {
                delay: self.delay,
                started: Arc::clone(&self.started),
                finished: Arc::clone(&self.finished),
                last: CdcLsn(0),
            }))
        }
        fn resumes_from_checkpoint(&self) -> bool {
            false
        }
    }

    impl CdcStream for SlowDeliverStream {
        fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
            self.started.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(self.delay);
            self.last = batch.lsn_end;
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
            Ok(self.last)
        }
        fn shutdown(&mut self) -> Result<(), FnError> {
            Ok(())
        }
    }

    /// A slow plugin `deliver` must not stall other tasks on the runtime.
    ///
    /// Same shape as the slow-checkpoint test above, one step earlier: the
    /// driver called the plugin's synchronous `deliver` inline (under the
    /// `streams` lock), so on a `current_thread` runtime a sink that took
    /// `SLOW` to accept a batch stalled every other task for that long.
    #[tokio::test]
    async fn slow_plugin_deliver_does_not_stall_the_runtime() {
        const SLOW: Duration = Duration::from_secs(2);
        let started = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let registry = Arc::new(PluginRegistry::new());
        let caps = CapabilitySet::from_iter_of([Capability::Cdc]);
        let mut r = PluginRegistrar::new(PluginId::new("test"), &caps, &registry);
        r.cdc_output(Arc::new(SlowDeliverProvider {
            delay: SLOW,
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
        }))
        .expect("register cdc_output");
        r.commit_to_registry().expect("commit");
        let (tx, rx) = broadcast::channel(16);
        let shutdown = ShutdownHandle::new(Duration::from_secs(10));
        let runtime = CdcRuntime::spawn(&registry, rx, None, &shutdown);

        tx.send(commit(100)).unwrap();
        let t0 = Instant::now();
        let unrelated = tokio::spawn(async {
            for _ in 0..20 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        unrelated.await.unwrap();
        let elapsed = t0.elapsed();

        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "control: the delivery must have started during the window"
        );
        assert!(
            elapsed < SLOW / 2,
            "an unrelated task took {elapsed:?} with a {SLOW:?} plugin deliver in flight: \
             the delivery is blocking the runtime"
        );
        assert_eq!(
            finished.load(Ordering::SeqCst),
            0,
            "the unrelated task must finish while the delivery is still in flight"
        );

        shutdown.shutdown_async().await.unwrap();
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert_eq!(
            runtime.active_stream_count(),
            0,
            "shutdown drops every stream"
        );
    }

    /// Commits queued behind a slow write coalesce, and the persisted LSN only
    /// ever moves forward and ends at the newest acknowledged commit.
    #[tokio::test]
    async fn checkpoint_writes_coalesce_and_never_go_backwards() {
        let tmp = TempDir::new().unwrap();
        let written = Arc::new(parking_lot::Mutex::new(Vec::<u64>::new()));
        let w = Arc::clone(&written);
        let sidecar = hooked_sidecar(tmp.path(), move |rows| {
            std::thread::sleep(Duration::from_millis(200));
            let lsn = rows.iter().find(|r| r.name == "durable").unwrap().last_lsn;
            w.lock().push(lsn);
        });
        let registry = registry(vec![AckProvider {
            name: "durable",
            resumes: true,
        }]);
        let (tx, rx) = broadcast::channel(64);
        let shutdown = ShutdownHandle::new(Duration::from_secs(10));
        let runtime = CdcRuntime::spawn_with_sidecar(&registry, rx, Some(sidecar), &shutdown);

        for v in 1..=20 {
            tx.send(commit(v)).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        runtime.flush_checkpoints().await;

        let written = written.lock().clone();
        assert!(
            written.windows(2).all(|w| w[0] < w[1]),
            "a stale checkpoint overwrote a newer one: {written:?}"
        );
        assert_eq!(written.last(), Some(&20), "writes: {written:?}");
        assert!(
            written.len() < 20,
            "commits queued behind a slow write must coalesce: {written:?}"
        );
        assert_eq!(
            runtime
                .checkpoint_sidecar()
                .unwrap()
                .lookup("durable")
                .unwrap(),
            Some(CdcLsn(20))
        );
        shutdown.shutdown_async().await.unwrap();
    }

    /// Shutdown waits for queued checkpoints, so a clean close loses none.
    #[tokio::test]
    async fn shutdown_persists_queued_checkpoints() {
        let tmp = TempDir::new().unwrap();
        let sidecar = hooked_sidecar(tmp.path(), |_| {
            std::thread::sleep(Duration::from_millis(300));
        });
        let registry = registry(vec![AckProvider {
            name: "durable",
            resumes: true,
        }]);
        let (tx, rx) = broadcast::channel(16);
        let shutdown = ShutdownHandle::new(Duration::from_secs(10));
        let runtime = CdcRuntime::spawn_with_sidecar(&registry, rx, Some(sidecar), &shutdown);

        tx.send(commit(7)).unwrap();
        tx.send(commit(8)).unwrap();
        // Let the driver deliver both before shutdown is signalled (the driver
        // prefers shutdown over pending commits).
        wait_for_submissions(&runtime, 2).await;
        shutdown.shutdown_async().await.unwrap();

        let fresh = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
        assert_eq!(fresh.lookup("durable").unwrap(), Some(CdcLsn(8)));
    }

    /// A provider that ignores `from_lsn` is not persisted; one that honors it
    /// is.
    #[tokio::test]
    async fn non_resuming_provider_skips_the_checkpoint_write() {
        let tmp = TempDir::new().unwrap();
        let registry = registry(vec![
            AckProvider {
                name: "ephemeral",
                resumes: false,
            },
            AckProvider {
                name: "durable",
                resumes: true,
            },
        ]);
        let (tx, rx) = broadcast::channel(16);
        let shutdown = ShutdownHandle::new(Duration::from_secs(10));
        let runtime = CdcRuntime::spawn(&registry, rx, Some(tmp.path().to_path_buf()), &shutdown);

        tx.send(commit(5)).unwrap();
        wait_for_submissions(&runtime, 1).await;
        runtime.flush_checkpoints().await;

        let sidecar = runtime.checkpoint_sidecar().unwrap();
        assert_eq!(sidecar.lookup("durable").unwrap(), Some(CdcLsn(5)));
        assert_eq!(sidecar.lookup("ephemeral").unwrap(), None);
        shutdown.shutdown_async().await.unwrap();
    }

    #[test]
    fn checkpoint_sidecar_round_trip() {
        let tmp = TempDir::new().unwrap();
        let s = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
        assert!(s.load_all().unwrap().is_empty());
        s.write_one("kafka", CdcLsn(42)).unwrap();
        s.write_one("pulsar", CdcLsn(7)).unwrap();
        let rows = s.load_all().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(s.lookup("kafka").unwrap(), Some(CdcLsn(42)));
        assert_eq!(s.lookup("pulsar").unwrap(), Some(CdcLsn(7)));
    }

    #[test]
    fn checkpoint_sidecar_survives_close_reopen() {
        let tmp = TempDir::new().unwrap();
        {
            let s = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
            s.write_one("kafka", CdcLsn(99)).unwrap();
        }
        let s2 = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
        assert_eq!(s2.lookup("kafka").unwrap(), Some(CdcLsn(99)));
    }

    #[test]
    fn checkpoint_sidecar_overwrites_existing_provider() {
        let tmp = TempDir::new().unwrap();
        let s = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
        s.write_one("kafka", CdcLsn(1)).unwrap();
        s.write_one("kafka", CdcLsn(2)).unwrap();
        s.write_one("kafka", CdcLsn(3)).unwrap();
        assert_eq!(s.lookup("kafka").unwrap(), Some(CdcLsn(3)));
        assert_eq!(s.load_all().unwrap().len(), 1);
    }
}
