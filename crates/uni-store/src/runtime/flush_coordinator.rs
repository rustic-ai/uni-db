// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Async-flush coordination.
//!
//! Bounds the number of in-flight L0→L1 flushes (via a semaphore),
//! assigns rotate-order sequence numbers, and serializes finalize so
//! the manifest parent-chain stays consistent.
//!
//! ## Architecture
//!
//! ```text
//! Writer
//!   ├── flush_lock              (brief: rotate + finalize)
//!   └── flush_coordinator
//!         ├── permits: Semaphore(max_pending_flushes)
//!         ├── next_seq: AtomicU64
//!         └── submit_tx → finalizer task
//!                          └─ mpsc<FlushSubmit>
//!                          └─ BinaryHeap reorder by seq
//! ```

use crate::storage::manager::{FlushInProgressGuard, StorageManager};
use parking_lot::RwLock as PlRwLock;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Semaphore, mpsc, oneshot};
use uni_common::core::snapshot::SnapshotManifest;

/// Result of a rotate phase: the snapshot of state needed to stream and
/// finalize. Send + 'static so it can travel into a spawned task.
pub struct RotatedFlush {
    pub seq: u64,
    pub old_l0_arc: Arc<PlRwLock<crate::runtime::l0::L0Buffer>>,
    pub wal_lsn: u64,
    pub current_version: u64,
    pub name: Option<String>,
    /// Snapshot of `cached_manifest` taken at rotate time. Stream uses this
    /// as a tentative parent; finalize may rewrite it if predecessors
    /// finalized in between.
    pub parent_manifest: Option<SnapshotManifest>,
    /// Permit holding the back-pressure slot. Released on finalize drop.
    pub permit: tokio::sync::OwnedSemaphorePermit,
    /// Acquired during rotate; dropped when this `RotatedFlush` is consumed
    /// by finalize (success or failure). Keeps `flush_in_progress` accurate
    /// for the full async pipeline duration.
    pub flush_in_progress_guard: FlushInProgressGuard,
}

/// Result of a stream phase: the manifest to publish.
pub struct FlushOutcome {
    pub new_manifest: SnapshotManifest,
    pub snapshot_id: String,
}

/// Carried across the spawn boundary so a finalize step can run without
/// touching `Writer` (which is `&self` and lives in the caller).
#[derive(Clone)]
pub struct SharedFlushCtx {
    pub storage: Arc<StorageManager>,
    pub l0_manager: Arc<crate::runtime::l0_manager::L0Manager>,
    pub adjacency_manager: Arc<crate::storage::adjacency_manager::AdjacencyManager>,
    pub property_manager: Option<Arc<crate::runtime::property_manager::PropertyManager>>,
    pub schema_manager: Arc<uni_common::core::schema::SchemaManager>,
    pub cached_manifest: Arc<parking_lot::Mutex<Option<SnapshotManifest>>>,
    pub last_flush_time: Arc<parking_lot::Mutex<std::time::Instant>>,
    pub fork_id: Option<uni_common::core::fork::ForkId>,
    pub fork_flush_count: Arc<std::sync::atomic::AtomicU64>,
    pub fork_fragment_warn_fired: Arc<std::sync::atomic::AtomicBool>,
    pub fork_fragment_warn_threshold: usize,
    /// Re-acquired by the static `flush_finalize_now` running on the
    /// finalizer task. NOT held during stream — that's the whole point.
    pub flush_lock: Arc<tokio::sync::Mutex<()>>,
    pub index_rebuild_manager:
        Arc<std::sync::OnceLock<Arc<crate::storage::index_rebuild::IndexRebuildManager>>>,
    pub compaction_handle: Arc<parking_lot::RwLock<Option<tokio::task::JoinHandle<()>>>>,
    pub compaction_config: uni_common::config::CompactionConfig,
    pub index_rebuild_config: uni_common::config::IndexRebuildConfig,
    pub auto_rebuild_enabled: bool,
}

/// A submission to the ordered finalizer.
struct FlushSubmit {
    seq: u64,
    rotated: RotatedFlush,
    result: anyhow::Result<FlushOutcome>,
    /// Optional notification when finalize completes (for `FlushTicket`).
    ack: Option<oneshot::Sender<anyhow::Result<String>>>,
}

/// User-facing handle to wait on an async-flush completion (proposal §5.6).
pub struct FlushTicket {
    /// `None` means the flush completed inline (sync path).
    rx: Option<oneshot::Receiver<anyhow::Result<String>>>,
}

impl FlushTicket {
    pub fn ready(snapshot_id: anyhow::Result<String>) -> Self {
        // For sync paths: pre-resolved.
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(snapshot_id);
        Self { rx: Some(rx) }
    }

    pub fn pending(rx: oneshot::Receiver<anyhow::Result<String>>) -> Self {
        Self { rx: Some(rx) }
    }

    /// Wait for the flush to finalize. Returns the snapshot id on success.
    pub async fn await_finalize(self) -> anyhow::Result<String> {
        match self.rx {
            Some(rx) => rx
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("flush ticket dropped before completion"))),
            None => Err(anyhow::anyhow!("flush ticket has no completion channel")),
        }
    }
}

pub struct FlushCoordinator {
    permits: Arc<Semaphore>,
    next_seq: AtomicU64,
    /// Wrapped in Mutex<Option<...>> so `shutdown()` can take and drop
    /// it explicitly, which closes the mpsc and lets the finalizer task
    /// exit. `submit()` reads through the option; if absent, the
    /// submission is silently dropped (coordinator is shutting down).
    submit_tx: parking_lot::Mutex<Option<mpsc::UnboundedSender<FlushSubmit>>>,
    /// Counter exposed for `drop_fork` to wait on. Incremented at rotate,
    /// decremented after finalize.
    pending_count: Arc<std::sync::atomic::AtomicUsize>,
    drain_notify: Arc<tokio::sync::Notify>,
    max_pending_flushes: usize,
    /// Wall-clock bound on a single stream phase. A stream that exceeds this
    /// is converted into a data-safe flush *failure* so its rotate-seq is
    /// still submitted and the finalizer never wedges (issue #132).
    stream_timeout: std::time::Duration,
    /// Tracked for `ShutdownHandle::track_task` registration AND for
    /// `shutdown()`'s await. Set to None after either takes it.
    finalizer_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Every spawned stream-phase task. `shutdown()` awaits each so
    /// the closure-captured `Arc<Writer>` (and through it
    /// `Arc<StorageManager>` + `Arc<ForkScope>` on a fork-scoped
    /// writer) actually drops before `shutdown` returns. Without this,
    /// `drop_fork` sees a transient `ForkInUse` because the stream
    /// task's destructor is still on tokio's scheduler queue after
    /// `drain()` returned. Opportunistically pruned in
    /// `submit_for_stream` to keep the vec bounded.
    stream_handles: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// RAII guard that guarantees a rotated flush's `seq` is ALWAYS submitted to
/// the finalizer — even if the stream task's future is dropped/cancelled
/// before it reaches the normal `submit` (issue #132). The finalizer advances
/// `expected` strictly in consecutive seq order, so a seq that is never
/// submitted wedges every later flush and holds its back-pressure permit
/// forever. On the normal path the caller [`disarm`](Self::disarm)s the guard
/// to hand the `RotatedFlush` + `ack` to `submit`; if the guard is dropped
/// while still armed, its `Drop` submits a synthetic failure so
/// `finalize_failure` runs (releasing the permit and advancing `expected`).
struct FlushSeqGuard {
    coord: Arc<FlushCoordinator>,
    seq: u64,
    rotated: Option<RotatedFlush>,
    ack: Option<oneshot::Sender<anyhow::Result<String>>>,
}

impl FlushSeqGuard {
    /// Normal completion path: take back ownership so the caller can submit the
    /// real stream result. Leaves the guard disarmed so its `Drop` is a no-op.
    fn disarm(
        mut self,
    ) -> (
        RotatedFlush,
        Option<oneshot::Sender<anyhow::Result<String>>>,
    ) {
        (
            self.rotated
                .take()
                .expect("FlushSeqGuard::disarm called more than once"),
            self.ack.take(),
        )
    }
}

impl Drop for FlushSeqGuard {
    fn drop(&mut self) {
        // Armed only if `disarm` never ran (the RotatedFlush is still here).
        // Submit a synthetic failure so the finalizer advances past this seq
        // and the back-pressure permit is released. No panics in Drop.
        if let Some(rotated) = self.rotated.take() {
            self.coord.submit(
                self.seq,
                rotated,
                Err(anyhow::anyhow!(
                    "flush stream task dropped before completion (seq {})",
                    self.seq
                )),
                self.ack.take(),
            );
        }
    }
}

impl FlushCoordinator {
    pub fn new(
        max_pending_flushes: usize,
        stream_timeout: std::time::Duration,
        shared: SharedFlushCtx,
        finalize_fn: Arc<dyn FinalizeFn>,
    ) -> Self {
        let permits = Arc::new(Semaphore::new(max_pending_flushes.max(1)));
        let next_seq = AtomicU64::new(0);
        let (submit_tx, submit_rx) = mpsc::unbounded_channel::<FlushSubmit>();
        let pending_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drain_notify = Arc::new(tokio::sync::Notify::new());

        let pending_count_for_task = pending_count.clone();
        let drain_notify_for_task = drain_notify.clone();
        let handle = tokio::spawn(finalizer_loop(
            submit_rx,
            shared,
            finalize_fn,
            pending_count_for_task,
            drain_notify_for_task,
        ));

        Self {
            permits,
            next_seq,
            submit_tx: parking_lot::Mutex::new(Some(submit_tx)),
            pending_count,
            drain_notify,
            max_pending_flushes,
            stream_timeout,
            finalizer_handle: parking_lot::Mutex::new(Some(handle)),
            stream_handles: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Drop the submit channel and await the finalizer task to exit.
    /// After this returns, the coordinator's spawned task is gone and
    /// any Arcs it held (including the writer's `Arc<StorageManager>`
    /// inside `SharedFlushCtx`, which on a fork-scoped writer pins
    /// `Arc<ForkScope>`) are released. Used by `drop_fork` so the
    /// ForkHolderGuard can finally drop. Idempotent: safe to call
    /// repeatedly.
    pub async fn shutdown(&self) {
        // 1. Drain every spawned stream task. Each task's destructor
        //    drops the closure-captured `Arc<Writer>` (and through it
        //    `Arc<StorageManager>` / `Arc<ForkScope>`). Awaiting forces
        //    those drops to happen before we return — closing the L8
        //    fork-drop race documented in the plan.
        let stream_handles: Vec<_> = self.stream_handles.lock().drain(..).collect();
        for h in stream_handles {
            let _ = h.await;
        }
        // 2. Drop submit_tx — closes the mpsc; the finalizer task will
        //    receive None and exit its loop.
        drop(self.submit_tx.lock().take());
        // 3. Await the finalizer task. If already taken (e.g. by
        //    ShutdownHandle::track_task), the JoinHandle is None and we
        //    have no way to await — accept that and return; the task
        //    is still on its way to exit because submit_tx is closed.
        let handle = self.finalizer_handle.lock().take();
        if let Some(h) = handle {
            let _ = h.await;
        }
    }

    /// Close the submit channel so the finalizer task exits, without awaiting.
    ///
    /// Step 2 of [`Self::shutdown`], usable from a synchronous context such as
    /// `Drop`. The finalizer blocks on `submit_rx.recv()` and so never observes
    /// the shutdown broadcast — closing the channel is the only thing that ends
    /// it. A teardown path that signals and then waits for the task to exit
    /// waits forever (or to its deadline) without this.
    ///
    /// Idempotent: the sender is taken out of the `Option`, so a later
    /// [`Self::shutdown`] is unaffected.
    pub fn close_submit_channel(&self) {
        drop(self.submit_tx.lock().take());
    }

    /// Hand off the finalizer task's JoinHandle for tracking by
    /// `ShutdownHandle`. Returns `None` if already taken.
    pub fn take_finalizer_handle(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.finalizer_handle.lock().take()
    }

    pub fn max_pending_flushes(&self) -> usize {
        self.max_pending_flushes
    }

    pub async fn acquire_permit(&self) -> anyhow::Result<tokio::sync::OwnedSemaphorePermit> {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("flush coordinator permit semaphore closed"))
    }

    /// Non-blocking variant of [`Self::acquire_permit`]. Returns `None`
    /// if the permit pool is at capacity. Used on the commit hot path
    /// to avoid awaiting under `flush_lock`.
    pub fn try_acquire_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.permits.clone().try_acquire_owned().ok()
    }

    pub fn next_rotate_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::AcqRel)
    }

    pub fn note_pending(&self) {
        self.pending_count.fetch_add(1, Ordering::AcqRel);
    }

    pub fn pending_flush_count(&self) -> usize {
        self.pending_count.load(Ordering::Acquire)
    }

    /// Submit a completed-stream flush for ordered finalization.
    /// Silently drops the submission if the coordinator has been shut
    /// down (submit_tx taken).
    pub fn submit(
        &self,
        seq: u64,
        rotated: RotatedFlush,
        result: anyhow::Result<FlushOutcome>,
        ack: Option<oneshot::Sender<anyhow::Result<String>>>,
    ) {
        let submit_msg = FlushSubmit {
            seq,
            rotated,
            result,
            ack,
        };
        if let Some(tx) = self.submit_tx.lock().as_ref() {
            let _ = tx.send(submit_msg);
        }
        // else: coordinator is shutting down; pending_count will be
        // decremented by the matching drop of submit_msg (RotatedFlush
        // contains the FlushInProgressGuard which already adjusts
        // flush_in_progress on drop). We must also decrement
        // pending_count manually because the finalizer won't see this.
        else {
            self.pending_count
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            self.drain_notify.notify_waiters();
        }
    }

    /// Spawn the stream phase on a tokio task and return a [`FlushTicket`].
    ///
    /// `run_stream` is the closure that actually performs the L1 stream
    /// work — it takes the rotate snapshot (`old_l0_arc`, `wal_lsn`,
    /// `current_version`, `name`) and returns the built (but not yet
    /// published) manifest as a `FlushOutcome`. The closure typically
    /// captures `Arc<Writer>` so it can call `writer.flush_stream_l1`.
    ///
    /// On stream completion, the result and the consumed `RotatedFlush`
    /// are sent through the coordinator's mpsc to the single-task
    /// finalizer, which preserves rotate-order via a BinaryHeap.
    ///
    /// The returned `FlushTicket` resolves when finalize completes
    /// (or fails). Dropping the ticket does NOT cancel the flush — the
    /// pipeline runs to completion either way.
    pub fn submit_for_stream<F, Fut>(
        self: &Arc<Self>,
        rotated: RotatedFlush,
        run_stream: F,
    ) -> FlushTicket
    where
        F: FnOnce(Arc<PlRwLock<crate::runtime::l0::L0Buffer>>, u64, u64, Option<String>) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = anyhow::Result<FlushOutcome>> + Send + 'static,
    {
        let (ack_tx, ack_rx) = oneshot::channel();
        let coord = self.clone();
        let seq = rotated.seq;
        let old_l0 = rotated.old_l0_arc.clone();
        let wal_lsn = rotated.wal_lsn;
        let current_version = rotated.current_version;
        let name = rotated.name.clone();
        let stream_timeout = self.stream_timeout;
        let handle = tokio::spawn(async move {
            // The seq guard guarantees this seq is submitted even if the task's
            // future is dropped before the normal `submit` below (issue #132).
            let guard = FlushSeqGuard {
                coord: coord.clone(),
                seq,
                rotated: Some(rotated),
                ack: Some(ack_tx),
            };
            // `run_stream_catching` converts a panic into a submitted *failure*
            // (review H2). `timeout` additionally converts a STALLED stream — a
            // lost-wakeup in the sparse/multivec Lance read-modify-write — into
            // a data-safe failure, so it can neither wedge the finalizer's
            // consecutive-seq pipeline nor hold a back-pressure permit forever
            // (issue #132). Both cases still finalize via `finalize_failure`,
            // which retains the old L0 in `pending_flush` and the WAL data.
            let stream_fut =
                run_stream_catching(run_stream(old_l0, wal_lsn, current_version, name));
            let result = match tokio::time::timeout(stream_timeout, stream_fut).await {
                Ok(r) => r,
                Err(_elapsed) => {
                    tracing::error!(
                        seq,
                        timeout_secs = stream_timeout.as_secs(),
                        "flush stream exceeded timeout; converting to a data-safe flush \
                         failure (old L0 retained in pending_flush, WAL retained, \
                         recovery via replay/retry)"
                    );
                    metrics::counter!("uni_flush_stream_timeouts_total").increment(1);
                    Err(anyhow::anyhow!(
                        "flush stream timed out after {:?} (seq {})",
                        stream_timeout,
                        seq
                    ))
                }
            };
            let (rotated, ack) = guard.disarm();
            coord.submit(seq, rotated, result, ack);
        });
        // Track the handle so `shutdown()` can await all stream tasks'
        // destructors. Opportunistically prune finished handles to keep
        // the vec bounded under high flush rates.
        let mut handles = self.stream_handles.lock();
        handles.retain(|h| !h.is_finished());
        handles.push(handle);
        FlushTicket::pending(ack_rx)
    }

    /// Wait until pending_count drops to zero. Used by `drop_fork`.
    pub async fn drain(&self, timeout: std::time::Duration) -> Result<(), &'static str> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.pending_flush_count() == 0 {
                return Ok(());
            }
            let notified = self.drain_notify.notified();
            tokio::select! {
                _ = notified => continue,
                _ = tokio::time::sleep_until(deadline) => {
                    return if self.pending_flush_count() == 0 {
                        Ok(())
                    } else {
                        Err("pending flushes did not drain before deadline")
                    };
                }
            }
        }
    }
}

/// Closure run by the finalizer task. Captures the parts of Writer that
/// finalize touches; runs without holding any Writer reference.
///
/// `Writer::flush_finalize_now` implements this and is bound to the
/// concrete WAL/storage state.
pub trait FinalizeFn: Send + Sync {
    fn finalize<'a>(
        &'a self,
        rotated: RotatedFlush,
        outcome: FlushOutcome,
        shared: SharedFlushCtx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + 'a>>;

    fn finalize_failure<'a>(
        &'a self,
        rotated: RotatedFlush,
        err: anyhow::Error,
        shared: SharedFlushCtx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Error> + Send + 'a>>;
}

/// Run a stream-phase future, converting a panic into an `Err` outcome instead
/// of letting it unwind the spawned task.
///
/// The async-flush finalizer advances `expected` strictly in consecutive seq
/// order and only when a `seq` is submitted. If a stream future panicked and the
/// task died without submitting, that seq would be missing forever and every
/// later flush — plus `drain()`/`shutdown()` — would block. Converting the panic
/// to a failed `FlushOutcome` keeps the pipeline live (the flush still fails, but
/// `finalize_failure` advances past it). (review H2)
async fn run_stream_catching<Fut>(fut: Fut) -> anyhow::Result<FlushOutcome>
where
    Fut: std::future::Future<Output = anyhow::Result<FlushOutcome>>,
{
    use futures::FutureExt as _;
    match std::panic::AssertUnwindSafe(fut).catch_unwind().await {
        Ok(result) => result,
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            tracing::error!(panic = %msg, "flush stream task panicked");
            Err(anyhow::anyhow!("flush stream task panicked: {msg}"))
        }
    }
}

async fn finalizer_loop(
    mut submit_rx: mpsc::UnboundedReceiver<FlushSubmit>,
    shared: SharedFlushCtx,
    finalize_fn: Arc<dyn FinalizeFn>,
    pending_count: Arc<std::sync::atomic::AtomicUsize>,
    drain_notify: Arc<tokio::sync::Notify>,
) {
    // Reorder-by-seq using a min-heap; finalize strictly in seq order.
    let mut pending: BinaryHeap<Reverse<(u64, FlushSubmit)>> = BinaryHeap::new();
    let mut expected: u64 = 0;
    while let Some(submit) = submit_rx.recv().await {
        pending.push(Reverse((submit.seq, submit)));
        while let Some(Reverse((seq, _))) = pending.peek() {
            if *seq != expected {
                break;
            }
            let Reverse((_, s)) = pending.pop().unwrap();
            let FlushSubmit {
                rotated,
                result,
                ack,
                ..
            } = s;
            let ack_result = match result {
                Ok(outcome) => finalize_fn.finalize(rotated, outcome, shared.clone()).await,
                Err(e) => {
                    let _err = finalize_fn
                        .finalize_failure(rotated, e, shared.clone())
                        .await;
                    Err(anyhow::anyhow!("flush stream failed: {}", _err))
                }
            };
            if let Some(ack) = ack {
                let _ = ack.send(ack_result);
            }
            pending_count.fetch_sub(1, Ordering::AcqRel);
            drain_notify.notify_waiters();
            expected += 1;
        }
    }
}

// We need a wrapper allowing FlushSubmit to be ordered by seq for the heap.
// Default Ord on tuples uses the first element so (u64, FlushSubmit) needs
// FlushSubmit to be Ord/PartialOrd. We don't actually compare FlushSubmits;
// the seq is at position 0 of the tuple and the heap is keyed off it. To
// avoid trait headaches we wrap manually:
impl PartialEq for FlushSubmit {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }
}
impl Eq for FlushSubmit {}
impl PartialOrd for FlushSubmit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FlushSubmit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq.cmp(&other.seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// H2: a panic inside the stream future must be converted into an `Err`
    /// outcome (so the seq can still be submitted and the finalizer advances),
    /// not propagated to abort the spawned task. A normal `Err` passes through
    /// unchanged.
    #[tokio::test]
    async fn run_stream_catching_converts_panic_to_err() {
        // (FlushOutcome isn't Debug, so we match instead of using expect_err.)
        // A normal failure is forwarded verbatim.
        let normal = run_stream_catching(async { Err(anyhow::anyhow!("normal failure")) }).await;
        let normal_err = match normal {
            Ok(_) => panic!("normal failure should stay an error"),
            Err(e) => e,
        };
        assert!(normal_err.to_string().contains("normal failure"));

        // A panic becomes an Err mentioning the panic, never unwinding.
        let panicked = run_stream_catching(async {
            panic!("boom in stream");
            #[allow(unreachable_code)]
            Ok(unreachable!())
        })
        .await;
        let panic_err = match panicked {
            Ok(_) => panic!("panic must be caught as an error"),
            Err(e) => e,
        };
        assert!(
            panic_err.to_string().contains("panicked"),
            "error should identify the panic, got: {panic_err}"
        );
    }
}
