// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graceful shutdown coordination for the host's background tasks.
//!
//! A shared broadcast channel signals shutdown to every spawned driver
//! (CDC, scheduler, deferral tick); tracked join handles are awaited up
//! to a configurable timeout so tasks drain before the process exits.

use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Coordinates graceful shutdown of all background tasks.
///
/// Cloning yields another handle onto the same state: a task tracked or a
/// shutdown signalled through any clone is seen by all of them. Components
/// that spawn work long after construction (the scheduler's per-run
/// completion tasks, async triggers) keep a clone so that work stays
/// countable and joinable.
///
/// [`Self::derive`] makes a handle for a dependent view (a fork or a pinned
/// snapshot of the database). It has its own signal but tracks its tasks in
/// the parent's list, so the parent's shutdown signals, counts, and waits for
/// them as well.
#[derive(Clone)]
pub struct ShutdownHandle {
    signal: Arc<Signal>,
    task_handles: Arc<Mutex<TrackedTasks>>,
    shutdown_initiated: Arc<RwLock<bool>>,
    timeout: Duration,
}

impl std::fmt::Debug for ShutdownHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownHandle")
            .field("live_tasks", &self.live_task_count())
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// One handle's shutdown broadcast, plus the signals of handles derived from it.
struct Signal {
    tx: broadcast::Sender<()>,
    /// Signals of handles made by [`ShutdownHandle::derive`], fired with this
    /// one. Weak, so a dropped dependent view does not stay listed.
    derived: Mutex<Vec<Weak<Signal>>>,
}

impl Signal {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(16);
        Self {
            tx,
            derived: Mutex::new(Vec::new()),
        }
    }

    /// Sends on this signal, then on every live derived one, recursively.
    fn fire(&self) {
        let _ = self.tx.send(());
        // Collected first so no lock is held while descending.
        let derived: Vec<Arc<Signal>> = {
            let mut derived = self.derived.lock().unwrap();
            derived.retain(|w| w.strong_count() > 0);
            derived.iter().filter_map(Weak::upgrade).collect()
        };
        for child in derived {
            child.fire();
        }
    }
}

/// Join handles of tracked tasks, pruned of finished ones as it grows.
///
/// Per-event tasks (one per async trigger fire or scheduled job run) are
/// tracked too, so without pruning the vector would grow with every event for
/// the life of the database. Pruning runs when the length reaches
/// `prune_at`, which is then reset to twice the surviving count, so the cost
/// stays amortized O(1) per push however many tasks are alive.
struct TrackedTasks {
    handles: Vec<JoinHandle<()>>,
    prune_at: usize,
}

/// Length at which [`TrackedTasks`] first prunes finished handles.
const MIN_PRUNE_AT: usize = 32;

impl TrackedTasks {
    fn new() -> Self {
        Self {
            handles: Vec::new(),
            prune_at: MIN_PRUNE_AT,
        }
    }

    fn push(&mut self, handle: JoinHandle<()>) {
        if self.handles.len() >= self.prune_at {
            let (finished, live): (Vec<_>, Vec<_>) = std::mem::take(&mut self.handles)
                .into_iter()
                .partition(JoinHandle::is_finished);
            self.handles = live;
            for h in finished {
                report_unclean_exit(futures::FutureExt::now_or_never(h));
            }
            self.prune_at = (self.handles.len() * 2).max(MIN_PRUNE_AT);
        }
        self.handles.push(handle);
    }
}

/// Logs a pruned task that panicked, which would otherwise vanish silently.
fn report_unclean_exit(outcome: Option<Result<(), tokio::task::JoinError>>) {
    if let Some(Err(e)) = outcome
        && !e.is_cancelled()
    {
        tracing::error!(error = %e, "background task did not exit cleanly");
    }
}

impl ShutdownHandle {
    pub fn new(timeout: Duration) -> Self {
        Self {
            signal: Arc::new(Signal::new()),
            task_handles: Arc::new(Mutex::new(TrackedTasks::new())),
            shutdown_initiated: Arc::new(RwLock::new(false)),
            timeout,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.signal.tx.subscribe()
    }

    /// Makes a handle for a view that depends on this one, such as a fork or
    /// a pinned snapshot.
    ///
    /// The derived handle has its own signal, so its view can stop its own
    /// tasks with [`Self::release`] when it goes away. Its tasks are tracked
    /// in this handle's list, and a shutdown signalled here also signals the
    /// derived handle (and the handles derived from it), so this handle's
    /// shutdown stops, counts, and waits for the dependent views' tasks too.
    /// A dependent view's tasks write the same database directory, so a
    /// shutdown that left them running would be no shutdown at all.
    ///
    /// Because the list is shared, a wait on the derived handle (such as
    /// [`Self::shutdown_async`]) waits for every task in it, the parent's
    /// included.
    #[must_use]
    pub fn derive(&self) -> Self {
        let signal = Arc::new(Signal::new());
        {
            let mut derived = self.signal.derived.lock().unwrap();
            derived.retain(|w| w.strong_count() > 0);
            derived.push(Arc::downgrade(&signal));
        }
        Self {
            signal,
            task_handles: Arc::clone(&self.task_handles),
            shutdown_initiated: Arc::new(RwLock::new(false)),
            timeout: self.timeout,
        }
    }

    /// Signals the tasks subscribed to this handle only, without the handles
    /// derived from it and without waiting.
    ///
    /// For a dependent view going away: its own tasks have nothing left to
    /// serve, while a view derived from it (a nested fork) may still be in
    /// use. The tasks stay tracked, so the root handle's shutdown still waits
    /// for any that are finishing.
    pub fn release(&self) {
        let _ = self.signal.tx.send(());
    }

    /// Tracks `handle` so shutdown waits for it and it is counted while alive.
    pub fn track_task(&self, handle: JoinHandle<()>) {
        self.task_handles.lock().unwrap().push(handle);
    }

    pub async fn shutdown_async(&self) -> anyhow::Result<()> {
        {
            let mut initiated = self.shutdown_initiated.write().unwrap();
            if *initiated {
                return Ok(());
            }
            *initiated = true;
        }

        self.signal.fire();

        let mut handles = {
            let mut tasks = self.task_handles.lock().unwrap();
            std::mem::take(&mut tasks.handles)
        };

        if !handles.is_empty() {
            tracing::info!("Waiting for {} background tasks", handles.len());

            let wait_future = async {
                for handle in handles.iter_mut() {
                    // #233 P3: `let _ =` made a PANICKING background task
                    // indistinguishable from one that exited cleanly, so
                    // "Background tasks completed gracefully" was logged either
                    // way. Nothing can be done about it at shutdown, but it
                    // should not be silent.
                    if let Err(e) = handle.await
                        && !e.is_cancelled()
                    {
                        tracing::error!(
                            error = %e,
                            "background task did not exit cleanly during shutdown",
                        );
                    }
                }
            };

            match tokio::time::timeout(self.timeout, wait_future).await {
                Ok(_) => tracing::info!("Background tasks completed gracefully"),
                Err(_) => tracing::warn!("Shutdown timeout reached"),
            }

            // Keep the stragglers: dropping a `JoinHandle` detaches its task,
            // which would make a still-running task uncountable.
            handles.retain(|h| !h.is_finished());
            if !handles.is_empty() {
                self.task_handles.lock().unwrap().handles.extend(handles);
            }
        }

        Ok(())
    }

    /// Signals shutdown, here and on every derived handle, without waiting
    /// for any task.
    pub fn shutdown_blocking(&self) {
        self.signal.fire();
    }

    /// Number of tracked tasks that have not finished yet.
    #[must_use]
    pub fn live_task_count(&self) -> usize {
        self.task_handles
            .lock()
            .unwrap()
            .handles
            .iter()
            .filter(|h| !h.is_finished())
            .count()
    }

    /// Signals shutdown, then blocks this thread until every tracked task has
    /// finished or `timeout` elapses.
    ///
    /// Waits by observing the tasks, not by driving them, so it makes progress
    /// only when a runtime is still running them on other threads. Calling it
    /// on a `current_thread` runtime's own thread therefore waits the full
    /// `timeout` for nothing; callers must not.
    ///
    /// Returns the number of tasks still running when it returned.
    pub fn shutdown_and_wait_blocking(&self, timeout: Duration) -> usize {
        self.shutdown_blocking();
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let live = self.live_task_count();
            if live == 0 || std::time::Instant::now() >= deadline {
                return live;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A task that overruns the grace period must stay countable: dropping its
    /// `JoinHandle` would detach it while it keeps running.
    #[tokio::test]
    async fn straggler_stays_counted_after_shutdown_timeout() {
        let handle = ShutdownHandle::new(Duration::from_millis(50));
        handle.track_task(tokio::spawn(tokio::time::sleep(Duration::from_secs(30))));
        let mut rx = handle.subscribe();
        handle.track_task(tokio::spawn(async move {
            let _ = rx.recv().await;
        }));

        handle.shutdown_async().await.unwrap();
        assert_eq!(handle.live_task_count(), 1, "the straggler was forgotten");
    }

    /// Finished per-event tasks are pruned, so tracking one task per event
    /// does not grow the handle list for the life of the database.
    #[tokio::test]
    async fn finished_tasks_are_pruned_as_tracking_grows() {
        let handle = ShutdownHandle::new(Duration::from_secs(1));
        for _ in 0..1_000 {
            let task = tokio::spawn(async {});
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
            handle.track_task(task);
        }
        let len = handle.task_handles.lock().unwrap().handles.len();
        assert!(
            len <= MIN_PRUNE_AT,
            "1000 finished tasks left {len} handles behind"
        );
        assert_eq!(handle.live_task_count(), 0);
    }

    /// A live task survives pruning and stays counted.
    #[tokio::test]
    async fn pruning_keeps_live_tasks() {
        let handle = ShutdownHandle::new(Duration::from_secs(1));
        let mut rx = handle.subscribe();
        handle.track_task(tokio::spawn(async move {
            let _ = rx.recv().await;
        }));
        for _ in 0..200 {
            let task = tokio::spawn(async {});
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
            handle.track_task(task);
        }
        assert_eq!(handle.live_task_count(), 1);
        handle.shutdown_async().await.unwrap();
        assert_eq!(handle.live_task_count(), 0);
    }

    /// The blocking wait returns as soon as every task has observed shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_wait_joins_tasks_that_observe_shutdown() {
        let handle = ShutdownHandle::new(Duration::from_secs(30));
        let mut rx = handle.subscribe();
        handle.track_task(tokio::spawn(async move {
            let _ = rx.recv().await;
        }));
        let live = tokio::task::block_in_place(|| {
            handle.shutdown_and_wait_blocking(Duration::from_secs(10))
        });
        assert_eq!(live, 0);
    }

    /// Tracks a task that exits when `handle` is signalled (or closed).
    fn track_waiter(handle: &ShutdownHandle) {
        let mut rx = handle.subscribe();
        handle.track_task(tokio::spawn(async move {
            let _ = rx.recv().await;
        }));
    }

    /// A derived handle's tasks are counted, signalled, and waited for by the
    /// parent's shutdown, through any depth of derivation.
    #[tokio::test]
    async fn parent_shutdown_reaches_derived_handles() {
        let root = ShutdownHandle::new(Duration::from_secs(5));
        let child = root.derive();
        let grandchild = child.derive();
        track_waiter(&child);
        track_waiter(&grandchild);
        assert_eq!(root.live_task_count(), 2, "derived tasks are not counted");

        root.shutdown_async().await.unwrap();
        assert_eq!(
            root.live_task_count(),
            0,
            "a derived task outlived shutdown"
        );
        drop((child, grandchild));
    }

    /// `release` stops the releasing view's tasks but not those of a view
    /// derived from it, which may still be in use.
    #[tokio::test]
    async fn release_does_not_cascade() {
        let root = ShutdownHandle::new(Duration::from_secs(5));
        let child = root.derive();
        let grandchild = child.derive();
        track_waiter(&child);
        track_waiter(&grandchild);

        child.release();
        let t = std::time::Instant::now();
        while root.live_task_count() > 1 {
            assert!(
                t.elapsed() < Duration::from_secs(5),
                "release did not signal"
            );
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            root.live_task_count(),
            1,
            "release cascaded to a derived view"
        );
        root.shutdown_async().await.unwrap();
        assert_eq!(root.live_task_count(), 0);
        drop(grandchild);
    }
}
