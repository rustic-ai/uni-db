// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graceful shutdown coordination for the host's background tasks.
//!
//! A shared broadcast channel signals shutdown to every spawned driver
//! (CDC, scheduler, deferral tick); tracked join handles are awaited up
//! to a configurable timeout so tasks drain before the process exits.

use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Coordinates graceful shutdown of all background tasks
pub struct ShutdownHandle {
    tx: broadcast::Sender<()>,
    task_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    shutdown_initiated: Arc<RwLock<bool>>,
    timeout: Duration,
}

impl ShutdownHandle {
    pub fn new(timeout: Duration) -> Self {
        let (tx, _) = broadcast::channel(16);
        Self {
            tx,
            task_handles: Arc::new(Mutex::new(Vec::new())),
            shutdown_initiated: Arc::new(RwLock::new(false)),
            timeout,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.tx.subscribe()
    }

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

        let _ = self.tx.send(());

        let mut handles = {
            let mut tasks = self.task_handles.lock().unwrap();
            std::mem::take(&mut *tasks)
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
                self.task_handles.lock().unwrap().extend(handles);
            }
        }

        Ok(())
    }

    /// Signals shutdown without waiting for any task.
    pub fn shutdown_blocking(&self) {
        let _ = self.tx.send(());
    }

    /// Number of tracked tasks that have not finished yet.
    #[must_use]
    pub fn live_task_count(&self) -> usize {
        self.task_handles
            .lock()
            .unwrap()
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
}
