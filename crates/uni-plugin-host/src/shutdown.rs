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

        let handles = {
            let mut tasks = self.task_handles.lock().unwrap();
            std::mem::take(&mut *tasks)
        };

        if !handles.is_empty() {
            tracing::info!("Waiting for {} background tasks", handles.len());

            let wait_future = async {
                for handle in handles {
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
        }

        Ok(())
    }

    pub fn shutdown_blocking(&self) {
        let _ = self.tx.send(());
    }
}
