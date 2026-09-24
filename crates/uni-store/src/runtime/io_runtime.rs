// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A process-wide tokio runtime that outlives every caller.
//!
//! Lance binds some cached state to the tokio runtime that first creates it:
//! `ScanScheduler::new` spawns its I/O loop onto the *current* runtime, and the
//! inverted (full-text) index caches its readers — and so that scheduler — in
//! the shared `lance::session::Session`. Posting lists load lazily, so a later
//! query for a term not yet cached submits I/O to that loop. If the runtime
//! that first loaded the index has since been dropped, the loop is gone and the
//! query waits forever with every thread idle (issue #290).
//!
//! Any short-lived runtime can be that first loader: the per-call runtimes the
//! query engine used to build to drive async work from a synchronous DataFusion
//! `evaluate`, a caller's own runtime that is dropped while the database lives
//! on, or the runtime `UniBuilder::build_sync` used to discard. Routing that
//! work here gives such state a runtime that is never shut down.
//!
//! The runtime is a `static` (M-AVOID-STATICS): correctness depends only on it
//! never being dropped, not on it being unique, so a second copy linked in from
//! another crate version would be equally correct.

use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::Runtime;

static IO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Returns the process-wide runtime, building it on first use.
///
/// The runtime is never shut down, so tasks and I/O resources created on it —
/// including ones Lance caches across queries — stay serviceable for the life
/// of the process.
///
/// # Errors
///
/// Returns an error if the runtime's worker threads cannot be spawned. A
/// failed build is not cached; the next call tries again.
pub fn io_runtime() -> std::io::Result<&'static Runtime> {
    if let Some(rt) = IO_RUNTIME.get() {
        return Ok(rt);
    }
    let workers = std::thread::available_parallelism()
        .map_or(2, std::num::NonZeroUsize::get)
        .clamp(2, 4);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("uni-io")
        .enable_all()
        .build()?;
    if let Err(lost) = IO_RUNTIME.set(rt) {
        // Another thread won the race. This call may itself be inside an async
        // context, where a plain drop of a runtime panics.
        lost.shutdown_background();
    }
    Ok(IO_RUNTIME
        .get()
        .expect("IO_RUNTIME was set by this call or by the thread that won the race"))
}

/// Why [`block_on_io_runtime`] could not produce the future's output.
#[derive(Debug)]
pub enum BridgeError {
    /// The process-wide runtime could not be built.
    Runtime(std::io::Error),
    /// The future panicked while being driven.
    Panicked,
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(e) => write!(f, "failed to build the uni-io runtime: {e}"),
            Self::Panicked => f.write_str("thread panicked"),
        }
    }
}

impl std::error::Error for BridgeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(e) => Some(e),
            Self::Panicked => None,
        }
    }
}

/// Drives `fut` to completion on the process-wide runtime from synchronous code.
///
/// This is the bridge for code that must produce a value synchronously while
/// possibly running on a tokio worker — a DataFusion `PhysicalExpr::evaluate`
/// or a stream's `poll_next`. Blocking the ambient runtime there panics, so the
/// future is driven on a scoped thread that has no runtime context. Tasks it
/// spawns land on the process-wide runtime, which outlives the call.
///
/// # Errors
///
/// Returns [`BridgeError::Runtime`] if the runtime cannot be built and
/// [`BridgeError::Panicked`] if the future panics.
pub fn block_on_io_runtime<F>(fut: F) -> Result<F::Output, BridgeError>
where
    F: Future + Send,
    F::Output: Send,
{
    let rt = io_runtime().map_err(BridgeError::Runtime)?;
    std::thread::scope(|s| {
        s.spawn(|| rt.block_on(fut))
            .join()
            .map_err(|_| BridgeError::Panicked)
    })
}

/// Runs `fut` as a task on the process-wide runtime and awaits its output.
///
/// Use this for async code whose first run may create runtime-bound state that
/// outlives the call, when the caller's own runtime may not. Dropping the
/// returned future aborts the task, so caller-side timeouts and cancellation
/// still take effect.
///
/// # Errors
///
/// Returns an error if the runtime cannot be built, or if the task panics or
/// is cancelled.
pub async fn run_on_io_runtime<F>(fut: F) -> anyhow::Result<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    /// Aborts the task if the awaiting future is dropped before it finishes.
    struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

    impl<T> Drop for AbortOnDrop<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    let mut task = AbortOnDrop(io_runtime()?.spawn(fut));
    (&mut task.0)
        .await
        .map_err(|e| anyhow::anyhow!("uni-io task failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// State created on the shared runtime stays serviceable after the runtime
    /// that requested it is gone — the property #290 depends on.
    #[test]
    fn spawned_state_survives_the_callers_runtime() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // A background loop in the style of Lance's I/O scheduler: created
        // while `caller` is running, then left behind for later requests.
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
        caller
            .block_on(run_on_io_runtime(async move {
                tokio::spawn(async move {
                    while let Some(v) = rx.recv().await {
                        let _ = reply_tx.send(v * 2);
                    }
                });
            }))
            .unwrap();
        drop(caller);

        tx.send(21).unwrap();
        let got = block_on_io_runtime(async move { reply_rx.recv().await }).unwrap();
        assert_eq!(got, Some(42));
    }

    #[test]
    fn a_panicking_future_is_reported_not_propagated() {
        let r = block_on_io_runtime(async { panic!("boom") });
        assert!(matches!(r, Err(BridgeError::Panicked)));
    }

    #[tokio::test]
    async fn dropping_the_awaiting_future_aborts_the_task() {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let fut = run_on_io_runtime(async move {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            let _ = done_tx.send(());
        });
        // Cancel the caller side; the task must not keep running.
        let _ = tokio::time::timeout(std::time::Duration::from_millis(50), fut).await;
        // Abort drops the task's future, which drops `done_tx` unsent.
        assert!(done_rx.await.is_err());
    }
}
