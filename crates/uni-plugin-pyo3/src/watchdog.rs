// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A wall-clock watchdog that forcibly interrupts a runaway Python guest.
//!
//! The cooperative per-kernel deadline (`GcSession::check_deadline`) only fires
//! when the guest calls back into a kernel — it cannot stop a guest spinning in
//! pure Python (`while True: pass`) or doing heavy pure-Python work between
//! kernel calls, and its `PyRuntimeError` is catchable. This watchdog closes
//! that gap (proposal §4.5 PyO3 loop-bounding prerequisite): a background thread
//! injects `KeyboardInterrupt` — a `BaseException`, so `except Exception: pass`
//! cannot swallow it — into the guest's Python thread once the deadline passes.
//!
//! Modeled on the WASM loader's epoch ticker
//! (`crates/uni-plugin-wasm/src/loader.rs`): a named thread that holds only a
//! deadline + a shared done-flag and exits cleanly on [`Drop`].
//!
//! # Caveat
//! Delivery uses `PyThreadState_SetAsyncExc`, which the CPython eval loop honors
//! at the next bytecode boundary. A guest blocked in a C extension that never
//! releases the GIL is therefore not interruptible — the same limitation as
//! CPython's own `KeyboardInterrupt`. The watchdog must be dropped *outside* the
//! guest's GIL-holding `Python::attach` block, or `Drop::join` would deadlock
//! against the watchdog's own GIL acquisition.
//
// Rust guideline compliant

#![cfg(feature = "pyo3")]

use std::os::raw::c_long;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pyo3::Python;
use pyo3::prelude::*;

/// Poll granularity: the watchdog re-checks the deadline / done-flag this often.
const TICK_MS: u64 = 25;

/// Returns the running Python thread's id, as accepted by `SetAsyncExc`.
///
/// `threading.get_ident()` is the value CPython stores in each thread state and
/// the same id `PyThreadState_SetAsyncExc` compares against; the cast to
/// `c_long` matches the FFI parameter (thread ids fit the positive range in
/// practice on the supported platforms).
///
/// # Errors
/// Returns a `PyErr` if `threading` cannot be imported or `get_ident` fails.
pub fn current_thread_id(py: Python<'_>) -> PyResult<c_long> {
    let ident: u64 = py
        .import("threading")?
        .getattr("get_ident")?
        .call0()?
        .extract()?;
    #[expect(
        clippy::cast_possible_wrap,
        reason = "SetAsyncExc takes c_long; get_ident's value round-trips against the stored thread_id"
    )]
    Ok(ident as c_long)
}

/// Acquires the GIL to read the current Python thread id (adapter convenience).
///
/// # Errors
/// Returns a `PyErr` if `threading.get_ident()` fails.
pub fn current_thread_id_attached() -> PyResult<c_long> {
    Python::attach(current_thread_id)
}

/// Cancels any pending async exception on Python thread `tid`.
///
/// Called after the guest returns and the watchdog is joined, so a
/// `KeyboardInterrupt` injected in the tiny window after the guest completed
/// cannot surface on a later CALL that reuses this worker thread.
pub fn cancel_pending_interrupt(tid: c_long) {
    Python::attach(|_py| {
        // SAFETY: passing a null exception clears any pending async exc on
        // `tid`; requires only the GIL, which `attach` holds.
        unsafe {
            pyo3::ffi::PyThreadState_SetAsyncExc(tid, std::ptr::null_mut());
        }
    });
}

/// A background thread that injects `KeyboardInterrupt` into `tid` at `deadline`.
///
/// Armed by [`DeadlineWatchdog::arm`] before the guest runs and dropped after it
/// returns (outside the guest's GIL scope). See the [module docs](self).
#[derive(Debug)]
pub struct DeadlineWatchdog {
    done: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl DeadlineWatchdog {
    /// Arms a watchdog that interrupts Python thread `tid` once `deadline` passes.
    ///
    /// # Errors
    ///
    /// Returns the spawn error if the watchdog thread cannot be started.
    ///
    /// #233: this used `.ok()`, so on a spawn failure the forced-deadline layer
    /// was silently absent while the caller believed it was armed. The
    /// cooperative per-kernel check only fires when the guest calls back, so a
    /// pure-Python `while True: pass` then became unbounded and hung the query
    /// — the exact case this watchdog exists to bound.
    pub fn arm(tid: c_long, deadline: Instant) -> std::io::Result<Self> {
        let done = Arc::new(AtomicBool::new(false));
        let done_bg = Arc::clone(&done);
        let handle = std::thread::Builder::new()
            .name("uni-pyo3-graph-watchdog".to_owned())
            .spawn(move || run(tid, deadline, &done_bg))?;
        Ok(Self {
            done,
            handle: Some(handle),
        })
    }
}

fn run(tid: c_long, deadline: Instant, done: &AtomicBool) {
    loop {
        if done.load(Ordering::Relaxed) {
            return;
        }
        if Instant::now() >= deadline {
            // Acquire the GIL (the guest yields it at the switch interval) and
            // inject the interrupt — re-checking `done` under the GIL so a guest
            // that finished in the meantime is not hit spuriously.
            Python::attach(|_py| {
                if !done.load(Ordering::Relaxed) {
                    // SAFETY: `PyExc_KeyboardInterrupt` is a valid interned
                    // exception object owned by the interpreter, and
                    // `SetAsyncExc` requires only the GIL, which `attach` holds.
                    unsafe {
                        let exc = pyo3::ffi::PyExc_KeyboardInterrupt;
                        pyo3::ffi::PyThreadState_SetAsyncExc(tid, exc);
                    }
                }
            });
            return;
        }
        std::thread::sleep(Duration::from_millis(TICK_MS));
    }
}

impl Drop for DeadlineWatchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
