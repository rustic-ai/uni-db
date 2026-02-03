// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Synchronization utilities for safe lock acquisition.
//!
//! This module provides helper functions that handle lock poisoning gracefully,
//! preventing panic cascades when a thread panics while holding a lock.

use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Error returned when a lock is poisoned.
///
/// A lock becomes poisoned when a thread panics while holding the lock.
/// This error type allows callers to decide how to handle poisoned locks.
#[derive(Debug)]
pub struct LockPoisonedError {
    /// Description of which lock was poisoned.
    pub lock_name: &'static str,
}

impl std::fmt::Display for LockPoisonedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Lock '{}' was poisoned by a panicked thread",
            self.lock_name
        )
    }
}

impl std::error::Error for LockPoisonedError {}

/// Acquires a read lock, returning an error if the lock is poisoned.
///
/// # Errors
///
/// Returns `LockPoisonedError` if another thread panicked while holding this lock.
///
/// # Examples
///
/// ```
/// use std::sync::RwLock;
/// use uni_common::sync::acquire_read;
///
/// let lock = RwLock::new(42);
/// let guard = acquire_read(&lock, "my_data").unwrap();
/// assert_eq!(*guard, 42);
/// ```
pub fn acquire_read<'a, T>(
    lock: &'a RwLock<T>,
    lock_name: &'static str,
) -> Result<RwLockReadGuard<'a, T>, LockPoisonedError> {
    lock.read()
        .map_err(|_: PoisonError<_>| LockPoisonedError { lock_name })
}

/// Acquires a write lock, returning an error if the lock is poisoned.
///
/// # Errors
///
/// Returns `LockPoisonedError` if another thread panicked while holding this lock.
///
/// # Examples
///
/// ```
/// use std::sync::RwLock;
/// use uni_common::sync::acquire_write;
///
/// let lock = RwLock::new(42);
/// let mut guard = acquire_write(&lock, "my_data").unwrap();
/// *guard = 100;
/// ```
pub fn acquire_write<'a, T>(
    lock: &'a RwLock<T>,
    lock_name: &'static str,
) -> Result<RwLockWriteGuard<'a, T>, LockPoisonedError> {
    lock.write()
        .map_err(|_: PoisonError<_>| LockPoisonedError { lock_name })
}

/// Acquires a mutex lock, returning an error if the lock is poisoned.
///
/// # Errors
///
/// Returns `LockPoisonedError` if another thread panicked while holding this lock.
///
/// # Examples
///
/// ```
/// use std::sync::Mutex;
/// use uni_common::sync::acquire_mutex;
///
/// let lock = Mutex::new(42);
/// let guard = acquire_mutex(&lock, "my_data").unwrap();
/// assert_eq!(*guard, 42);
/// ```
pub fn acquire_mutex<'a, T>(
    lock: &'a Mutex<T>,
    lock_name: &'static str,
) -> Result<MutexGuard<'a, T>, LockPoisonedError> {
    lock.lock()
        .map_err(|_: PoisonError<_>| LockPoisonedError { lock_name })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_acquire_read_success() {
        let lock = RwLock::new(42);
        let guard = acquire_read(&lock, "test").unwrap();
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_acquire_write_success() {
        let lock = RwLock::new(42);
        let mut guard = acquire_write(&lock, "test").unwrap();
        *guard = 100;
        drop(guard);

        let guard = acquire_read(&lock, "test").unwrap();
        assert_eq!(*guard, 100);
    }

    #[test]
    fn test_acquire_mutex_success() {
        let lock = Mutex::new(42);
        let guard = acquire_mutex(&lock, "test").unwrap();
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_poisoned_mutex_returns_error() {
        let lock = Arc::new(Mutex::new(42));
        let lock_clone = Arc::clone(&lock);

        // Spawn a thread that panics while holding the lock
        let handle = thread::spawn(move || {
            let _guard = lock_clone.lock().unwrap();
            panic!("Intentional panic to poison the lock");
        });

        // Wait for the thread to finish (it will panic)
        let _ = handle.join();

        // Now the lock should be poisoned
        let result = acquire_mutex(&lock, "test_mutex");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().lock_name, "test_mutex");
    }
}
