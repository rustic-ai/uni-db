// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::CrdtMerge;
use serde::{Deserialize, Serialize};

/// A Last-Writer-Wins (LWW) Register.
///
/// Conflicts are resolved by keeping the value with the highest timestamp.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LWWRegister<T: Clone> {
    value: T,
    timestamp: i64,
}

impl<T: Clone> LWWRegister<T> {
    /// Create a new LWWRegister.
    pub fn new(value: T, timestamp: i64) -> Self {
        Self { value, timestamp }
    }

    /// Set a new value with a timestamp.
    pub fn set(&mut self, value: T, timestamp: i64) {
        if timestamp >= self.timestamp {
            self.value = value;
            self.timestamp = timestamp;
        }
    }

    /// Get the current value.
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Get the current timestamp.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

impl<T: Clone + Serialize> CrdtMerge for LWWRegister<T> {
    fn merge(&mut self, other: &Self) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
        } else if other.timestamp == self.timestamp {
            // Deterministic tie-break: compare serialized forms so merge is commutative.
            //
            // #233 Tier 1: both sides used `unwrap_or_default()`, which yields
            // EMPTY bytes on failure. If exactly one side fails the order is
            // still total and both replicas agree, so they converge. If BOTH
            // fail, both compare equal, each replica keeps its own value at an
            // identical timestamp, and they diverge permanently with no signal.
            // Unreachable for the in-tree `T = serde_json::Value` (infallible),
            // but `T` is public generic API.
            //
            // Two unserializable values cannot be ordered, so this cannot be
            // repaired here: the structural remedy is a fallible `merge` or a
            // `T: Ord` bound. Reported instead of diverging silently.
            let self_bytes = serde_json::to_vec(&self.value);
            let other_bytes = serde_json::to_vec(&other.value);
            match (&self_bytes, &other_bytes) {
                (Err(e), Err(_)) => {
                    tracing::error!(
                        error = %e,
                        "LWWRegister: neither value could be serialized for the equal-timestamp \
                         tie-break; replicas may diverge permanently",
                    );
                }
                _ => {
                    // `Err` sorts below `Ok`, so a side that failed loses to one
                    // that did not — computed identically on every replica.
                    let self_key = self_bytes.ok();
                    let other_key = other_bytes.ok();
                    if other_key > self_key {
                        self.value = other.value.clone();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set() {
        let mut reg = LWWRegister::new("initial".to_string(), 100);
        reg.set("newer".to_string(), 110);
        assert_eq!(reg.get(), "newer");
        assert_eq!(reg.timestamp(), 110);

        reg.set("older".to_string(), 105);
        assert_eq!(reg.get(), "newer"); // remains "newer"
    }

    #[test]
    fn test_merge() {
        let a = LWWRegister::new("A".to_string(), 100);
        let mut b = LWWRegister::new("B".to_string(), 110);

        let mut a_clone = a.clone();
        a_clone.merge(&b);
        assert_eq!(a_clone.get(), "B");

        b.merge(&a);
        assert_eq!(b.get(), "B"); // B wins
    }
}
