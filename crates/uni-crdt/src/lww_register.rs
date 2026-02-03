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
            let self_bytes = serde_json::to_vec(&self.value).unwrap_or_default();
            let other_bytes = serde_json::to_vec(&other.value).unwrap_or_default();
            if other_bytes > self_bytes {
                self.value = other.value.clone();
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
