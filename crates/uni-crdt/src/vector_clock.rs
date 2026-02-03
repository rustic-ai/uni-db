// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::CrdtMerge;
use fxhash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Vector clock for causal ordering in distributed systems.
///
/// Maps actor IDs to logical counters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct VectorClock {
    pub clocks: FxHashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the clock for an actor.
    pub fn increment(&mut self, actor: &str) {
        *self.clocks.entry(actor.to_string()).or_insert(0) += 1;
    }

    /// Get the clock value for an actor.
    pub fn get(&self, actor: &str) -> u64 {
        *self.clocks.get(actor).unwrap_or(&0)
    }

    /// Merge another vector clock into this one (point-wise maximum).
    pub fn merge_clock(&mut self, other: &VectorClock) {
        for (actor, &count) in &other.clocks {
            let entry = self.clocks.entry(actor.clone()).or_insert(0);
            *entry = std::cmp::max(*entry, count);
        }
    }

    /// Returns true if self causally happened before other.
    /// A < B iff for all k: A[k] <= B[k] AND exists k: A[k] < B[k].
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        let mut strictly_less = false;

        // Check if all our entries are <= other's
        for (actor, &count) in &self.clocks {
            if count > other.get(actor) {
                return false;
            }
            if count < other.get(actor) {
                strictly_less = true;
            }
        }

        // Also check if other has keys we don't (implicitly our 0 < their count)
        if !strictly_less {
            for (actor, &count) in &other.clocks {
                if !self.clocks.contains_key(actor) && count > 0 {
                    strictly_less = true;
                    break;
                }
            }
        }

        strictly_less
    }

    /// Returns true if neither clock happened before the other (concurrent).
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }

    /// Partial comparison for causal ordering.
    pub fn partial_cmp(&self, other: &VectorClock) -> Option<Ordering> {
        if self == other {
            Some(Ordering::Equal)
        } else if self.happened_before(other) {
            Some(Ordering::Less)
        } else if other.happened_before(self) {
            Some(Ordering::Greater)
        } else {
            None // Concurrent
        }
    }
}

impl CrdtMerge for VectorClock {
    fn merge(&mut self, other: &Self) {
        self.merge_clock(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_ordering() {
        let mut a = VectorClock::new();
        a.increment("node1"); // {node1: 1}

        let mut b = a.clone();
        b.increment("node1"); // {node1: 2}

        // a < b
        assert!(a.happened_before(&b));
        assert!(!b.happened_before(&a));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));

        let mut c = a.clone();
        c.increment("node2"); // {node1: 1, node2: 1}

        // b {node1: 2} vs c {node1: 1, node2: 1} -> Concurrent
        // b has node1: 2 > 1
        // c has node2: 1 > 0
        assert!(!b.happened_before(&c));
        assert!(!c.happened_before(&b));
        assert!(b.is_concurrent(&c));
        assert_eq!(b.partial_cmp(&c), None);
    }

    #[test]
    fn test_merge() {
        let mut a = VectorClock::new();
        a.increment("node1"); // {node1: 1}

        let mut b = VectorClock::new();
        b.increment("node2"); // {node2: 1}

        a.merge(&b); // {node1: 1, node2: 1}
        assert_eq!(a.get("node1"), 1);
        assert_eq!(a.get("node2"), 1);
    }
}
