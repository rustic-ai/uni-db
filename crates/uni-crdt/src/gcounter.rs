// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::CrdtMerge;
use fxhash::FxHashMap;
use serde::{Deserialize, Serialize};

/// A Grow-only Counter (GCounter).
///
/// Increments are tracked per actor. The total value is the sum of all actor counts.
/// Merging takes the maximum count for each actor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct GCounter {
    /// actor_id -> count
    /// Using FxHashMap for better performance.
    counts: FxHashMap<String, u64>,
}

impl GCounter {
    /// Create a new, empty GCounter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the counter for a specific actor.
    pub fn increment(&mut self, actor: &str, value: u64) {
        if value == 0 {
            return;
        }
        let count = self.counts.entry(actor.to_string()).or_insert(0);
        *count += value;
    }

    /// Get the current total value of the counter.
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Get a specific actor's count.
    pub fn actor_count(&self, actor: &str) -> u64 {
        *self.counts.get(actor).unwrap_or(&0)
    }

    /// Returns an iterator over all actor counts.
    pub fn counts(&self) -> impl Iterator<Item = (&String, &u64)> {
        self.counts.iter()
    }
}

impl CrdtMerge for GCounter {
    fn merge(&mut self, other: &Self) {
        for (actor, other_count) in &other.counts {
            let count = self.counts.entry(actor.clone()).or_insert(0);
            if *other_count > *count {
                *count = *other_count;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment() {
        let mut gc = GCounter::new();
        gc.increment("A", 5);
        gc.increment("B", 3);
        gc.increment("A", 2);
        assert_eq!(gc.value(), 10);
        assert_eq!(gc.actor_count("A"), 7);
        assert_eq!(gc.actor_count("B"), 3);
    }

    #[test]
    fn test_merge() {
        let mut a = GCounter::new();
        a.increment("A", 5);
        a.increment("B", 2);

        let mut b = GCounter::new();
        b.increment("B", 7);
        b.increment("C", 3);

        a.merge(&b);

        assert_eq!(a.actor_count("A"), 5);
        assert_eq!(a.actor_count("B"), 7);
        assert_eq!(a.actor_count("C"), 3);
        assert_eq!(a.value(), 15);
    }

    #[test]
    fn test_merge_idempotency() {
        let mut a = GCounter::new();
        a.increment("A", 5);
        let b = a.clone();
        a.merge(&b);
        assert_eq!(a, b);
    }

    #[test]
    fn test_merge_commutativity() {
        let mut a = GCounter::new();
        a.increment("A", 5);
        a.increment("B", 2);

        let mut b = GCounter::new();
        b.increment("B", 7);
        b.increment("C", 3);

        let mut a_merge_b = a.clone();
        a_merge_b.merge(&b);

        let mut b_merge_a = b.clone();
        b_merge_a.merge(&a);

        assert_eq!(a_merge_b, b_merge_a);
    }
}
