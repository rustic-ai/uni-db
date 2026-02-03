// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::{CrdtMerge, VectorClock};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Result of a merge operation on a VCRegister.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeResult {
    KeptSelf,
    TookOther,
    Concurrent, // Merged clocks, kept value based on policy (e.g. LWW on wall clock or arbitrary)
}

/// Last-writer-wins register using vector clocks for causal ordering.
///
/// If `other` is causally newer, we take `other`.
/// If `self` is causally newer, we keep `self`.
/// If concurrent, we currently keep `self`'s value but merge clocks (arbitrary tie-break).
/// A real system might use a secondary wall-clock for concurrent tie-breaking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VCRegister<T: Clone> {
    pub value: T,
    pub clock: VectorClock,
}

impl<T: Clone> VCRegister<T> {
    pub fn new(value: T, actor: &str) -> Self {
        let mut clock = VectorClock::new();
        clock.increment(actor);
        Self { value, clock }
    }

    pub fn get(&self) -> &T {
        &self.value
    }

    pub fn clock(&self) -> &VectorClock {
        &self.clock
    }

    pub fn set(&mut self, value: T, actor: &str) {
        self.clock.increment(actor);
        self.value = value;
    }

    pub fn merge_register(&mut self, other: &VCRegister<T>) -> MergeResult {
        match self.clock.partial_cmp(&other.clock) {
            Some(Ordering::Less) => {
                // Other is causally newer
                self.value = other.value.clone();
                self.clock.merge(&other.clock);
                MergeResult::TookOther
            }
            Some(Ordering::Greater) | Some(Ordering::Equal) => {
                // Self is newer or equal
                self.clock.merge(&other.clock);
                MergeResult::KeptSelf
            }
            None => {
                // Concurrent! Merge clocks.
                // Tie-breaking: Arbitrarily keep self's value for now.
                // Ideally, we'd have a wall clock to break ties.
                self.clock.merge(&other.clock);
                MergeResult::Concurrent
            }
        }
    }
}

impl<T: Clone> CrdtMerge for VCRegister<T> {
    fn merge(&mut self, other: &Self) {
        self.merge_register(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vc_register_causal() {
        let mut r1 = VCRegister::new("A".to_string(), "node1"); // {node1: 1}
        let r2 = r1.clone();

        r1.set("B".to_string(), "node1"); // {node1: 2} -> r1 > r2

        // r1 merge r2 -> keep r1 (B)
        let mut r1_copy = r1.clone();
        r1_copy.merge(&r2);
        assert_eq!(r1_copy.get(), "B");

        // r2 merge r1 -> take r1 (B)
        let mut r2_copy = r2.clone();
        r2_copy.merge(&r1);
        assert_eq!(r2_copy.get(), "B");
    }

    #[test]
    fn test_vc_register_concurrent() {
        let r_base = VCRegister::new("Base".to_string(), "node1"); // {node1: 1}

        let mut r1 = r_base.clone();
        r1.set("A".to_string(), "node1"); // {node1: 2}

        let mut r2 = r_base.clone();
        r2.set("B".to_string(), "node2"); // {node1: 1, node2: 1}

        // Concurrent:
        // r1 {node1: 2} vs r2 {node1: 1, node2: 1}
        // r1 has node1 > r2
        // r2 has node2 > r1

        let mut r1_copy = r1.clone();
        let res = r1_copy.merge_register(&r2);
        assert_eq!(res, MergeResult::Concurrent);
        // We keep self ("A") in tie-break
        assert_eq!(r1_copy.get(), "A");
        // But clock should be merged {node1: 2, node2: 1}
        assert_eq!(r1_copy.clock.get("node1"), 2);
        assert_eq!(r1_copy.clock.get("node2"), 1);
    }
}
