// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::{CrdtMerge, VectorClock};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Outcome of merging another [`VCRegister`] into this one.
///
/// Public diagnostic returned by [`VCRegister::merge_register`] so callers
/// (and the conflict-resolution test suites) can observe which branch the
/// causal comparison took. The blanket [`CrdtMerge::merge`] impl discards it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeResult {
    KeptSelf,
    TookOther,
    /// Clocks were concurrent: clocks merged and the value chosen by a
    /// deterministic serialized-byte tie-break (so the merge is commutative).
    Concurrent,
}

/// Last-writer-wins register using vector clocks for causal ordering.
///
/// If `other` is causally newer, we take `other`.
/// If `self` is causally newer, we keep `self`.
/// If concurrent, the value is chosen by a deterministic tie-break (the larger
/// serialized form wins) and the clocks are merged, so the merge is commutative:
/// `a.merge(b)` and `b.merge(a)` converge to the same value — a CRDT requirement.
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
}

impl<T: Clone + Serialize> VCRegister<T> {
    /// Merge `other` into `self`, returning which causal branch was taken.
    ///
    /// On concurrent (incomparable) clocks the value is chosen by a deterministic
    /// tie-break — the lexicographically-larger serialized form wins — so the
    /// merge is commutative and replicas converge. Keeping `self` arbitrarily
    /// (the previous behavior) let two replicas hold equal clocks but different
    /// values forever, violating CRDT convergence (review #4).
    pub fn merge_register(&mut self, other: &VCRegister<T>) -> MergeResult {
        match self.clock.causal_cmp(&other.clock) {
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
                // Concurrent: deterministic tie-break on the serialized value so
                // `a.merge(b)` and `b.merge(a)` converge to the same value
                // (mirrors `LWWRegister`'s equal-timestamp tie-break).
                // #233 Tier 1: see `LWWRegister::merge`. `unwrap_or_default()`
                // made two unserializable values compare equal, so concurrent
                // writes each kept their own and diverged permanently.
                let self_bytes = serde_json::to_vec(&self.value);
                let other_bytes = serde_json::to_vec(&other.value);
                if let (Err(e), Err(_)) = (&self_bytes, &other_bytes) {
                    tracing::error!(
                        error = %e,
                        "VCRegister: neither value could be serialized for the concurrent-write \
                         tie-break; replicas may diverge permanently",
                    );
                } else {
                    let self_key = self_bytes.ok();
                    let other_key = other_bytes.ok();
                    if other_key > self_key {
                        self.value = other.value.clone();
                    }
                }
                self.clock.merge(&other.clock);
                MergeResult::Concurrent
            }
        }
    }
}

impl<T: Clone + Serialize> CrdtMerge for VCRegister<T> {
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
        // Deterministic tie-break: the larger serialized value ("B" > "A") wins,
        // regardless of which side is `self`.
        assert_eq!(r1_copy.get(), "B");
        // Clock should be merged {node1: 2, node2: 1}
        assert_eq!(r1_copy.clock.get("node1"), 2);
        assert_eq!(r1_copy.clock.get("node2"), 1);
    }

    /// Regression for the 2026-06-10 review bug #4: on concurrent clocks the
    /// merge must be commutative — both replicas must converge to the SAME value,
    /// not arbitrarily keep their own (which left equal clocks but divergent
    /// values forever).
    #[test]
    fn test_vc_register_concurrent_merge_converges() {
        let r_base = VCRegister::new("Base".to_string(), "node1");

        let mut a = r_base.clone();
        a.set("A".to_string(), "node1"); // {node1: 2}
        let mut b = r_base.clone();
        b.set("B".to_string(), "node2"); // {node1: 1, node2: 1}

        // a.merge(b) on replica 1, b.merge(a) on replica 2.
        let mut a_into = a.clone();
        a_into.merge(&b);
        let mut b_into = b.clone();
        b_into.merge(&a);

        // Both replicas converge to the same value and the same merged clock.
        assert_eq!(
            a_into.get(),
            b_into.get(),
            "concurrent merge must converge to the same value (was divergent)"
        );
        assert_eq!(a_into.clock.get("node1"), b_into.clock.get("node1"));
        assert_eq!(a_into.clock.get("node2"), b_into.clock.get("node2"));
    }
}
