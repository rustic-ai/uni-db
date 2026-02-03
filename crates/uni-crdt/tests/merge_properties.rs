// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Property-based tests for CRDT merge operations.
//!
//! Tests the fundamental CRDT properties using proptest:
//! - Commutativity: a.merge(b) == b.merge(a)
//! - Associativity: (a.merge(b)).merge(c) == a.merge(b.merge(c))
//! - Idempotency: a.merge(a) == a

use proptest::prelude::*;
use uni_crdt::{CrdtMerge, GCounter, GSet, LWWMap, LWWRegister, ORSet, VectorClock};

// ============================================================================
// Arbitrary Implementations for proptest
// ============================================================================

fn arb_gcounter() -> impl Strategy<Value = GCounter> {
    prop::collection::vec(("[a-z]{1,5}", 0u64..1000), 0..5).prop_map(|entries| {
        let mut gc = GCounter::new();
        for (actor, count) in entries {
            gc.increment(&actor, count);
        }
        gc
    })
}

fn arb_gset() -> impl Strategy<Value = GSet<String>> {
    prop::collection::vec("[a-z]{1,10}", 0..10).prop_map(|elements| {
        let mut gs = GSet::new();
        for elem in elements {
            gs.add(elem);
        }
        gs
    })
}

fn arb_orset() -> impl Strategy<Value = ORSet<String>> {
    // Generate a sequence of add/remove operations
    prop::collection::vec(
        prop::bool::ANY.prop_flat_map(|is_add| "[a-z]{1,5}".prop_map(move |elem| (is_add, elem))),
        0..10,
    )
    .prop_map(|ops| {
        let mut os = ORSet::new();
        for (is_add, elem) in ops {
            if is_add {
                os.add(elem);
            } else {
                os.remove(&elem);
            }
        }
        os
    })
}

fn arb_lww_register() -> impl Strategy<Value = LWWRegister<String>> {
    ("[a-z]{1,10}", 0i64..10000).prop_map(|(value, ts)| LWWRegister::new(value, ts))
}

fn arb_lww_map() -> impl Strategy<Value = LWWMap<String, i32>> {
    prop::collection::vec(("[a-z]{1,5}", 0i32..100, 0i64..1000), 0..5).prop_map(|entries| {
        let mut map = LWWMap::new();
        for (key, value, ts) in entries {
            map.put(key, value, ts);
        }
        map
    })
}

fn arb_vector_clock() -> impl Strategy<Value = VectorClock> {
    prop::collection::vec(("[a-z]{1,3}", 1usize..10), 0..5).prop_map(|entries| {
        let mut vc = VectorClock::new();
        for (actor, count) in entries {
            for _ in 0..count {
                vc.increment(&actor);
            }
        }
        vc
    })
}

// ============================================================================
// GCounter Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn gcounter_merge_commutative(a in arb_gcounter(), b in arb_gcounter()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn gcounter_merge_associative(
        a in arb_gcounter(),
        b in arb_gcounter(),
        c in arb_gcounter()
    ) {
        // (a ∪ b) ∪ c
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        // a ∪ (b ∪ c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        prop_assert_eq!(ab_c.value(), a_bc.value());
    }

    #[test]
    fn gcounter_merge_idempotent(a in arb_gcounter()) {
        let before = a.value();
        let mut merged = a.clone();
        merged.merge(&a);
        prop_assert_eq!(merged.value(), before);
    }

    #[test]
    fn gcounter_value_monotonic(a in arb_gcounter(), b in arb_gcounter()) {
        let before = a.value();
        let mut merged = a.clone();
        merged.merge(&b);
        // Value can only increase or stay the same after merge
        prop_assert!(merged.value() >= before);
    }
}

// ============================================================================
// GSet Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn gset_merge_commutative(a in arb_gset(), b in arb_gset()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab.len(), ba.len());

        // Check all elements are the same
        for elem in ab.elements() {
            prop_assert!(ba.contains(elem));
        }
    }

    #[test]
    fn gset_merge_associative(
        a in arb_gset(),
        b in arb_gset(),
        c in arb_gset()
    ) {
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        prop_assert_eq!(ab_c.len(), a_bc.len());
    }

    #[test]
    fn gset_merge_idempotent(a in arb_gset()) {
        let before_len = a.len();
        let mut merged = a.clone();
        merged.merge(&a);
        prop_assert_eq!(merged.len(), before_len);
    }

    #[test]
    fn gset_size_monotonic(a in arb_gset(), b in arb_gset()) {
        let before = a.len();
        let mut merged = a.clone();
        merged.merge(&b);
        prop_assert!(merged.len() >= before);
    }
}

// ============================================================================
// ORSet Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn orset_merge_commutative(a in arb_orset(), b in arb_orset()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        // Check visible elements are the same
        let ab_elems = ab.elements();
        let ba_elems = ba.elements();
        prop_assert_eq!(ab_elems.len(), ba_elems.len());

        for elem in &ab_elems {
            prop_assert!(ba_elems.contains(elem));
        }
    }

    #[test]
    fn orset_merge_associative(
        a in arb_orset(),
        b in arb_orset(),
        c in arb_orset()
    ) {
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        let ab_c_elems = ab_c.elements();
        let a_bc_elems = a_bc.elements();
        prop_assert_eq!(ab_c_elems.len(), a_bc_elems.len());
    }

    #[test]
    fn orset_merge_idempotent(a in arb_orset()) {
        let before_elems = a.elements();
        let mut merged = a.clone();
        merged.merge(&a);
        let after_elems = merged.elements();

        prop_assert_eq!(before_elems.len(), after_elems.len());
        for elem in &before_elems {
            prop_assert!(after_elems.contains(elem));
        }
    }
}

// ============================================================================
// LWWRegister Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn lww_register_merge_commutative(a in arb_lww_register(), b in arb_lww_register()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        // Both should have the same timestamp (the higher one)
        prop_assert_eq!(ab.timestamp(), ba.timestamp());

        // If timestamps are different, both should have the same value
        if a.timestamp() != b.timestamp() {
            prop_assert_eq!(ab.get(), ba.get());
        }
    }

    #[test]
    fn lww_register_merge_associative(
        a in arb_lww_register(),
        b in arb_lww_register(),
        c in arb_lww_register()
    ) {
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        // Both should converge to the same timestamp
        prop_assert_eq!(ab_c.timestamp(), a_bc.timestamp());
    }

    #[test]
    fn lww_register_merge_idempotent(a in arb_lww_register()) {
        let before_value = a.get().clone();
        let before_ts = a.timestamp();
        let mut merged = a.clone();
        merged.merge(&a);

        prop_assert_eq!(merged.get(), &before_value);
        prop_assert_eq!(merged.timestamp(), before_ts);
    }

    #[test]
    fn lww_register_newer_wins(a in arb_lww_register(), b in arb_lww_register()) {
        let mut merged = a.clone();
        merged.merge(&b);

        let expected_ts = std::cmp::max(a.timestamp(), b.timestamp());
        prop_assert_eq!(merged.timestamp(), expected_ts);
    }
}

// ============================================================================
// LWWMap Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn lww_map_merge_commutative(a in arb_lww_map(), b in arb_lww_map()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        // Check same keys are visible
        let ab_keys: Vec<_> = ab.keys().cloned().collect();
        let ba_keys: Vec<_> = ba.keys().cloned().collect();
        prop_assert_eq!(ab_keys.len(), ba_keys.len());

        // Check values match
        for key in &ab_keys {
            prop_assert_eq!(ab.get(key), ba.get(key));
        }
    }

    #[test]
    fn lww_map_merge_associative(
        a in arb_lww_map(),
        b in arb_lww_map(),
        c in arb_lww_map()
    ) {
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        let ab_c_keys: Vec<_> = ab_c.keys().cloned().collect();
        let a_bc_keys: Vec<_> = a_bc.keys().cloned().collect();
        prop_assert_eq!(ab_c_keys.len(), a_bc_keys.len());

        for key in &ab_c_keys {
            prop_assert_eq!(ab_c.get(key), a_bc.get(key));
        }
    }

    #[test]
    fn lww_map_merge_idempotent(a in arb_lww_map()) {
        let before_len = a.len();
        let mut merged = a.clone();
        merged.merge(&a);
        prop_assert_eq!(merged.len(), before_len);
    }
}

// ============================================================================
// VectorClock Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn vector_clock_merge_commutative(a in arb_vector_clock(), b in arb_vector_clock()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        // Check all actors have the same values
        for (actor, &count) in &ab.clocks {
            prop_assert_eq!(count, ba.get(actor));
        }
        for (actor, &count) in &ba.clocks {
            prop_assert_eq!(count, ab.get(actor));
        }
    }

    #[test]
    fn vector_clock_merge_associative(
        a in arb_vector_clock(),
        b in arb_vector_clock(),
        c in arb_vector_clock()
    ) {
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ab_c = ab;
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        for (actor, &count) in &ab_c.clocks {
            prop_assert_eq!(count, a_bc.get(actor));
        }
    }

    #[test]
    fn vector_clock_merge_idempotent(a in arb_vector_clock()) {
        let before = a.clone();
        let mut merged = a.clone();
        merged.merge(&a);

        for (actor, &count) in &before.clocks {
            prop_assert_eq!(count, merged.get(actor));
        }
    }

    #[test]
    fn vector_clock_merge_monotonic(a in arb_vector_clock(), b in arb_vector_clock()) {
        let mut merged = a.clone();
        merged.merge(&b);

        // All values in merged should be >= corresponding values in a
        for (actor, &count) in &a.clocks {
            prop_assert!(merged.get(actor) >= count);
        }
    }
}

// ============================================================================
// Cross-Type Consistency Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    /// Test that multiple sequential merges produce consistent results
    #[test]
    fn gcounter_multiple_merges_consistent(
        counters in prop::collection::vec(arb_gcounter(), 2..5)
    ) {
        // Merge in order: 0, 1, 2, ...
        let mut forward = counters[0].clone();
        for c in &counters[1..] {
            forward.merge(c);
        }

        // Merge in reverse order
        let mut backward = counters.last().unwrap().clone();
        for c in counters[..counters.len()-1].iter().rev() {
            backward.merge(c);
        }

        // Results should be the same due to commutativity and associativity
        prop_assert_eq!(forward.value(), backward.value());
    }

    /// Test that GSet union is consistent regardless of merge order
    #[test]
    fn gset_union_consistent(
        sets in prop::collection::vec(arb_gset(), 2..5)
    ) {
        let mut forward = sets[0].clone();
        for s in &sets[1..] {
            forward.merge(s);
        }

        let mut backward = sets.last().unwrap().clone();
        for s in sets[..sets.len()-1].iter().rev() {
            backward.merge(s);
        }

        prop_assert_eq!(forward.len(), backward.len());
    }
}
