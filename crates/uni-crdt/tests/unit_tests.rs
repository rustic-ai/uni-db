// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Unit tests for all CRDT types.
//!
//! Tests each CRDT type's core operations, merge behavior, and edge cases.
//! Organized by CRDT type for clarity.

use uni_crdt::{
    Crdt, CrdtMerge, GCounter, GSet, LWWMap, LWWRegister, ORSet, Rga, VCRegister, VectorClock,
};

// ============================================================================
// GCounter Tests
// ============================================================================

mod gcounter {
    use super::*;

    #[test]
    fn test_new_empty() {
        let gc = GCounter::new();
        assert_eq!(gc.value(), 0);
    }

    #[test]
    fn test_increment_single_actor() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 5);
        assert_eq!(gc.actor_count("actor1"), 5);
        assert_eq!(gc.value(), 5);
    }

    #[test]
    fn test_increment_multiple_actors() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 10);
        gc.increment("actor2", 20);
        gc.increment("actor3", 30);
        assert_eq!(gc.actor_count("actor1"), 10);
        assert_eq!(gc.actor_count("actor2"), 20);
        assert_eq!(gc.actor_count("actor3"), 30);
        assert_eq!(gc.value(), 60);
    }

    #[test]
    fn test_increment_accumulates() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 5);
        gc.increment("actor1", 3);
        gc.increment("actor1", 2);
        assert_eq!(gc.actor_count("actor1"), 10);
        assert_eq!(gc.value(), 10);
    }

    #[test]
    fn test_increment_zero_ignored() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 0);
        assert_eq!(gc.actor_count("actor1"), 0);
        assert_eq!(gc.value(), 0);
    }

    #[test]
    fn test_actor_count_missing() {
        let gc = GCounter::new();
        assert_eq!(gc.actor_count("nonexistent"), 0);
    }

    #[test]
    fn test_merge_disjoint_actors() {
        let mut a = GCounter::new();
        a.increment("A", 5);

        let mut b = GCounter::new();
        b.increment("B", 3);

        a.merge(&b);

        assert_eq!(a.actor_count("A"), 5);
        assert_eq!(a.actor_count("B"), 3);
        assert_eq!(a.value(), 8);
    }

    #[test]
    fn test_merge_overlapping_actors() {
        let mut a = GCounter::new();
        a.increment("A", 5);

        let mut b = GCounter::new();
        b.increment("A", 8);

        a.merge(&b);

        // Max per actor: max(5, 8) = 8
        assert_eq!(a.actor_count("A"), 8);
        assert_eq!(a.value(), 8);
    }

    #[test]
    fn test_merge_idempotent() {
        let mut gc = GCounter::new();
        gc.increment("A", 5);
        gc.increment("B", 3);

        let before = gc.clone();
        gc.merge(&before);

        assert_eq!(gc.value(), before.value());
        assert_eq!(gc.actor_count("A"), before.actor_count("A"));
        assert_eq!(gc.actor_count("B"), before.actor_count("B"));
    }

    #[test]
    fn test_merge_commutative() {
        let mut a = GCounter::new();
        a.increment("A", 5);
        a.increment("B", 2);

        let mut b = GCounter::new();
        b.increment("B", 7);
        b.increment("C", 3);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn test_merge_associative() {
        let mut a = GCounter::new();
        a.increment("A", 5);

        let mut b = GCounter::new();
        b.increment("B", 3);

        let mut c = GCounter::new();
        c.increment("C", 7);

        // (a ∪ b) ∪ c
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        // a ∪ (b ∪ c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c.value(), a_bc.value());
    }

    #[test]
    fn test_counts_iterator() {
        let mut gc = GCounter::new();
        gc.increment("A", 5);
        gc.increment("B", 3);

        let counts: Vec<_> = gc.counts().collect();
        assert_eq!(counts.len(), 2);
    }
}

// ============================================================================
// GSet Tests
// ============================================================================

mod gset {
    use super::*;

    #[test]
    fn test_new_empty() {
        let gs: GSet<String> = GSet::new();
        assert!(gs.is_empty());
        assert_eq!(gs.len(), 0);
    }

    #[test]
    fn test_add_single() {
        let mut gs = GSet::new();
        gs.add("apple".to_string());
        assert!(gs.contains(&"apple".to_string()));
        assert_eq!(gs.len(), 1);
    }

    #[test]
    fn test_add_duplicate() {
        let mut gs = GSet::new();
        gs.add("apple".to_string());
        gs.add("apple".to_string());
        // Idempotent: adding twice doesn't change the set
        assert_eq!(gs.len(), 1);
    }

    #[test]
    fn test_contains() {
        let mut gs = GSet::new();
        gs.add("apple".to_string());
        gs.add("banana".to_string());
        assert!(gs.contains(&"apple".to_string()));
        assert!(gs.contains(&"banana".to_string()));
        assert!(!gs.contains(&"cherry".to_string()));
    }

    #[test]
    fn test_elements_iterator() {
        let mut gs = GSet::new();
        gs.add("apple".to_string());
        gs.add("banana".to_string());
        gs.add("cherry".to_string());

        let elements: Vec<_> = gs.elements().collect();
        assert_eq!(elements.len(), 3);
    }

    #[test]
    fn test_merge_disjoint() {
        let mut a = GSet::new();
        a.add("a".to_string());
        a.add("b".to_string());

        let mut b = GSet::new();
        b.add("c".to_string());
        b.add("d".to_string());

        a.merge(&b);

        assert_eq!(a.len(), 4);
        assert!(a.contains(&"a".to_string()));
        assert!(a.contains(&"b".to_string()));
        assert!(a.contains(&"c".to_string()));
        assert!(a.contains(&"d".to_string()));
    }

    #[test]
    fn test_merge_overlapping() {
        let mut a = GSet::new();
        a.add("a".to_string());
        a.add("b".to_string());

        let mut b = GSet::new();
        b.add("b".to_string());
        b.add("c".to_string());

        a.merge(&b);

        assert_eq!(a.len(), 3);
        assert!(a.contains(&"a".to_string()));
        assert!(a.contains(&"b".to_string()));
        assert!(a.contains(&"c".to_string()));
    }

    #[test]
    fn test_merge_idempotent() {
        let mut gs = GSet::new();
        gs.add("a".to_string());
        gs.add("b".to_string());

        let before_len = gs.len();
        let clone = gs.clone();
        gs.merge(&clone);

        assert_eq!(gs.len(), before_len);
    }

    #[test]
    fn test_merge_commutative() {
        let mut a = GSet::new();
        a.add("a".to_string());

        let mut b = GSet::new();
        b.add("b".to_string());

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.len(), ba.len());
        assert!(ab.contains(&"a".to_string()));
        assert!(ab.contains(&"b".to_string()));
    }
}

// ============================================================================
// ORSet Tests
// ============================================================================

mod orset {
    use super::*;

    #[test]
    fn test_new_empty() {
        let os: ORSet<String> = ORSet::new();
        assert!(os.is_empty());
        assert_eq!(os.len(), 0);
    }

    #[test]
    fn test_add_returns_unique_tag() {
        let mut os = ORSet::new();
        let tag1 = os.add("item".to_string());
        let tag2 = os.add("item".to_string());
        // Each add returns a unique tag
        assert_ne!(tag1, tag2);
    }

    #[test]
    fn test_add_and_contains() {
        let mut os = ORSet::new();
        os.add("apple".to_string());
        assert!(os.contains(&"apple".to_string()));
        assert!(!os.contains(&"banana".to_string()));
    }

    #[test]
    fn test_remove_after_add() {
        let mut os = ORSet::new();
        os.add("apple".to_string());
        assert!(os.contains(&"apple".to_string()));

        os.remove(&"apple".to_string());
        assert!(!os.contains(&"apple".to_string()));
    }

    #[test]
    fn test_add_after_remove() {
        let mut os = ORSet::new();
        os.add("apple".to_string());
        os.remove(&"apple".to_string());
        assert!(!os.contains(&"apple".to_string()));

        // Add with new tag
        os.add("apple".to_string());
        // Element is visible again with new tag
        assert!(os.contains(&"apple".to_string()));
    }

    #[test]
    fn test_concurrent_add_remove_add_wins() {
        let mut a = ORSet::new();
        a.add("apple".to_string());

        // Clone and simulate concurrent operations
        let mut b = a.clone();
        b.remove(&"apple".to_string());

        // Concurrent add on 'a'
        a.add("apple".to_string());

        a.merge(&b);

        // Add wins: the new tag in 'a' was not tombstoned in 'b'
        assert!(a.contains(&"apple".to_string()));
    }

    #[test]
    fn test_merge_propagates_tombstones() {
        let mut a = ORSet::new();
        let tag = a.add("apple".to_string());

        let mut b = a.clone();
        b.remove(&"apple".to_string());

        // No concurrent add on 'a', just merge
        a.merge(&b);

        // Tombstone propagated, element is removed
        assert!(!a.contains(&"apple".to_string()));
        // The tag should still exist in elements but be tombstoned
        let _ = tag;
    }

    #[test]
    fn test_elements_returns_visible_only() {
        let mut os = ORSet::new();
        os.add("apple".to_string());
        os.add("banana".to_string());
        os.remove(&"apple".to_string());

        let elements = os.elements();
        assert_eq!(elements.len(), 1);
        assert!(elements.contains(&"banana".to_string()));
        assert!(!elements.contains(&"apple".to_string()));
    }

    #[test]
    fn test_len_counts_visible_only() {
        let mut os = ORSet::new();
        os.add("a".to_string());
        os.add("b".to_string());
        os.add("c".to_string());
        os.remove(&"b".to_string());

        assert_eq!(os.len(), 2);
    }

    #[test]
    fn test_merge_idempotent() {
        let mut os = ORSet::new();
        os.add("a".to_string());
        os.add("b".to_string());
        os.remove(&"a".to_string());

        let before_len = os.len();
        let clone = os.clone();
        os.merge(&clone);

        assert_eq!(os.len(), before_len);
    }
}

// ============================================================================
// LWWRegister Tests
// ============================================================================

mod lww_register {
    use super::*;

    #[test]
    fn test_new_with_timestamp() {
        let reg = LWWRegister::new("hello".to_string(), 100);
        assert_eq!(reg.get(), "hello");
        assert_eq!(reg.timestamp(), 100);
    }

    #[test]
    fn test_set_newer_wins() {
        let mut reg = LWWRegister::new("old".to_string(), 100);
        reg.set("new".to_string(), 200);
        assert_eq!(reg.get(), "new");
        assert_eq!(reg.timestamp(), 200);
    }

    #[test]
    fn test_set_older_ignored() {
        let mut reg = LWWRegister::new("current".to_string(), 200);
        reg.set("older".to_string(), 100);
        // Older timestamp is ignored
        assert_eq!(reg.get(), "current");
        assert_eq!(reg.timestamp(), 200);
    }

    #[test]
    fn test_set_equal_timestamp_accepted() {
        let mut reg = LWWRegister::new("first".to_string(), 100);
        reg.set("second".to_string(), 100);
        // Equal timestamp: set is accepted (>= semantics)
        assert_eq!(reg.get(), "second");
    }

    #[test]
    fn test_merge_takes_newer() {
        let a = LWWRegister::new("A".to_string(), 100);
        let b = LWWRegister::new("B".to_string(), 200);

        let mut a_clone = a.clone();
        a_clone.merge(&b);
        assert_eq!(a_clone.get(), "B");
        assert_eq!(a_clone.timestamp(), 200);

        let mut b_clone = b.clone();
        b_clone.merge(&a);
        // B remains since it's newer
        assert_eq!(b_clone.get(), "B");
    }

    #[test]
    fn test_merge_tie_deterministic() {
        let a = LWWRegister::new("A".to_string(), 100);
        let b = LWWRegister::new("B".to_string(), 100);

        // With deterministic tie-breaking, both sides converge to the same value.
        // "B" > "A" when serialized, so "B" wins the tie.
        let mut a_clone = a.clone();
        a_clone.merge(&b);
        assert_eq!(a_clone.get(), "B");

        let mut b_clone = b.clone();
        b_clone.merge(&a);
        assert_eq!(b_clone.get(), "B");

        // Both converge — commutativity holds.
        assert_eq!(a_clone.get(), b_clone.get());
    }

    #[test]
    fn test_merge_idempotent() {
        let reg = LWWRegister::new("value".to_string(), 100);
        let mut reg_clone = reg.clone();
        reg_clone.merge(&reg);
        assert_eq!(reg_clone.get(), reg.get());
        assert_eq!(reg_clone.timestamp(), reg.timestamp());
    }
}

// ============================================================================
// LWWMap Tests
// ============================================================================

mod lww_map {
    use super::*;

    #[test]
    fn test_new_empty() {
        let map: LWWMap<String, i32> = LWWMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_put_and_get() {
        let mut map = LWWMap::new();
        map.put("key1".to_string(), 42, 100);
        assert_eq!(map.get(&"key1".to_string()), Some(&42));
    }

    #[test]
    fn test_put_newer_overwrites() {
        let mut map = LWWMap::new();
        map.put("key".to_string(), 1, 100);
        map.put("key".to_string(), 2, 200);
        assert_eq!(map.get(&"key".to_string()), Some(&2));
    }

    #[test]
    fn test_put_older_ignored() {
        let mut map = LWWMap::new();
        map.put("key".to_string(), 2, 200);
        map.put("key".to_string(), 1, 100);
        // Older timestamp ignored
        assert_eq!(map.get(&"key".to_string()), Some(&2));
    }

    #[test]
    fn test_remove_with_timestamp() {
        let mut map = LWWMap::new();
        map.put("key".to_string(), 1, 100);
        map.remove(&"key".to_string(), 200);
        assert_eq!(map.get(&"key".to_string()), None);
    }

    #[test]
    fn test_put_after_remove_newer_wins() {
        let mut map = LWWMap::new();
        map.put("key".to_string(), 1, 100);
        map.remove(&"key".to_string(), 200);
        map.put("key".to_string(), 2, 300);
        // Newer put wins
        assert_eq!(map.get(&"key".to_string()), Some(&2));
    }

    #[test]
    fn test_put_after_remove_older_loses() {
        let mut map = LWWMap::new();
        map.put("key".to_string(), 1, 100);
        map.remove(&"key".to_string(), 200);
        map.put("key".to_string(), 2, 150);
        // Put at 150 < remove at 200, so still removed
        assert_eq!(map.get(&"key".to_string()), None);
    }

    #[test]
    fn test_merge_per_key() {
        let mut a = LWWMap::new();
        a.put("k1".to_string(), 1, 100);

        let mut b = LWWMap::new();
        b.put("k1".to_string(), 2, 200);
        b.put("k2".to_string(), 3, 100);

        a.merge(&b);

        // k1 should have value from b (higher timestamp)
        assert_eq!(a.get(&"k1".to_string()), Some(&2));
        // k2 should be present
        assert_eq!(a.get(&"k2".to_string()), Some(&3));
    }

    #[test]
    fn test_keys_excludes_tombstoned() {
        let mut map = LWWMap::new();
        map.put("a".to_string(), 1, 100);
        map.put("b".to_string(), 2, 100);
        map.remove(&"a".to_string(), 200);

        let keys: Vec<_> = map.keys().collect();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&&"b".to_string()));
    }

    #[test]
    fn test_merge_idempotent() {
        let mut map = LWWMap::new();
        map.put("a".to_string(), 1, 100);

        let before_len = map.len();
        let clone = map.clone();
        map.merge(&clone);

        assert_eq!(map.len(), before_len);
    }
}

// ============================================================================
// Rga Tests
// ============================================================================

mod rga {
    use super::*;

    #[test]
    fn test_new_empty() {
        let rga: Rga<char> = Rga::new();
        assert!(rga.is_empty());
        assert_eq!(rga.len(), 0);
    }

    #[test]
    fn test_insert_at_beginning() {
        let mut rga = Rga::new();
        rga.insert(None, 'A', 1);
        let vec = rga.to_vec();
        assert_eq!(vec, vec!['A']);
    }

    #[test]
    fn test_insert_after_node() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, 'H', 1);
        let id2 = rga.insert(Some(id1), 'e', 2);
        let id3 = rga.insert(Some(id2), 'l', 3);
        let id4 = rga.insert(Some(id3), 'l', 4);
        rga.insert(Some(id4), 'o', 5);

        let result: String = rga.to_vec().into_iter().collect();
        assert_eq!(result, "Hello");
    }

    #[test]
    fn test_delete_tombstones() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, 'H', 1);
        let id2 = rga.insert(Some(id1), 'i', 2);

        assert_eq!(rga.len(), 2);
        assert_eq!(rga.to_vec(), vec!['H', 'i']);

        rga.delete(id2);
        assert_eq!(rga.len(), 1);
        assert_eq!(rga.to_vec(), vec!['H']);
    }

    #[test]
    fn test_to_vec_ordering_deterministic() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, 'A', 1);
        rga.insert(Some(id1), 'B', 2);
        rga.insert(Some(id1), 'C', 3);

        // With two concurrent inserts after id1:
        // - C (timestamp 3) should come before B (timestamp 2) due to desc timestamp ordering
        let result: String = rga.to_vec().into_iter().collect();
        assert_eq!(result, "ACB");
    }

    #[test]
    fn test_merge_concurrent_inserts() {
        let mut a = Rga::new();
        let id0 = a.insert(None, 'A', 1);

        let mut b = a.clone();

        // Concurrent inserts after id0
        a.insert(Some(id0), 'B', 2);
        b.insert(Some(id0), 'C', 3);

        a.merge(&b);

        let result: String = a.to_vec().into_iter().collect();
        // C (ts=3) comes before B (ts=2)
        assert_eq!(result, "ACB");
    }

    #[test]
    fn test_merge_propagates_tombstones() {
        let mut a = Rga::new();
        let id1 = a.insert(None, 'A', 1);
        let id2 = a.insert(Some(id1), 'B', 2);

        let mut b = a.clone();
        b.delete(id2);

        a.merge(&b);

        assert_eq!(a.to_vec(), vec!['A']);
    }

    #[test]
    fn test_merge_idempotent() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, 'A', 1);
        rga.insert(Some(id1), 'B', 2);
        rga.delete(id1);

        let before_vec = rga.to_vec();
        let clone = rga.clone();
        rga.merge(&clone);

        assert_eq!(rga.to_vec(), before_vec);
    }

    #[test]
    fn test_len_excludes_tombstones() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, 'A', 1);
        let id2 = rga.insert(Some(id1), 'B', 2);
        rga.insert(Some(id2), 'C', 3);
        rga.delete(id2);

        assert_eq!(rga.len(), 2);
    }
}

// ============================================================================
// VectorClock Tests
// ============================================================================

mod vector_clock {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_new_empty() {
        let vc = VectorClock::new();
        assert_eq!(vc.get("any"), 0);
    }

    #[test]
    fn test_increment() {
        let mut vc = VectorClock::new();
        vc.increment("node1");
        assert_eq!(vc.get("node1"), 1);
        vc.increment("node1");
        assert_eq!(vc.get("node1"), 2);
    }

    #[test]
    fn test_get_missing_actor() {
        let vc = VectorClock::new();
        assert_eq!(vc.get("nonexistent"), 0);
    }

    #[test]
    fn test_happened_before_simple() {
        let mut a = VectorClock::new();
        a.increment("node1"); // {node1: 1}

        let mut b = a.clone();
        b.increment("node1"); // {node1: 2}

        assert!(a.happened_before(&b));
        assert!(!b.happened_before(&a));
    }

    #[test]
    fn test_happened_before_disjoint() {
        let mut a = VectorClock::new();
        a.increment("node1"); // {node1: 1}

        let mut b = VectorClock::new();
        b.increment("node2"); // {node2: 1}

        // Neither happened before the other
        assert!(!a.happened_before(&b));
        assert!(!b.happened_before(&a));
    }

    #[test]
    fn test_is_concurrent() {
        let mut a = VectorClock::new();
        a.increment("node1"); // {node1: 1}

        let mut b = VectorClock::new();
        b.increment("node2"); // {node2: 1}

        assert!(a.is_concurrent(&b));
        assert!(b.is_concurrent(&a));
    }

    #[test]
    fn test_is_concurrent_complex() {
        let mut base = VectorClock::new();
        base.increment("node1"); // {node1: 1}

        let mut a = base.clone();
        a.increment("node1"); // {node1: 2}

        let mut b = base.clone();
        b.increment("node2"); // {node1: 1, node2: 1}

        // a has node1: 2 > b's node1: 1
        // b has node2: 1 > a's node2: 0
        // So they are concurrent
        assert!(a.is_concurrent(&b));
    }

    #[test]
    fn test_partial_cmp_equal() {
        let mut a = VectorClock::new();
        a.increment("node1");

        let b = a.clone();

        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));
    }

    #[test]
    fn test_partial_cmp_less() {
        let mut a = VectorClock::new();
        a.increment("node1");

        let mut b = a.clone();
        b.increment("node1");

        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));
    }

    #[test]
    fn test_partial_cmp_greater() {
        let mut a = VectorClock::new();
        a.increment("node1");

        let mut b = a.clone();
        b.increment("node1");

        assert_eq!(b.partial_cmp(&a), Some(Ordering::Greater));
    }

    #[test]
    fn test_partial_cmp_concurrent() {
        let mut a = VectorClock::new();
        a.increment("node1");

        let mut b = VectorClock::new();
        b.increment("node2");

        assert_eq!(a.partial_cmp(&b), None);
    }

    #[test]
    fn test_merge_pointwise_max() {
        let mut a = VectorClock::new();
        a.increment("node1"); // {node1: 1}
        a.increment("node1"); // {node1: 2}

        let mut b = VectorClock::new();
        b.increment("node1"); // {node1: 1}
        b.increment("node2"); // {node1: 1, node2: 1}

        a.merge(&b);

        assert_eq!(a.get("node1"), 2); // max(2, 1)
        assert_eq!(a.get("node2"), 1); // max(0, 1)
    }

    #[test]
    fn test_merge_idempotent() {
        let mut vc = VectorClock::new();
        vc.increment("node1");
        vc.increment("node2");

        let before = vc.clone();
        vc.merge(&before);

        assert_eq!(vc.get("node1"), before.get("node1"));
        assert_eq!(vc.get("node2"), before.get("node2"));
    }

    #[test]
    fn test_merge_commutative() {
        let mut a = VectorClock::new();
        a.increment("node1");

        let mut b = VectorClock::new();
        b.increment("node2");

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.get("node1"), ba.get("node1"));
        assert_eq!(ab.get("node2"), ba.get("node2"));
    }
}

// ============================================================================
// VCRegister Tests
// ============================================================================

mod vc_register {
    use super::*;
    use uni_crdt::vc_register::MergeResult;

    #[test]
    fn test_new_increments_clock() {
        let reg = VCRegister::new("value".to_string(), "actor1");
        assert_eq!(reg.get(), "value");
        assert_eq!(reg.clock().get("actor1"), 1);
    }

    #[test]
    fn test_set_increments_clock() {
        let mut reg = VCRegister::new("initial".to_string(), "actor1");
        reg.set("updated".to_string(), "actor1");
        assert_eq!(reg.get(), "updated");
        assert_eq!(reg.clock().get("actor1"), 2);
    }

    #[test]
    fn test_merge_takes_causally_newer() {
        let mut r1 = VCRegister::new("A".to_string(), "node1"); // {node1: 1}
        let r2 = r1.clone(); // {node1: 1}

        r1.set("B".to_string(), "node1"); // {node1: 2}

        // r1 > r2 causally
        let mut r2_copy = r2.clone();
        let result = r2_copy.merge_register(&r1);
        assert_eq!(result, MergeResult::TookOther);
        assert_eq!(r2_copy.get(), "B");

        // r1 merging r2 should keep self
        let mut r1_copy = r1.clone();
        let result = r1_copy.merge_register(&r2);
        assert_eq!(result, MergeResult::KeptSelf);
        assert_eq!(r1_copy.get(), "B");
    }

    #[test]
    fn test_merge_concurrent_keeps_self() {
        let base = VCRegister::new("Base".to_string(), "node1"); // {node1: 1}

        let mut r1 = base.clone();
        r1.set("A".to_string(), "node1"); // {node1: 2}

        let mut r2 = base.clone();
        r2.set("B".to_string(), "node2"); // {node1: 1, node2: 1}

        // r1 and r2 are concurrent
        let mut r1_copy = r1.clone();
        let result = r1_copy.merge_register(&r2);
        assert_eq!(result, MergeResult::Concurrent);
        // Tie-break: keeps self
        assert_eq!(r1_copy.get(), "A");
    }

    #[test]
    fn test_merge_result_enum() {
        let base = VCRegister::new("Base".to_string(), "node1");

        // KeptSelf: merge with older
        let mut r1 = base.clone();
        r1.set("A".to_string(), "node1");
        let result = r1.merge_register(&base);
        assert_eq!(result, MergeResult::KeptSelf);

        // TookOther: merge with newer
        let mut newer = base.clone();
        newer.set("X".to_string(), "node1");
        newer.set("Y".to_string(), "node1");
        let mut r2 = base.clone();
        let result = r2.merge_register(&newer);
        assert_eq!(result, MergeResult::TookOther);

        // Concurrent: concurrent updates
        let mut r3 = base.clone();
        r3.set("C".to_string(), "node2");
        let mut r4 = base.clone();
        r4.set("D".to_string(), "node1");
        let result = r3.merge_register(&r4);
        assert_eq!(result, MergeResult::Concurrent);
    }

    #[test]
    fn test_merge_clock_always_merged() {
        let base = VCRegister::new("Base".to_string(), "node1"); // {node1: 1}

        let mut r1 = base.clone();
        r1.set("A".to_string(), "node1"); // {node1: 2}

        let mut r2 = base.clone();
        r2.set("B".to_string(), "node2"); // {node1: 1, node2: 1}

        r1.merge(&r2);

        // Clocks should be merged regardless of which value wins
        assert_eq!(r1.clock().get("node1"), 2);
        assert_eq!(r1.clock().get("node2"), 1);
    }

    #[test]
    fn test_merge_idempotent() {
        let reg = VCRegister::new("value".to_string(), "actor1");
        let mut reg_clone = reg.clone();
        reg_clone.merge(&reg);
        assert_eq!(reg_clone.get(), reg.get());
    }
}

// ============================================================================
// Crdt Wrapper Tests
// ============================================================================

mod crdt_wrapper {
    use super::*;

    #[test]
    fn test_try_merge_same_type() {
        let mut gc1 = GCounter::new();
        gc1.increment("A", 5);
        let mut crdt1 = Crdt::GCounter(gc1);

        let mut gc2 = GCounter::new();
        gc2.increment("B", 3);
        let crdt2 = Crdt::GCounter(gc2);

        let result = crdt1.try_merge(&crdt2);
        assert!(result.is_ok());

        if let Crdt::GCounter(gc) = &crdt1 {
            assert_eq!(gc.value(), 8);
        } else {
            panic!("Expected GCounter");
        }
    }

    #[test]
    fn test_try_merge_different_types_fails() {
        let mut crdt1 = Crdt::GCounter(GCounter::new());
        let crdt2 = Crdt::GSet(GSet::new());

        let result = crdt1.try_merge(&crdt2);
        assert!(result.is_err());
    }

    #[test]
    fn test_type_name() {
        assert_eq!(Crdt::GCounter(GCounter::new()).type_name(), "GCounter");
        assert_eq!(Crdt::GSet(GSet::new()).type_name(), "GSet");
        assert_eq!(Crdt::ORSet(ORSet::new()).type_name(), "ORSet");
        assert_eq!(
            Crdt::LWWRegister(LWWRegister::new(serde_json::json!(null), 0)).type_name(),
            "LWWRegister"
        );
        assert_eq!(Crdt::LWWMap(LWWMap::new()).type_name(), "LWWMap");
        assert_eq!(Crdt::Rga(Rga::new()).type_name(), "Rga");
        assert_eq!(
            Crdt::VectorClock(VectorClock::new()).type_name(),
            "VectorClock"
        );
        assert_eq!(
            Crdt::VCRegister(VCRegister::new(serde_json::json!(null), "a")).type_name(),
            "VCRegister"
        );
    }

    #[test]
    #[should_panic(expected = "Cannot merge different CRDT types")]
    fn test_merge_panics_on_type_mismatch() {
        let mut crdt1 = Crdt::GCounter(GCounter::new());
        let crdt2 = Crdt::GSet(GSet::new());
        crdt1.merge(&crdt2);
    }
}
