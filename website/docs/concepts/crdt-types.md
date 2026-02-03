# CRDT Types

Uni includes a comprehensive library of Conflict-free Replicated Data Types (CRDTs) for distributed, eventually-consistent data synchronization.

## Overview

CRDTs are data structures that can be replicated across multiple nodes, updated independently, and merged without conflicts. They guarantee eventual consistency without coordination.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            CRDT TYPES IN UNI                                 │
├──────────────────┬──────────────────┬───────────────────────────────────────┤
│    COUNTERS      │      SETS        │           REGISTERS & MAPS            │
├──────────────────┼──────────────────┼───────────────────────────────────────┤
│ • GCounter       │ • GSet           │ • LWWRegister                         │
│   (Grow-only)    │   (Grow-only)    │   (Last-Writer-Wins)                  │
│                  │ • ORSet          │ • LWWMap                              │
│                  │   (Add-wins)     │   (Per-key LWW)                       │
│                  │                  │ • VCRegister                          │
│                  │                  │   (Vector-clock-based)                │
├──────────────────┴──────────────────┴───────────────────────────────────────┤
│                          CAUSAL ORDERING                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ • VectorClock - Causal ordering primitive for distributed systems           │
├──────────────────┴──────────────────┴───────────────────────────────────────┤
│                          SEQUENCES                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Rga (Replicated Growable Array) - Ordered sequences with insert/delete    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## CrdtMerge Trait

All CRDT types implement the `CrdtMerge` trait:

```rust
pub trait CrdtMerge {
    /// Merge another instance into self.
    /// Must satisfy: commutativity, associativity, idempotency.
    fn merge(&mut self, other: &Self);
}
```

**Properties Guaranteed:**
- **Commutativity**: `a.merge(b) == b.merge(a)`
- **Associativity**: `(a.merge(b)).merge(c) == a.merge(b.merge(c))`
- **Idempotency**: `a.merge(a) == a`

---

## GCounter (Grow-only Counter)

A counter that can only be incremented. Each node maintains its own count, and the total is the sum across all nodes.

### Use Cases
- View counts
- Like counters
- Event tallies
- Any monotonically increasing metric

### API

```rust
use uni_crdt::GCounter;

let mut counter = GCounter::new();

// Increment for a specific actor/node
counter.increment("node_a", 5);
counter.increment("node_b", 3);
counter.increment("node_a", 2);

// Get total value
assert_eq!(counter.value(), 10);

// Get per-actor count
assert_eq!(counter.actor_count("node_a"), 7);
assert_eq!(counter.actor_count("node_b"), 3);
```

### Merge Behavior

```rust
let mut a = GCounter::new();
a.increment("A", 5);
a.increment("B", 2);

let mut b = GCounter::new();
b.increment("B", 7);  // B has higher count here
b.increment("C", 3);

a.merge(&b);

// Takes maximum for each actor
assert_eq!(a.actor_count("A"), 5);
assert_eq!(a.actor_count("B"), 7);  // max(2, 7)
assert_eq!(a.actor_count("C"), 3);
assert_eq!(a.value(), 15);
```

---

## GSet (Grow-only Set)

A set that only supports additions. Elements can never be removed.

### Use Cases
- Append-only logs
- Immutable tag collections
- User IDs that have seen content
- One-time events

### API

```rust
use uni_crdt::GSet;

let mut set: GSet<String> = GSet::new();

// Add elements
set.add("apple".to_string());
set.add("banana".to_string());

// Check membership
assert!(set.contains(&"apple".to_string()));
assert!(!set.contains(&"cherry".to_string()));

// Iterate elements
for elem in set.elements() {
    println!("{}", elem);
}

assert_eq!(set.len(), 2);
```

### Merge Behavior

```rust
let mut a: GSet<i32> = GSet::new();
a.add(1);
a.add(2);

let mut b: GSet<i32> = GSet::new();
b.add(2);
b.add(3);

a.merge(&b);

// Union of both sets
assert!(a.contains(&1));
assert!(a.contains(&2));
assert!(a.contains(&3));
assert_eq!(a.len(), 3);
```

---

## ORSet (Observed-Remove Set)

A set that supports both add and remove operations. Uses unique tags to track element provenance. Concurrent add + remove results in the element being present (**add-wins** semantics).

### Use Cases
- Collaborative editing (selected items)
- User preferences
- Shopping carts
- Any set with deletion support

### API

```rust
use uni_crdt::ORSet;

let mut set: ORSet<String> = ORSet::new();

// Add elements (returns a unique tag)
let tag = set.add("apple".to_string());
set.add("banana".to_string());

// Remove elements
set.remove(&"apple".to_string());

// Check membership
assert!(!set.contains(&"apple".to_string()));
assert!(set.contains(&"banana".to_string()));

// Get visible elements
let elements = set.elements();
```

### Add-Wins Semantics

```rust
let mut a: ORSet<String> = ORSet::new();
a.add("apple".to_string());

let mut b = a.clone();
b.remove(&"apple".to_string());  // Remove on replica B

// Concurrent add on replica A
a.add("apple".to_string());  // New tag created

a.merge(&b);

// Add wins! The new tag in 'a' wasn't tombstoned in 'b'
assert!(a.contains(&"apple".to_string()));
```

---

## LWWRegister (Last-Writer-Wins Register)

A single-value register where conflicts are resolved by timestamp. The value with the highest timestamp wins.

### Use Cases
- User profile fields
- Document titles
- Configuration values
- Any single-value property

### API

```rust
use uni_crdt::LWWRegister;

let mut reg = LWWRegister::new("initial".to_string(), 100);

// Set with timestamp
reg.set("newer".to_string(), 110);
assert_eq!(reg.get(), "newer");

// Older timestamp is ignored
reg.set("older".to_string(), 105);
assert_eq!(reg.get(), "newer");  // Still "newer"

// Get current timestamp
assert_eq!(reg.timestamp(), 110);
```

### Merge Behavior

```rust
let a = LWWRegister::new("A".to_string(), 100);
let b = LWWRegister::new("B".to_string(), 110);

let mut merged = a.clone();
merged.merge(&b);

// B wins (higher timestamp)
assert_eq!(merged.get(), "B");
```

---

## LWWMap (Last-Writer-Wins Map)

A map where each key is managed by an independent LWWRegister. Supports put and remove operations.

### Use Cases
- User preferences (key-value)
- Entity properties
- Configuration maps
- JSON-like documents

### API

```rust
use uni_crdt::LWWMap;

let mut map: LWWMap<String, i32> = LWWMap::new();

// Put key-value pairs with timestamp
map.put("a".to_string(), 1, 100);
map.put("b".to_string(), 2, 110);

// Get values
assert_eq!(map.get(&"a".to_string()), Some(&1));
assert_eq!(map.get(&"b".to_string()), Some(&2));

// Remove with timestamp (tombstone)
map.remove(&"a".to_string(), 120);
assert_eq!(map.get(&"a".to_string()), None);

// Iterate keys
for key in map.keys() {
    println!("{}", key);
}
```

### Merge Behavior

```rust
let mut a: LWWMap<String, i32> = LWWMap::new();
a.put("a".to_string(), 1, 100);

let mut b: LWWMap<String, i32> = LWWMap::new();
b.put("a".to_string(), 2, 110);  // Higher timestamp
b.put("b".to_string(), 3, 100);

a.merge(&b);

// Per-key LWW resolution
assert_eq!(a.get(&"a".to_string()), Some(&2));  // b's value wins
assert_eq!(a.get(&"b".to_string()), Some(&3));
```

---

## Rga (Replicated Growable Array)

An ordered sequence supporting insertion and deletion at any position. Used for collaborative text editing and ordered collections.

### Use Cases
- Collaborative text editing
- Ordered lists
- Comment threads
- Version histories

### API

```rust
use uni_crdt::Rga;

let mut rga: Rga<char> = Rga::new();

// Insert elements (returns ID for future reference)
let id1 = rga.insert(None, 'H', 1);           // Insert at beginning
let id2 = rga.insert(Some(id1), 'e', 2);      // Insert after id1
let id3 = rga.insert(Some(id2), 'l', 3);
let id4 = rga.insert(Some(id3), 'l', 4);
rga.insert(Some(id4), 'o', 5);

// Convert to vector
let text: String = rga.to_vec().into_iter().collect();
assert_eq!(text, "Hello");

// Delete element (tombstone)
rga.delete(id2);  // Remove 'e'
let text: String = rga.to_vec().into_iter().collect();
assert_eq!(text, "Hllo");
```

### Concurrent Insert Resolution

```rust
let mut a: Rga<char> = Rga::new();
let id0 = a.insert(None, 'A', 1);

let mut b = a.clone();

// Concurrent inserts after id0
a.insert(Some(id0), 'B', 2);  // timestamp 2
b.insert(Some(id0), 'C', 3);  // timestamp 3

a.merge(&b);

let result: String = a.to_vec().into_iter().collect();
// C comes before B (higher timestamp)
assert_eq!(result, "ACB");
```

---

## VectorClock (Causal Ordering)

A vector clock tracks causal relationships between events in a distributed system. Each node maintains a map of actor IDs to logical counters.

### Use Cases
- Causal ordering of operations
- Detecting concurrent updates
- Building causally-consistent CRDTs
- Conflict detection in distributed systems

### API

```rust
use uni_crdt::VectorClock;

let mut clock = VectorClock::new();

// Increment clock for an actor
clock.increment("node_a");
clock.increment("node_a");
clock.increment("node_b");

// Get clock value for an actor
assert_eq!(clock.get("node_a"), 2);
assert_eq!(clock.get("node_b"), 1);
assert_eq!(clock.get("node_c"), 0);  // Unknown actors return 0
```

### Causal Ordering

```rust
let mut a = VectorClock::new();
a.increment("node1");  // {node1: 1}

let mut b = a.clone();
b.increment("node1");  // {node1: 2}

// a happened before b
assert!(a.happened_before(&b));
assert!(!b.happened_before(&a));

// Concurrent clocks (neither happened before the other)
let mut c = a.clone();
c.increment("node2");  // {node1: 1, node2: 1}

// b {node1: 2} and c {node1: 1, node2: 1} are concurrent
assert!(b.is_concurrent(&c));
```

### Merge Behavior

```rust
let mut a = VectorClock::new();
a.increment("node1");  // {node1: 1}

let mut b = VectorClock::new();
b.increment("node2");  // {node2: 1}

a.merge(&b);

// Point-wise maximum
assert_eq!(a.get("node1"), 1);
assert_eq!(a.get("node2"), 1);
```

---

## VCRegister (Vector-Clock Register)

A register using vector clocks for causal ordering instead of timestamps. Provides better conflict detection than LWWRegister when nodes have clock skew.

### Use Cases
- Causally-consistent single values
- Conflict detection required (not just resolution)
- Multi-datacenter replication
- Systems where clock synchronization is unreliable

### API

```rust
use uni_crdt::VCRegister;

// Create with initial value and actor ID
let mut reg = VCRegister::new("initial".to_string(), "node_a");

// Get current value
assert_eq!(reg.get(), "initial");

// Set new value (increments clock for actor)
reg.set("updated".to_string(), "node_a");
assert_eq!(reg.get(), "updated");

// Access underlying clock
let clock = reg.clock();
assert_eq!(clock.get("node_a"), 2);  // Incremented twice (new + set)
```

### Merge Behavior

```rust
use uni_crdt::{VCRegister, MergeResult};

let mut r1 = VCRegister::new("A".to_string(), "node1");
let r2 = r1.clone();

// r1 advances causally
r1.set("B".to_string(), "node1");

// Merging: r1 is causally newer, keeps its value
let mut r1_copy = r1.clone();
let result = r1_copy.merge_register(&r2);
assert_eq!(result, MergeResult::KeptSelf);
assert_eq!(r1_copy.get(), "B");

// Merging other direction: r2 takes r1's value
let mut r2_copy = r2.clone();
let result = r2_copy.merge_register(&r1);
assert_eq!(result, MergeResult::TookOther);
assert_eq!(r2_copy.get(), "B");
```

### Concurrent Updates

```rust
let base = VCRegister::new("Base".to_string(), "node1");

let mut r1 = base.clone();
r1.set("A".to_string(), "node1");  // {node1: 2}

let mut r2 = base.clone();
r2.set("B".to_string(), "node2");  // {node1: 1, node2: 1}

// These are concurrent (neither causally happened before the other)
let result = r1.merge_register(&r2);
assert_eq!(result, MergeResult::Concurrent);

// On conflict: keeps self's value, merges clocks
assert_eq!(r1.get(), "A");
assert_eq!(r1.clock().get("node1"), 2);
assert_eq!(r1.clock().get("node2"), 1);
```

### MergeResult Enum

| Result | Meaning |
|--------|---------|
| `KeptSelf` | Self was causally newer or equal |
| `TookOther` | Other was causally newer |
| `Concurrent` | Updates were concurrent; kept self's value, merged clocks |

---

## Dynamic CRDT Wrapper

For storage and serialization, Uni provides a dynamic `Crdt` enum:

```rust
use uni_crdt::{Crdt, GCounter, CrdtMerge};

// Wrap any CRDT type
let mut gc = GCounter::new();
gc.increment("actor1", 42);
let crdt = Crdt::GCounter(gc);

// Serialize to MessagePack
let bytes = crdt.to_msgpack().unwrap();

// Deserialize
let decoded = Crdt::from_msgpack(&bytes).unwrap();

// Merge works on wrapped types
let mut a = Crdt::GCounter(GCounter::new());
let b = Crdt::GCounter(GCounter::new());
a.merge(&b);
```

### Supported Variants

| Variant | Type | Description |
|---------|------|-------------|
| `Crdt::GCounter` | `GCounter` | Grow-only counter |
| `Crdt::GSet` | `GSet<String>` | Grow-only set |
| `Crdt::ORSet` | `ORSet<String>` | Observed-remove set |
| `Crdt::LWWRegister` | `LWWRegister<serde_json::Value>` | Last-writer-wins register |
| `Crdt::LWWMap` | `LWWMap<String, serde_json::Value>` | Last-writer-wins map |
| `Crdt::Rga` | `Rga<String>` | Replicated growable array |

---

## Choosing the Right CRDT

| Use Case | CRDT Type | Why |
|----------|-----------|-----|
| Page views, likes | GCounter | Only increments, no conflict |
| Tags (append-only) | GSet | Never need removal |
| User selections | ORSet | Need add/remove with add-wins |
| Single property value | LWWRegister | Simple timestamp-based resolution |
| Causally-consistent value | VCRegister | Conflict detection, no clock sync needed |
| Key-value properties | LWWMap | Per-key conflict resolution |
| Collaborative text | Rga | Ordered with concurrent edits |
| Causal ordering | VectorClock | Track happened-before relationships |

---

## Best Practices

### 1. Choose Appropriate Granularity

```rust
// Bad: Single LWWRegister for entire document
let doc = LWWRegister::new(large_json, timestamp);

// Good: LWWMap with fine-grained keys
let mut props = LWWMap::new();
props.put("title".to_string(), title_value, ts);
props.put("author".to_string(), author_value, ts);
```

### 2. Use Logical Timestamps

```rust
// Use hybrid logical clocks or Lamport timestamps
// Not wall-clock time alone (clock skew issues)
let hlc_timestamp = hlc.now();
register.set(value, hlc_timestamp);
```

### 3. Consider Tombstone Growth

```rust
// ORSet and LWWMap accumulate tombstones
// Periodically compact if history not needed
// Or use garbage collection with causal stability
```

### 4. Match Semantics to Requirements

```rust
// Need "add wins"? Use ORSet
// Need "remove wins"? Use 2P-Set or LWW with remove timestamp
// Need both? Consider custom CRDT composition
```

---

## Next Steps

- [Identity Model](identity.md) — UniId for content-addressed CRDT operations
- [Architecture](architecture.md) — How CRDTs integrate with Uni storage
- [Data Model](data-model.md) — Property types and schema definition
