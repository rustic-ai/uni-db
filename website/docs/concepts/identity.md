# Identity Model

Uni uses a dual-identity system to balance performance and distributed computing requirements. This document explains the two identity types and their roles.

## Overview

Every entity in Uni has two primary identifiers:

| Identity | Bits | Purpose | Locality |
|----------|------|---------|----------|
| **VID/EID** | 64 | Internal array indexing | Local to database |
| **UniId** | 256 | Content-addressed provenance | Global / distributed |

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         VERTEX IDENTITY STACK                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Internal (VID)                                    │   │
│  │                    0x0001_0000_0000_002A                             │   │
│  │                    Fast array indexing, label-encoded                │   │
│  └───────────────────────────────┬─────────────────────────────────────┘   │
│                                  │ content-hashes to                       │
│  ┌───────────────────────────────▼─────────────────────────────────────┐   │
│  │                    Provenance (UniId)                                │   │
│  │                    bafkreihdwdcefgh4dqkjv67uzcmw7o...               │   │
│  │                    Content-addressed, CRDT-compatible                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Vertex ID (VID)

The Vertex ID is a 64-bit packed integer optimized for O(1) array indexing.

### Encoding

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              VID (64 bits)                                  │
├──────────────────┬─────────────────────────────────────────────────────────┤
│   label_id (16)  │                  local_offset (48)                       │
├──────────────────┴─────────────────────────────────────────────────────────┤
│                                                                            │
│   Example: 0x0001_0000_0000_002A                                           │
│            ────── ──────────────                                           │
│            label    offset                                                 │
│            (Paper)  (42)                                                   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

| Component | Bits | Range | Purpose |
|-----------|------|-------|---------|
| `label_id` | 16 | 0 - 65,535 | Identifies vertex type (label) |
| `local_offset` | 48 | 0 - 281 trillion | Per-label sequential offset |

### Usage in Code

```rust
use uni_db::core::Vid;

// Create a VID
let vid = Vid::new(1, 42);  // label_id=1 (Paper), offset=42

// Access components
assert_eq!(vid.label_id(), 1);
assert_eq!(vid.local_offset(), 42);
assert_eq!(vid.as_u64(), 0x0001_0000_0000_002A);

// Parse from u64
let vid = Vid::from(0x0001_0000_0000_002A_u64);
```

### Why This Design?

1. **O(1) Array Indexing**: The offset directly indexes into per-label property arrays
2. **Label Partitioning**: Queries on a single label only scan that label's data
3. **Dense Storage**: Offsets are sequential, enabling compact columnar storage
4. **Type Safety**: Label ID embedded in VID prevents cross-label confusion

### Capacity

| Component | Maximum | Practical Limit |
|-----------|---------|-----------------|
| Labels | 65,535 | Typically 10-100 |
| Vertices per label | 281 trillion | Limited by storage |
| Total vertices | 18 quintillion | Theoretical max |

---

## Edge ID (EID)

Edge IDs follow the same packed 64-bit structure as VIDs.

### Encoding

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              EID (64 bits)                                  │
├──────────────────┬─────────────────────────────────────────────────────────┤
│   type_id (16)   │                  local_offset (48)                       │
├──────────────────┴─────────────────────────────────────────────────────────┤
│                                                                            │
│   Example: 0x0002_0000_0000_0015                                           │
│            ────── ──────────────                                           │
│            type     offset                                                 │
│            (CITES)  (21)                                                   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Usage

```rust
use uni_db::core::Eid;

let eid = Eid::new(2, 21);  // type_id=2 (CITES), offset=21

assert_eq!(eid.type_id(), 2);
assert_eq!(eid.local_offset(), 21);
```

---

## UniId

The UniId is a content-addressed identifier for distributed systems and provenance tracking.

### Characteristics

| Property | Description |
|----------|-------------|
| Algorithm | SHA3-256 |
| Encoding | Multibase (base32) |
| Length | 44 characters |
| Determinism | Same content → same UID |

### Format

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           UniId Structure                                │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│   Multibase prefix: 'b' (base32 lowercase)                                 │
│                                                                            │
│   Example: bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku     │
│            ─                                                               │
│            └── multibase prefix                                            │
│             ────────────────────────────────────────────────────────       │
│             └── base32 encoded SHA3-256 hash (43 chars)                    │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Generation

UniId is computed from vertex content:

```rust
use uni_db::core::UniId;
use sha3::{Sha3_256, Digest};

// Content to hash
let content = serde_json::json!({
    "label": "Paper",
    "properties": {
        "title": "Attention Is All You Need",
        "year": 2017
    }
});

// Compute SHA3-256
let mut hasher = Sha3_256::new();
hasher.update(content.to_string().as_bytes());
let hash = hasher.finalize();

// Create UniId
let uid = UniId::from_bytes(&hash);
println!("{}", uid.to_multibase());  // bafkrei...
```

### Use Cases

1. **Content Deduplication**: Same content always produces same UID
2. **Distributed Sync**: UIDs are globally unique without coordination
3. **Audit Trail**: Track data provenance across systems
4. **CRDT Integration**: UIDs enable conflict-free replication across distributed nodes

---

## ID Resolution

### VID Lookup by UniId

```cypher
MATCH (p:Paper)
WHERE p._uid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
```

Resolution path:
1. Query UID index (separate Lance dataset)
2. Get VID from index
3. Load vertex data using VID

---

## Direction Enum

For edge traversal, Uni uses a Direction enum:

```rust
pub enum Direction {
    Outgoing,  // Source → Destination
    Incoming,  // Destination ← Source
    Both,      // Either direction
}
```

### Cypher Syntax Mapping

| Cypher Pattern | Direction |
|----------------|-----------|
| `(a)-[:TYPE]->(b)` | Outgoing from a |
| `(a)<-[:TYPE]-(b)` | Incoming to a |
| `(a)-[:TYPE]-(b)` | Both |

---

## ID Allocation

IDs are allocated by the `IdAllocator`, which is backed by the object store for durability:

```rust
pub struct IdAllocator {
    path: PathBuf,
    store: Arc<dyn ObjectStore>,
    state: Arc<Mutex<AllocatorState>>,
    batch_size: usize,  // Pre-allocate IDs in batches
}

impl IdAllocator {
    /// Allocate a new VID for a label (async, object-store backed)
    pub async fn allocate_vid(&self, label_id: u16) -> Result<Vid>;

    /// Allocate a new EID for an edge type
    pub async fn allocate_eid(&self, type_id: u16) -> Result<Eid>;

    /// Persist current state to object store
    pub async fn persist(&self) -> Result<()>;
}
```

**Allocation Properties:**
- Object-store backed for durability (S3, GCS, local filesystem)
- Async API with `Result<Vid>` return type
- Batch allocation for performance (configurable `batch_size`)
- Manifest-based persistence for recovery
- Sequential within each label/type
- Never reuses IDs (even after deletes)

---

## Storage Layout

### UID Index Structure

```
indexes/uid_to_vid/{label}/index.lance
├── _uid: FixedSizeBinary(32)  // SHA3-256 hash bytes
└── _vid: UInt64               // Corresponding VID
```

### Resolution Performance

| Lookup Type | Index | Complexity | Typical Latency |
|-------------|-------|------------|-----------------|
| VID direct | None | O(1) | ~10µs |
| UniId | BTree | O(log n) | ~100µs |
| Property lookup | Scalar Index | O(log n) | ~100µs |
| Full scan | None | O(n) | Varies |

---

## Best Practices

### When to Use Each ID

| Use Case | Recommended ID |
|----------|----------------|
| Internal operations | VID |
| Cross-system sync | UniId |
| Provenance tracking | UniId |
| Array indexing | VID offset |

---

## Next Steps

- [Data Model](data-model.md) — Vertices, edges, and properties
- [Indexing](indexing.md) — Index types and configuration
- [Architecture](architecture.md) — System overview
