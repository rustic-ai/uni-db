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
│  │                    Dense id; labels resolved separately                │   │
│  └───────────────────────────────┬─────────────────────────────────────┘   │
│                                  │ associated provenance                       │
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

The Vertex ID is a **dense, sequential 64-bit auto-increment** integer assigned
at vertex creation.

!!! warning "VIDs do not encode the label"
    Earlier revisions of this page described a packed layout of a 16-bit
    `label_id` plus a 48-bit `local_offset`, with `label_id()` /
    `local_offset()` accessors and a two-argument `Vid::new(label, offset)`.
    That design was **removed**. `crates/uni-common/src/core/id.rs` states it
    directly: VIDs "no longer embed label information (label lookups go via the
    `VidLabelsIndex`)". `Vid::new` takes a single `u64`, and there are no
    `label_id()` / `local_offset()` accessors.

### Encoding

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              VID (64 bits)                                  │
├───┬────────────────────────────────────────────────────────────────────────┤
│ E │                    auto-increment id (63 bits)                          │
└───┴────────────────────────────────────────────────────────────────────────┘
  │
  └── EPHEMERAL_BIT (1 << 63): set for transient in-query identities minted
      by the engine; clear for persisted vertices.
```

Resolving a VID to its label goes through the `VidLabelsIndex`, not through bit
manipulation. For O(1) array indexing during query execution, remap a VID to a
`DenseIdx` via `VidRemapper`.

### Usage

```rust
use uni_db::core::Vid;

let vid = Vid::new(42);       // a single u64 id
assert_eq!(vid.as_u64(), 42);
```

Persisted VIDs use the lower 63 bits; the high bit is reserved for ephemeral
identities. Capacity is not divided into per-label bit ranges.

---

## Edge ID (EID)

Edge IDs follow the same shape as VIDs: a dense 64-bit auto-increment id. They
do **not** pack a `type_id` — the edge type is resolved from the edge's own
storage, not from bits of the id.

### Usage

```rust
use uni_db::core::Eid;

let eid = Eid::new(21);
assert_eq!(eid.as_u64(), 21);
```

---

## UniId

The UniId is a content-addressed identifier for distributed systems and provenance tracking. It serves as a **lookup index**, not a uniqueness constraint — multiple vertices with identical content can coexist with different VIDs.

### Characteristics

| Property | Description |
|----------|-------------|
| Algorithm | SHA3-256 |
| Encoding | Multibase (base32) |
| Length | 53 characters (one prefix + 52 base32 characters) |
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
│             └── base32 encoded SHA3-256 hash (52 chars)                    │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Generation

UniId is computed by `VertexDataset::compute_vertex_uid` in
`crates/uni-store/src/storage/vertex.rs`: SHA3-256 over the label, optional
external id, and properties sorted by name, with explicit separators. Hashing
an arbitrary JSON object's serialization does not reproduce this format.

```rust
use std::collections::HashMap;
use uni_db::{Value, store::storage::vertex::VertexDataset};

let properties = HashMap::from([
    ("title".to_string(), Value::String("Attention Is All You Need".into())),
    ("year".to_string(), Value::Int(2017)),
]);
let uid = VertexDataset::compute_vertex_uid("Paper", None, &properties);
assert_eq!(uid.to_multibase().len(), 53);
println!("{}", uid.to_multibase()); // b followed by 52 base32 characters
```

### Use Cases

1. **Content Lookup**: Find vertices by content hash (multiple vertices may share a UID)
2. **Distributed Sync**: Deterministic IDs enable coordination-free references
3. **Audit Trail**: Track data provenance across systems
4. **CRDT Integration**: UIDs enable conflict-free replication across distributed nodes

---

## ID Resolution

### VID Lookup by UniId

```cypher
MATCH (p:Paper)
WHERE p._uid = $uid
RETURN p
```

Resolution path:
1. Query UID index (separate Lance dataset)
2. Get VID from index
3. Load visible vertex data using VID. On primary, verify candidates against
   primary visibility: the UID index also contains fork entries.

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

IDs are allocated internally by the `IdAllocator`. This is an internal component not exposed in the user-facing API -- VIDs and EIDs are assigned automatically when vertices and edges are created via Cypher `CREATE` statements or the bulk writer.

**Allocation Properties:**
- Object-store backed for durability (S3, GCS, local filesystem)
- Batch allocation for performance (configurable batch size)
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
