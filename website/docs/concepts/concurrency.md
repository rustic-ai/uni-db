# Concurrency Model

Uni uses a single-writer, multi-reader concurrency model with snapshot-based isolation. This design provides simplicity, consistency, and predictable performance without the complexity of distributed consensus.

## Design Philosophy

Traditional distributed databases require complex consensus protocols (Raft, Paxos) to maintain consistency across replicas. Uni takes a different approach:

| Traditional Distributed | Uni's Approach |
|------------------------|----------------|
| Multiple writers | Single writer |
| Network consensus | Local coordination |
| Eventual consistency | Snapshot isolation |
| Complex conflict resolution | No conflicts by design |
| Operational complexity | Embedded simplicity |

This model is ideal for:
- Embedded databases in applications
- Batch processing pipelines
- Single-node analytics workloads
- Development and testing environments

---

## Architecture

```mermaid
flowchart TB
    subgraph Writer["WRITER (Single)"]
        L0["L0 Buffer<br/>(Exclusive)"]
        WAL["WAL<br/>(Append-only)"]
        Lance["Lance Datasets<br/>(Versioned)"]
        L0 --> WAL --> Lance
    end

    subgraph Snapshots["SNAPSHOT MANIFESTS"]
        V1[v1] --> V2[v2] --> V3[v3] --> V4["v4 (current)"]
    end

    subgraph Readers["READERS (Multiple)"]
        R1["Reader 1<br/>(v4)"]
        R2["Reader 2<br/>(v4)"]
        R3["Reader 3<br/>(v3)"]
        RN["Reader N<br/>(v4)"]
    end

    Writer -->|Creates snapshots| Snapshots
    Snapshots -->|Readers bind to| Readers
```

---

## Single Writer

### Exclusive Write Access

Only one writer can modify the database at a time:

```rust
pub struct Writer {
    l0_manager: Arc<L0Manager>,      // Exclusive L0 access
    storage: Arc<StorageManager>,     // Storage coordination
    allocator: Arc<IdAllocator>,      // ID allocation
    // ...
}
```

### Write Flow

```mermaid
flowchart TB
    A["1. Application calls<br/>writer.insert_vertex(vid, props)"]
    B["2. Acquire write lock<br/>on L0 buffer (Mutex)"]
    C["3. Append to WAL<br/>(durability)"]
    D["4. Insert into L0 buffer<br/>(in-memory graph + properties)"]
    E["5. Increment version counter"]
    F["6. Release lock,<br/>return to application"]

    A --> B --> C --> D --> E --> F
```

### Why Single Writer?

| Benefit | Explanation |
|---------|-------------|
| **No conflicts** | Writes are serialized, no concurrent modification issues |
| **Simple recovery** | WAL replay is deterministic |
| **Predictable latency** | No lock contention or retry loops |
| **Easier reasoning** | No need to reason about interleaved operations |
| **Efficient batching** | Buffer many writes before flush |

---

## Multiple Readers

### Snapshot Isolation

Readers operate on consistent snapshots of the database. Each snapshot represents a point-in-time view:

```rust
pub struct SnapshotManifest {
    snapshot_id: String,
    version: u64,
    timestamp: DateTime<Utc>,
    labels: HashMap<String, LabelSnapshot>,      // Lance version per label
    edge_types: HashMap<String, EdgeSnapshot>,   // Lance version per type
    adjacencies: HashMap<String, Vec<String>>,   // Adjacency chunks
}
```

### Reader Independence

```mermaid
sequenceDiagram
    participant W as Writer
    participant S as Snapshots
    participant R1 as Reader 1
    participant R2 as Reader 2
    participant R3 as Reader 3

    Note over S: v1 exists
    R1->>S: Start (binds to v1)
    W->>W: Write A
    W->>W: Write B
    R1->>S: Query (sees A, B in L0)
    W->>S: Flush → creates v2
    R2->>S: Start (binds to v2)
    W->>W: Write C
    R2->>S: Query (sees A, B flushed)
    R1->>R1: End (still v1)
    W->>S: Flush → creates v3
    R3->>S: Start (binds to v3)
    R2->>R2: End
```

### Read-Your-Writes

Readers can optionally include uncommitted L0 buffer data:

```rust
pub struct ExecutionContext {
    storage: Arc<StorageManager>,
    l0_buffer: Option<Arc<RwLock<L0Buffer>>>,  // Optional L0 access
    snapshot: SnapshotManifest,
}
```

**With L0 access:**
```cypher
-- Sees both committed (Lance) and uncommitted (L0) data
CREATE (n:Paper {title: "New Paper"})
MATCH (p:Paper) WHERE p.title = "New Paper"  -- Finds it immediately
RETURN p
```

**Without L0 access (snapshot-only):**
```cypher
-- Reader bound to v2, sees only committed data at v2
MATCH (p:Paper) RETURN COUNT(p)  -- Consistent count at v2
```

---

## Snapshot Management

### Snapshot Creation

Snapshots are created when L0 buffer is flushed to Lance:

```mermaid
flowchart TB
    Trigger["L0 Buffer Full<br/>(or manual flush)"]
    S1["1. Write L0 vertices to Lance datasets<br/>vertices_Paper.lance → new version"]
    S2["2. Write L0 edges to Lance datasets<br/>edges_CITES.lance → new version"]
    S3["3. Update adjacency datasets<br/>adj_out_CITES_Paper.lance → new version"]
    S4["4. Create snapshot manifest (atomic write)<br/>snapshots/manifest_v42.json"]
    S5["5. Clear L0 buffer,<br/>invalidate adjacency cache"]

    Trigger --> S1 --> S2 --> S3 --> S4 --> S5
```

### Snapshot Manifest Structure

```json
{
  "snapshot_id": "snap_20240115_103045_abc123",
  "version": 42,
  "timestamp": "2024-01-15T10:30:45.123Z",
  "labels": {
    "Paper": {
      "dataset_version": 15,
      "vertex_count": 1000000
    },
    "Author": {
      "dataset_version": 8,
      "vertex_count": 250000
    }
  },
  "edge_types": {
    "CITES": {
      "dataset_version": 12,
      "edge_count": 5000000
    }
  },
  "adjacencies": {
    "CITES_out": ["chunk_0.lance", "chunk_1.lance"],
    "CITES_in": ["chunk_0.lance", "chunk_1.lance"]
  }
}
```

### Snapshot Lifecycle

| State | Description |
|-------|-------------|
| **Active** | Current snapshot, new readers bind to this |
| **Referenced** | Old snapshot still in use by active readers |
| **Unreferenced** | No readers, candidate for garbage collection |
| **Deleted** | Removed, storage reclaimed |

---

## Consistency Guarantees

### ACID Properties

| Property | Guarantee |
|----------|-----------|
| **Atomicity** | Flush is all-or-nothing (manifest written last) |
| **Consistency** | Schema validated on write, constraints enforced |
| **Isolation** | Snapshot isolation for readers |
| **Durability** | WAL ensures writes survive crashes |

### Isolation Level Comparison

| Level | Uni Equivalent | Anomalies Prevented |
|-------|----------------|---------------------|
| Read Uncommitted | With L0 access | None |
| Read Committed | N/A | Dirty reads |
| Repeatable Read | Snapshot isolation | Dirty reads, non-repeatable reads |
| Serializable | Single writer | All anomalies |

---

## Write-Ahead Log (WAL)

The WAL ensures durability for uncommitted writes:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WAL STRUCTURE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   wal/                                                                      │
│   ├── 00000000000000000001_<uuid>.wal                                       │
│   ├── 00000000000000000002_<uuid>.wal                                       │
│   └── 00000000000000000003_<uuid>.wal (current)                             │
│                                                                             │
│   Segment Format:                                                           │
│   ┌──────────┬──────────┬──────────┬──────────────────────────────────┐    │
│   │  Length  │   CRC    │   Type   │            Payload               │    │
│   │ (4 bytes)│ (4 bytes)│ (1 byte) │         (variable)               │    │
│   └──────────┴──────────┴──────────┴──────────────────────────────────┘    │
│                                                                             │
│   Record Types:                                                             │
│   ├── INSERT_VERTEX { vid, properties }                                    │
│   ├── DELETE_VERTEX { vid }                                                │
│   ├── INSERT_EDGE { eid, src_vid, dst_vid, edge_type, properties }         │
│   └── DELETE_EDGE { eid, src_vid, dst_vid, edge_type }                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Recovery Process

```mermaid
flowchart TB
    R1["1. Find latest snapshot manifest<br/>→ manifest_v42.json"]
    R2["2. Load snapshot state<br/>→ Lance datasets at recorded versions"]
    R3["3. Replay WAL after last snapshot LSN<br/>→ Apply INSERT_VERTEX, DELETE_EDGE, etc.<br/>→ Rebuild L0 buffer"]
    R4["4. Resume normal operation<br/>→ Writer ready<br/>→ Readers can bind to snapshot"]

    R1 --> R2 --> R3 --> R4
```

---

## Concurrency Primitives

### Thread-Safe Components

| Component | Primitive | Pattern |
|-----------|-----------|---------|
| L0 Buffer | `Mutex<L0Buffer>` | Exclusive write access |
| Adjacency Cache | `DashMap<K, V>` | Concurrent read, partitioned write |
| Property Cache | `Mutex<LruCache>` | Exclusive access with LRU eviction |
| ID Allocator | `AtomicU64` | Lock-free increment |
| Snapshot Manager | `RwLock<SnapshotManager>` | Read-heavy access pattern |

### Example: Concurrent Reads

```rust
// Multiple readers can execute concurrently
let snapshot = snapshot_manager.current().await;

// Reader 1 (thread A)
tokio::spawn(async move {
    let results = executor.execute(query1, &snapshot).await?;
});

// Reader 2 (thread B)
tokio::spawn(async move {
    let results = executor.execute(query2, &snapshot).await?;
});

// Both run concurrently, both see same consistent snapshot
```

---

## Scaling Considerations

### Single-Writer Throughput

| Operation | Latency | Throughput |
|-----------|---------|------------|
| L0 insert | ~550µs / 1K vertices | ~1.8M vertices/sec |
| WAL append | ~10µs per record | ~100K records/sec |
| Flush | ~6.3ms / 1K vertices | Batched |

### Scaling Strategies

1. **Batch Writes**: Group many operations before commit
2. **Async Flush**: Flush in background while accepting new writes
3. **Multiple Databases**: Shard data across independent Uni instances
4. **Read Replicas**: Sync snapshots to read-only replicas

---

## Best Practices

### Write Optimization

```rust
// Good: Batch many writes
let mut batch = Vec::with_capacity(1000);
for item in items {
    batch.push(create_vertex(item));
}
writer.insert_batch(batch).await?;

// Bad: Many small writes
for item in items {
    writer.insert_vertex(item).await?;  // Overhead per call
}
```

### Reader Management

```rust
// Good: Bind to snapshot once, reuse
let snapshot = snapshot_manager.current().await;
for query in queries {
    executor.execute(query, &snapshot).await?;  // Same snapshot
}

// Bad: New snapshot per query (may see inconsistent data)
for query in queries {
    let snapshot = snapshot_manager.current().await;  // Might change
    executor.execute(query, &snapshot).await?;
}
```

---

## Next Steps

- [Architecture](architecture.md) — System overview
- [Storage Engine](../internals/storage-engine.md) — Lance integration and LSM design
- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
