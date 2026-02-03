# API Gap Implementation Plan

This document outlines the plan to expose internal Uni features. Many features have now been implemented.

## Gap Summary

| Gap | Status | Notes |
|-----|--------|-------|
| [Background Compaction](#1-background-compaction) | ✅ **Implemented** | `compact_label()`, `compact_edge_type()`, `wait_for_compaction()` |
| [S3/GCS Storage](#2-s3gcs-storage-backend) | 🚧 **Planned** | `std::fs` in metadata ops still blocks this |
| [FTS Queries](#3-full-text-search-queries) | ✅ **Implemented** | CONTAINS, STARTS WITH, ENDS WITH operators working |
| [Snapshot Management](#4-snapshot-management) | ✅ **Implemented** | `create_snapshot()`, `create_named_snapshot()`, `list_snapshots()`, `restore_snapshot()` |

---

## 1. Background Compaction

### Status: ✅ Implemented

The following APIs are now available:
- `compact_label(label)` - Manually trigger compaction for a label
- `compact_edge_type(edge_type)` - Manually trigger compaction for an edge type
- `wait_for_compaction()` - Wait for ongoing compaction to complete
- `CompactionConfig` - Configuration for automatic background compaction
- `WriteThrottleConfig` - Write throttling/backpressure configuration

### Original State (Historical)

- **L0 → L1 Flush**: ✅ Automatic (triggers at 10K mutations via `check_flush()`)
- **L1 → L2 Compaction**: ✅ Now exposed via `compact_label()` and `compact_edge_type()`
- ✅ Background compaction with `CompactionConfig`
- ✅ Write throttling with `WriteThrottleConfig`

### Industry Comparison

| Database | Compaction Model |
|----------|------------------|
| **RocksDB** | Background threads, automatic, write stalling |
| **LanceDB Cloud** | Automatic background compaction |
| **LanceDB OSS** | Manual via `table.optimize()` |
| **SQLite** | `auto_vacuum` pragma or manual `VACUUM` |
| **Uni (current)** | Manual only, not exposed |

### Proposed Solution

#### Phase 1: Background Compaction Thread

```rust
pub struct CompactionConfig {
    /// Enable background compaction (default: true)
    pub enabled: bool,

    /// Max L1 runs before triggering compaction (default: 4)
    pub max_l1_runs: usize,

    /// Max L1 size in bytes before compaction (default: 256MB)
    pub max_l1_size_bytes: u64,

    /// Max age of oldest L1 run before compaction (default: 1 hour)
    pub max_l1_age: Duration,

    /// Background check interval (default: 30s)
    pub check_interval: Duration,

    /// Number of compaction worker threads (default: 1)
    pub worker_threads: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_l1_runs: 4,
            max_l1_size_bytes: 256 * 1024 * 1024,
            max_l1_age: Duration::from_secs(3600),
            check_interval: Duration::from_secs(30),
            worker_threads: 1,
        }
    }
}
```

#### Phase 2: Compaction Scheduler

```rust
impl Uni {
    /// Starts background compaction worker (called internally on build)
    fn start_background_compaction(&self) {
        if !self.config.compaction.enabled {
            return;
        }

        let uni = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(uni.config.compaction.check_interval);
            loop {
                interval.tick().await;

                if let Some(task) = uni.pick_compaction_task() {
                    if let Err(e) = uni.execute_compaction(task).await {
                        log::error!("Compaction failed: {}", e);
                    }
                }
            }
        });
    }

    fn pick_compaction_task(&self) -> Option<CompactionTask> {
        let status = self.compaction_status();

        // Check triggers in priority order
        if status.l1_runs >= self.config.compaction.max_l1_runs {
            return Some(CompactionTask::ByRunCount);
        }
        if status.l1_size_bytes >= self.config.compaction.max_l1_size_bytes {
            return Some(CompactionTask::BySize);
        }
        if status.oldest_l1_age >= self.config.compaction.max_l1_age {
            return Some(CompactionTask::ByAge);
        }
        None
    }
}
```

#### Phase 3: Write Throttling (Backpressure)

Like RocksDB, slow down writes when compaction can't keep up:

```rust
pub struct WriteThrottleConfig {
    /// L1 run count to start throttling (default: 8)
    pub soft_limit: usize,

    /// L1 run count to stop writes entirely (default: 16)
    pub hard_limit: usize,

    /// Base delay when throttling (default: 10ms)
    pub base_delay: Duration,
}

impl Writer {
    async fn check_write_pressure(&self) -> Result<()> {
        let l1_runs = self.storage.l1_run_count();

        if l1_runs >= self.config.throttle.hard_limit {
            // Block until compaction catches up
            log::warn!("Write stalled: L1 runs ({}) at hard limit", l1_runs);
            self.wait_for_compaction().await?;
        } else if l1_runs >= self.config.throttle.soft_limit {
            // Progressive delay
            let delay = self.calculate_backpressure_delay(l1_runs);
            log::debug!("Write throttled: {}ms delay", delay.as_millis());
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    fn calculate_backpressure_delay(&self, l1_runs: usize) -> Duration {
        let excess = l1_runs - self.config.throttle.soft_limit;
        let multiplier = 2_u32.pow(excess as u32);
        self.config.throttle.base_delay * multiplier
    }
}
```

#### Phase 4: Public API

```rust
impl Uni {
    /// Trigger manual compaction (all labels, all edge types)
    pub async fn compact(&self) -> Result<CompactionStats>;

    /// Compact specific label
    pub async fn compact_label(&self, label: &str) -> Result<CompactionStats>;

    /// Compact specific edge type
    pub async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats>;

    /// Get compaction status
    pub fn compaction_status(&self) -> CompactionStatus;

    /// Wait for all pending compactions to complete
    pub async fn wait_for_compaction(&self) -> Result<()>;
}

pub struct CompactionStats {
    pub files_compacted: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub duration: Duration,
    pub crdt_merges: usize,
}

pub struct CompactionStatus {
    pub l1_runs: usize,
    pub l1_size_bytes: u64,
    pub oldest_l1_age: Duration,
    pub compaction_in_progress: bool,
    pub compaction_pending: usize,
    pub last_compaction: Option<SystemTime>,
    pub total_compactions: u64,
    pub total_bytes_compacted: u64,
}
```

#### Cypher Procedures

```cypher
-- Trigger compaction
CALL uni.admin.compact()
YIELD filesCompacted, bytesBefore, bytesAfter, durationMs, crdtMerges

-- Check status
CALL uni.admin.compactionStatus()
YIELD l1Runs, l1SizeBytes, compactionInProgress, lastCompaction
```

### Tasks

| Task | Effort |
|------|--------|
| `CompactionConfig` struct | 0.5 day |
| Background compaction thread | 1 day |
| Compaction task picker/scheduler | 1 day |
| Write throttling/backpressure | 1 day |
| Public API (`compact()`, `compaction_status()`) | 0.5 day |
| Cypher procedures | 0.5 day |
| Tests | 1 day |

**Total: 5-6 days**

---

## 2. S3/GCS Storage Backend

### Current State

Lance datasets support S3/GCS/Azure natively, but Uni's metadata operations use `std::fs`:

| Component | Blocking Code | Challenge |
|-----------|---------------|-----------|
| `UniBuilder.build()` | `std::fs::create_dir_all()` | Directory creation |
| `SchemaManager` | `fs::read_to_string()`, `fs::write()` | File I/O |
| `SnapshotManager` | `fs::create_dir_all()`, `fs::write()` | File I/O |
| `WriteAheadLog` | `File::open()`, append | Append-only semantics |
| `IdAllocator` | `fs::rename()` | Atomic rename |

### Object Store Challenges

| Challenge | Local FS | S3/GCS |
|-----------|----------|--------|
| Latency | <1ms | 50-200ms |
| Atomic rename | ✅ `fs::rename()` | ❌ Copy + Delete |
| Cost | Free | Per-operation cost |
| Append | ✅ Native | ❌ Rewrite entire object |
| Concurrent writes | File locks | Requires coordination |

### Proposed Solution: Phased Approach

#### Phase 1: Local-Only with Background Compaction (5-6 days)

Focus on compaction first (see above). This provides immediate value for all deployments.

#### Phase 2: Hybrid Architecture (5-7 days)

**Recommended for most cloud deployments.**

```
┌─────────────────────────────────────────────────────────────┐
│  LOCAL (fast, atomic)                                        │
│  ├── WAL (append-only log, needs low latency)               │
│  ├── IdAllocator (atomic counter, needs atomicity)          │
│  └── L0 Buffer (in-memory, already local)                   │
├─────────────────────────────────────────────────────────────┤
│  OBJECT STORE (S3/GCS/Azure)                                │
│  ├── Lance Datasets (vertices, edges, adjacency)            │
│  ├── Schema (versioned JSON, infrequent writes)             │
│  └── Snapshots (manifest files, infrequent writes)          │
└─────────────────────────────────────────────────────────────┘
```

**Implementation:**

```rust
pub struct HybridStorageConfig {
    /// Local path for WAL and ID allocation
    pub local_path: PathBuf,

    /// Object store URL for data (s3://bucket/prefix)
    pub data_url: String,

    /// Object store credentials
    pub credentials: Option<ObjectStoreCredentials>,
}

impl UniBuilder {
    /// Configure hybrid storage (local WAL + S3 data)
    pub fn hybrid(
        self,
        local_path: impl AsRef<Path>,
        data_url: &str,
    ) -> Self;
}

// Usage
let db = Uni::hybrid("./local-wal", "s3://my-bucket/graphs/prod")
    .credentials(ObjectStoreCredentials::from_env())
    .build()
    .await?;
```

**Tasks:**

| Task | Effort |
|------|--------|
| `HybridStorageConfig` | 0.5 day |
| Refactor `SchemaManager` to use object_store | 1 day |
| Refactor `SnapshotManager` to use object_store | 1 day |
| Keep WAL and IdAllocator local | 0 days (no change) |
| `UniBuilder::hybrid()` configuration | 0.5 day |
| Lance dataset URL passthrough | 0.5 day |
| Integration tests with LocalStack/MinIO | 1.5 days |

**Total Phase 2: 5-7 days**

#### Phase 3: Full Object Store (Optional, 5-7 days)

For serverless/ephemeral compute where local storage isn't available.

```rust
pub struct FullObjectStoreConfig {
    /// Object store URL for everything
    pub url: String,

    /// Credentials
    pub credentials: ObjectStoreCredentials,

    /// ID allocation strategy
    pub id_strategy: IdAllocationStrategy,

    /// WAL strategy
    pub wal_strategy: WalStrategy,
}

pub enum IdAllocationStrategy {
    /// Use conditional writes with ETag (optimistic locking)
    ConditionalWrite { batch_size: u64 },

    /// Use external coordination service (DynamoDB, etcd)
    External { endpoint: String },
}

pub enum WalStrategy {
    /// Buffer locally, flush segments to object store
    BufferedSegments { segment_size: usize },

    /// Disable WAL (data loss risk on crash)
    Disabled,
}
```

**IdAllocator on S3 (conditional writes):**

```rust
impl ObjectStoreIdAllocator {
    async fn allocate_batch(&self) -> Result<Range<u64>> {
        loop {
            // Read current counter
            let (current, etag) = self.read_counter_with_etag().await?;
            let new = current + self.batch_size;

            // Conditional write (fails if etag changed)
            match self.conditional_write(new, &etag).await {
                Ok(_) => return Ok(current..new),
                Err(PreconditionFailed) => continue, // Retry
            }
        }
    }
}
```

**WAL on S3 (segment files):**

```rust
impl ObjectStoreWal {
    async fn append(&mut self, entry: WalEntry) -> Result<()> {
        self.local_buffer.push(entry);

        if self.local_buffer.len() >= self.segment_size {
            // Flush segment to S3
            let segment_name = format!("wal/segment_{:016x}.bin", self.next_segment_id);
            self.store.put(&segment_name, self.serialize_buffer()).await?;
            self.local_buffer.clear();
            self.next_segment_id += 1;
        }
        Ok(())
    }
}
```

**Tasks:**

| Task | Effort |
|------|--------|
| `ObjectStoreIdAllocator` with conditional writes | 1.5 days |
| `ObjectStoreWal` with segment flushing | 2 days |
| Recovery logic for WAL segments | 1 day |
| `UniBuilder::object_store()` configuration | 0.5 day |
| Integration tests | 1.5 days |

**Total Phase 3: 5-7 days**

### Recommended Approach

| Deployment | Recommended Config | Effort |
|------------|-------------------|--------|
| **Local development** | `Uni::open("./data")` | Already works |
| **Cloud (EC2/GKE/AKS)** | `Uni::hybrid("./wal", "s3://...")` | Phase 2 |
| **Serverless (Lambda)** | `Uni::object_store("s3://...")` | Phase 3 |

**Start with Phase 2 (Hybrid)** - covers 90% of cloud use cases with less complexity.

---

## 3. Full-Text Search Queries

### Status: ✅ Implemented

Full-text search is now fully supported:

- FTS indexes can be created via DDL: `CREATE FULLTEXT INDEX ... FOR (n:Label) ON EACH [n.prop]`
- Index stored using Lance's `InvertedIndex`
- **Query support implemented**: `CONTAINS`, `STARTS WITH`, `ENDS WITH` operators
- **Predicate pushdown** routes FTS predicates to Lance indexes

### Usage

```cypher
-- Text containment search
MATCH (p:Person)
WHERE p.bio CONTAINS 'machine learning'
RETURN p

-- Prefix matching
MATCH (p:Person)
WHERE p.name STARTS WITH 'John'
RETURN p

-- Suffix matching
MATCH (p:Person)
WHERE p.email ENDS WITH '@example.com'
RETURN p
```

### Implementation Details

The following was implemented:

1. `Operator::Contains`, `Operator::StartsWith`, `Operator::EndsWith` in `ast.rs`
2. Parser support in `parser.rs` for keyword recognition
3. Predicate pushdown in `pushdown.rs` for index acceleration
4. Query execution in `operators.rs` for string matching

---

## 4. Snapshot Management

### Status: ✅ Implemented

The following APIs are now available:
- `create_snapshot(name)` - Create a point-in-time snapshot
- `create_named_snapshot(name)` - Create a persisted named snapshot
- `list_snapshots()` - List all available snapshots
- `restore_snapshot(snapshot_id)` - Restore database to snapshot state
- Time travel queries via `VERSION AS OF` / `TIMESTAMP AS OF`

### Original State (Historical)

- `SnapshotManager` exists internally
- Creates snapshots automatically on flush
- Snapshots stored as JSON manifests
- ✅ Public API now available for create/restore/list snapshots

### Proposed Solution

#### Rust API

```rust
impl Uni {
    pub async fn create_snapshot(&self, name: Option<&str>) -> Result<String>;
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotManifest>>;
    pub async fn restore_snapshot(&self, id: &str) -> Result<()>;
}
```

#### Cypher Procedures

```cypher
-- Create snapshot
CALL uni.admin.snapshot.create('before-migration')
YIELD snapshotId, createdAt

-- List snapshots
CALL uni.admin.snapshot.list()
YIELD snapshot_id, name, created_at, version_hwm

-- Restore snapshot
CALL uni.admin.snapshot.restore($snapshotId)
```

### Tasks

| Task | Effort |
|------|--------|
| Polish snapshot metadata (optional) | 0.5 day |
| Expand time-travel query coverage | 0.5 day |

**Total: ~1 day**

---

## Implementation Roadmap

### Phase 1: Foundation (Week 1-2)

| Feature | Priority | Effort | Rationale |
|---------|----------|--------|-----------|
| Background Compaction | P1 | 5-6 days | Required for production use |
| Manual Compact API | P1 | (included above) | Operational control |

### Phase 2: Cloud Support (Week 3-4)

| Feature | Priority | Effort | Rationale |
|---------|----------|--------|-----------|
| Hybrid S3/GCS (Phase 2) | P1 | 5-7 days | Enables cloud deployment |
| FTS Queries | P2 | 3-4 days | Completes FTS feature |

### Phase 3: Advanced (Week 5+)

| Feature | Priority | Effort | Rationale |
|---------|----------|--------|-----------|
| Full S3 Support (Phase 3) | P3 | 5-7 days | Serverless only |
| Snapshot Management | P3 | 2-3 days | Backup/restore |

### Total Effort

| Phase | Features | Effort |
|-------|----------|--------|
| Phase 1 | Background Compaction | 5-6 days |
| Phase 2 | Hybrid S3 + FTS | 8-11 days |
| Phase 3 | Full S3 + Snapshots | 7-10 days |
| **Total** | All features | **20-27 days** |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                APPLICATION                                    │
│                                                                              │
│   let db = Uni::hybrid("./wal", "s3://bucket/data").build().await?;         │
│                                                                              │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
┌──────────────────────────────────┼───────────────────────────────────────────┐
│                               UNI                                             │
│                                  │                                            │
│  ┌───────────────────────────────┴────────────────────────────────────────┐  │
│  │                        RUNTIME LAYER                                    │  │
│  │                                                                         │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐   │  │
│  │  │ L0 Buffer   │  │   Writer    │  │ Compaction  │  │  Adjacency   │   │  │
│  │  │ (in-memory) │  │             │  │  Scheduler  │  │    Cache     │   │  │
│  │  └─────────────┘  └──────┬──────┘  └──────┬──────┘  └──────────────┘   │  │
│  │                          │                │                             │  │
│  │                          │    ┌───────────┴───────────┐                 │  │
│  │                          │    │  Background Thread    │                 │  │
│  │                          │    │  - Check every 30s    │                 │  │
│  │                          │    │  - Pick compaction    │                 │  │
│  │                          │    │  - Write throttling   │                 │  │
│  │                          │    └───────────────────────┘                 │  │
│  └──────────────────────────┼──────────────────────────────────────────────┘  │
│                             │                                                  │
│  ┌──────────────────────────┴──────────────────────────────────────────────┐  │
│  │                        STORAGE LAYER                                     │  │
│  │                                                                          │  │
│  │  ┌─────────────────────────────┐  ┌─────────────────────────────────┐   │  │
│  │  │     LOCAL (./wal)           │  │     OBJECT STORE (s3://...)     │   │  │
│  │  │                             │  │                                  │   │  │
│  │  │  ├── WAL (append-only)      │  │  ├── vertices_*/  (Lance)       │   │  │
│  │  │  ├── IdAllocator (atomic)   │  │  ├── edges_*/     (Lance)       │   │  │
│  │  │  └── L0 state (optional)    │  │  ├── adjacency_*/ (Lance)       │   │  │
│  │  │                             │  │  ├── schema.json                │   │  │
│  │  │                             │  │  └── snapshots/                 │   │  │
│  │  └─────────────────────────────┘  └─────────────────────────────────┘   │  │
│  │                                                                          │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## Success Criteria

| Criteria | Status |
|----------|--------|
| Background compaction runs automatically based on configurable policies | ✅ Implemented |
| Write throttling prevents L1 from growing unbounded | ✅ Implemented |
| `db.compact_label()` / `db.compact_edge_type()` triggers manual compaction | ✅ Implemented |
| `db.wait_for_compaction()` waits for completion | ✅ Implemented |
| `Uni::hybrid("./wal", "s3://...")` works for cloud deployments | 🚧 Not yet |
| `WHERE p.bio CONTAINS 'term'` uses FTS index automatically | 🚧 In progress |
| `db.create_snapshot()` / `db.restore_snapshot()` work | ✅ Implemented |
| All new APIs have tests and documentation | ✅ Implemented |

---

## References

- [RocksDB Compaction Wiki](https://github.com/facebook/rocksdb/wiki/Compaction)
- [LanceDB Data Management](https://lancedb.github.io/lancedb/concepts/data_management/)
- [Lance Format Documentation](https://lancedb.github.io/lance/)
