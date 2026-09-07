# Rust API Reference

Complete reference for the Uni 2.0.0 embedded graph database Rust API.

## Quick Start

```rust
use uni_db::{Uni, Value};

#[tokio::main]
async fn main() -> uni_db::Result<()> {
    // Open or create a database
    let db = Uni::open("./my-graph").build().await?;

    // Define schema
    db.schema()
        .label("Person")
            .property("name", uni_db::DataType::String)
            .property("age", uni_db::DataType::Int64)
        .edge_type("KNOWS", &["Person"], &["Person"])
            .property("since", uni_db::DataType::Date)
        .apply()
        .await?;

    // All data access goes through Sessions
    let session = db.session();

    // Read with Cypher
    let results = session.query("MATCH (p:Person) RETURN p.name, p.age").await?;
    for row in results.rows() {
        let name: String = row.get("p.name")?;
        println!("{name}");
    }

    // Write through Transactions
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
    let result = tx.commit().await?;
    println!("Committed {} mutations at version {}", result.mutations_committed, result.version);

    db.shutdown().await?;
    Ok(())
}
```

---

## Architecture Overview

Uni 2.0.0 uses a three-tier access model:

```
Uni  (lifecycle / admin)
 |
 +-- Session  (read scope, cheap to create)
      |
      +-- Transaction  (write scope, ACID)
```

- **`Uni`** -- lifecycle handle. Creates sessions, manages schema, provides database-level metrics. Does **not** execute queries directly.
- **`Session`** -- primary read scope. Executes Cypher queries, evaluates Locy programs, holds scoped parameters and rule registries, creates transactions. Cheap, synchronous, infallible to create.
- **`Transaction`** -- explicit write scope. Executes mutations in a private L0 buffer with commit-time serialization. Changes are isolated until `commit()`.

---

## Uni

The top-level database handle. Created via builder methods.

### Opening a Database

```rust
// Open or create at path
let db = Uni::open("./my-graph").build().await?;

// Open existing only (fails if missing)
let db = Uni::open_existing("./my-graph").build().await?;

// Create new (fails if already exists)
let db = Uni::create("./new-graph").build().await?;

// In-memory / temporary database
let db = Uni::in_memory().build().await?;
let db = Uni::temporary().build().await?;
```

### Factory Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `session()` | `Session` | Create a new session. Cheap, sync, infallible. |
| `session_template()` | `SessionTemplateBuilder` | Create a pre-configured session factory. |
| `schema()` | `SchemaBuilder` | Start building schema changes. |
| `rules()` | `RuleRegistry` | Access global Locy rule registry. |

### Admin / Lifecycle

| Method | Returns | Description |
|--------|---------|-------------|
| `metrics()` | `DatabaseMetrics` | Snapshot database-level metrics (sync, cheap). |
| `flush()` | `Result<()>` | Flush L0 buffer to persistent L1 storage. |
| `create_snapshot(name)` | `Result<String>` | Create a named point-in-time snapshot. Returns snapshot ID. |
| `list_snapshots()` | `Result<Vec<SnapshotManifest>>` | List all available snapshots. |
| `restore_snapshot(id)` | `Result<()>` | Restore database to a specific snapshot. |
| `shutdown()` | `Result<()>` | Graceful shutdown: flush data, stop background tasks. |
| `config()` | `&UniConfig` | Get current configuration. |
| `write_lease()` | `Option<&WriteLease>` | Get write lease configuration, if any. |
| `compaction()` | `Compaction` | Access compaction operations. |
| `indexes()` | `Indexes` | Access index management operations. |
| `functions()` | `Functions` | Access custom Cypher function management. |

### Schema Introspection

| Method | Returns | Description |
|--------|---------|-------------|
| `label_exists(name)` | `Result<bool>` | Check if a label exists in the schema. |
| `edge_type_exists(name)` | `Result<bool>` | Check if an edge type exists. |
| `list_labels()` | `Result<Vec<String>>` | List all label names (schema + data). |
| `list_edge_types()` | `Result<Vec<String>>` | List all edge type names. |
| `get_label_info(name)` | `Result<Option<LabelInfo>>` | Detailed label info (properties, indexes, constraints, count). |
| `get_edge_type_info(name)` | `Result<Option<EdgeTypeInfo>>` | Detailed edge type info. |
| `load_schema(path)` | `Result<()>` | Load schema from a JSON file. |
| `save_schema(path)` | `Result<()>` | Save current schema to a JSON file. |

---

## UniBuilder

Returned by `Uni::open()`, `Uni::create()`, etc. Configures the database before opening.

```rust
let db = Uni::open("./my-graph")
    .schema_file("schema.json")
    .config(UniConfig { /* ... */ })
    .read_only()
    .write_lease(WriteLease::Local)
    .remote_storage("s3://bucket/data", cloud_config)
    .build()
    .await?;
```

| Method | Description |
|--------|-------------|
| `schema_file(path)` | Load schema from JSON file on initialization. |
| `config(config)` | Set `UniConfig` options (includes `strict_schema`, timeouts, cache, etc.). |
| `read_only()` | Open in read-only mode (no writer created). |
| `write_lease(lease)` | Set write lease strategy for multi-agent access. |
| `xervo_catalog(catalog)` | Set Uni-Xervo model catalog. |
| `xervo_runtime(runtime)` | Set pre-built Xervo `ModelRuntime` directly (for testing or advanced use). |
| `remote_storage(url, config)` | Configure hybrid local+remote storage. |

!!! note "`remote_storage` is the Rust spelling"
    The Python builder exposes this as `.hybrid(local, remote)` +
    `.cloud_config(cfg)`. The Rust builder has neither — `remote_storage` takes
    both in one call.

**Concurrency control — `UniConfig::ssi_enabled`**

`ssi_enabled` defaults to **`true`**: Uni runs Serializable Snapshot Isolation,
so two concurrent writers that would produce a lost update cause one to **abort
and retry** rather than silently last-writer-wins.

```rust
// Opt out only when writes never conflict, or the caller guards
// read-modify-write externally. Silent lost updates are the trade.
let db = Uni::open("./my_db")
    .config(UniConfig { ssi_enabled: false, ..Default::default() })
    .build()
    .await?;
```

| `build()` | `async` -- Open the database. Returns `Result<Uni>`. |

---

## Session

The primary read scope. Created via `db.session()`. Cheap, synchronous, infallible. Implements `Clone` (clones share the plan cache).

`session.query()` is **read-only** — mutation clauses (CREATE, SET, DELETE, MERGE, REMOVE) return an error. Use `session.tx()` for writes.

```rust
let session = db.session();

// Queries (read-only)
let rows = session.query("MATCH (n:Person) RETURN n.name").await?;

// Parameterized queries
let rows = session.query_with("MATCH (n) WHERE n.age > $min RETURN n")
    .param("min", 21)
    .timeout(Duration::from_secs(5))
    .fetch_all()
    .await?;

// Transactions for writes
let tx = session.tx().await?;
tx.execute("CREATE (:Person {name: 'Bob'})").await?;
tx.commit().await?;

// Cloning (shares plan cache, independent params/metrics)
let session2 = session.clone();
```

### Cypher Reads

| Method | Returns | Description |
|--------|---------|-------------|
| `query(cypher)` | `Result<QueryResult>` | Execute a read-only Cypher query. Rejects mutations with an error. Uses transparent plan cache. |
| `query_with(cypher)` | `QueryBuilder` | Build a parameterized read query with `.param()`, `.timeout()`, `.max_memory()`. |

### Locy Evaluation

| Method | Returns | Description |
|--------|---------|-------------|
| `locy(program)` | `Result<LocyResult>` | Evaluate a Locy program with default configuration. |
| `locy_with(program)` | `LocyBuilder` | Build a parameterized Locy evaluation. |
| `compile_locy(program)` | `Result<CompiledProgram>` | Compile without executing (uses session's rule registry). |

### Rule Management

| Method | Returns | Description |
|--------|---------|-------------|
| `rules()` | `RuleRegistry` | Access the session-scoped rule registry. |

### Transaction Factory

| Method | Returns | Description |
|--------|---------|-------------|
| `tx()` | `Result<Transaction>` | Create a new transaction. Fails if session is pinned or another write context is active. |
| `tx_with()` | `TransactionBuilder` | Build a transaction with `.timeout()` and `.isolation()` options. |

### Scoped Parameters

```rust
let session = db.session();
session.params().set("tenant", 42);
session.params().set("region", "us-east");

// Parameters are auto-merged into queries as $session.tenant, $session.region
let rows = session.query("MATCH (n) WHERE n.tenant = $session.tenant RETURN n").await?;
```

| Method | Returns | Description |
|--------|---------|-------------|
| `params()` | `Params` | Access the session-scoped parameter store. |

**`Params` methods:**

| Method | Description |
|--------|-------------|
| `set(key, value)` | Set a parameter. |
| `get(key)` | Get a parameter value. Returns `Option<Value>`. |
| `unset(key)` | Remove a parameter. Returns previous value. |
| `get_all()` | Snapshot all parameters as `HashMap<String, Value>`. |
| `set_all(iter)` | Set multiple parameters from an iterator. |

### Version Pinning

```rust
let mut session = db.session();

// Pin to a named snapshot (read-only after pinning)
session.pin_to_version("snapshot_20240101").await?;

// Pin to a timestamp
session.pin_to_timestamp(chrono::Utc::now() - chrono::Duration::hours(1)).await?;

// Unpin and return to live state
session.refresh().await?;
```

| Method | Description |
|--------|-------------|
| `pin_to_version(snapshot_id)` | Pin session to a specific snapshot. Writes rejected while pinned. |
| `pin_to_timestamp(ts)` | Pin to the closest snapshot at or before the given timestamp. |
| `refresh()` | Unpin and return to the live database state. |
| `is_pinned()` | Returns `true` if pinned. |

### Prepared Statements

| Method | Returns | Description |
|--------|---------|-------------|
| `prepare(cypher)` | `Result<PreparedQuery>` | Prepare a Cypher query for repeated execution. |
| `prepare_locy(program)` | `Result<PreparedLocy>` | Prepare a Locy program for repeated evaluation. |

### Notifications

| Method | Returns | Description |
|--------|---------|-------------|
| `watch()` | `CommitStream` | Watch for all commit notifications. |
| `watch_with()` | `WatchBuilder` | Build a filtered commit notification stream. |

### Hooks

| Method | Description |
|--------|-------------|
| `add_hook(name, hook)` | Add a named session hook. Takes `&mut self`. |
| `remove_hook(name)` | Remove a hook by name. Returns `true` if it existed. |
| `list_hooks()` | List names of all registered hooks. |
| `clear_hooks()` | Remove all hooks. |

### Lifecycle & Observability

| Method | Returns | Description |
|--------|---------|-------------|
| `id()` | `&str` | Session ID (UUID). |
| `metrics()` | `SessionMetrics` | Snapshot session-level metrics. |
| `capabilities()` | `SessionCapabilities` | Query what the session can do in its current mode. |
| `cancel()` | `()` | Cancel all in-flight queries and Locy evaluations. Session remains usable after. |
| `cancellation_token()` | `CancellationToken` | Get a clone of the session's cancellation token. |

---

## QueryBuilder

Fluent builder for parameterized Cypher queries within a session. Created by `session.query_with(cypher)`.

```rust
let result = session.query_with("MATCH (n:Person) WHERE n.age > $min RETURN n")
    .param("min", 25)
    .param("limit", 100)
    .timeout(Duration::from_secs(10))
    .max_memory(512 * 1024 * 1024) // 512 MB
    .fetch_all()
    .await?;
```

| Method | Returns | Description |
|--------|---------|-------------|
| `param(name, value)` | `Self` | Bind a named parameter (no `$` prefix). |
| `params(iter)` | `Self` | Bind multiple parameters from an iterator. |
| `timeout(duration)` | `Self` | Set maximum execution time. |
| `max_memory(bytes)` | `Self` | Set maximum memory per query. |
| `cancellation_token(token)` | `Self` | Attach a cancellation token. |
| `fetch_all()` | `Result<QueryResult>` | Execute and return all rows. |
| `fetch_one()` | `Result<Option<Row>>` | Execute and return first row. |
| `cursor()` | `Result<QueryCursor>` | Execute and return a streaming cursor. |
| `explain()` | `Result<ExplainOutput>` | Explain the query plan without executing. |
| `profile()` | `Result<(QueryResult, ProfileOutput)>` | Execute with profiling. |

---

## Transaction

The explicit write scope. Created via `session.tx()`. Provides ACID guarantees with commit-time serialization.

Each transaction owns a private L0 buffer. Reads within the transaction see both committed data and the transaction's own uncommitted writes. Changes are isolated from other transactions and sessions until `commit()`.

```rust
let tx = session.tx().await?;

// Reads see uncommitted writes
tx.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
let rows = tx.query("MATCH (p:Person {name: 'Alice'}) RETURN p.age").await?;

// Parameterized mutations
tx.execute_with("CREATE (:Person {name: $name, age: $age})")
    .param("name", "Bob")
    .param("age", 25)
    .run()
    .await?;

let result = tx.commit().await?;
println!("Version: {}, Mutations: {}", result.version, result.mutations_committed);
```

### Cypher Reads (sees uncommitted writes)

| Method | Returns | Description |
|--------|---------|-------------|
| `query(cypher)` | `Result<QueryResult>` | Execute a Cypher query within the transaction. |
| `query_with(cypher)` | `TxQueryBuilder` | Build a parameterized query. |

**`TxQueryBuilder` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `param(name, value)` | `Self` | Bind a parameter. |
| `cancellation_token(token)` | `Self` | Attach a cancellation token. |
| `timeout(duration)` | `Self` | Set maximum execution time. |
| `execute()` | `Result<ExecuteResult>` | Execute as mutation; returns affected rows. |
| `fetch_all()` | `Result<QueryResult>` | Execute as query; returns rows. |
| `fetch_one()` | `Result<Option<Row>>` | Execute and return first row. |
| `cursor()` | `Result<QueryCursor>` | Execute and return a streaming cursor. |

### Cypher Writes

| Method | Returns | Description |
|--------|---------|-------------|
| `execute(cypher)` | `Result<ExecuteResult>` | Execute a Cypher mutation. |
| `execute_with(cypher)` | `ExecuteBuilder` | Build a parameterized mutation. |

**`ExecuteBuilder` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `param(key, value)` | `Self` | Bind a parameter. |
| `params(iter)` | `Self` | Bind multiple parameters. |
| `timeout(duration)` | `Self` | Set maximum execution time. |
| `run()` | `Result<ExecuteResult>` | Execute the mutation. |
| `profile()` | `Result<(ExecuteResult, ProfileOutput)>` | Execute the mutation with profiling. |

```rust
// Profile a transaction write end-to-end
let tx = session.tx().await?;
let (res, prof) = tx
    .execute_with("CREATE (p:Person {name: $name}) RETURN p")
    .param("name", "Alice")
    .profile()
    .await?;
tx.commit().await?;
println!("nodes_created = {}, total_time_ms = {}", res.nodes_created(), prof.total_time_ms);
```

### DerivedFactSet Application

Apply Locy DERIVE results to the transaction.

```rust
// Evaluate a Locy DERIVE at session level (collects derived facts)
let result = session.locy("
    CREATE RULE reachable AS
    MATCH (a:Node)-[:KNOWS]->(b:Node)
    YIELD KEY a, KEY b

    CREATE RULE reachable AS
    MATCH (a:Node)-[:KNOWS]->(mid:Node)
    WHERE mid IS reachable TO b
    YIELD KEY a, KEY b

    DERIVE reachable
").await?;

// Apply the derived facts inside a transaction
let tx = session.tx().await?;
let apply_result = tx.apply(result.derived().unwrap().clone()).await?;
println!("Applied {} facts, version gap: {}", apply_result.facts_applied, apply_result.version_gap);
tx.commit().await?;
```

| Method | Returns | Description |
|--------|---------|-------------|
| `apply(derived)` | `Result<ApplyResult>` | Apply a `DerivedFactSet` to this transaction. |
| `apply_with(derived)` | `ApplyBuilder` | Build with staleness controls. |

**`ApplyBuilder` methods:**

| Method | Description |
|--------|-------------|
| `require_fresh()` | Reject if any commits occurred between evaluation and apply. |
| `max_version_gap(n)` | Reject if gap exceeds `n` versions. |
| `run()` | Execute the apply operation. Returns `Result<ApplyResult>`. |

**`ApplyResult`:**

| Field | Type | Description |
|-------|------|-------------|
| `facts_applied` | `usize` | Number of mutation queries replayed. |
| `version_gap` | `u64` | Versions committed between DERIVE and apply. 0 = fresh. |

### Locy Evaluation (within Transaction)

DERIVE commands auto-apply to the transaction's private L0.

| Method | Returns | Description |
|--------|---------|-------------|
| `locy(program)` | `Result<LocyResult>` | Evaluate a Locy program. DERIVE auto-applies to tx. |
| `locy_with(program)` | `TxLocyBuilder` | Build a parameterized Locy evaluation. |

### Rule Management

| Method | Returns | Description |
|--------|---------|-------------|
| `rules()` | `RuleRegistry` | Access the transaction-scoped rule registry. Rules promoted to session on commit. |

### Bulk Loading (within Transaction)

| Method | Returns | Description |
|--------|---------|-------------|
| `bulk_writer()` | `BulkWriterBuilder` | Create a bulk writer that writes directly to storage. |
| `appender(label)` | `AppenderBuilder` | Create a streaming appender for a single label. |
| `bulk_insert_vertices(label, props)` | `Result<Vec<Vid>>` | Bulk insert vertices to the tx L0. Returns allocated VIDs. |
| `bulk_insert_edges(type, edges)` | `Result<()>` | Bulk insert edges to the tx L0. |

### Prepared Statements

| Method | Returns | Description |
|--------|---------|-------------|
| `prepare(cypher)` | `Result<PreparedQuery>` | Prepare a Cypher query. |
| `prepare_locy(program)` | `Result<PreparedLocy>` | Prepare a Locy program. |

### Lifecycle

| Method | Returns | Description |
|--------|---------|-------------|
| `commit()` | `Result<CommitResult>` | Commit all changes. Consumes the transaction. |
| `rollback()` | `()` | Rollback all changes. Consumes the transaction. Infallible. |
| `is_dirty()` | `bool` | Whether the transaction has uncommitted changes. |
| `id()` | `&str` | Transaction ID (UUID). |
| `started_at_version()` | `u64` | Database version when the transaction was created. |
| `cancel()` | `()` | Cancel all in-flight queries, Locy evaluations, and `apply()` calls in this transaction. |

### Drop Behavior

If a transaction is dropped without calling `commit()` or `rollback()`, the private L0 buffer is silently discarded. A warning is logged if the transaction was dirty.

---

## CommitResult

Returned by `Transaction::commit()`.

```rust
let result = tx.commit().await?;
println!("Version: {}", result.version);
println!("Mutations: {}", result.mutations_committed);
println!("Duration: {:?}", result.duration);
println!("Version gap: {}", result.version_gap());
```

| Field | Type | Description |
|-------|------|-------------|
| `mutations_committed` | `usize` | Number of mutations committed. |
| `rules_promoted` | `usize` | Number of rules promoted from tx to session. |
| `version` | `u64` | Database version after commit. |
| `started_at_version` | `u64` | Database version when the transaction started. |
| `wal_lsn` | `u64` | WAL log sequence number (0 when no WAL). |
| `duration` | `Duration` | Time for the commit operation (lock + WAL + merge). |
| `rule_promotion_errors` | `Vec<RulePromotionError>` | Errors from best-effort rule promotion. |

| Method | Returns | Description |
|--------|---------|-------------|
| `version_gap()` | `u64` | Number of concurrent commits between tx start and commit. 0 = no contention. |

---

## IsolationLevel

```rust
pub enum IsolationLevel {
    Serialized, // default — commit-time serialization with private L0 per tx
}
```

Used with `session.tx_with().isolation(IsolationLevel::Serialized).start().await?`.

---

## Schema Management

### SchemaBuilder

Define labels, edge types, properties, and indexes with a fluent API. Changes are batched and applied atomically.

```rust
db.schema()
    .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .property_nullable("email", DataType::String)
        .vector("embedding", 1536)
        .index("name", IndexType::Scalar(ScalarType::BTree))
    .label("Document")
        .property("title", DataType::String)
        .index("title", IndexType::FullText)
    .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Date)
        .property_nullable("weight", DataType::Float64)
    .apply()
    .await?;
```

**`SchemaBuilder` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `label(name)` | `LabelBuilder` | Start defining a label. |
| `edge_type(name, from, to)` | `EdgeTypeBuilder` | Start defining an edge type. |
| `current()` | `Arc<Schema>` | Get the current schema (read-only snapshot). |
| `with_changes(changes)` | `Self` | Add pre-built schema changes. |
| `apply()` | `Result<()>` | Apply all batched changes atomically. |

**`LabelBuilder` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `property(name, data_type)` | `Self` | Add a required property. |
| `property_nullable(name, data_type)` | `Self` | Add a nullable property. |
| `vector(name, dimensions)` | `Self` | Add a vector property (shorthand for `DataType::Vector`). |
| `index(property, index_type)` | `Self` | Add an index on a property. |
| `done()` | `SchemaBuilder` | Return to the parent builder. |
| `label(name)` | `LabelBuilder` | Chain to another label (calls `done()` first). |
| `edge_type(name, from, to)` | `EdgeTypeBuilder` | Chain to an edge type. |
| `apply()` | `Result<()>` | Apply (calls `done()` then `apply()`). |

**`EdgeTypeBuilder` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `property(name, data_type)` | `Self` | Add a required property. |
| `property_nullable(name, data_type)` | `Self` | Add a nullable property. |
| `done()` | `SchemaBuilder` | Return to the parent builder. |
| `label(name)` | `LabelBuilder` | Chain to a label. |
| `edge_type(name, from, to)` | `EdgeTypeBuilder` | Chain to another edge type. |
| `apply()` | `Result<()>` | Apply. |

### DataType Enum

```rust
pub enum DataType {
    String,
    Int64,
    Float64,
    Boolean,
    Date,
    DateTime,
    Duration,
    Bytes,
    Json,
    Vector { dimensions: usize },
    // ... additional variants
}
```

### IndexType Enum

```rust
pub enum IndexType {
    Vector(VectorIndexCfg),
    FullText,
    Scalar(ScalarType),
    Inverted(InvertedIndexConfig),
}

pub enum ScalarType {
    BTree,
    Hash,
    Bitmap,
    LabelList,
}

pub struct VectorIndexCfg {
    pub algorithm: VectorAlgo,
    pub metric: VectorMetric,
    pub embedding: Option<EmbeddingCfg>,
}

pub enum VectorAlgo {
    Flat,
    IvfFlat { partitions: u32 },
    IvfPq { partitions: u32, sub_vectors: u32 },     // default algorithm
    IvfSq { partitions: u32 },
    IvfRq { partitions: u32, num_bits: Option<u8> },
    Hnsw { m: u32, ef_construction: u32, partitions: Option<u32> },      // alias for HnswSq
    HnswFlat { m: u32, ef_construction: u32, partitions: Option<u32> },
    HnswSq { m: u32, ef_construction: u32, partitions: Option<u32> },
    HnswPq { m: u32, ef_construction: u32, sub_vectors: u32, partitions: Option<u32> },
    // Multi-vector (ColBERT / MaxSim): encodes a List<Vector> column into a derived
    // FDE column indexed by `inner`. Use on a `DataType::List(Vector { .. })` property.
    Muvera { k_sim: u32, reps: u32, d_proj: u32, seed: u64, inner: Box<VectorAlgo> },
}

pub enum VectorMetric {
    Cosine,
    L2,
    Dot,
}
```

### Schema Info Types

**`LabelInfo`:**

| Field | Type |
|-------|------|
| `name` | `String` |
| `count` | `usize` |
| `properties` | `Vec<PropertyInfo>` |
| `indexes` | `Vec<IndexInfo>` |
| `constraints` | `Vec<ConstraintInfo>` |
| `description` | `Option<String>` |

**`EdgeTypeInfo`:**

| Field | Type |
|-------|------|
| `name` | `String` |
| `count` | `usize` |
| `source_labels` | `Vec<String>` |
| `target_labels` | `Vec<String>` |
| `properties` | `Vec<PropertyInfo>` |
| `indexes` | `Vec<IndexInfo>` |
| `constraints` | `Vec<ConstraintInfo>` |
| `description` | `Option<String>` |

On both `LabelInfo` and `EdgeTypeInfo`, `count` is obtained with a Cypher
`count(...)` over the element, so it includes rows still buffered in L0 and
excludes tombstoned or superseded versions. The
element name is backtick-quoted, so names containing punctuation (such as `.`),
leading digits, or non-ASCII characters count correctly; a name containing a
backtick cannot be quoted and is refused with `UniError::Query`. Counting
failures propagate as an `Err` rather than being reported as `count: 0`.

**`PropertyInfo`:** `name: String`, `data_type: String`, `nullable: bool`, `is_indexed: bool`, `description: Option<String>`

**`IndexInfo`:** `name: String`, `index_type: String`, `properties: Vec<String>`, `status: String`

**`ConstraintInfo`:** `name: String`, `constraint_type: String`, `properties: Vec<String>`, `enabled: bool`

---

## Bulk Loading

Two bulk loading mechanisms are available on `Transaction`:

### BulkWriter

High-throughput bulk writer that writes directly to storage, bypassing the L0 buffer. Supports deferred index building, constraint validation, progress callbacks, and automatic checkpointing.

```rust
let tx = session.tx().await?;

let mut writer = tx.bulk_writer()
    .batch_size(10_000)
    .defer_vector_indexes(true)
    .async_indexes(false)
    .validate_constraints(true)
    .max_buffer_size_bytes(1_073_741_824) // 1 GB
    .on_progress(|p| println!("{:?}", p.phase))
    .build()?;

let vids = writer.insert_vertices("Person", vec![
    HashMap::from([("name".into(), Value::from("Alice")), ("age".into(), Value::from(30))]),
    HashMap::from([("name".into(), Value::from("Bob")), ("age".into(), Value::from(25))]),
]).await?;

writer.insert_edges("KNOWS", vec![
    EdgeData::new(vids[0], vids[1], HashMap::new()),
]).await?;

let stats = writer.commit().await?;
println!("Inserted {} vertices, {} edges", stats.vertices_inserted, stats.edges_inserted);

tx.commit().await?;
```

**`BulkWriterBuilder` methods:**

| Method | Description |
|--------|-------------|
| `batch_size(n)` | Rows to buffer before flush (default: 10,000). |
| `defer_vector_indexes(bool)` | Defer vector index building until commit (default: true). |
| `defer_scalar_indexes(bool)` | Defer scalar index building until commit (default: true). |
| `async_indexes(bool)` | Build indexes asynchronously after commit (default: false). |
| `validate_constraints(bool)` | Validate NOT NULL, UNIQUE, CHECK constraints (default: true). |
| `max_buffer_size_bytes(n)` | Trigger checkpoint when buffer exceeds this (default: 1 GB). |
| `on_progress(callback)` | Set progress callback. |
| `build()` | Build the `BulkWriter`. Returns `Result<BulkWriter>`. |

**`BulkWriter` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `insert_vertices(label, data)` | `Result<Vec<Vid>>` | Insert vertices. Accepts `Vec<HashMap>` or `RecordBatch`. |
| `insert_edges(type, edges)` | `Result<Vec<Eid>>` | Insert edges via `Vec<EdgeData>`. |
| `commit()` | `Result<BulkStats>` | Flush and commit. Rebuilds deferred indexes. |
| `abort()` | `Result<()>` | Discard all changes and roll back. |
| `stats()` | `&BulkStats` | Current statistics snapshot. |

**`BulkStats`:**

| Field | Type | Description |
|-------|------|-------------|
| `vertices_inserted` | `usize` | Total vertices inserted. |
| `edges_inserted` | `usize` | Total edges inserted. |
| `indexes_rebuilt` | `usize` | Indexes rebuilt at commit. |
| `duration` | `Duration` | Total elapsed time. |
| `index_build_duration` | `Duration` | Time spent rebuilding indexes. |
| `index_task_ids` | `Vec<String>` | Background index task IDs (async mode). |
| `indexes_pending` | `bool` | True if index builds are running in background. |

### StreamingAppender

Buffered, single-label row-by-row data loading.

```rust
let tx = session.tx().await?;

let mut appender = tx.appender("Person")
    .batch_size(5000)
    .defer_vector_indexes(true)
    .build()?;

appender.append(HashMap::from([("name".into(), Value::from("Alice"))])).await?;
appender.append(HashMap::from([("name".into(), Value::from("Bob"))])).await?;

// Also supports Arrow RecordBatch
// appender.write_batch(&record_batch).await?;

let stats = appender.finish().await?;

tx.commit().await?;
```

**`AppenderBuilder` methods:**

| Method | Description |
|--------|-------------|
| `batch_size(n)` | Auto-flush threshold (default: 5000). |
| `defer_vector_indexes(bool)` | Defer vector index building (default: true). |
| `max_buffer_size_bytes(n)` | Max buffer size before checkpoint. |
| `build()` | Build the `StreamingAppender`. Returns `Result<StreamingAppender>`. |

**`StreamingAppender` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `append(properties)` | `Result<()>` | Append a single row. Auto-flushes when buffer is full. |
| `write_batch(record_batch)` | `Result<()>` | Append an Arrow `RecordBatch`. |
| `finish()` | `Result<BulkStats>` | Flush remaining rows and commit. Consumes self. |
| `abort()` | `()` | Discard all data. Consumes self. |
| `buffered_count()` | `usize` | Number of rows currently buffered. |

---

## Locy Integration

### LocyBuilder (Session-level)

Created by `session.locy_with(program)`. Fluent builder for Locy evaluation with parameters.

```rust
let result = session.locy_with("
    CREATE RULE edge_rule AS
    MATCH (a:Node)-[:KNOWS]->(b:Node)
    YIELD KEY a, KEY b

    QUERY edge_rule RETURN a, b
")
    .param("threshold", 0.5)
    .timeout(Duration::from_secs(30))
    .max_iterations(1000)
    .run()
    .await?;

for row in result.rows() {
    println!("{:?}", row);
}
```

| Method | Returns | Description |
|--------|---------|-------------|
| `param(name, value)` | `Self` | Bind a parameter. |
| `params(iter)` | `Self` | Bind multiple parameters. |
| `params_map(map)` | `Self` | Bind from a `HashMap`. |
| `timeout(duration)` | `Self` | Override evaluation timeout. |
| `max_iterations(n)` | `Self` | Override max fixpoint iterations. |
| `cancellation_token(token)` | `Self` | Attach a cancellation token. |
| `with_config(config)` | `Self` | Apply a full `LocyConfig`. |
| `run()` | `Result<LocyResult>` | Evaluate and return results. |
| `explain()` | `Result<LocyExplainOutput>` | Explain without executing. |

Cancellation is enforced against the whole evaluation, not just the Cypher
statements the program dispatches, so a long-running fixpoint is interruptible.
A cancelled evaluation returns `UniError::Cancelled`. The session's own token
(`Session::cancel()`) applies in addition to any token passed to
`cancellation_token(...)`.

### TxLocyBuilder (Transaction-level)

Created by `tx.locy_with(program)`. Same API as `LocyBuilder` but sees the transaction's uncommitted writes. DERIVE commands auto-apply to the private L0.

### LocyResult

Wraps `uni_locy::LocyResult` with additional accessors.

```rust
let result = session.locy("
    CREATE RULE edge_rule AS
    MATCH (a:Node)-[:KNOWS]->(b:Node)
    YIELD KEY a, KEY b

    QUERY edge_rule RETURN a, b
").await?;

// Access rows (via Deref to inner LocyResult)
for row in result.rows() { /* ... */ }

// Execution metrics
let metrics = result.metrics();
println!("Duration: {:?}", metrics.duration);

// Derived facts (from DERIVE commands)
if let Some(derived) = result.derived() {
    let tx = session.tx().await?;
    tx.apply(derived.clone()).await?;
    tx.commit().await?;
}

// Warnings (from inner LocyResult via Deref)
for warning in result.warnings() {
    println!("Warning: {}", warning.message);
}
```

| Method | Returns | Description |
|--------|---------|-------------|
| `metrics()` | `&QueryMetrics` | Execution metrics (timing, row counts). |
| `derived()` | `Option<&DerivedFactSet>` | Derived facts from DERIVE commands. |
| `into_inner()` | `uni_locy::LocyResult` | Unwrap, discarding metrics. |
| `into_parts()` | `(LocyResult, QueryMetrics)` | Decompose into parts. |
| *Deref* | `uni_locy::LocyResult` | All inner fields/methods (`rows()`, `stats`, `warnings()`, etc.). |

### LocyExplainOutput

Returned by `session.locy_with(program).explain()`.

| Field | Type | Description |
|-------|------|-------------|
| `plan_text` | `String` | Human-readable evaluation plan. |
| `strata_count` | `usize` | Number of evaluation strata. |
| `rule_names` | `Vec<String>` | Names of rules in the program. |
| `has_recursive_strata` | `bool` | Whether fixpoint iteration is needed. |
| `warnings` | `Vec<String>` | Compiler warnings from static analysis. |
| `command_count` | `usize` | Number of commands (QUERY, DERIVE, ASSUME, etc.). |

For full Locy language documentation, see the [Locy Overview](../locy/index.md).

---

## Prepared Statements

Cache parse/plan results for repeated execution. Auto-replan on schema changes.

### PreparedQuery

```rust
let prepared = session.prepare("MATCH (n:Person) WHERE n.age > $min RETURN n").await?;

// Execute with positional params
let result = prepared.execute(&[("min", Value::from(21))]).await?;

// Or use the fluent binder
let result = prepared.bind()
    .param("min", 21)
    .execute()
    .await?;

// Can be shared across threads via Arc
let shared = Arc::new(prepared);
```

| Method | Returns | Description |
|--------|---------|-------------|
| `execute(params)` | `Result<QueryResult>` | Execute with `&[(&str, Value)]` params. |
| `bind()` | `PreparedQueryBinder` | Fluent parameter binder. |
| `query_text()` | `&str` | Original query text. |

### PreparedLocy

```rust
let prepared = session.prepare_locy("
    CREATE RULE edge_rule AS
    MATCH (a:Node)-[:KNOWS]->(b:Node)
    YIELD KEY a, KEY b

    QUERY edge_rule RETURN a, b
").await?;

let result = prepared.execute(&[("threshold", Value::from(0.5))]).await?;

let result = prepared.bind()
    .param("threshold", 0.5)
    .execute()
    .await?;
```

| Method | Returns | Description |
|--------|---------|-------------|
| `execute(params)` | `Result<LocyResult>` | Execute with `&[(&str, Value)]` params. |
| `bind()` | `PreparedLocyBinder` | Fluent parameter binder. |
| `program_text()` | `&str` | Original program text. |

---

## Notifications

Reactive awareness of database changes. Sessions can subscribe to commit events.

### CommitNotification

| Field | Type | Description |
|-------|------|-------------|
| `version` | `u64` | Database version after commit. |
| `mutation_count` | `usize` | Number of mutations in the transaction. |
| `labels_affected` | `Vec<String>` | Vertex labels affected. |
| `edge_types_affected` | `Vec<String>` | Edge types affected. |
| `rules_promoted` | `usize` | Rules promoted from tx to session. |
| `timestamp` | `DateTime<Utc>` | Commit timestamp. |
| `tx_id` | `String` | Transaction ID. |
| `session_id` | `String` | Session ID that committed. |
| `causal_version` | `u64` | Version when the transaction started (for causal ordering). |

### CommitStream

Async stream of commit notifications.

```rust
let mut stream = session.watch();
while let Some(notif) = stream.next().await {
    println!("Version {} committed with {} mutations", notif.version, notif.mutation_count);
}
```

| Method | Returns | Description |
|--------|---------|-------------|
| `next()` | `Option<CommitNotification>` | Wait for the next matching notification. `None` if channel closed. |

#### Delivery: best-effort, not a feed

`watch()` is a **notification** stream, not a change feed. It runs over a
bounded broadcast channel (`UniConfig::commit_channel_capacity`, default 256).
A consumer that falls behind loses the **oldest** commits — it always still
receives the newest.

That is deliberate, and correct for the intended use. A consumer that
invalidates a cache and re-reads is unaffected by lag: the final notification
always arrives, so the re-read picks up whatever the lost commits did.
`debounce()` drops notifications for the same reason.

A consumer doing **non-idempotent per-commit work** — an audit mirror, a
counter, anything that appends once per commit — must check
`CommitNotification::dropped_before`, which reports how many commits were lost
immediately before the one you are holding. Filtered skips are never counted
there, so a `labels()` or `debounce()` filter cannot raise a false alarm.

```rust
while let Some(n) = stream.next().await {
    if n.dropped_before > 0 {
        // These commits cannot be redelivered.
        return Err(anyhow!("lost {} commits before version {}", n.dropped_before, n.version));
    }
    audit_log.append(&n)?;
}
```

**If you need a contiguous feed, use CDC instead.** A `CdcOutputProvider`
guarantees contiguity, checkpoints its position, and halts on a gap rather
than silently continuing past one. `watch()` will never provide that
guarantee; `dropped_before` exists so you find out you need it.

### WatchBuilder

Build a filtered commit stream. Created by `session.watch_with()`.

```rust
let mut stream = session.watch_with()
    .labels(&["Person", "Document"])
    .edge_types(&["KNOWS"])
    .exclude_session(session.id())
    .debounce(Duration::from_millis(100))
    .build();
```

| Method | Returns | Description |
|--------|---------|-------------|
| `labels(labels)` | `Self` | Only receive notifications affecting these labels. |
| `edge_types(types)` | `Self` | Only receive notifications affecting these edge types. |
| `exclude_session(id)` | `Self` | Exclude notifications from a specific session. |
| `debounce(interval)` | `Self` | Collapse notifications within interval. |
| `build()` | `CommitStream` | Build the filtered stream. |

---

## Session Hooks

Intercept queries and commits at the session level. Useful for audit logging, authorization, and metrics.

### SessionHook Trait

```rust
pub trait SessionHook: Send + Sync {
    /// Called before a query. Return Err to reject.
    fn before_query(&self, ctx: &HookContext) -> Result<()> { Ok(()) }

    /// Called after a query completes. Panics are caught and logged.
    fn after_query(&self, ctx: &HookContext, metrics: &QueryMetrics) {}

    /// Called before commit. Return Err to reject.
    fn before_commit(&self, ctx: &CommitHookContext) -> Result<()> { Ok(()) }

    /// Called after commit succeeds. Panics are caught and logged.
    fn after_commit(&self, ctx: &CommitHookContext, result: &CommitResult) {}
}
```

### Hook Context Types

**`HookContext`:**

| Field | Type |
|-------|------|
| `session_id` | `String` |
| `query_text` | `String` |
| `query_type` | `QueryType` (`Cypher`, `Locy`, `Execute`) |
| `params` | `HashMap<String, Value>` |

**`CommitHookContext`:**

| Field | Type |
|-------|------|
| `session_id` | `String` |
| `tx_id` | `String` |
| `mutation_count` | `usize` |

### Usage Example

```rust
struct AuditHook;

impl SessionHook for AuditHook {
    fn before_query(&self, ctx: &HookContext) -> uni_db::Result<()> {
        tracing::info!(session = %ctx.session_id, query = %ctx.query_text, "Query started");
        Ok(())
    }

    fn before_commit(&self, ctx: &CommitHookContext) -> uni_db::Result<()> {
        if ctx.mutation_count > 10_000 {
            return Err(uni_db::UniError::HookRejected {
                message: "Too many mutations in one commit".into(),
            });
        }
        Ok(())
    }
}

let mut session = db.session();
session.add_hook("audit", AuditHook);
```

---

## Multi-Agent Access

Coordinate write access across multiple processes sharing the same database.

### WriteLease

```rust
pub enum WriteLease {
    Local,                          // Single-process lock (default)
    DynamoDB { table: String },     // DynamoDB-based distributed lease
    Custom(Box<dyn WriteLeaseProvider>),  // Custom provider
}
```

### WriteLeaseProvider Trait

```rust
#[async_trait]
pub trait WriteLeaseProvider: Send + Sync {
    async fn acquire(&self) -> Result<LeaseGuard>;
    async fn heartbeat(&self, guard: &LeaseGuard) -> Result<()>;
    async fn release(&self, guard: LeaseGuard) -> Result<()>;
}
```

**`LeaseGuard`:**

| Field | Type | Description |
|-------|------|-------------|
| `lease_id` | `String` | Unique lease acquisition ID. |
| `expires_at` | `DateTime<Utc>` | Expiration time (renew via heartbeat before this). |

### Configuration

```rust
let db = Uni::open("./shared-db")
    .write_lease(WriteLease::Local)
    .build()
    .await?;

// Or with read-only mode (no writer)
let db = Uni::open("./shared-db")
    .read_only()
    .build()
    .await?;
```

---

## Synchronous Wrappers

Blocking API wrappers for non-async contexts. Each wraps the corresponding async type with a Tokio runtime.

### UniSync

```rust
let db = UniSync::in_memory()?;
let session = db.session();
let rows = session.query("MATCH (n) RETURN count(n)")?;
db.shutdown()?;
```

| Method | Returns | Description |
|--------|---------|-------------|
| `UniSync::new(uni)` | `Result<Self>` | Wrap an existing `Uni` in a blocking runtime. |
| `UniSync::in_memory()` | `Result<Self>` | Open an in-memory database (blocking). |
| `session()` | `SessionSync` | Create a sync session. |
| `schema()` | `SchemaBuilderSync` | Access schema builder (sync). |
| `schema_meta()` | `Arc<Schema>` | Get current schema snapshot. |
| `shutdown()` | `Result<()>` | Graceful shutdown (blocking). |

### SessionSync

Mirrors the `Session` API, with all async methods wrapped in `block_on()`.

| Method | Returns | Notes |
|--------|---------|-------|
| `query(cypher)` | `Result<QueryResult>` | |
| `query_with(cypher)` | `QueryBuilderSync` | `.param().fetch_all()` |
| `locy(program)` | `Result<LocyResult>` | |
| `locy_with(program)` | `LocyBuilderSync` | `.param().run()` |
| `rules()` | `RuleRegistry` | |
| `compile_locy(program)` | `Result<CompiledProgram>` | |
| `tx()` | `Result<TransactionSync>` | |
| `tx_with()` | `TransactionBuilderSync` | |
| `watch()` | `CommitStream` | |
| `watch_with()` | `WatchBuilder` | |
| `add_hook(name, hook)` | | Takes `&mut self` |
| `remove_hook(name)` | `bool` | |
| `pin_to_version(id)` | `Result<()>` | Takes `&mut self` |
| `pin_to_timestamp(ts)` | `Result<()>` | Takes `&mut self` |
| `refresh()` | `Result<()>` | Takes `&mut self` |
| `prepare(cypher)` | `Result<PreparedQuery>` | |
| `prepare_locy(program)` | `Result<PreparedLocy>` | |
| `params()` | `Params` | |
| `id()` | `&str` | |
| `capabilities()` | `SessionCapabilities` | |
| `metrics()` | `SessionMetrics` | |
| `cancel()` | | |

### TransactionSync

| Method | Returns | Notes |
|--------|---------|-------|
| `query(cypher)` | `Result<QueryResult>` | |
| `query_with(cypher)` | `TxQueryBuilderSync` | |
| `execute(cypher)` | `Result<ExecuteResult>` | |
| `execute_with(cypher)` | `ExecuteBuilderSync` | `.param().run()`, `.param().profile()` |
| `locy(program)` | `Result<LocyResult>` | |
| `locy_with(program)` | `TxLocyBuilderSync` | `.param().run()` |
| `apply(derived)` | `Result<ApplyResult>` | |
| `apply_with(derived)` | `ApplyBuilderSync` | |
| `prepare(cypher)` | `Result<PreparedQuery>` | |
| `prepare_locy(program)` | `Result<PreparedLocy>` | |
| `bulk_writer()` | `BulkWriterBuilder` | |
| `appender(label)` | `AppenderBuilder` | |
| `bulk_insert_vertices(label, props)` | `Result<Vec<Vid>>` | |
| `bulk_insert_edges(type, edges)` | `Result<()>` | |
| `commit()` | `Result<CommitResult>` | Consumes self |
| `rollback()` | `()` | Consumes self |
| `is_dirty()` | `bool` | |
| `id()` | `&str` | |

---

## Session Templates

Pre-configured session factories for per-request session creation.

```rust
let template = db.session_template()
    .param("tenant", 42)
    .rules("CREATE RULE edge_rule AS MATCH (a:Node)-[:KNOWS]->(b:Node) YIELD KEY a, KEY b")?
    .hook("audit", AuditHook)
    .query_timeout(Duration::from_secs(30))
    .transaction_timeout(Duration::from_secs(60))
    .build()?;

// Cheap per-request session creation (sync, no recompilation)
let session = template.create();
```

**`SessionTemplateBuilder` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `param(key, value)` | `Self` | Bind a parameter for all sessions. |
| `rules(program)` | `Result<Self>` | Pre-compile Locy rules (eager compilation). |
| `hook(name, hook)` | `Self` | Attach a named session hook. |
| `query_timeout(duration)` | `Self` | Default query timeout. |
| `transaction_timeout(duration)` | `Self` | Default transaction timeout. |
| `build()` | `Result<SessionTemplate>` | Build the template. |

**`SessionTemplate` methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `create()` | `Session` | Create a new session. Cheap: rules cloned, params cloned, hooks Arc-cloned. |

---

## Database Metrics

Snapshot of database-level metrics. Returned by `db.metrics()`.

```rust
let m = db.metrics();
println!("L0 mutations: {}", m.l0_mutation_count);
println!("Active sessions: {}", m.active_sessions);
println!("Uptime: {:?}", m.uptime);
println!("Total queries: {}", m.total_queries);
```

| Field | Type | Description |
|-------|------|-------------|
| `l0_mutation_count` | `usize` | Cumulative L0 mutations since last flush. |
| `l0_estimated_size_bytes` | `usize` | Estimated L0 buffer size. |
| `schema_version` | `u64` | Current schema version number. |
| `uptime` | `Duration` | Time since database instance was created. |
| `active_sessions` | `usize` | Currently active sessions. |
| `l1_run_count` | `usize` | L1 compaction runs completed. |
| `write_throttle_pressure` | `ThrottlePressure` | Write back-pressure (0.0--1.0). |
| `compaction_status` | `CompactionStatus` | Current compaction state. |
| `wal_size_bytes` | `u64` | WAL size in bytes. |
| `wal_lsn` | `u64` | Highest flushed WAL log sequence number. |
| `total_queries` | `u64` | Total queries across all sessions. |
| `total_commits` | `u64` | Total committed transactions. |

### SessionMetrics

Returned by `session.metrics()`.

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | `String` | The session ID. |
| `active_since` | `Instant` | When the session was created. |
| `queries_executed` | `u64` | Number of queries. |
| `locy_evaluations` | `u64` | Number of Locy evaluations. |
| `total_query_time` | `Duration` | Cumulative query execution time. |
| `transactions_committed` | `u64` | Committed transactions. |
| `transactions_rolled_back` | `u64` | Rolled-back transactions. |
| `total_rows_returned` | `u64` | Rows returned across all queries. |
| `total_rows_scanned` | `u64` | Rows scanned. |
| `plan_cache_hits` | `u64` | Plan cache hits. |
| `plan_cache_misses` | `u64` | Plan cache misses. |
| `plan_cache_size` | `usize` | Current plan cache entries. |

### SessionCapabilities

Returned by `session.capabilities()`.

| Field | Type | Description |
|-------|------|-------------|
| `can_write` | `bool` | Can create transactions and execute writes. |
| `can_pin` | `bool` | Supports version pinning. |
| `isolation` | `IsolationLevel` | Isolation level for transactions. |
| `has_notifications` | `bool` | Commit notifications available. |
| `write_lease` | `Option<WriteLeaseSummary>` | Write lease strategy summary. |

---

## Result Types

### QueryResult

Returned by `session.query()`, `tx.query()`, and builder terminals.

```rust
let result = session.query("MATCH (n:Person) RETURN n.name, n.age").await?;

// Iterate rows
for row in result.rows() {
    let name: String = row.get("n.name")?;
    let age: i64 = row.get("n.age")?;
}

// Metadata
println!("Columns: {:?}", result.columns());
println!("Row count: {}", result.len());
println!("Empty: {}", result.is_empty());

// Metrics
let metrics = result.metrics();
println!("Duration: {:?}", metrics.duration);

// Consume into owned rows
let rows: Vec<Row> = result.into_rows();
```

### ExecuteResult

Returned by `tx.execute()` and `ExecuteBuilder::run()`.

```rust
let result = tx.execute("CREATE (:Person {name: 'Alice'})").await?;
println!("Affected rows: {}", result.affected_rows);
println!("Nodes created: {}", result.nodes_created);
println!("Relationships created: {}", result.relationships_created);
println!("Properties set: {}", result.properties_set);
```

### Row

```rust
let row: &Row = result.rows().first().unwrap();

// Typed access by column name
let name: String = row.get("n.name")?;
let age: i64 = row.get("n.age")?;
let maybe_email: Option<String> = row.get("n.email").ok();

// Access by index
let first_value: &Value = row.get_by_index(0)?;
```

### QueryCursor

Streaming cursor for large result sets.

```rust
let mut cursor = session.query_with("MATCH (n) RETURN n")
    .cursor()
    .await?;

while let Some(row) = cursor.next().await? {
    // Process row without loading entire result set
}
```

---

## Value Types

The `Value` enum represents all data types in Uni:

```rust
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Vector(Vec<f32>),
    Node { id: Vid, labels: Vec<String>, properties: HashMap<String, Value> },
    Edge { id: Eid, src: Vid, dst: Vid, edge_type: String, properties: HashMap<String, Value> },
    Path { nodes: Vec<Value>, edges: Vec<Value> },
    // ... additional variants (Date, DateTime, Duration)
}
```

Conversions from Rust types are provided via `Into<Value>`:

```rust
Value::from(42_i64)        // Int
Value::from(3.14_f64)      // Float
Value::from("hello")       // String
Value::from(true)          // Bool
Value::from(vec![1.0_f32, 2.0, 3.0])  // Vector
```

---

## Compaction & Index Management

### Compaction

Accessed via `db.compaction()`.

```rust
// Compact a specific label or edge type
let stats = db.compaction().compact("Person").await?;

// Wait for all background compaction
db.compaction().wait().await?;
```

| Method | Returns | Description |
|--------|---------|-------------|
| `compact(name)` | `Result<CompactionStats>` | Compact data for a label or edge type. |
| `wait()` | `Result<()>` | Wait for all background compaction. |

### Index Management

Accessed via `db.indexes()`.

```rust
// List all indexes
let all_indexes = db.indexes().list(None);

// List indexes for a label
let person_indexes = db.indexes().list(Some("Person"));

// Rebuild indexes (blocking)
db.indexes().rebuild("Person", false).await?;

// Rebuild indexes (background)
let task_id = db.indexes().rebuild("Person", true).await?;

// Check rebuild status
let status = db.indexes().rebuild_status().await?;

// Retry failed rebuilds
let retried = db.indexes().retry_failed().await?;
```

| Method | Returns | Description |
|--------|---------|-------------|
| `list(label)` | `Vec<IndexDefinition>` | List index definitions. `None` for all indexes. |
| `rebuild(label, background)` | `Result<Option<String>>` | Rebuild indexes. Returns task ID if background. |
| `rebuild_status()` | `Result<Vec<IndexRebuildTask>>` | Status of all rebuild tasks. |
| `retry_failed()` | `Result<Vec<String>>` | Retry failed rebuild tasks. Returns retried IDs. |

---

## Snapshots

Named point-in-time snapshots for time travel and backup.

```rust
// Create a snapshot
let snapshot_id = db.create_snapshot("before_migration").await?;

// List snapshots
let snapshots = db.list_snapshots().await?;
for s in &snapshots {
    println!("{}: {:?}", s.id, s.created_at);
}

// Pin a session to a snapshot
let mut session = db.session();
session.pin_to_version(&snapshot_id).await?;
let old_data = session.query("MATCH (n) RETURN count(n)").await?;
session.refresh().await?;  // Return to live state

// Restore to a snapshot (requires restart to fully take effect)
db.restore_snapshot(&snapshot_id).await?;
```

---

## Rule Registry

Manage pre-compiled Locy rules at database, session, or transaction scope. Rules registered at the database level are cloned into every new session.

Database-level rules are durable: their source is persisted to
`catalog/locy_rules.json` and recompiled on open, so they survive restarts.
Session-, transaction-, and fork-scoped rules are ephemeral. Mutating methods
are `async` because the database-level variants persist.

Registered rules are compiled against the database's plugin aggregate registry,
so an aggregate supplied by a plugin and declared monotone is accepted in a
recursive stratum. Database-level rules are recompiled against the same
registry when the database is reopened.

```rust
// Database-level (global, durable across restarts)
db.rules().register("CREATE RULE edge_rule AS MATCH (a:Node)-[:KNOWS]->(b:Node) YIELD KEY a, KEY b").await?;

// Session-level (ephemeral)
let session = db.session();
session.rules().register("
    CREATE RULE path_rule AS
    MATCH (a:Node)-[:KNOWS]->(mid:Node)
    WHERE mid IS edge_rule TO c
    YIELD KEY a, KEY c
").await?;

// Query registered rules
let names = session.rules().list();
let info = session.rules().get("edge_rule");
println!("Rules: {:?}, count: {}", names, session.rules().count());

// Remove and clear
session.rules().remove("edge_rule").await?;
session.rules().clear().await?;
```

| Method | Returns | Description |
|--------|---------|-------------|
| `register(program)` | `Result<()>` | Compile and register rules from a Locy program. |
| `remove(name)` | `Result<bool>` | Remove a rule by name. Recompiles remaining rules. |
| `list()` | `Vec<String>` | List all rule names. |
| `get(name)` | `Option<RuleInfo>` | Get metadata about a rule. |
| `count()` | `usize` | Number of registered rules. |
| `clear()` | `()` | Clear all rules. |

**`RuleInfo`:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | `String` | Rule name. |
| `clause_count` | `usize` | Number of clauses. |
| `is_recursive` | `bool` | Whether the rule is recursive. |
