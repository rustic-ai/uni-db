---
name: uni-db
description: >-
  Comprehensive developer reference for uni-db, an embedded multi-model graph database
  with OpenCypher querying, Locy logic programming, vector/hybrid search, and Pydantic OGM.
  ALWAYS use this skill when: writing code that imports uni_db or uni_pydantic, writing
  Cypher queries for uni-db, writing Locy rules, configuring vector or hybrid search,
  defining uni-db schemas, using graph algorithms, or answering questions about uni-db
  architecture, APIs, or features. Trigger on mentions of: uni-db, uni, Cypher queries
  in graph DB context, Locy rules, vector search with graph data, Pydantic OGM for graphs,
  graph algorithms, ALONG/FOLD/BEST BY/DERIVE/ASSUME/ABDUCE, embedded graph database.
---

# uni-db Developer Skill

## What is uni-db?

uni-db is an **embedded, serverless multi-model graph database** (graph + vector + document + columnar) that runs inside your process with no server required. It supports **OpenCypher** queries with extensions for vector search, full-text search, DDL, and time travel. **Locy** is its Datalog-inspired logic programming language for recursive rules, probabilistic reasoning, abductive inference, and inline neural predicates with calibration (`CREATE MODEL` / `CALIBRATE` / `VALIDATE` / `NeuralProvenance` in `EXPLAIN`). APIs are available in **Python** (sync/async via PyO3) and **Rust** (async/blocking), with a **Pydantic OGM** layer. Built-in capabilities include 8 single-vector ANN index algorithms (Flat, IVF-Flat/SQ/PQ/RQ, HNSW-Flat/SQ/PQ) with scalar, product, and RaBitQ quantization plus MUVERA for multi-vector columns, 4 scalar index types (BTree, Hash, Bitmap, LabelList), BM25 full-text search, hybrid search with RRF fusion, and 42 graph algorithms.

---

## Architecture: Three Scopes

uni-db uses three scoping levels that separate lifecycle, reads, and writes:

```
Uni / AsyncUni (database handle)
  +-- Factory: open(), session(), schema()
  +-- Admin: flush(), snapshots, indexes, compaction
  +-- NO direct query or mutation

Session / AsyncSession (read scope)
  +-- Parameters: params().set(), params().get()
  +-- Reads: query(), locy()
  +-- Analysis: explain(), profile()
  +-- Factory: tx() -> Transaction

Transaction / AsyncTransaction (write scope)
  +-- Reads: query() (sees uncommitted writes)
  +-- Writes: execute(), bulk loading
  +-- Locy: locy() (DERIVE auto-applies mutations)
  +-- Lifecycle: commit(), rollback()
```

**Pattern**: `db = Uni.open(path)` -> `session = db.session()` -> `tx = session.tx()` -> `tx.execute(...)` -> `tx.commit()`

**Sync/async duality**: every type has an async counterpart (`Uni` / `AsyncUni`, `Session` / `AsyncSession`, `Transaction` / `AsyncTransaction`). Shared types (results, data classes, exceptions) are the same for both.

**Concurrent transactions**: transactions use private L0 buffers and execute concurrently. Commits serialize under the writer lock; default SSI/OCC validation rejects conflicting writes. Multiple Sessions can read concurrently.

**Facade accessors** on `Uni` / `AsyncUni`:

| Accessor | Returns | Purpose |
|---|---|---|
| `db.rules()` | `RuleRegistry` | Locy rule management (register, list, remove) |
| `db.compaction()` | `Compaction` / `AsyncCompaction` | Storage compaction |
| `db.indexes()` | `Indexes` / `AsyncIndexes` | Index management (list, rebuild) |
| `db.xervo()` | `Xervo` / `AsyncXervo` | ML model runtime (embed, generate, prefetch) |

**Key builder terminal methods:**

| Builder | Created by | Terminal |
|---|---|---|
| `SessionQueryBuilder` | `session.query_with(cypher)` | `.fetch_all()`, `.fetch_one()`, `.cursor()` |
| `SessionLocyBuilder` | `session.locy_with(program)` | `.run()` |
| `TxExecuteBuilder` | `tx.execute_with(cypher)` | `.run()`, `.profile()` |
| `TxQueryBuilder` | `tx.query_with(cypher)` | `.fetch_all()`, `.fetch_one()`, `.execute()` |
| `SchemaBuilder` | `db.schema()` | `.apply()` |
| `BulkWriterBuilder` | `tx.bulk_writer()` | `.build()` |
| `TransactionBuilder` | `session.tx_with()` | `.start()` |

---

## Quick Start: Python

```python
from uni_db import Uni, DataType

# Open or create a database
db = Uni.open("./my_db")

# Define schema
db.schema() \
    .label("Person") \
        .property("name", DataType.STRING()) \
        .property("age", DataType.INT64()) \
    .apply()

# Write via transaction
session = db.session()
with session.tx() as tx:
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
    tx.commit()

# Read via session
result = session.query("MATCH (p:Person) RETURN p.name, p.age")
for row in result:
    print(f"{row['p.name']}: {row['p.age']}")

db.shutdown()
```

---

## Quick Start: Rust

```rust
use uni_db::{Uni, DataType, Value};

#[tokio::main]
async fn main() -> uni_db::Result<()> {
    // Open (or create) a database
    let db = Uni::open("./my_db").build().await?;

    // Define schema
    db.schema()
        .label("Person")
            .property("name", DataType::String)
            .property("age", DataType::Int64)
        .apply().await?;

    // Write via transaction
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
    tx.commit().await?;

    // Read via session
    let result = session.query("MATCH (p:Person) RETURN p.name, p.age").await?;
    for row in result.rows() {
        println!("{}: {}", row.get::<String>("p.name")?, row.get::<i64>("p.age")?);
    }

    db.shutdown().await
}
```

**Python database factory methods** (Rust factories return a `UniBuilder`, finalized with `.build().await?`; Rust has no `Uni::builder()`):

| Method | Behavior |
|---|---|
| `Uni.open(path)` | Open existing or create new database at path |
| `Uni.create(path)` | Create new database; error if path exists |
| `Uni.open_existing(path)` | Open existing; error if path does not exist |
| `Uni.temporary()` | Temp directory, auto-cleaned on drop |
| `Uni.in_memory()` | Alias for `temporary()`; uses a temporary filesystem directory |
| `Uni.builder()` | Advanced configuration via `UniBuilder` |

**Storage backends**: local filesystem, S3 (`s3://bucket/path`), GCS (`gs://bucket/path`), Azure (`az://account/container/path`).

---

## Data Types Quick Reference

| Uni Type | Python Factory | Rust Enum | Cypher DDL |
|---|---|---|---|
| String | `DataType.STRING()` | `DataType::String` | `STRING` |
| Int32 | `DataType.INT32()` | `DataType::Int32` | `INT32` |
| Int64 | `DataType.INT64()` | `DataType::Int64` | `INT64` |
| Float32 | `DataType.FLOAT32()` | `DataType::Float32` | `FLOAT32` |
| Float64 | `DataType.FLOAT64()` | `DataType::Float64` | `FLOAT64` |
| Bool | `DataType.BOOL()` | `DataType::Bool` | `BOOL` |
| Timestamp | `DataType.TIMESTAMP()` | `DataType::Timestamp` | `TIMESTAMP` |
| Date | `DataType.DATE()` | `DataType::Date` | `DATE` |
| DateTime | `DataType.DATETIME()` | `DataType::DateTime` | `DATETIME` |
| Duration | `DataType.DURATION()` | `DataType::Duration` | `DURATION` |
| Btic | `DataType.BTIC()` | `DataType::Btic` | `BTIC` |
| Vector(N) | `DataType.vector(N)` | `DataType::Vector { dimensions: N }` | `VECTOR(N)` |
| List(T) | `DataType.list(inner)` | `DataType::List(Box<T>)` | `LIST(T)` |
| Map(K,V) | `DataType.map(k, v)` | `DataType::Map(Box<K>, Box<V>)` | `MAP(K, V)` |
| JSON | `DataType.JSON()` | `DataType::CypherValue` | `JSON` |
| Bytes | `DataType.BYTES()` | `DataType::Bytes` | `BYTES` |
| CRDT types | `DataType.crdt(CrdtType.G_COUNTER())` | `DataType::Crdt(CrdtKind::GCounter)` | `CRDT(GCOUNTER)` |

CRDT types: `GCounter`, `GSet`, `ORSet`, `LWWRegister`, `LWWMap`, `Rga`, `VectorClock`, `VCRegister`.

---

## Essential Patterns

### CRUD

```cypher
// Create node
CREATE (n:Person {name: 'Alice', age: 30})

// Create node with ext_id (for MERGE/lookup)
CREATE (n:Person {ext_id: 'user-123', name: 'Alice'})

// Create edge
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2023}]->(b)

// Read
MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age

// Update
MATCH (n:Person {name: 'Alice'}) SET n.age = 31, n.updated = datetime()

// Delete (must have no edges)
MATCH (n:Person {name: 'Alice'}) DELETE n

// Detach delete (removes edges first)
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n
```

### Parameters

```python
# Python — inline params
result = session.query(
    "MATCH (n:Person {name: $name}) RETURN n",
    params={"name": "Alice"}
)

# Python — session-level params
session.params().set("min_age", 25)
result = session.query("MATCH (p:Person) WHERE p.age > $min_age RETURN p")

# Python — builder pattern
result = session.query_with("MATCH (n:Person) WHERE n.age > $age") \
    .param("age", 25) \
    .timeout(5.0) \
    .fetch_all()
```

### Transactions

```python
# Context manager — auto-rollback on exception
with session.tx() as tx:
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
    tx.execute("CREATE (:Person {name: 'Bob', age: 25})")
    result = tx.commit()
    print(f"Committed {result.mutations_committed} mutations at version {result.version}")

# Async equivalent
async with await session.tx() as tx:
    await tx.execute("CREATE (:Person {name: 'Alice'})")
    await tx.commit()
```

### Bulk Loading

```python
with session.tx() as tx:
    with tx.bulk_writer().batch_size(5000).build() as writer:
        vids = writer.insert_vertices("Person", [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ])
        writer.insert_edges("KNOWS", [(vids[0], vids[1], {"since": 2024})])
        stats = writer.commit()
    tx.commit()
print(f"Inserted {stats.vertices_inserted} vertices, {stats.edges_inserted} edges")
```

### Schema Definition

```python
db.schema() \
    .label("Person") \
        .property("name", DataType.STRING()) \
        .property("age", DataType.INT64()) \
        .vector("embedding", 384) \
        .index("name", "btree") \
    .done().label("Company") \
        .property("name", DataType.STRING()) \
    .done().edge_type("WORKS_AT", ["Person"], ["Company"]) \
        .property("since", DataType.DATE()) \
    .apply()
```

### Session Parameters

```python
session = db.session()
session.params().set("company", "Acme")
result = session.query("MATCH (c:Company {name: $company}) RETURN c")
```

### Vector Search

```cypher
// Basic vector search (top-K scan)
CALL uni.vector.query('Document', 'embedding', $query_vector, 10)
YIELD node, score
RETURN node.title, score ORDER BY score DESC

// IVF-PQ is the DEFAULT index and stores lossy codes. Since 3.5 a refine pass is
// applied automatically (0.978 measured on SIFT-1M vs 0.562 without); pass
// refine_factor to tune it, or refine_factor: 1 to opt out. nprobes alone does
// not lift the quantization ceiling.
CALL uni.vector.query('Document', 'embedding', $query_vector, 10, NULL, NULL,
    { nprobes: 64, refine_factor: 20 })
YIELD node, score
RETURN node.title, score ORDER BY score DESC

// ~= operator (shorthand for vector top-K scan, desugars to uni.vector.query)
MATCH (d:Doc) WHERE d.embedding ~= $query_vector RETURN d.title LIMIT 10

// Inline per-row similarity scoring (no CALL/YIELD needed)
MATCH (d:Doc)
RETURN d.title, similar_to(d.embedding, $query_vector) AS score
ORDER BY score DESC

// Hybrid search: vector + FTS with RRF fusion (correct way)
MATCH (d:Doc)
RETURN d.title,
  similar_to([d.embedding, d.content], [$query_vector, $query_text]) AS score
ORDER BY score DESC

// Hybrid search via procedure
CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
    'graph databases', null, 10)
YIELD node, score, vector_score, fts_score
RETURN node.title, score
```

> **Note:** `=~` is regex match, `~=` is vector similarity — they are unrelated operators.

### Locy Quick Example

```python
result = session.locy("""
    CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.name = 'Alice'
        YIELD KEY b

    CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a IS reachable
        YIELD KEY b

    QUERY reachable
""")
for row in result["reachable"]:
    print(row)
```

---

## Cypher Cheat Sheet

**1. Match by property:**
```cypher
MATCH (n:Person {name: 'Alice'}) RETURN n
```

**2. Match by ext_id:**
```cypher
MATCH (n:Person {ext_id: 'user-123'}) RETURN n
```

**3. Create node with properties:**
```cypher
CREATE (n:Person {ext_id: 'user-456', name: 'Bob', age: 25}) RETURN n
```

**4. Create edge:**
```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2023}]->(b)
```

**5. MERGE (upsert) -- match by pattern properties:**
```cypher
MERGE (n:Person {ext_id: 'user-123'})
ON CREATE SET n.name = 'Alice', n.created = datetime()
ON MATCH SET n.last_seen = datetime()
RETURN n
```

**6. Variable-length path:** (bounded hops, endpoints not `p` -- see gotcha 10)
```cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = 'Alice'
RETURN DISTINCT b.name
```

**7. Aggregation with WITH:**
```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WITH c.name AS company, count(p) AS employees
WHERE employees > 10
RETURN company, employees ORDER BY employees DESC
```

**8. UNWIND list:**
```cypher
UNWIND $names AS name
MATCH (n:Person {name: name})
RETURN n
```

**9. OPTIONAL MATCH:**
```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[r:MANAGES]->(m:Person)
RETURN p.name, collect(m.name) AS manages
```

**10. RETURN with ORDER BY, LIMIT, SKIP:**
```cypher
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age DESC
SKIP 10 LIMIT 20
```

**11. System-managed timestamps (`created_at` / `updated_at`):**
Every vertex and edge automatically carries a creation and modification timestamp. Access via Cypher functions, returns `DateTime` (UTC, ns). Read-only, no schema declaration needed.
```cypher
MATCH (n:Person) WHERE created_at(n) > datetime("2026-05-01")
RETURN n, updated_at(n) AS last_modified

MATCH (a)-[r:KNOWS]->(b) RETURN r, created_at(r), updated_at(r)
```
`updated_at` bumps on any write that touches the row — including idempotent MERGE / same-value SET.

---

## Critical Gotchas

1. **MERGE matches pattern properties** -- `MERGE (n:Person {name: $name})` works without `ext_id`. An optional `ext_id` is a globally unique external key; choose stable keys and index them for repeated upserts.

2. **CREATE creates NEW nodes per expression** -- `CREATE (a:Node), (b:Node)` creates two separate nodes. To reference the same node later in a pattern, use variable binding: `CREATE (a:Node {name: 'X'}), (a)-[:REL]->(b:Other)`.

3. **Handle transaction conflicts** -- Concurrent transactions are allowed. Default SSI/OCC validates at commit; retry the whole transaction on a retryable conflict.

4. **Commit durability and L1 flush differ** -- With `wal_enabled: true` (default), commit persists the WAL before publishing mutations. `flush()` moves committed L0 data into Lance L1; defaults trigger it at 10,000 mutations or after 5 seconds with pending mutations. Disabling WAL removes crash recovery for unflushed writes.

5. **VID vs UniId vs ext_id** -- VID is internal (u64 auto-increment), UniId is content-hash (SHA3-256), ext_id is user-supplied string. Use `ext_id` for MERGE and user-facing lookups.

6. **Schema-first for columnar performance** -- Define labels and properties via `db.schema()` before bulk loading. Without schema, properties go to JSONB overflow and lose columnar benefits. Use `strict_schema: true` in `UniConfig` to reject writes with undeclared labels/edge types.

7. **DETACH DELETE required when node has edges** -- `DELETE` alone fails if the node has any edges. Use `DETACH DELETE` to remove edges first.

8. **Vector index metric must match embedding model** -- Use `cosine` for normalized embeddings (most models), `l2` for raw/unnormalized embeddings. Mismatched metric produces poor search results.

9. **Locy rules are NOT standard Datalog** -- Locy has `ALONG`, `FOLD`, `BEST BY`, `PROB`, `DERIVE`, `ASSUME`, `ABDUCE` which do not exist in standard Datalog. IS/IS NOT references invoke other rules. A recursive `FOLD` rolls up **per KEY, one level at a time** -- a self-reference reads the target's folded value, not the derivations behind it. Use `ALONG` for per-path accumulation.

10. **Variable-length paths: the cost is *path expansion*, not the search** -- A `[*1..n]` pattern searches the graph, then expands that search into individual paths **only if the query binds a path variable**. On a cyclic graph the number of distinct paths grows combinatorially with the hop bound while the search does not, so `RETURN DISTINCT b` is cheap where `RETURN p` is not. Prefer endpoints; use `shortestPath` when one path will do; a `LIMIT` stops the expansion (but the search still runs in full, and the limit applies one 8192-row batch at a time, so any limit up to 8192 costs the same). Two traps: `[*]` writes no upper bound, so **the planner supplies 100 hops**, and truncating there warns that results are incomplete -- write `[*..5]` when you know the depth; and an unbounded `[*]` binding a path with no `LIMIT` is unbounded work that will hit `query_timeout` or `max_query_memory`.

11. **Always use $param parameters** -- String concatenation in Cypher causes injection risk and prevents plan caching.

12. **Cartesian products from disconnected patterns** -- `MATCH (a:Person), (b:Company)` creates a cross product of all persons and companies. Connect patterns or use WITH to pipeline results.

13. **BulkWriter for initial data loading** -- `tx.bulk_writer()` bypasses WAL and can defer index rebuilds. Already-flushed batches cannot be rolled back; use ordinary transactions when atomic rollback is required. Measure throughput for your workload.

14. **Context managers for transactions** -- Always use `with session.tx() as tx:` (or `async with`) to guarantee auto-rollback on exceptions. Explicitly commit successful work; leaving the context without committing rolls it back.

15. **Locy DERIVE in a transaction** -- When `locy()` is called on a Transaction, DERIVE commands automatically apply mutations to the transaction. On a Session (read-only), DERIVE returns a `DerivedFactSet` that must be explicitly applied via `tx.apply(derived)`.

16. **Index your MERGE keys for batched upserts** -- A single-node, single-label `MERGE (n:Label {key: ...})` with a literal `{...}` key map takes a batched fast path (one L0 snapshot per statement, no per-row query planning) -- this is what makes `UNWIND $rows AS e MERGE (n:Label {key: e.key}) ...` fast. For the per-row match to be an index point-lookup rather than a filtered label scan, put a **scalar index** on the key (`db.schema().label("Label").index("key", IndexType::Scalar(ScalarType::Hash))` or `CREATE INDEX`); without one the lookup is a full label scan (fine in-memory, O(N x label_size) for large on-disk labels). **Multi-node/edge MERGE** (e.g. `MERGE (a)-[:R]->(b)`) and **non-literal property maps** (`MERGE (n:Label $props)`) fall back to the slower per-row general path -- prefer a single-node MERGE for the node, then batched `CREATE`/`MATCH` for edges.

---

## Tuning for Ingest-Heavy Workloads

Per-read latency in uni-db is sensitive to how many *frozen overlay segments* have accumulated since the last CSR compaction. Each L0 → L1 flush adds one frozen segment; subsequent reads consult main CSR + every frozen segment + the active overlay. The defaults are tuned for mixed read/write traffic, but ingest-heavy benchmarks (embedding pipelines, bulk imports interleaved with reads) can accumulate segments faster than the default compaction threshold catches up.

Symptoms: read latency that is flat early in the run and then steps up to several milliseconds and stays there. See issue #55 for the reference investigation.

Levers (`UniConfig`):

```rust
let config = UniConfig {
    // Suppress the 5-second timer-based flush; rely only on the count
    // threshold (default 10,000 mutations). Useful for benchmark runs
    // where you want fewer L1 checkpoints (WAL durability is separate).
    auto_flush_interval: None,

    // OR: keep the timer but require more mutations before it fires.
    // Default 1 — single inserts wake the timer. Raise to coalesce
    // small bursts. Higher values reduce flush frequency, which can
    // hurt read latency if not paired with aggressive compaction below.
    auto_flush_min_mutations: 100,

    // Bigger flushes amortize the per-flush cost; fewer frozen segments.
    auto_flush_threshold: 50_000,

    compaction: CompactionConfig {
        // Compact frozen segments back into Main CSR sooner. Default 2
        // (lowered from 4 in the issue #55 fix). Set to 1 to compact
        // after every flush — minimal read latency, max compaction CPU.
        frozen_segments_compact_threshold: 1,
        ..CompactionConfig::default()
    },
    ..UniConfig::default()
};
```

Diagnostic: if `MATCH (a)-[r:LINK]->(b) WHERE id(a)=$nid` for a small constant out-degree gets noticeably slower over a long write-heavy run, this is the regime.

### Allocator: use mimalloc for mutation-heavy workloads

For workloads that run many small mutations (per-statement Cypher `CREATE`/`MERGE`, concurrent writers, mutation streams), the **default glibc allocator becomes the dominant bottleneck**. At 24-session concurrency, profile showed ~50% of CPU time in `__memset` (zeroing fresh heap pages), kernel `clear_page_erms`, and glibc arena locks.

**Python users:** every PyO3 wheel (`uni-db`, `uni-db-cuda`, `uni-db-metal`, `uni-db-onnx`, etc.) **ships with mimalloc built in** as the Rust-side global allocator. No configuration needed.

**Rust library consumers:** opt in via the `mimalloc` feature flag:

```toml
[dependencies]
uni-db = { version = "...", features = ["mimalloc"] }
```

```rust
// in your binary's main.rs:
#[global_allocator]
static GLOBAL: uni_db::MiMalloc = uni_db::MiMalloc;
```

**CLI users:** the `uni` binary already uses mimalloc by default.

Measured win on `concurrent_mutations` benchmark (24 sessions × 100 `CREATE` each):

| N sessions | glibc | mimalloc | speedup |
|---|---|---|---|
| 1 | 139 ms | 45 ms | 3.08× |
| 24 | 984 ms | 394 ms | 2.50× |

The 3× win at sess=1 reveals glibc was bloated even single-threaded for the per-statement parse + plan + DataFusion churn. mimalloc's thread-local arenas avoid the page-fault traffic. CPython's `PyMem_*` allocator is untouched in Python wheels — only Rust allocations route through mimalloc.

**When to recommend it:** any code that runs `tx.execute("CREATE ...")` in a loop, multi-session writers, ingest pipelines, or benchmarks. Skip if the consumer has a strong reason to pick a different allocator (custom allocators for embedded use, etc.).

---

## When to Load References

When the SKILL.md overview is insufficient for the user's task, load the appropriate reference file for detailed API signatures, examples, and patterns.

| User's task | Load reference |
|---|---|
| Writing/debugging Cypher queries, WHERE clauses, pattern matching | `references/cypher.md` |
| Python API usage, async patterns, builders, result types | `references/python-api.md` |
| Rust API usage, builders, error handling, blocking API | `references/rust-api.md` |
| Pydantic models, OGM, QueryBuilder (incl. `vector_search`/`sparse_search`/`hybrid_search` + `.search_scores`), relationships | `references/pydantic-ogm.md` |
| Vector search, sparse vectors, FTS, 3-way hybrid search, similar_to, embeddings | `references/vector-hybrid-search.md` |
| Locy rules, recursive logic, ALONG/FOLD/DERIVE/ASSUME/ABDUCE | `references/locy.md` |
| Locy neural predicates: CREATE MODEL/FEATURES/CALIBRATE/VALIDATE/NeuralProvenance EXPLAIN | `references/neural-predicates.md` |
| Schema design, data types, indexes, identity (ext_id/VID) | `references/schema-indexing.md` |
| BTIC temporal intervals, Allen algebra, certainty/granularity | `references/btic.md` |
| Xervo ML runtime, providers, model catalog, auto-embedding | `references/xervo.md` |
| Durable or nested forks, diff/promote, fork-local schema and indexes | `references/forks.md` |
| Graph algorithms (PageRank, WCC, shortest path, etc.); guest-authorable GraphCompute kernels | `references/graph-algorithms.md` |

Load **multiple references** when a task spans domains. Examples:
- RAG pipeline: `references/vector-hybrid-search.md` + `references/python-api.md`
- Locy with vector similarity: `references/locy.md` + `references/vector-hybrid-search.md`
- Schema + bulk loading: `references/schema-indexing.md` + `references/python-api.md`
- Rust graph algorithms: `references/graph-algorithms.md` + `references/rust-api.md`
- BTIC temporal queries in Python: `references/btic.md` + `references/python-api.md`
- RAG with Xervo embeddings: `references/xervo.md` + `references/vector-hybrid-search.md`
- BGE-M3 multi-head hybrid (dense + sparse + ColBERT): `references/xervo.md` + `references/vector-hybrid-search.md`
