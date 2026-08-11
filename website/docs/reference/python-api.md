# Python API Reference

Complete reference for the `uni_db` Python bindings (v3.4.0). All operations go through the **Session** (reads, Locy) or **Transaction** (writes, bulk loading) obtained from a `Uni` instance.

## Installation

```bash
pip install uni-db
```

Or build from source:

```bash
cd bindings/uni-db
pip install maturin
maturin develop --release
```

## Quick Start (Sync)

```python
import uni_db

# Open or create a database
db = uni_db.Uni.open("/path/to/db")

# Define schema
db.schema() \
  .label("Person").property("name", "string").property("age", "int").done() \
  .edge_type("KNOWS", ["Person"], ["Person"]).done() \
  .apply()

# Create a session for reads
session = db.session()

# Write data in a transaction
with session.tx() as tx:
    tx.execute("CREATE (a:Person {name: 'Alice', age: 30})")
    tx.execute("CREATE (b:Person {name: 'Bob', age: 25})")
    tx.execute("""
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS {since: 2020}]->(b)
    """)
    tx.commit()

# Query through the session
result = session.query("MATCH (n:Person) WHERE n.age > 20 RETURN n.name AS name, n.age AS age")
for row in result:
    print(row["name"], row["age"])

# Parameterized query
result = session.query(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    {"name": "Alice"}
)
```

## Async Quick Start

```python
import uni_db
import asyncio

async def main():
    async with uni_db.AsyncUni.open("/path/to/db") as db:
        session = db.session()

        # Reads work on the session
        result = await session.query("MATCH (n:Person) RETURN n.name AS name")
        for row in result:
            print(row["name"])

        # Writes go through a transaction
        tx = await session.tx()
        await tx.execute("CREATE (:Person {name: 'Carol', age: 28})")
        await tx.commit()

asyncio.run(main())
```

---

## Core Classes

### Uni

The main synchronous entry point. Exposed as `uni_db.Uni` in Python.

#### Factory Methods (static)

| Method | Description |
|--------|-------------|
| `Uni.open(path)` | Open or create a database at the given path |
| `Uni.create(path)` | Create a new database (fails if it already exists) |
| `Uni.open_existing(path)` | Open an existing database (fails if it does not exist) |
| `Uni.temporary()` | Create an ephemeral in-memory database |
| `Uni.in_memory()` | Alias for `temporary()` |
| `Uni.builder()` | Return a `UniBuilder` for advanced configuration |

```python
# Simplest usage
db = uni_db.Uni.open("./my_graph")

# In-memory for tests
db = uni_db.Uni.temporary()

# Context manager (flushes on exit)
with uni_db.Uni.open("./my_graph") as db:
    session = db.session()
    # ...
```

#### Instance Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `db.session()` | `Session` | Create a new query session |
| `db.session_template()` | `SessionTemplateBuilder` | Create a reusable session factory |
| `db.schema()` | `SchemaBuilder` | Start a fluent schema definition |
| `db.rules()` | `RuleRegistry` | Access the database-level Locy rule registry |
| `db.xervo()` | `Xervo` | Access embedding and generation operations |
| `db.indexes()` | `Indexes` | Access index management |
| `db.compaction()` | `Compaction` | Access compaction operations |
| `db.metrics()` | `DatabaseMetrics` | Database-wide metrics snapshot |
| `db.config()` | `dict` | Current database configuration |
| `db.flush()` | `None` | Flush uncommitted changes to persistent storage |
| `db.shutdown()` | `None` | Flush and prepare for shutdown |
| `db.label_exists(name)` | `bool` | Check if a label exists |
| `db.edge_type_exists(name)` | `bool` | Check if an edge type exists |
| `db.list_labels()` | `list[str]` | List all label names |
| `db.list_edge_types()` | `list[str]` | List all edge type names |
| `db.get_label_info(name)` | `LabelInfo \| None` | Detailed label metadata |
| `db.get_edge_type_info(name)` | `EdgeTypeInfo \| None` | Detailed edge type metadata |
| `db.load_schema(path)` | `None` | Load schema from a JSON file |
| `db.save_schema(path)` | `None` | Save schema to a JSON file |
| `db.create_snapshot(name)` | `str` | Create a named snapshot, returns snapshot ID |
| `db.list_snapshots()` | `list[SnapshotInfo]` | List all snapshots |
| `db.restore_snapshot(snapshot_id)` | `None` | Restore database to a snapshot |
| `db.write_lease()` | `WriteLease \| None` | Current write lease configuration |

The `count` returned by `db.get_label_info()` / `db.get_edge_type_info()` is
obtained with a Cypher `count(...)`, so it includes rows still buffered in L0
and excludes tombstoned
or superseded versions. The element name is backtick-quoted, so names containing
punctuation (such as `.`), leading digits, or non-ASCII characters count
correctly; a name containing a backtick cannot be quoted and raises
`UniQueryError`. Counting failures raise rather than being reported as
`count == 0`.

---

### UniBuilder

Fluent builder for advanced database configuration. Exposed as `uni_db.UniBuilder`.

#### Factory Methods (static)

| Method | Description |
|--------|-------------|
| `UniBuilder.open(path)` | Open or create |
| `UniBuilder.create(path)` | Create new (fail if exists) |
| `UniBuilder.open_existing(path)` | Open existing (fail if missing) |
| `UniBuilder.temporary()` | Ephemeral in-memory |
| `UniBuilder.in_memory()` | Alias for `temporary()` |

#### Configuration Methods (chainable)

| Method | Description |
|--------|-------------|
| `.cache_size(bytes)` | Maximum cache size in bytes |
| `.parallelism(n)` | Number of worker threads |
| `.schema_file(path)` | Load schema from JSON on init |
| `.xervo_catalog_from_file(path)` | Configure Xervo models from a JSON file |
| `.xervo_catalog_from_str(json)` | Configure Xervo models from a JSON string |
| `.xervo_runtime(runtime)` | Reuse an existing `ModelRuntime` instead of building one ([sharing models](#sharing-models-across-databases)) |
| `.cloud_config(config_dict)` | Cloud storage credentials (`s3`, `gcs`, `azure`) |
| `.config(config_dict)` | Database options (`query_timeout`, `max_query_memory`, etc.) |
| `.batch_size(n)` | I/O batch size (default 1024) |
| `.wal_enabled(bool)` | Enable/disable write-ahead log (default `True`) |
| `.strict_schema(bool)` | Reject writes with undeclared labels/edge types (default `False`) |
| `.read_only()` | Open in read-only mode |
| `.write_lease(lease)` | Multi-agent write coordination |
| `.hybrid(local_path, remote_url)` | Hybrid storage (local metadata + remote data) |
| `.build()` | Build and return the `Uni` instance |

```python
db = (
    uni_db.UniBuilder.open("/path/to/db")
    .cache_size(1024 * 1024 * 100)   # 100 MB cache
    .parallelism(4)
    .wal_enabled(True)
    .build()
)
```

---

### Session

The primary scope for reads and the factory for transactions. Created via `db.session()`.

Sessions are lightweight and can be created freely. Each session maintains its own plan cache, parameter store, rule registry, and version pin state.

`session.query()` is **read-only** — mutation clauses (CREATE, SET, DELETE, MERGE, REMOVE) return an error. Use `session.tx()` for writes.

```python
session = db.session()
```

#### Query Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `session.query(cypher, params=None)` | `QueryResult` | Execute a read-only query. Rejects mutations with an error. |
| `session.query_with(cypher)` | `SessionQueryBuilder` | Fluent read query builder |

```python
# Simple query
result = session.query("MATCH (n:Person) RETURN n.name AS name")

# Parameterized
result = session.query(
    "MATCH (n:Person) WHERE n.age > $min RETURN n",
    {"min": 21}
)

# Builder pattern with timeout
result = (
    session.query_with("MATCH (n:Person) WHERE n.age > $min RETURN n")
    .param("min", 21)
    .timeout(5.0)
    .max_memory(50_000_000)
    .fetch_all()
)
```

#### Locy Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `session.locy(program, params=None)` | `LocyResult` | Evaluate a Locy program |
| `session.locy_with(program)` | `SessionLocyBuilder` | Fluent Locy builder |
| `session.compile_locy(program)` | `CompiledProgram` | Compile without executing |

#### Transaction Factory

| Method | Returns | Description |
|--------|---------|-------------|
| `session.tx()` | `Transaction` | Create a new transaction |
| `session.tx_with()` | `TransactionBuilder` | Transaction with options (timeout, isolation) |

```python
# Default transaction
tx = session.tx()

# Configured transaction
tx = session.tx_with().timeout(30.0).isolation("serialized").start()
```

#### Prepared Statements

| Method | Returns | Description |
|--------|---------|-------------|
| `session.prepare(cypher)` | `PreparedQuery` | Prepare a Cypher query |
| `session.prepare_locy(program)` | `PreparedLocy` | Prepare a Locy program |

#### Registry and Parameters

| Method | Returns | Description |
|--------|---------|-------------|
| `session.rules()` | `RuleRegistry` | Session-scoped rule registry |
| `session.params()` | `Params` | Session-scoped parameter store |

#### Version Pinning

| Method | Description |
|--------|-------------|
| `session.pin_to_version(snapshot_id)` | Pin to a specific snapshot version |
| `session.pin_to_timestamp(epoch_secs)` | Pin to a specific timestamp |
| `session.refresh()` | Unpin and refresh to latest version |
| `session.is_pinned()` | Check if session is pinned |

#### Notifications

| Method | Returns | Description |
|--------|---------|-------------|
| `session.watch()` | `CommitStream` | Subscribe to commit notifications |
| `session.watch_with()` | `WatchBuilder` | Filtered commit notifications |

```python
# Watch all commits
with session.watch() as stream:
    for notification in stream:
        print(f"Version {notification.version}: {notification.labels_affected}")
        break  # just one

# Watch specific labels
stream = session.watch_with().labels(["Person"]).build()
```

#### Hooks

| Method | Description |
|--------|-------------|
| `session.add_hook(name, hook_obj)` | Register a named session hook |
| `session.remove_hook(name)` | Remove a hook by name (returns `bool`) |
| `session.list_hooks()` | List all registered hook names |
| `session.clear_hooks()` | Remove all hooks |

Hooks can implement `before_query(ctx)`, `after_query(ctx, metrics)`, `before_commit(ctx)`, and `after_commit(ctx, result)`.

```python
class AuditHook:
    def before_query(self, ctx):
        print(f"Query: {ctx['query_text']}")

    def after_commit(self, ctx, result):
        print(f"Committed version {result.version}")

session.add_hook("audit", AuditHook())
```

#### Metrics and Introspection

| Method | Returns | Description |
|--------|---------|-------------|
| `session.metrics()` | `SessionMetrics` | Session lifetime metrics |
| `session.capabilities()` | `SessionCapabilities` | Session capabilities snapshot |
| `session.id` | `str` | Session identifier |
| `session.cancellation_token()` | `CancellationToken` | Cooperative cancellation token |
| `session.cancel()` | `None` | Cancel in-progress operations |

Context manager support cancels in-progress operations on exit:

```python
with db.session() as session:
    result = session.query("MATCH (n) RETURN n LIMIT 10")
```

---

### Transaction

ACID transactions for atomic writes. Created via `session.tx()`. Supports context manager protocol (auto-rollback on exception).

```python
session = db.session()
with session.tx() as tx:
    tx.execute("CREATE (n:Person {name: 'Alice', age: 30})")
    tx.execute("CREATE (n:Person {name: 'Bob', age: 25})")
    result = tx.query("MATCH (n:Person) RETURN count(n) AS cnt")
    tx.commit()
# If an exception occurs before commit(), the transaction is rolled back automatically.
```

#### Read Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `tx.query(cypher, params=None)` | `QueryResult` | Read query within the transaction |
| `tx.query_with(cypher)` | `TxQueryBuilder` | Fluent query builder |

#### Write Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `tx.execute(cypher, params=None)` | `ExecuteResult` | Execute a mutation |
| `tx.execute_with(cypher)` | `TxExecuteBuilder` | Fluent mutation builder |

#### Locy Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `tx.locy(program, params=None)` | `LocyResult` | Evaluate Locy within the transaction |
| `tx.locy_with(program)` | `TxLocyBuilder` | Fluent Locy builder |
| `tx.apply(derived_fact_set)` | `ApplyResult` | Materialize a `DerivedFactSet` |
| `tx.apply_with(derived_fact_set)` | `ApplyBuilder` | Apply with options (`require_fresh`, `max_version_gap`) |

#### Prepared Statements

| Method | Returns | Description |
|--------|---------|-------------|
| `tx.prepare(cypher)` | `PreparedQuery` | Prepare a Cypher query |
| `tx.prepare_locy(program)` | `PreparedLocy` | Prepare a Locy program |

#### Bulk Loading

| Method | Returns | Description |
|--------|---------|-------------|
| `tx.bulk_writer()` | `TxBulkWriterBuilder` | High-throughput bulk data loader |
| `tx.appender(label)` | `StreamingAppender` | Streaming single-label appender |
| `tx.appender_builder(label)` | `TxAppenderBuilder` | Configurable appender builder |

#### Lifecycle

| Method | Returns | Description |
|--------|---------|-------------|
| `tx.commit()` | `CommitResult` | Commit the transaction |
| `tx.rollback()` | `None` | Rollback the transaction |
| `tx.cancel()` | `None` | Cancel in-progress operations |
| `tx.rules()` | `RuleRegistry` | Transaction-scoped rule registry |
| `tx.id()` | `str` | Transaction identifier |
| `tx.started_at_version()` | `int` | Database version when started |
| `tx.is_dirty()` | `bool` | Whether the transaction has uncommitted changes |
| `tx.is_completed()` | `bool` | Whether committed or rolled back |
| `tx.cancellation_token()` | `CancellationToken` | Cooperative cancellation token |

---

### Builder Classes

#### SessionQueryBuilder

Returned by `session.query_with(cypher)`. Supports chaining.

| Method | Returns | Description |
|--------|---------|-------------|
| `.param(name, value)` | self | Bind a parameter |
| `.params(dict)` | self | Bind multiple parameters |
| `.timeout(seconds)` | self | Set query timeout |
| `.max_memory(bytes)` | self | Set memory limit |
| `.cancellation_token(token)` | self | Attach cancellation token |
| `.fetch_all()` | `QueryResult` | Execute and return all rows |
| `.fetch_one()` | `dict \| None` | Execute and return first row |
| `.cursor()` | `QueryCursor` | Open a streaming cursor |
| `.explain()` | `ExplainOutput` | Explain without executing |
| `.profile()` | `(QueryResult, ProfileOutput)` | Execute with profiling |

#### SessionLocyBuilder

Returned by `session.locy_with(program)`. Supports chaining.

| Method | Returns | Description |
|--------|---------|-------------|
| `.param(name, value)` | self | Bind a parameter |
| `.params(dict)` | self | Bind multiple parameters |
| `.timeout(seconds)` | self | Set evaluation timeout |
| `.max_iterations(n)` | self | Set fixpoint iteration limit |
| `.with_config(config)` | self | Apply a `LocyConfig` or config dict |
| `.cancellation_token(token)` | self | Attach cancellation token |
| `.run()` | `LocyResult` | Execute the evaluation |
| `.explain()` | `LocyExplainOutput` | Explain without executing |

Cancellation is enforced against the whole evaluation, not just the Cypher
statements the program dispatches, so a long-running fixpoint is interruptible.
A cancelled evaluation raises `UniCancelledError`. `session.cancel()` applies in
addition to any token passed to `.cancellation_token(token)`.

#### TransactionBuilder

Returned by `session.tx_with()`.

| Method | Returns | Description |
|--------|---------|-------------|
| `.timeout(seconds)` | self | Set transaction timeout |
| `.isolation(level)` | self | Set isolation level (currently `"serialized"`) |
| `.start()` | `Transaction` | Start the transaction |

#### TxQueryBuilder

Returned by `tx.query_with(cypher)`.

| Method | Returns | Description |
|--------|---------|-------------|
| `.param(name, value)` | self | Bind a parameter |
| `.timeout(seconds)` | self | Set query timeout |
| `.fetch_all()` | `QueryResult` | Execute and return all rows |
| `.fetch_one()` | `dict \| None` | Execute and return first row |
| `.execute()` | `ExecuteResult` | Execute as a mutation |
| `.cursor()` | `QueryCursor` | Open a streaming cursor |

#### TxExecuteBuilder

Returned by `tx.execute_with(cypher)`.

| Method | Returns | Description |
|--------|---------|-------------|
| `.param(name, value)` | self | Bind a parameter |
| `.timeout(seconds)` | self | Set execution timeout |
| `.run()` | `ExecuteResult` | Execute the mutation |
| `.profile()` | `(ExecuteResult, ProfileOutput)` | Execute the mutation with profiling |

```python
# Profile a transaction write end-to-end
with db.session().tx() as tx:
    res, prof = (
        tx.execute_with("CREATE (p:Person {name: $name}) RETURN p")
        .param("name", "Alice")
        .profile()
    )
    tx.commit()
    print(f"nodes_created={res.nodes_created}, total_time_ms={prof.total_time_ms}")
```

The async equivalent (`AsyncTxExecuteBuilder.profile()`) returns an awaitable that resolves to the same `(ExecuteResult, ProfileOutput)` tuple.

#### TxLocyBuilder

Returned by `tx.locy_with(program)`.

| Method | Returns | Description |
|--------|---------|-------------|
| `.param(name, value)` | self | Bind a parameter |
| `.timeout(seconds)` | self | Set evaluation timeout |
| `.max_iterations(n)` | self | Set fixpoint iteration limit |
| `.with_config(config)` | self | Apply a `LocyConfig` or config dict |
| `.cancellation_token(token)` | self | Attach cancellation token |
| `.run()` | `LocyResult` | Execute the evaluation |

As with `SessionLocyBuilder`, cancellation races the whole evaluation and raises
`UniCancelledError`. `tx.cancel()` applies in addition to any token passed to
`.cancellation_token(token)`, and also reaches `tx.apply(...)`.

#### ApplyBuilder

Returned by `tx.apply_with(derived_fact_set)`.

| Method | Returns | Description |
|--------|---------|-------------|
| `.require_fresh(bool)` | self | Fail if version gap is non-zero |
| `.max_version_gap(n)` | self | Maximum allowed version gap |
| `.run()` | `ApplyResult` | Execute the apply |

---

## Schema Management

### SchemaBuilder

Returned by `db.schema()`. Accumulates label and edge type definitions, then applies them atomically.

```python
db.schema() \
  .label("Person") \
      .property("name", "string") \
      .property("age", "int") \
      .property_nullable("email", "string") \
      .vector("embedding", 128) \
      .index("name", "btree") \
      .done() \
  .label("Company") \
      .property("name", "string") \
      .done() \
  .edge_type("WORKS_AT", ["Person"], ["Company"]) \
      .property("since", "int") \
      .done() \
  .apply()
```

| Method | Returns | Description |
|--------|---------|-------------|
| `.label(name)` | `LabelBuilder` | Start defining a label |
| `.edge_type(name, from_labels, to_labels)` | `EdgeTypeBuilder` | Start defining an edge type |
| `.current()` | `dict` | Get the current schema as a dictionary |
| `.current_typed()` | `Schema` | Get the current schema as a typed object |
| `.apply()` | `None` | Apply all pending schema changes |

### LabelBuilder

Returned by `schema.label(name)`.

| Method | Returns | Description |
|--------|---------|-------------|
| `.property(name, data_type)` | self | Add a required property |
| `.property_nullable(name, data_type)` | self | Add a nullable property |
| `.vector(name, dimensions)` | self | Add a vector property |
| `.index(property, index_type)` | self | Add an index on a property |
| `.done()` | `SchemaBuilder` | Return to parent builder |
| `.apply()` | `None` | Apply schema changes immediately |

The `index_type` parameter can be a string (`"btree"`, `"vector"`, `"fulltext"`, `"inverted"`) or a dict with detailed configuration:

```python
# Simple index
builder.index("name", "btree")

# Vector index (default algorithm is ivf_pq when "algorithm" is omitted)
builder.index("embedding", {
    "type": "vector",
    "algorithm": "ivf_pq",
    "partitions": 256,
    "sub_vectors": 16,
    "metric": "cosine"
})

# HNSW configuration
builder.index("embedding", {
    "type": "vector",
    "algorithm": "hnsw",
    "m": 32,
    "ef_construction": 400,
    "metric": "cosine"
})

# MUVERA index on a multi-vector ("list:vector:N") property (ColBERT / MaxSim)
builder.index("tokens", {
    "type": "vector",
    "algorithm": "muvera",
    "k_sim": 4,
    "reps": 20,
    "d_proj": 16,
    "inner": "ivf_pq"
})

# Full-text index with n-gram tokenizer
builder.index("content", {
    "type": "fulltext",
    "tokenizer": "ngram",
    "ngram_min": 2,
    "ngram_max": 4
})
```

### EdgeTypeBuilder

Returned by `schema.edge_type(name, from_labels, to_labels)`.

| Method | Returns | Description |
|--------|---------|-------------|
| `.property(name, data_type)` | self | Add a required property |
| `.property_nullable(name, data_type)` | self | Add a nullable property |
| `.done()` | `SchemaBuilder` | Return to parent builder |
| `.apply()` | `None` | Apply schema changes immediately |

---

## Bulk Loading

Bulk loading APIs are accessed through a **Transaction**.

### BulkWriter

High-throughput bulk data ingestion. Created via `tx.bulk_writer().build()`.

```python
session = db.session()
with session.tx() as tx:
    writer = tx.bulk_writer().batch_size(10000).build()

    # Insert vertices — returns list of allocated VIDs
    vids = writer.insert_vertices("Person", [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ])

    # Insert edges — (source_vid, target_vid, properties)
    writer.insert_edges("KNOWS", [
        (vids[0], vids[1], {"since": 2020}),
    ])

    # Commit bulk data and rebuild indexes
    stats = writer.commit()
    print(f"Inserted {stats.vertices_inserted} vertices, {stats.edges_inserted} edges")

    tx.commit()
```

#### TxBulkWriterBuilder

Returned by `tx.bulk_writer()`. All methods are chainable.

| Method | Description |
|--------|-------------|
| `.batch_size(n)` | Set batch size for writes |
| `.defer_vector_indexes(bool)` | Defer vector index rebuilds (default `True`) |
| `.defer_scalar_indexes(bool)` | Defer scalar index rebuilds (default `True`) |
| `.async_indexes(bool)` | Build indexes asynchronously |
| `.validate_constraints(bool)` | Enable/disable constraint validation |
| `.max_buffer_size_bytes(n)` | Maximum in-memory buffer size |
| `.on_progress(callback)` | Register a progress callback (receives `BulkProgress`) |
| `.build()` | Build the `BulkWriter` |

#### BulkWriter Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `.insert_vertices(label, props_list)` | `list[int]` | Insert vertices, returns VIDs |
| `.insert_edges(edge_type, edges)` | `None` | Insert edges as `(src, dst, props)` tuples |
| `.stats()` | `BulkStats` | Current load statistics |
| `.touched_labels()` | `list[str]` | Labels written to |
| `.touched_edge_types()` | `list[str]` | Edge types written to |
| `.commit()` | `BulkStats` | Commit data and rebuild indexes |
| `.abort()` | `None` | Discard uncommitted changes |

Context manager support (auto-aborts on exception):

```python
with tx.bulk_writer().build() as writer:
    writer.insert_vertices("Person", [...])
    writer.commit()
```

### StreamingAppender

Single-label streaming appender for incremental loading. Created via `tx.appender(label)`.

```python
with session.tx() as tx:
    appender = tx.appender("Person")
    for record in data_source:
        appender.append({"name": record["name"], "age": record["age"]})
    stats = appender.finish()
    tx.commit()
```

| Method | Description |
|--------|-------------|
| `.append(properties)` | Append a single row |
| `.finish()` | Flush remaining rows (returns `BulkStats`) |

#### TxAppenderBuilder

Returned by `tx.appender_builder(label)`. Configurable variant of `tx.appender()`.

| Method | Description |
|--------|-------------|
| `.batch_size(n)` | Set batch size |
| `.defer_vector_indexes(bool)` | Defer vector index rebuilds |
| `.max_buffer_size_bytes(n)` | Maximum buffer size |
| `.build()` | Build the `StreamingAppender` |

---

## Vector Search

Vector similarity search is exposed via Cypher using the `uni.vector.query` procedure. First, define a vector property and index in the schema:

```python
db.schema() \
  .label("Document") \
      .property("title", "string") \
      .vector("embedding", 128) \
      .index("embedding", {"type": "vector", "metric": "cosine"}) \
      .done() \
  .apply()
```

Then query:

```python
session = db.session()
query_vec = [0.1, 0.2, ...]  # 128 dimensions
result = session.query(
    "CALL uni.vector.query('Document', 'embedding', $vec, 10) "
    "YIELD vid, distance RETURN vid, distance",
    {"vec": query_vec}
)
for row in result:
    print(row["vid"], row["distance"])
```

---

## Forks

Named, durable, isolated branches of the graph. A fork sees primary state plus
its own writes; primary is unaffected until you explicitly promote.

Every method below exists on both `Uni` and `AsyncUni` (await the async form).
Fork builders return **synchronously** — only `.build()` / `.apply()` is
awaitable — so the `await session.fork(name).ttl(td).build()` chain works as
written.

### Creating and using a fork

```python
from datetime import timedelta

session = db.session()

# Builders are synchronous; only .build() awaits.
fork = session.fork("scenario_1").ttl(timedelta(hours=1)).build()

tx = fork.tx()
tx.execute("CREATE (:Person {name: 'fork-only'})")
tx.commit()

# The fork sees primary state + its own writes; primary is unchanged.
fork.query("MATCH (p:Person) RETURN count(p) AS n")
session.is_forked()      # False on the primary session
```

### Session methods

| Method | Returns | Description |
|---|---|---|
| `session.fork(name)` | `ForkBuilder` | Open or create a fork. `.ttl()`, then `.build()` |
| `session.fork_schema()` | `ForkSchemaBuilder` | Fork-local schema additions; `.apply()` |
| `session.is_forked()` | `bool` | Whether this session is scoped to a fork |

### Lifecycle and admin (on `Uni` / `AsyncUni`)

| Method | Returns | Description |
|---|---|---|
| `list_forks()` | `list[ForkInfo]` | Every known fork |
| `fork_info(name)` | `ForkInfo \| None` | One fork's metadata |
| `drop_fork(name)` | `None` | Drop a leaf fork. **Refuses a fork with children** |
| `drop_fork_cascade(name)` | `None` | Drop a fork and its whole subtree |

### Tags

Tags pin the fork branch's *current* version and survive the drop, which is what
makes tag-then-drop safe for audit retention.

| Method | Returns | Description |
|---|---|---|
| `tag_fork(fork_name, tag)` | `None` | Pin the fork's current version |
| `untag_fork(fork_name, tag)` | `None` | Remove a tag |
| `list_fork_tags(fork_name)` | `list[str]` | Tags for a fork |

```python
db.tag_fork("scenario_1", "audit-2026-q1")
del fork
db.drop_fork("scenario_1")
db.list_fork_tags("scenario_1")   # the tag is still resolvable
```

### Diff and promote

| Method | Returns | Description |
|---|---|---|
| `diff_fork_primary(fork_name)` | `ForkDiff` | Fork vs primary |
| `diff_forks(a, b)` | `ForkDiff` | Two forks against each other |
| `promote_from_fork(fork_name, patterns)` | `PromoteReport` | Apply fork changes to primary |
| `promote_from_fork_with_options(fork_name, patterns, ...)` | `PromoteReport` | As above, with options |

Diff identity is a **content-addressed UID**, not a VID, so sibling forks and
entirely unrelated forks compare correctly even when their VIDs collide.

Promote runs inside **one** primary transaction across every pattern, so a
failure on pattern *N* rolls back patterns 1..*N*-1. Mix vertex and edge patterns
in a single call when edges need their endpoints promoted alongside them —
otherwise fork-only endpoints surface as `edges_skipped_no_endpoint`.

```python
report = db.promote_from_fork("scenario_1", [
    PromotePattern.label("Person"),
    PromotePattern.edge_type("KNOWS"),
])
```

A fork-only label or edge type must exist on primary first; promote checks for
it before opening the transaction and raises rather than half-committing.

---

## Locy Reasoning

Locy is Uni's built-in Datalog-based reasoning engine. Evaluate programs through a Session or Transaction.

### Basic Evaluation

```python
session = db.session()
result = session.locy("""
    CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        YIELD KEY b

    CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a IS reachable
        YIELD KEY b

    QUERY reachable
""")

# Access command results (QUERY output)
rows = result.rows()
if rows:
    for row in rows:
        print(row)

# Access derived relations
derived = result.derived  # dict: rule_name -> list[dict]

# Access evaluation statistics
print(result.stats)  # LocyStats(strata=..., iterations=..., time=...)
```

### Builder Pattern

```python
result = (
    session.locy_with(program)
    .param("start_node", "Alice")
    .timeout(30.0)
    .max_iterations(500)
    .with_config({
        "strict_probability_domain": True,
        "exact_probability": True,
        "max_bdd_variables": 1000,
    })
    .run()
)
```

### Materializing Derived Facts

After evaluating a Locy program, materialize the results into the graph:

```python
result = session.locy(program)

with session.tx() as tx:
    apply_result = tx.apply(result.derived_fact_set)
    print(f"Applied {apply_result.facts_applied} facts")
    tx.commit()
```

For controlled application with version gap checks:

```python
with session.tx() as tx:
    apply_result = (
        tx.apply_with(result.derived_fact_set)
        .require_fresh(True)
        .max_version_gap(5)
        .run()
    )
    tx.commit()
```

### LocyResult

| Attribute/Method | Type | Description |
|------------------|------|-------------|
| `.derived` | `dict` | Derived relations: `rule_name -> list[dict]` |
| `.stats` | `LocyStats` | Evaluation statistics |
| `.command_results` | `list` | QUERY/ABDUCE/EXPLAIN RULE output |
| `.warnings` | `list` | Runtime warnings |
| `.approximate_groups` | `list` | Groups with approximate probabilities |
| `.derived_fact_set` | `DerivedFactSet` | Opaque fact set for `tx.apply()` |
| `.rows()` | `list \| None` | Rows from the first QUERY command |
| `.columns()` | `list[str] \| None` | Column names from the first QUERY |
| `.derived_facts(rule)` | `list \| None` | Facts for a specific rule |
| `.has_warning(code)` | `bool` | Check for a specific warning code |
| `.iterations` | `int` | Total fixpoint iterations (property) |

See [Locy Overview](../locy/index.md) for the language reference.

---

## Xervo (Embedding & Generation)

Access the configured model runtime via `db.xervo()`. Requires a Xervo catalog configured at database open time.

```python
db = (
    uni_db.UniBuilder.open("./graph")
    .xervo_catalog_from_file("./models.json")
    .build()
)

xervo = db.xervo()

# Check availability
if xervo.is_available():
    # Embed text -> list[list[float]]
    vectors = xervo.embed("embed/default", ["graph databases", "vector search"])

    # Generate with Message objects
    from uni_db import Message
    result = xervo.generate(
        "llm/default",
        [
            Message.system("You are a concise technical assistant."),
            Message.user("What is snapshot isolation?"),
        ],
        max_tokens=256,
        temperature=0.7,
    )
    print(result.text)    # Generated string
    print(result.usage)   # TokenUsage | None

    # Convenience wrapper -- single prompt string
    result = xervo.generate_text("llm/default", "Explain hybrid search in one sentence.")

    # NOTE: `Xervo` has no `rerank()` method. Cross-encoder reranking is a
    # *query option*, applied by the retrieval procedure:
    #
    #   CALL uni.vector.query('Doc', 'embedding', $q, 10, null, null,
    #        {reranker: 'rerank/minilm', reranker_property: 'content',
    #         reranker_query: 'How do graph databases work?'})
    #   YIELD node, score, rerank_score

    # Prefetch models at startup (best practice)
    xervo.prefetch(["embed/default", "llm/default"])   # specific aliases
    xervo.prefetch_all()                                # everything in catalog
```

### Prefetch (Best Practice)

Pre-load models at startup to avoid cold-start latency on first inference. Without prefetch, the first call to `embed()`, `generate()`, or `rerank()` pays the full model load cost (download, session init, warmup).

```python
xervo = db.xervo()
xervo.prefetch(["embed/default", "llm/default"])  # blocks until loaded
# All subsequent calls are instant
```

Both methods are **blocking/awaitable** and **fail-fast** — they return when all models are loaded, or raise on the first failure. Models already loaded are skipped.

### Sharing models across databases

Each database built from a catalog gets its **own** model runtime, and a runtime
holds its own copy of the loaded weights. Opening N databases on the same catalog
therefore costs N copies of every model.

Build the runtime once and pass the handle to each builder to share one resident
copy:

```python
runtime = uni_db.ModelRuntime.from_catalog_str(catalog_json)
# or: uni_db.ModelRuntime.from_catalog_file("./models.json")

tenants = {
    name: uni_db.UniBuilder.open(f"./kb/{name}").xervo_runtime(runtime).build()
    for name in ("alice", "bob", "carol")
}
```

Measured with `all-MiniLM-L6-v2` (22M parameters — the *smallest* useful
embedder), four databases in one process:

| | first instance | each additional |
|---|---|---|
| `xervo_catalog_from_str` | 205 MiB | **+107 MiB** |
| `xervo_runtime` | 194 MiB | **+5 MiB** |

The residual 5 MiB is the database itself; the model is no longer duplicated.
With a larger embedder, a reranker, or a generator, the saving scales with the
model rather than staying flat.

A runtime can also come from a database that already has one, which is useful
when a bootstrap database owns the catalog:

```python
runtime = bootstrap.xervo().raw_runtime()   # None if no Xervo is configured
db = uni_db.UniBuilder.open("./graph").xervo_runtime(runtime).build()
```

`xervo_runtime()` and the two `xervo_catalog_*` methods are mutually exclusive —
the last one called wins. The same handle works with `AsyncUniBuilder`; use
`ModelRuntime.from_catalog_str_async(...)` to build it without blocking an event
loop, which matters when the catalog uses `"warmup": "eager"`.

!!! note "Alias checking on a shared runtime"

    Opening a database whose schema binds a vector index to an embedding alias
    verifies that the alias exists in the runtime's catalog. It cannot yet verify
    that the alias's *task* produces the embedding heads those columns need — that
    check runs only on the `xervo_catalog_*` path, so a task mismatch on a shared
    runtime surfaces at first inference rather than at open.

### Async Xervo

```python
db = await uni_db.AsyncUni.builder() \
    .open("./graph") \
    .xervo_catalog_from_file("./models.json") \
    .build()

xervo = db.xervo()
await xervo.prefetch(["embed/default"])  # async prefetch
vectors = await xervo.embed("embed/default", ["hello"])
result = await xervo.generate_text("llm/default", "Hello!")
```

### Message Constructors

```python
Message.user("content")        # role = "user"
Message.assistant("content")   # role = "assistant"
Message.system("content")      # role = "system"
Message(role, content)         # explicit
```

`generate()` also accepts plain dicts with `"role"` and `"content"` keys instead of `Message` objects.

---

## Prepared Statements

Prepare a query or Locy program once, then execute it many times with different parameters. Avoids repeated parsing and planning.

### PreparedQuery

```python
session = db.session()

# Prepare once
prepared = session.prepare("MATCH (n:Person) WHERE n.name = $name RETURN n")

# Execute many times
result1 = prepared.execute({"name": "Alice"})
result2 = prepared.execute({"name": "Bob"})

# Fluent binder pattern
result = prepared.bind().param("name", "Carol").execute()

# Get the original query text
print(prepared.query_text())
```

### PreparedLocy

```python
prepared = session.prepare_locy("""
    CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.name = $start
        YIELD KEY b

    QUERY reachable
""")

result = prepared.execute({"start": "Alice"})

# Fluent binder
result = prepared.bind().param("start", "Bob").execute()
```

Prepared statements are also available on Transactions via `tx.prepare()` and `tx.prepare_locy()`.

---

## Notifications & Hooks

### Commit Notifications (Watch)

Subscribe to real-time commit notifications from any session:

```python
session = db.session()

# Watch all commits
with session.watch() as stream:
    for notification in stream:
        print(f"v{notification.version}: {notification.labels_affected}")

# Watch with filters
stream = (
    session.watch_with()
    .labels(["Person", "Company"])
    .edge_types(["WORKS_AT"])
    .build()
)
```

`CommitNotification` fields:

| Field | Type | Description |
|-------|------|-------------|
| `version` | `int` | Database version after commit |
| `mutation_count` | `int` | Number of mutations |
| `labels_affected` | `list[str]` | Vertex labels affected |
| `edge_types_affected` | `list[str]` | Edge types affected |
| `rules_promoted` | `int` | Locy rules promoted |
| `timestamp` | `str` | ISO 8601 commit timestamp |
| `tx_id` | `str` | Transaction ID |
| `session_id` | `str` | Committing session ID |
| `causal_version` | `int` | Version when the transaction started |

### Session Hooks

Register Python callbacks for query and commit lifecycle events:

```python
class MyHook:
    def before_query(self, ctx):
        """Called before each query. Raise to reject."""
        if "DROP" in ctx["query_text"]:
            raise ValueError("DROP queries are forbidden")

    def after_query(self, ctx, metrics):
        """Called after each query with metrics."""
        if metrics.total_time_ms > 1000:
            print(f"Slow query: {ctx['query_text']}")

    def before_commit(self, ctx):
        """Called before commit. Raise to reject."""
        pass

    def after_commit(self, ctx, result):
        """Called after successful commit."""
        print(f"Committed v{result.version}: {result.mutations_committed} mutations")

session.add_hook("my_hook", MyHook())
```

---

## Data Types

### Schema Type Strings

When defining properties with strings in `SchemaBuilder`:

| Type String | Python Type | Description |
|-------------|-------------|-------------|
| `"string"` | `str` | UTF-8 string |
| `"int"` / `"int64"` | `int` | 64-bit signed integer |
| `"int32"` | `int` | 32-bit signed integer |
| `"float"` / `"float64"` | `float` | 64-bit float |
| `"float32"` | `float` | 32-bit float |
| `"bool"` | `bool` | Boolean |
| `"vector:N"` | `list[float]` | N-dimensional float vector |
| `"timestamp"` | `datetime` | Timestamp |
| `"date"` | `date` | Calendar date |
| `"time"` | `time` | Time of day |
| `"datetime"` | `datetime` | Date and time |
| `"duration"` | `timedelta` | Duration |
| `"json"` | `Any` | Arbitrary JSON-like value |
| `"bytes"` / `"blob"` / `"binary"` | `bytes` | Raw byte buffer |

### DataType Class

For programmatic type definitions, use `uni_db.DataType`:

```python
from uni_db import DataType

schema.label("Person") \
    .property("name", DataType.STRING()) \
    .property("age", DataType.INT64()) \
    .property("score", DataType.FLOAT64()) \
    .property("active", DataType.BOOL()) \
    .property("embedding", DataType.vector(128)) \
    .property("tags", DataType.list(DataType.STRING())) \
    .property("metadata", DataType.map(DataType.STRING(), DataType.JSON())) \
    .done()
```

Available constructors:

| Constructor | Description |
|-------------|-------------|
| `DataType.STRING()` | UTF-8 string |
| `DataType.INT32()` | 32-bit integer |
| `DataType.INT64()` | 64-bit integer |
| `DataType.FLOAT32()` | 32-bit float |
| `DataType.FLOAT64()` | 64-bit float |
| `DataType.BOOL()` | Boolean |
| `DataType.TIMESTAMP()` | Timestamp |
| `DataType.DATE()` | Calendar date |
| `DataType.TIME()` | Time of day |
| `DataType.DATETIME()` | Date and time |
| `DataType.DURATION()` | Duration |
| `DataType.JSON()` | Arbitrary value |
| `DataType.BYTES()` | Raw byte buffer (images, audio, blobs) |
| `DataType.vector(dimensions)` | N-dimensional float vector |
| `DataType.list(element_type)` | Typed list |
| `DataType.map(key_type, value_type)` | Typed map |
| `DataType.crdt(crdt_type)` | CRDT type |

---

## Query Results

### QueryResult

Returned by `session.query()` and `tx.query()`. Implements the sequence protocol.

```python
result = session.query("MATCH (n:Person) RETURN n.name AS name, n.age AS age")

# Iterate rows (each row is a dict)
for row in result:
    print(row["name"], row["age"])

# Index access
first = result[0]
last = result[-1]
slice = result[1:3]

# Length
print(len(result))

# Boolean (true if non-empty)
if result:
    print("Got results")
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `.rows` | `list[dict]` | Row dictionaries |
| `.columns` | `list[str]` | Column names |
| `.metrics` | `QueryMetrics` | Performance metrics |
| `.warnings` | `list[QueryWarning]` | Execution warnings |

### QueryMetrics

| Attribute | Type | Description |
|-----------|------|-------------|
| `.parse_time_ms` | `float` | Parse time in ms |
| `.plan_time_ms` | `float` | Planning time in ms |
| `.exec_time_ms` | `float` | Execution time in ms |
| `.total_time_ms` | `float` | Total time in ms |
| `.rows_returned` | `int` | Number of rows returned |
| `.rows_scanned` | `int` | Number of rows scanned |
| `.bytes_read` | `int` | Bytes read from storage |
| `.plan_cache_hit` | `bool` | Whether the plan was cached |
| `.l0_reads` | `int` | In-memory reads |
| `.storage_reads` | `int` | Persistent storage reads |
| `.cache_hits` | `int` | Cache hits |

### ExecuteResult

Returned by `tx.execute()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `.affected_rows` | `int` | Total rows affected |
| `.nodes_created` | `int` | Nodes created |
| `.nodes_deleted` | `int` | Nodes deleted |
| `.relationships_created` | `int` | Relationships created |
| `.relationships_deleted` | `int` | Relationships deleted |
| `.properties_set` | `int` | Properties set |
| `.labels_added` | `int` | Labels added |
| `.labels_removed` | `int` | Labels removed |
| `.metrics` | `dict` | Execution metrics |

### CommitResult

Returned by `tx.commit()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `.mutations_committed` | `int` | Number of mutations committed |
| `.rules_promoted` | `int` | Locy rules promoted |
| `.version` | `int` | Database version after commit |
| `.started_at_version` | `int` | Version when the transaction started |
| `.wal_lsn` | `int` | WAL log sequence number |
| `.duration_secs` | `float` | Commit duration in seconds |
| `.rule_promotion_errors` | `list[RulePromotionError]` | Any rule promotion errors |
| `.version_gap()` | `int` | Versions between start and commit |

### ApplyResult

Returned by `tx.apply()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `.facts_applied` | `int` | Number of facts materialized |
| `.version_gap` | `int` | Version gap at apply time |

### Graph Element Types

Query results can contain graph elements with rich object types:

**Node** -- returned when a query returns a node variable (e.g., `RETURN n`):

| Attribute/Method | Type | Description |
|------------------|------|-------------|
| `.id` | `Vid` | Internal vertex identifier |
| `.labels` | `list[str]` | Node labels |
| `.properties` | `dict` | Property dictionary |
| `.get(key, default=None)` | `Any` | Get a property with default |
| `.keys()` / `.values()` / `.items()` | | Dict-like access |
| `node["key"]` | | Dict-style property access |
| `"key" in node` | | Membership test |

**Edge** -- returned when a query returns a relationship variable:

| Attribute/Method | Type | Description |
|------------------|------|-------------|
| `.id` | `Eid` | Internal edge identifier |
| `.type` | `str` | Relationship type name |
| `.start_id` | `Vid` | Source vertex ID |
| `.end_id` | `Vid` | Target vertex ID |
| `.properties` | `dict` | Property dictionary |
| `.get(key, default=None)` | `Any` | Get a property with default |

**Path** -- returned for path expressions:

| Attribute/Method | Type | Description |
|------------------|------|-------------|
| `.nodes` | `list[Node]` | Nodes along the path |
| `.edges` | `list[Edge]` | Edges connecting the nodes |
| `.start` | `Node \| None` | First node |
| `.end` | `Node \| None` | Last node |
| `.is_empty()` | `bool` | True if no edges |
| `len(path)` | `int` | Number of hops |
| `path[i]` | `Node \| Edge` | Interleaved access (even=node, odd=edge) |

### QueryCursor

Streaming cursor for large result sets. Created via `session.query_with(cypher).cursor()`.

```python
with session.query_with("MATCH (n) RETURN n").cursor() as cursor:
    print(cursor.columns)           # Column names
    row = cursor.fetch_one()        # Single row or None
    batch = cursor.fetch_many(100)  # Up to 100 rows
    rest = cursor.fetch_all()       # All remaining rows
```

Also supports Python iterator protocol:

```python
for row in cursor:
    print(row)
```

---

## EXPLAIN and PROFILE

Analyze query execution plans without (or with) running the query.

### EXPLAIN

```python
explain = session.query_with("MATCH (n:Person) RETURN n").explain()

print(explain.plan_text)       # Human-readable plan
print(explain.warnings)        # Planner warnings
print(explain.cost_estimates)  # Estimated rows and cost
print(explain.index_usage)     # Index usage details
print(explain.suggestions)     # Index suggestions
```

### PROFILE

```python
result, profile = session.query_with("MATCH (n:Person) RETURN n").profile()

print(f"Total time: {profile.total_time_ms}ms")
print(f"Peak memory: {profile.peak_memory_bytes} bytes")
print(profile.plan_text)   # Plan with actual row counts
print(profile.operators)   # Per-operator statistics
```

### Locy EXPLAIN

```python
explain = session.locy_with(program).explain()

print(explain.plan_text)
print(f"Strata: {explain.strata_count}")
print(f"Rules: {explain.rule_names}")
print(f"Recursive: {explain.has_recursive_strata}")
print(f"Commands: {explain.command_count}")
```

---

## Snapshots

Point-in-time snapshots for backup and recovery.

```python
# Create a named snapshot
snapshot_id = db.create_snapshot("before-migration")

# List all snapshots
for snap in db.list_snapshots():
    print(snap.snapshot_id, snap.name, snap.created_at, snap.version_hwm)

# Restore to a snapshot
db.restore_snapshot(snapshot_id)
```

### Time-Travel Reads

Pin a session to a historical version:

```python
session = db.session()
session.pin_to_version(snapshot_id)

# All queries now read from the snapshot
result = session.query("MATCH (n:Person) RETURN n")

# Or pin to a timestamp (seconds since epoch)
session.pin_to_timestamp(1700000000.0)

# Unpin and return to latest
session.refresh()
```

### Async Snapshots

```python
snapshot_id = await db.create_snapshot("pre-migration")
snapshots = await db.list_snapshots()
await db.restore_snapshot(snapshot_id)
```

`SnapshotInfo` fields: `snapshot_id`, `name`, `created_at`, `version_hwm`.

---

## Error Handling

All exceptions inherit from `uni_db.UniError`, which inherits from Python's `Exception`.

```python
import uni_db

try:
    session = db.session()
    result = session.query("INVALID CYPHER")
except uni_db.UniParseError as e:
    print(f"Parse error: {e}")
except uni_db.UniQueryError as e:
    print(f"Query error: {e}")
except uni_db.UniError as e:
    print(f"Database error: {e}")
```

### Exception Hierarchy

**Base:**

| Exception | Description |
|-----------|-------------|
| `UniError` | Base for all Uni database errors |

**Database Lifecycle:**

| Exception | Description |
|-----------|-------------|
| `UniNotFoundError` | Database path does not exist |
| `UniDatabaseLockedError` | Database is locked by another process |

**Schema:**

| Exception | Description |
|-----------|-------------|
| `UniSchemaError` | Schema definition or migration error |
| `UniLabelNotFoundError` | Label not found in schema |
| `UniEdgeTypeNotFoundError` | Edge type not found in schema |
| `UniPropertyNotFoundError` | Property not found on entity |
| `UniIndexNotFoundError` | Index not found |
| `UniLabelAlreadyExistsError` | Label already exists |
| `UniEdgeTypeAlreadyExistsError` | Edge type already exists |
| `UniConstraintError` | Constraint violation |
| `UniInvalidIdentifierError` | Invalid identifier name |

**Query & Parse:**

| Exception | Description |
|-----------|-------------|
| `UniParseError` | Cypher or Locy parse error |
| `UniQueryError` | Query execution error |
| `UniTypeError` | Type mismatch error |

**Transaction:**

| Exception | Description |
|-----------|-------------|
| `UniTransactionError` | General transaction error |
| `UniTransactionConflictError` | Transaction serialization conflict |
| `UniTransactionAlreadyCompletedError` | Transaction already committed or rolled back |
| `UniTransactionExpiredError` | Transaction exceeded its deadline |
| `UniCommitTimeoutError` | Commit timed out waiting for writer lock |

**Resource Limits:**

| Exception | Description |
|-----------|-------------|
| `UniMemoryLimitExceededError` | Query exceeded memory limit |
| `UniTimeoutError` | Operation timed out |

**Access Control:**

| Exception | Description |
|-----------|-------------|
| `UniReadOnlyError` | Write on a read-only database |
| `UniPermissionDeniedError` | Permission denied |

**Storage & I/O:**

| Exception | Description |
|-----------|-------------|
| `UniStorageError` | Storage layer error |
| `UniIOError` | I/O error |
| `UniInternalError` | Internal error |

**Snapshot:**

| Exception | Description |
|-----------|-------------|
| `UniSnapshotNotFoundError` | Snapshot not found |

**Arguments:**

| Exception | Description |
|-----------|-------------|
| `UniInvalidArgumentError` | Invalid argument |

**Concurrency:**

| Exception | Description |
|-----------|-------------|
| `UniWriteContextAlreadyActiveError` | A write context is already active on the session |
| `UniCancelledError` | Operation was cancelled |

**Locy:**

| Exception | Description |
|-----------|-------------|
| `UniStaleDerivedFactsError` | Derived facts are stale relative to current database version |
| `UniRuleConflictError` | Locy rule conflict during promotion |
| `UniHookRejectedError` | A session hook rejected the operation |
| `UniLocyCompileError` | Locy program compilation error |
| `UniLocyRuntimeError` | Locy program runtime error |

---

## Async API

The async API mirrors the sync API. All classes are prefixed with `Async` and methods return awaitables.

| Sync | Async |
|------|-------|
| `Uni` | `AsyncUni` |
| `UniBuilder` | `AsyncUniBuilder` |
| `Session` | `AsyncSession` |
| `Transaction` | `AsyncTransaction` |

```python
async with uni_db.AsyncUni.open("./graph") as db:
    session = db.session()

    # Reads
    result = await session.query("MATCH (n:Person) RETURN n.name AS name")

    # Writes
    tx = await session.tx()
    await tx.execute("CREATE (:Person {name: 'Dave', age: 35})")
    await tx.commit()

    # Locy
    result = await session.locy(program)

    # Xervo
    xervo = db.xervo()
    vectors = await xervo.embed("embed/default", ["hello world"])
```

---

## Full API Documentation

See the [auto-generated pdoc documentation](../api/python/index.md) for complete API details including all method signatures and type annotations.
