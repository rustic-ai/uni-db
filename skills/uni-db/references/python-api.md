# Uni Python API Reference

**Package:** `uni-db` | **Async:** prefix with `async`/`await`, use `AsyncUni`/`AsyncSession`/`AsyncTransaction`. Every sync type has an `Async` counterpart; result/data types are shared.

---

## 1. Quick Start

```python
from uni_db import Uni, DataType

db = Uni.open("./my_db")
db.schema() \
    .label("Person") \
        .property("name", DataType.STRING()) \
        .property("age", DataType.INT64()) \
    .apply()

session = db.session()
with session.tx() as tx:
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
    tx.commit()

result = session.query("MATCH (p:Person) RETURN p.name, p.age")
for row in result:
    print(f"{row['p.name']}: {row['p.age']}")
db.shutdown()
```

---

## 2. Architecture Overview

- **Uni** -- database handle. Opens/creates DB, manages schema & storage, provides session factory. No direct queries.
- **Session** -- read scope. Holds parameters, runs queries/Locy, creates transactions.
- **Transaction** -- write scope. ACID reads+writes, bulk loading, commit/rollback. Auto-rollback via context manager.

---

## 3. Uni / AsyncUni

```python
class Uni:
    def background_task_count(self) -> int: ...
    def load_python_plugin(self, module_src: str, module_name: str, grants: list[str] | None=None) -> dict[str, Any]: ...
    @staticmethod
    def open(path: str) -> Uni: ...
    @staticmethod
    def create(path: str) -> Uni: ...
    @staticmethod
    def open_existing(path: str) -> Uni: ...
    @staticmethod
    def temporary() -> Uni: ...
    @staticmethod
    def in_memory() -> Uni: ...
    @staticmethod
    def builder() -> UniBuilder: ...

    def session(self) -> Session: ...
    def session_template(self) -> SessionTemplateBuilder: ...

    def schema(self) -> SchemaBuilder: ...
    def label_exists(self, name: str) -> bool: ...
    def edge_type_exists(self, name: str) -> bool: ...
    def list_labels(self) -> list[str]: ...
    def list_edge_types(self) -> list[str]: ...
    def get_label_info(self, name: str) -> LabelInfo | None: ...
    def get_edge_type_info(self, name: str) -> EdgeTypeInfo | None: ...
    def load_schema(self, path: str) -> None: ...
    def save_schema(self, path: str) -> None: ...

    def rules(self) -> RuleRegistry: ...
    def xervo(self) -> Xervo: ...
    def compaction(self) -> Compaction: ...
    def indexes(self) -> Indexes: ...

    def flush(self) -> None: ...
    def create_snapshot(self, name: str) -> str: ...
    def list_snapshots(self) -> list[SnapshotInfo]: ...
    def restore_snapshot(self, snapshot_id: str) -> None: ...
    def metrics(self) -> DatabaseMetrics: ...
    def config(self) -> dict[str, Any]: ...
    def write_lease(self) -> WriteLease | None: ...
    def shutdown(self) -> None: ...
    # Context manager: __enter__ / __exit__
```

> **AsyncUni:** `open/create/temporary/flush/shutdown` and schema inspection are `async`. `session()/builder()/rules()` stay sync.

---

## 4. UniBuilder / AsyncUniBuilder

`.config(dict)` accepts all 44 active `UniConfig` fields, including nested
`compaction`, `throttle`, `file_sandbox`, `object_store`, and `index_rebuild`.
Unknown fields and deprecated `partial_lance_writes` raise `ValueError`.
Durations accept seconds or `datetime.timedelta`; optional durations accept
`None`. Nested overrides merge and later setters win. `db.config()` returns
these fields with durations in seconds. Use `.cloud_config(...)` separately
for S3/GCS/Azure credentials.

Locy `.with_config(...)` accepts a `LocyConfig` or dictionary. Dictionary
`timeout` uses seconds/timedelta; the object constructor uses `timeout_secs`.
`semiring` accepts `"add_mult_prob"`, `"max_min_prob"`, `"bdd_exact"`, or
`{"top_k_proofs": positive_u32}`. `goal_directed` defaults to `"auto"`;
`neural_predicates_preview` defaults to `True`, and `classifier_cache_max` to
100,000. Use `.compile_locy_with_config(program, config)` for configured
compile-only checks; `.compile_locy(program)` disables neural syntax.
Classifier registration uses `.register_classifier(...)`; internal cache and
provenance-store handles are not Python config keys.

```python
class UniBuilder:
    @staticmethod
    def open(path: str) -> UniBuilder: ...
    @staticmethod
    def create(path: str) -> UniBuilder: ...
    @staticmethod
    def open_existing(path: str) -> UniBuilder: ...
    @staticmethod
    def temporary() -> UniBuilder: ...
    @staticmethod
    def in_memory() -> UniBuilder: ...

    def config(self, config: dict[str, Any]) -> UniBuilder: ...
    def cache_size(self, bytes: int) -> UniBuilder: ...
    def parallelism(self, n: int) -> UniBuilder: ...
    def batch_size(self, size: int) -> UniBuilder: ...
    def wal_enabled(self, enabled: bool) -> UniBuilder: ...
    def schema_file(self, path: str) -> UniBuilder: ...
    def hybrid(self, local_path: str, remote_url: str) -> UniBuilder: ...
    def cloud_config(self, config: dict[str, Any]) -> UniBuilder: ...
    def xervo_catalog_from_str(self, json: str) -> UniBuilder: ...
    def xervo_catalog_from_file(self, path: str) -> UniBuilder: ...
    def read_only(self) -> UniBuilder: ...
    def write_lease(self, lease: WriteLease) -> UniBuilder: ...
    def build(self) -> Uni: ...  # AsyncUniBuilder: async def build() -> AsyncUni
```

---

## 5. Session / AsyncSession

```python
class Session:
    def compile_locy_with_config(self, program: str, config: LocyConfig | dict[str, Any]) -> CompiledProgram: ...
    def build_fork_local_index(self, label: str, column: str, kind: str) -> None: ...
    def execute_with_retry(self, cypher: str) -> ExecuteResult: ...
    def replacement_scans_enabled(self) -> bool: ...
    def set_replacement_scans(self, enabled: bool) -> None: ...
    def params(self) -> Params: ...

    def query(self, cypher: str, params: dict[str, Any] | None = None) -> QueryResult: ...
    def query_with(self, cypher: str) -> SessionQueryBuilder: ...

    def locy(self, program: str, params: dict[str, Any] | None = None) -> LocyResult: ...
    def locy_with(self, program: str) -> SessionLocyBuilder: ...
    def rules(self) -> RuleRegistry: ...
    def compile_locy(self, program: str) -> CompiledProgram: ...


    def prepare(self, cypher: str) -> PreparedQuery: ...
    def prepare_locy(self, program: str) -> PreparedLocy: ...

    def tx(self) -> Transaction: ...
    def tx_with(self) -> TransactionBuilder: ...

    def pin_to_version(self, snapshot_id: str) -> None: ...
    def pin_to_timestamp(self, epoch_secs: float) -> None: ...
    def refresh(self) -> None: ...
    def is_pinned(self) -> bool: ...

    def add_hook(self, hook: Any) -> None: ...
    def watch(self) -> CommitStream: ...
    def watch_with(self) -> WatchBuilder: ...
    def cancel(self) -> None: ...
    def cancellation_token(self) -> CancellationToken: ...
    def id(self) -> str: ...
    def capabilities(self) -> SessionCapabilities: ...
    def metrics(self) -> SessionMetrics: ...
    # Context manager: __enter__ / __exit__
```

> **AsyncSession:** `query/locy/explain/profile/tx/pin_*/prepare*/register_function/watch/cancel` are `async`. Builder factories (`query_with/locy_with/tx_with/rules`) stay sync.

---

## 6. Transaction / AsyncTransaction

```python
class Transaction:
    def query(self, cypher: str, params: dict[str, Any] | None = None) -> QueryResult: ...
    def query_with(self, cypher: str) -> TxQueryBuilder: ...

    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> ExecuteResult: ...
    def execute_with(self, cypher: str) -> TxExecuteBuilder: ...

    def locy(self, program: str, params: dict[str, Any] | None = None) -> LocyResult: ...
    def locy_with(self, program: str) -> TxLocyBuilder: ...

    def apply(self, derived: DerivedFactSet) -> ApplyResult: ...
    def apply_with(self, derived: DerivedFactSet) -> ApplyBuilder: ...

    def commit(self) -> CommitResult: ...
    def rollback(self) -> None: ...
    def rules(self) -> RuleRegistry: ...
    def id(self) -> str: ...
    def started_at_version(self) -> int: ...
    def is_dirty(self) -> bool: ...
    def is_completed(self) -> bool: ...

    def prepare(self, cypher: str) -> PreparedQuery: ...
    def prepare_locy(self, program: str) -> PreparedLocy: ...
    def cancel(self) -> None: ...
    def cancellation_token(self) -> CancellationToken: ...

    def bulk_writer(self) -> TxBulkWriterBuilder: ...
    def appender(self, label: str) -> StreamingAppender: ...
    def appender_builder(self, label: str) -> TxAppenderBuilder: ...
    # Context manager: __enter__ / __exit__ (auto-rollback if not committed)
```

> **AsyncTransaction:** `query/execute/locy/apply/commit/rollback/prepare*/cancel/appender` are `async`. Builder factories stay sync.

**Gotcha:** Always call `tx.commit()` explicitly -- the context manager auto-rollbacks on exception, it does NOT auto-commit.

---

## 7. Query Builders

### SessionQueryBuilder (`session.query_with(cypher)`)

| Method | Returns | Terminal? |
|---|---|---|
| `.param(name, value)` | self | |
| `.params(dict)` | self | |
| `.timeout(seconds)` | self | |
| `.max_memory(bytes)` | self | |
| `.cancellation_token(token)` | self | |
| `.fetch_all()` | `QueryResult` | yes |
| `.fetch_one()` | `dict \| None` | yes |
| `.cursor()` | `QueryCursor` | yes |

### TxQueryBuilder (`tx.query_with(cypher)`)

| Method | Returns | Terminal? |
|---|---|---|
| `.param(name, value)` | self | |
| `.timeout(seconds)` | self | |
| `.fetch_all()` | `QueryResult` | yes |
| `.fetch_one()` | `dict \| None` | yes |
| `.execute()` | `ExecuteResult` | yes |
| `.cursor()` | `QueryCursor` | yes |

### TxExecuteBuilder (`tx.execute_with(cypher)`)

`.param(name, value)`, `.params(dict)`, `.timeout(seconds)` -> self. Terminals:
- `.run()` -> `ExecuteResult` (mutation counters from the tx's private L0).
- `.profile()` -> `tuple[ExecuteResult, ProfileOutput]` (counters + per-operator timings/memory). The async variant on `AsyncTxExecuteBuilder.profile()` is awaitable.

### TransactionBuilder (`session.tx_with()`)

`.timeout(seconds)`, `.isolation(level)` -> self. Terminal: `.start()` -> `Transaction`.

### Query analysis

Use `session.query_with(cypher).explain()` or
`session.query_with(cypher).profile()`. Streaming reads use
`session.query_with(cypher).cursor()`; Locy compile-only analysis uses
`session.locy_with(program).explain()`. These are builder terminals, not
methods directly on Session. Register Python UDFs with `session.scalar_fn(...)`.


---

## 8. Locy Builders

### SessionLocyBuilder (`session.locy_with(program)`)

| Method | Returns | Terminal? |
|---|---|---|
| `.param(name, value)` | self | |
| `.params(dict)` | self | |
| `.timeout(seconds)` | self | |
| `.max_iterations(n)` | self | |
| `.with_config(dict \| LocyConfig)` | self | |
| `.cancellation_token(token)` | self | |
| `.run()` | `LocyResult` | yes |

### TxLocyBuilder (`tx.locy_with(program)`)

Same methods as SessionLocyBuilder, including `.params(dict)`. Terminal: `.run()` -> `LocyResult`.

`.cancellation_token(token)` races the token against the whole evaluation, so a long-running fixpoint is cancellable, not merely at statement boundaries; it raises `UniCancelledError`. `Transaction.cancel()` likewise reaches `tx.locy()` / `tx.locy_with()` and `tx.apply()`, not just the transaction's Cypher statements.

### ApplyBuilder (`tx.apply_with(derived)`)

`.require_fresh(bool)`, `.max_version_gap(int)` -> self. Terminal: `.run()` -> `ApplyResult`.

---

## 9. Schema Builders

```python
class SchemaBuilder:
    def current(self) -> dict[str, Any]: ...
    def current_typed(self) -> Schema: ...
    def label(self, name: str) -> LabelBuilder: ...
    def edge_type(self, name: str, from_labels: list[str], to_labels: list[str]) -> EdgeTypeBuilder: ...
    def apply(self) -> None: ...

class LabelBuilder:
    def property(self, name: str, data_type: str | DataType) -> LabelBuilder: ...
    def property_nullable(self, name: str, data_type: str | DataType) -> LabelBuilder: ...
    def vector(self, name: str, dimensions: int) -> LabelBuilder: ...
    def index(self, property: str, index_type: str | dict[str, Any]) -> LabelBuilder: ...
    def done(self) -> SchemaBuilder: ...
    def apply(self) -> None: ...

class EdgeTypeBuilder:
    def property(self, name: str, data_type: str | DataType) -> EdgeTypeBuilder: ...
    def property_nullable(self, name: str, data_type: str | DataType) -> EdgeTypeBuilder: ...
    def done(self) -> SchemaBuilder: ...
    def apply(self) -> None: ...
```

---

## 10. BulkWriter & StreamingAppender

### TxBulkWriterBuilder (`tx.bulk_writer()`)

| Method | Returns |
|---|---|
| `.defer_vector_indexes(bool)` | self |
| `.defer_scalar_indexes(bool)` | self |
| `.batch_size(int)` | self |
| `.async_indexes(bool)` | self |
| `.validate_constraints(bool)` | self |
| `.max_buffer_size_bytes(int)` | self |
| `.on_progress(Callable[[BulkProgress], None])` | self |
| `.build()` | `BulkWriter` |

### BulkWriter

```python
class BulkWriter:
    def insert_vertices(self, label: str, vertices: list[dict[str, Any]]) -> list[int]: ...
    def insert_edges(self, edge_type: str, edges: list[tuple[int, int, dict[str, Any]]]) -> None: ...
    def stats(self) -> BulkStats: ...
    def commit(self) -> BulkStats: ...
    def abort(self) -> None: ...
    # Context manager: __enter__ / __exit__
```

### StreamingAppender (`tx.appender_builder(label).build()`)

```python
class StreamingAppender:
    def append(self, properties: dict[str, Any]) -> None: ...
    def write_batch(self, batch: Any) -> None: ...  # Arrow RecordBatch
    def finish(self) -> BulkStats: ...
    def abort(self) -> None: ...
    def buffered_count(self) -> int: ...
    # Context manager
```

---

## 11. Facade Types

### RuleRegistry (`db.rules()`, `session.rules()`, `tx.rules()`)

```python
class RuleRegistry:
    def register(self, program: str) -> None: ...
    def remove(self, name: str) -> bool: ...
    def list(self) -> list[str]: ...
    def get(self, name: str) -> RuleInfo | None: ...
    def clear(self) -> None: ...
    def count(self) -> int: ...
```

### Compaction (`db.compaction()`)

`compact(name: str) -> CompactionStats`, `wait() -> None`.

### Indexes (`db.indexes()`)

`list(label=None) -> list[IndexDefinitionInfo]`, `rebuild(label, background=False) -> str|None`, `rebuild_status() -> list[IndexRebuildTaskInfo]`, `retry_failed() -> list[str]`.

---

## 12. Result Types

### QueryResult

Sequence protocol: `for row in result`, `result[0]`, `len(result)`, `bool(result)`. Fields: `metrics: QueryMetrics`, `warnings: list[QueryWarning]`, `columns: list[str]`, `.rows: list[Row]`.

### Row

Dict-like: `row["col"]`, `row[0]`, `"col" in row`, `row.to_dict()`, `row.get("col")`.

### QueryCursor

`columns: list[str]`, `fetch_one() -> dict|None`, `fetch_many(n) -> list[dict]`, `fetch_all() -> list[dict]`, `close()`. Iterable + context manager.

### ExecuteResult

`affected_rows`, `nodes_created`, `nodes_deleted`, `relationships_created`, `relationships_deleted`, `properties_set`, `labels_added`, `labels_removed`: all `int`. `metrics: dict[str, Any]`.

### CommitResult

`mutations_committed: int`, `version: int`, `started_at_version: int`, `wal_lsn: int`, `duration_secs: float`, `rules_promoted: int`, `rule_promotion_errors: list[RulePromotionError]`, `.version_gap() -> int`.

### ApplyResult

`facts_applied: int`, `version_gap: int`.

### BulkStats

`vertices_inserted`, `edges_inserted`, `indexes_rebuilt`: `int`. `duration_secs`, `index_build_duration_secs`: `float`. `index_task_ids: list[str]`, `indexes_pending: bool`.

### LocyResult

```python
class LocyResult:
    async def derived_async(self) -> dict[str, list[dict[str, Any]]]: ...
    derived: Any; stats: Any; command_results: Any; warnings: Any
    approximate_groups: Any; derived_fact_set: Any

    def has_warning(self, code: str) -> bool: ...
    def derived_facts(self, rule: str) -> list[dict[str, Any]] | None: ...
    def rows(self) -> list[dict[str, Any]] | None: ...
    def columns(self) -> list[str] | None: ...
    @property
    def iterations(self) -> int: ...
```

`.derived` materializes full rule relations synchronously and can raise an
execution error; async callers can use `await result.derived_async()`.
`rows()` and `columns()` are methods. Do not confuse these with the
`QueryResult.rows` property or with `derived_fact_set`, the mutations for apply.

### LocyStats

`strata_evaluated`, `total_iterations`, `derived_nodes`, `derived_edges`, `queries_executed`, `mutations_executed`: `int`. `evaluation_time_secs: float`, `peak_memory_bytes: int`.

### DerivedFactSet

Opaque wrapper from `LocyResult.derived_fact_set`, passed to `tx.apply()`. Properties: `evaluated_at_version`, `vertex_count`, `edge_count`, `fact_count`: `int`. `vertices: dict[str, list[dict]]`, `edges: list[dict]`, `is_empty() -> bool`.

### Node

`.id`/`.element_id: Vid`, `.labels: list[str]`, `.properties: dict[str, Any]`. Dict-like access, hashable.

### Edge

`.id`/`.element_id: Eid`, `.type: str`, `.start_id: Vid`, `.end_id: Vid`, `.properties: dict[str, Any]`. Dict-like access.

### Path

`.nodes: list[Node]`, `.edges: list[Edge]`, `.start`/`.end: Node|None`, `.is_empty() -> bool`, `len(path)` = hops.

### QueryMetrics

`parse_time_ms`, `plan_time_ms`, `exec_time_ms`, `total_time_ms`: `float`. `rows_returned`, `rows_scanned`, `bytes_read`, `l0_reads`, `storage_reads`, `cache_hits`: `int`. `plan_cache_hit: bool`.

---

## 13. Configuration

### LocyConfig

```python
class LocyConfig:
    def __init__(
        self,
        max_iterations: int | None = None,
        timeout_secs: float | None = None,
        max_explain_depth: int | None = None,
        max_slg_depth: int | None = None,
        max_abduce_candidates: int | None = None,
        max_abduce_results: int | None = None,
        max_derived_bytes: int | None = None,
        deterministic_best_by: bool | None = None,
        strict_probability_domain: bool | None = None,
        probability_epsilon: float | None = None,
        exact_probability: bool | None = None,
        max_bdd_variables: int | None = None,
        top_k_proofs: int | None = None,
        top_k_proofs_training: int | None = None,
    ) -> None: ...
    # All params available as read-only properties
```

### DataType

Simple: `STRING()`, `INT32()`, `INT64()`, `FLOAT32()`, `FLOAT64()`, `BOOL()`, `TIMESTAMP()`, `DATE()`, `TIME()`, `DATETIME()`, `DURATION()`, `JSON()`. Parameterized: `vector(dimensions)`, `list(element_type)`, `map(key_type, value_type)`, `crdt(crdt_type)`.

### CrdtType

`G_COUNTER`, `G_SET`, `OR_SET`, `LWW_REGISTER`, `LWW_MAP`, `RGA`, `VECTOR_CLOCK`, `VC_REGISTER` -- all static factories.

### WriteLease

`WriteLease.LOCAL()` (single-process), `WriteLease.DYNAMODB(table: str)` (distributed).

### Schema Info Types

```python
class LabelInfo:
    name: str; count: int; properties: list[PropertyInfo]
    indexes: list[IndexInfo]; constraints: list[ConstraintInfo]
class EdgeTypeInfo:
    name: str; count: int; source_labels: list[str]; target_labels: list[str]
    properties: list[PropertyInfo]; indexes: list[IndexInfo]
class PropertyInfo:
    name: str; data_type: str; nullable: bool; is_indexed: bool
class IndexInfo:
    name: str; index_type: str; properties: list[str]; status: str
class SnapshotInfo:
    snapshot_id: str; name: str | None; created_at: str; version_hwm: int
```

### Prepared Statements

```python
class PreparedQuery:
    def execute(self, params: dict[str, Any] | None = None) -> QueryResult: ...
    def query_text(self) -> str: ...
    def bind(self) -> PreparedQueryBinder: ...
class PreparedLocy:
    def execute(self, params: dict[str, Any] | None = None) -> LocyResult: ...
    def program_text(self) -> str: ...
    def bind(self) -> PreparedLocyBinder: ...
```

### Session Templates

```python
class SessionTemplateBuilder:
    def param(self, key: str, value: Any) -> SessionTemplateBuilder: ...
    def rules(self, program: str) -> SessionTemplateBuilder: ...
    def hook(self, hook: Any) -> SessionTemplateBuilder: ...
    def query_timeout(self, seconds: float) -> SessionTemplateBuilder: ...
    def transaction_timeout(self, seconds: float) -> SessionTemplateBuilder: ...
    def build(self) -> SessionTemplate: ...
class SessionTemplate:
    def create(self) -> Session: ...
```

### Commit Notifications

```python
class CommitNotification:
    version: int; mutation_count: int; labels_affected: list[str]
    edge_types_affected: list[str]; timestamp: str; tx_id: str; session_id: str
class WatchBuilder:
    def labels(self, labels: list[str]) -> WatchBuilder: ...
    def edge_types(self, types: list[str]) -> WatchBuilder: ...
    def debounce(self, seconds: float) -> WatchBuilder: ...
    def build(self) -> CommitStream: ...
    def build_async(self) -> AsyncCommitStream: ...
class CancellationToken:
    def cancel(self) -> None: ...
    def is_cancelled(self) -> bool: ...
```

### ID Types

`Vid(id: int)` / `Eid(id: int)` -- `.as_int() -> int`, hashable, comparable.

---

## 14. Xervo ML Runtime

Returned by `db.xervo()`.

```python
class Xervo:
    def is_available(self) -> bool: ...
    def embed(self, alias: str, texts: list[str]) -> list[list[float]]: ...
    def generate(
        self, alias: str,
        messages: list[Message | dict[str, Any]],
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
    ) -> GenerationResult: ...
    def generate_text(
        self, alias: str, prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
    ) -> GenerationResult: ...

class Message:
    role: str; content: str
    @staticmethod
    def user(text: str) -> Message: ...
    @staticmethod
    def assistant(text: str) -> Message: ...
    @staticmethod
    def system(text: str) -> Message: ...

class GenerationResult:
    text: str; usage: TokenUsage | None
class TokenUsage:
    prompt_tokens: int; completion_tokens: int; total_tokens: int
```

---

## 15. Exception Hierarchy

```
UniError (base)
  +-- UniNotFoundError
  +-- UniDatabaseLockedError
  +-- UniSchemaError
  |     +-- UniLabelNotFoundError
  |     +-- UniEdgeTypeNotFoundError
  |     +-- UniPropertyNotFoundError
  |     +-- UniIndexNotFoundError
  |     +-- UniLabelAlreadyExistsError
  |     +-- UniEdgeTypeAlreadyExistsError
  |     +-- UniConstraintError
  |     +-- UniInvalidIdentifierError
  +-- UniParseError
  +-- UniQueryError
  +-- UniTypeError
  +-- UniTransactionError
  |     +-- UniTransactionConflictError
  |     +-- UniTransactionAlreadyCompletedError
  |     +-- UniTransactionExpiredError
  |     +-- UniCommitTimeoutError
  +-- UniMemoryLimitExceededError
  +-- UniTimeoutError
  +-- UniReadOnlyError
  +-- UniPermissionDeniedError
  +-- UniWriteContextAlreadyActiveError
  +-- UniCancelledError
  +-- UniStorageError
  +-- UniIOError
  +-- UniInternalError
  +-- UniSnapshotNotFoundError
  +-- UniInvalidArgumentError
  +-- UniStaleDerivedFactsError
  +-- UniRuleConflictError
  +-- UniHookRejectedError
  +-- UniLocyCompileError
  +-- UniLocyRuntimeError
```

---

## 16. Examples

### RAG Pipeline

```python
from uni_db import Uni, DataType

db = Uni.open("./rag_db")
xervo = db.xervo()

db.schema().label("Document") \
    .property("text", DataType.STRING()).vector("embedding", 384).apply()

session = db.session()
with session.tx() as tx:
    docs = ["Uni is a graph database.", "Locy is a Datalog rule language."]
    embs = xervo.embed("e5-small", docs)
    for text, emb in zip(docs, embs):
        tx.execute("CREATE (:Document {text: $t, embedding: $e})", {"t": text, "e": emb})
    tx.commit()

qe = xervo.embed("e5-small", ["How does Locy work?"])[0]
result = session.query(
    """MATCH (d:Document)
       WITH d, vector.cosine_similarity(d.embedding, $qe) AS score
       WHERE score > 0.5 RETURN d.text, score ORDER BY score DESC LIMIT 5""",
    {"qe": qe},
)
context = "\n".join(row["d.text"] for row in result)
print(xervo.generate_text("llama3", f"Context:\n{context}\n\nQ: How does Locy work?").text)
```

### Bulk Ingest + Query

```python
from uni_db import Uni, DataType

db = Uni.open("./social_db")
db.schema() \
    .label("Person").property("name", DataType.STRING()).property("age", DataType.INT64()) \
        .index("name", "btree") \
    .done().edge_type("KNOWS", ["Person"], ["Person"]).property("since", DataType.INT32()) \
    .apply()

session = db.session()
with session.tx() as tx:
    with tx.bulk_writer().batch_size(10000).build() as writer:
        vids = writer.insert_vertices("Person", [
            {"name": f"user_{i}", "age": 20 + (i % 50)} for i in range(100_000)
        ])
        writer.insert_edges("KNOWS", [
            (vids[i], vids[(i+1) % len(vids)], {"since": 2020}) for i in range(100_000)
        ])
        writer.commit()
    tx.commit()

for row in session.query("""
    MATCH (a:Person {name: 'user_0'})-[:KNOWS]->(b)-[:KNOWS]->(c)
    WHERE a <> c RETURN DISTINCT c.name LIMIT 10"""):
    print(row["c.name"])
```

### Locy Derivation + Apply

```python
session = db.session()
locy_result = session.locy("""
    reachable(x, y) :- Person(x), KNOWS(x, y, _).
    reachable(x, z) :- reachable(x, y), KNOWS(y, z, _).
    QUERY reachable(x, y).
""")

derived = locy_result.derived_fact_set
if derived and not derived.is_empty():
    with session.tx() as tx:
        tx.apply(derived)
        tx.commit()
```
