<!-- GENERATED FILE — DO NOT EDIT.
     Regenerate with: python3 scripts/gen_python_api_reference.py
     Source of truth: bindings/uni-db/uni_db/__init__.pyi -->

# Python API — Symbol Reference

Complete symbol surface of the `uni_db` Python bindings, **generated from `bindings/uni-db/uni_db/__init__.pyi`** at version 3.4.0.

This page is exhaustive and always in sync with the type stubs — it is regenerated in CI. For narrative documentation, worked examples and the recommended patterns, start at the [Python API guide](python-api.md).

**186 classes.**

---

## `AbduceCommandResult`

Result from a Locy ABDUCE command.

| Signature | Description |
|---|---|
| `command_type() -> str` *(property)* | — |
| `modifications() -> list[dict[str, Any]]` *(property)* | — |
| `__getitem__(key: str) -> Any` | — |

---

## `ApplyBuilder`

Fluent builder for applying derived facts.

| Signature | Description |
|---|---|
| `require_fresh(require: bool) -> ApplyBuilder` | — |
| `allow_stale() -> ApplyBuilder` | — |
| `max_version_gap(gap: int) -> ApplyBuilder` | — |
| `run() -> ApplyResult` | — |

---

## `ApplyResult`

Result of applying a DerivedFactSet to a transaction.

**Attributes**

| Name | Type |
|---|---|
| `facts_applied` | `int` |
| `version_gap` | `int` |

---

## `AssumeCommandResult`

Result from a Locy ASSUME command.

| Signature | Description |
|---|---|
| `command_type() -> str` *(property)* | — |
| `rows() -> list[dict[str, Any]]` *(property)* | — |
| `__getitem__(key: str) -> Any` | — |

---

## `AsyncApplyBuilder`

Async fluent builder for applying derived facts.

| Signature | Description |
|---|---|
| `require_fresh(require: bool) -> AsyncApplyBuilder` | — |
| `allow_stale() -> AsyncApplyBuilder` | — |
| `max_version_gap(gap: int) -> AsyncApplyBuilder` | — |
| `async run() -> ApplyResult` | — |

---

## `AsyncBulkWriter`

Async bulk writer for high-throughput data ingestion.

| Signature | Description |
|---|---|
| `async insert_vertices(label: str, vertices: list[dict[str, Any]]) -> list[int]` | — |
| `async insert_edges(edge_type: str, edges: list[tuple[int, int, dict[str, Any]]]) -> None` | — |
| `stats() -> BulkStats` | — |
| `touched_labels() -> list[str]` | — |
| `touched_edge_types() -> list[str]` | — |
| `async commit() -> BulkStats` | — |
| `async abort() -> None` | — |

---

## `AsyncCommitStream`

Async iterator over commit notifications.

| Signature | Description |
|---|---|
| `async close() -> None` | — |

---

## `AsyncCompaction`

Facade for compaction operations (async).

| Signature | Description |
|---|---|
| `async compact(name: str) -> CompactionStats` | — |
| `async wait() -> None` | — |

---

## `AsyncEdgeTypeBuilder`

Async builder for defining an edge type.

| Signature | Description |
|---|---|
| `property(name: str, data_type: str | DataType, *, description: str | None=None) -> AsyncEdgeTypeBuilder` | — |
| `property_nullable(name: str, data_type: str | DataType, *, description: str | None=None) -> AsyncEdgeTypeBuilder` | — |
| `done() -> AsyncSchemaBuilder` | — |
| `async apply() -> None` | — |

---

## `AsyncForkBuilder`

Async builder returned by `AsyncSession.fork(name)`. Drive via `await .build()`.

| Signature | Description |
|---|---|
| `new_() -> AsyncForkBuilder` | — |
| `ttl(ttl: timedelta) -> AsyncForkBuilder` | — |
| `async build() -> AsyncSession` | — |

---

## `AsyncForkSchemaBuilder`

Async builder returned by `AsyncSession.fork_schema()`. Drive via `await .apply()`.

| Signature | Description |
|---|---|
| `label(name: str, description: str | None=None) -> AsyncForkSchemaBuilder` | — |
| `edge_type(name: str, from_labels: list[str], to_labels: list[str], description: str | None=None) -> AsyncForkSchemaBuilder` | — |
| `async apply() -> None` | — |

---

## `AsyncIndexes`

Facade for index management (async).

| Signature | Description |
|---|---|
| `list(label: str | None=None) -> builtins.list[IndexDefinitionInfo]` | — |
| `async rebuild(label: str, background: bool=False) -> str | None` | — |
| `async rebuild_status() -> builtins.list[IndexRebuildTaskInfo]` | — |
| `async retry_failed() -> builtins.list[str]` | — |

---

## `AsyncLabelBuilder`

Async builder for defining a vertex label.

| Signature | Description |
|---|---|
| `property(name: str, data_type: str | DataType, *, description: str | None=None) -> AsyncLabelBuilder` | — |
| `property_nullable(name: str, data_type: str | DataType, *, description: str | None=None) -> AsyncLabelBuilder` | — |
| `vector(name: str, dimensions: int) -> AsyncLabelBuilder` | — |
| `index(property: str, index_type: str | dict[str, Any]) -> AsyncLabelBuilder` | Add an index. ``index_type`` is a name (``"btree"``, ``"vector"``, |
| `done() -> AsyncSchemaBuilder` | — |
| `async apply() -> None` | — |

---

## `AsyncQueryCursor`

Async streaming cursor for large result sets.

| Signature | Description |
|---|---|
| `columns() -> list[str]` *(property)* | — |
| `async fetch_one() -> dict[str, Any] | None` | — |
| `async fetch_many(n: int) -> list[dict[str, Any]]` | — |
| `async fetch_all() -> list[dict[str, Any]]` | — |
| `async close() -> None` | — |

---

## `AsyncRuleRegistry`

Async, durable facade for the database-level Locy rule registry.

| Signature | Description |
|---|---|
| `async register(program: str) -> None` | — |
| `async remove(name: str) -> bool` | — |
| `list() -> list[str]` | — |
| `get(name: str) -> RuleInfo | None` | — |
| `async clear() -> None` | — |
| `count() -> int` | — |

---

## `AsyncSchemaBuilder`

Async builder for defining database schema.

| Signature | Description |
|---|---|
| `current() -> dict[str, Any]` | — |
| `current_typed() -> Schema` | — |
| `label(name: str, *, description: str | None=None) -> AsyncLabelBuilder` | — |
| `edge_type(name: str, from_labels: list[str], to_labels: list[str], *, description: str | None=None) -> AsyncEdgeTypeBuilder` | — |
| `async apply() -> None` | — |

---

## `AsyncSession`

An async query session with scoped variables.

| Signature | Description |
|---|---|
| `params() -> Params` | — |
| `async query(cypher: str, params: dict[str, Any] | None=None) -> QueryResult` | — |
| `query_with(cypher: str) -> AsyncSessionQueryBuilder` | — |
| `async locy(program: str, params: dict[str, Any] | None=None) -> LocyResult` | — |
| `locy_with(program: str) -> AsyncSessionLocyBuilder` | — |
| `rules() -> RuleRegistry` | — |
| `async compile_locy(program: str) -> CompiledProgram` | — |
| `async prepare(cypher: str) -> PreparedQuery` | — |
| `async prepare_locy(program: str) -> PreparedLocy` | — |
| `async tx(timeout: float | None=None) -> AsyncTransaction` | — |
| `tx_with() -> AsyncTransactionBuilder` | — |
| `fork(name: str) -> AsyncForkBuilder` | — |
| `fork_schema() -> AsyncForkSchemaBuilder` | — |
| `async is_forked() -> bool` | — |
| `async flush() -> None` | — |
| `async pin_to_version(snapshot_id: str) -> None` | — |
| `async pin_to_timestamp(epoch_secs: float) -> None` | — |
| `async refresh() -> None` | — |
| `async is_pinned() -> bool` | — |
| `async add_hook(hook: Any) -> None` | — |
| `async remove_hook(name: str) -> bool` | — |
| `async list_hooks() -> list[str]` | — |
| `async clear_hooks() -> None` | — |
| `async watch() -> AsyncCommitStream` | — |
| `async watch_with() -> WatchBuilder` | — |
| `async cancel() -> None` | — |
| `async cancellation_token() -> CancellationToken` | — |
| `async id() -> str` | — |
| `async capabilities() -> SessionCapabilities` | — |
| `async metrics() -> SessionMetrics` | — |
| `set_plugin_id(plugin_id: str) -> None` | — |
| `set_plugin_version(version: str) -> None` | — |
| `async load_python_plugin(module_src: str, module_name: str, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `async finalize_plugin(plugin_id: str, version: str | None=None, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `scalar_fn(name: str, args: Any, returns: str, vectorized: bool=False, determinism: str='pure') -> Callable[..., Any]` | — |
| `aggregate_fn(name: str, args: Any, returns: str, determinism: str='pure') -> Callable[..., Any]` | — |
| `procedure(name: str, args: Any, yields: Any, mode: str='read') -> Callable[..., Any]` | — |

---

## `AsyncSessionLocyBuilder`

Async fluent builder for Locy evaluation on a session.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> AsyncSessionLocyBuilder` | — |
| `params(params: dict[str, Any]) -> AsyncSessionLocyBuilder` | — |
| `timeout(seconds: float) -> AsyncSessionLocyBuilder` | — |
| `max_iterations(n: int) -> AsyncSessionLocyBuilder` | — |
| `with_config(config: dict[str, Any] | LocyConfig) -> AsyncSessionLocyBuilder` | — |
| `cancellation_token(token: CancellationToken) -> AsyncSessionLocyBuilder` | — |
| `async run() -> LocyResult` | — |
| `async explain() -> LocyExplainOutput` | — |
| `async profile() -> tuple[LocyResult, LocyProfileOutput]` | — |

---

## `AsyncSessionQueryBuilder`

Async fluent builder for parameterized read queries on a session.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> AsyncSessionQueryBuilder` | — |
| `params(params: dict[str, Any]) -> AsyncSessionQueryBuilder` | — |
| `timeout(seconds: float) -> AsyncSessionQueryBuilder` | — |
| `max_memory(bytes: int) -> AsyncSessionQueryBuilder` | — |
| `cancellation_token(token: CancellationToken) -> AsyncSessionQueryBuilder` | — |
| `async fetch_all() -> QueryResult` | — |
| `async fetch_one() -> dict[str, Any] | None` | — |
| `async cursor() -> AsyncQueryCursor` | — |
| `async explain() -> ExplainOutput` | — |
| `async profile() -> tuple[QueryResult, ProfileOutput]` | — |

---

## `AsyncSessionTemplate`

A pre-configured async session factory.

| Signature | Description |
|---|---|
| `create() -> AsyncSession` | — |

---

## `AsyncSessionTemplateBuilder`

Async builder for pre-configured session templates.

| Signature | Description |
|---|---|
| `param(key: str, value: Any) -> AsyncSessionTemplateBuilder` | — |
| `rules(program: str) -> AsyncSessionTemplateBuilder` | — |
| `hook(name: str, hook: Any) -> AsyncSessionTemplateBuilder` | — |
| `query_timeout(seconds: float) -> AsyncSessionTemplateBuilder` | — |
| `transaction_timeout(seconds: float) -> AsyncSessionTemplateBuilder` | — |
| `build() -> AsyncSessionTemplate` | — |

---

## `AsyncStreamingAppender`

Async streaming data appender for a single label.

| Signature | Description |
|---|---|
| `async append(properties: dict[str, Any]) -> None` | — |
| `async write_batch(batch: Any) -> None` | — |
| `async finish() -> BulkStats` | — |
| `async abort() -> None` | — |
| `buffered_count() -> int` | — |

---

## `AsyncTransaction`

An async database transaction with context manager support.

| Signature | Description |
|---|---|
| `async query(cypher: str, params: dict[str, Any] | None=None) -> QueryResult` | — |
| `query_with(cypher: str) -> AsyncTxQueryBuilder` | — |
| `async execute(cypher: str, params: dict[str, Any] | None=None) -> ExecuteResult` | — |
| `execute_with(cypher: str) -> AsyncTxExecuteBuilder` | — |
| `async locy(program: str, params: dict[str, Any] | None=None) -> LocyResult` | — |
| `locy_with(program: str) -> AsyncTxLocyBuilder` | — |
| `async apply(derived: DerivedFactSet, require_fresh: bool=True, max_version_gap: int | None=None) -> ApplyResult` | — |
| `async apply_with(derived: DerivedFactSet) -> AsyncApplyBuilder` | — |
| `rules() -> RuleRegistry` | — |
| `async prepare(cypher: str) -> PreparedQuery` | — |
| `async prepare_locy(program: str) -> PreparedLocy` | — |
| `async commit() -> CommitResult` | — |
| `async rollback() -> None` | — |
| `async id() -> str` | — |
| `async started_at_version() -> int` | — |
| `async is_dirty() -> bool` | — |
| `async is_completed() -> bool` | — |
| `async cancel() -> None` | — |
| `async cancellation_token() -> CancellationToken` | — |
| `bulk_writer() -> AsyncTxBulkWriterBuilder` | — |
| `async appender(label: str) -> AsyncStreamingAppender` | — |
| `appender_builder(label: str) -> AsyncTxAppenderBuilder` | — |

---

## `AsyncTransactionBuilder`

Async builder for creating a transaction with options.

| Signature | Description |
|---|---|
| `timeout(seconds: float) -> AsyncTransactionBuilder` | — |
| `isolation(level: str) -> AsyncTransactionBuilder` | — |
| `async start() -> AsyncTransaction` | — |

---

## `AsyncTxAppenderBuilder`

Async builder for a StreamingAppender within a transaction.

| Signature | Description |
|---|---|
| `batch_size(size: int) -> AsyncTxAppenderBuilder` | — |
| `defer_vector_indexes(defer: bool) -> AsyncTxAppenderBuilder` | — |
| `max_buffer_size_bytes(size: int) -> AsyncTxAppenderBuilder` | — |
| `async build() -> AsyncStreamingAppender` | — |

---

## `AsyncTxBulkWriterBuilder`

Async builder for configuring bulk data loading within a transaction.

| Signature | Description |
|---|---|
| `defer_vector_indexes(defer: bool) -> AsyncTxBulkWriterBuilder` | — |
| `defer_scalar_indexes(defer: bool) -> AsyncTxBulkWriterBuilder` | — |
| `batch_size(size: int) -> AsyncTxBulkWriterBuilder` | — |
| `async_indexes(async_: bool) -> AsyncTxBulkWriterBuilder` | — |
| `validate_constraints(validate: bool) -> AsyncTxBulkWriterBuilder` | — |
| `max_buffer_size_bytes(size: int) -> AsyncTxBulkWriterBuilder` | — |
| `on_progress(callback: Callable[[BulkProgress], None]) -> AsyncTxBulkWriterBuilder` | — |
| `async build() -> AsyncBulkWriter` | — |

---

## `AsyncTxExecuteBuilder`

Async fluent builder for mutations within a transaction.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> AsyncTxExecuteBuilder` | — |
| `timeout(seconds: float) -> AsyncTxExecuteBuilder` | — |
| `async run() -> ExecuteResult` | — |
| `async profile() -> tuple[ExecuteResult, ProfileOutput]` | — |

---

## `AsyncTxLocyBuilder`

Async fluent builder for Locy evaluation within a transaction.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> AsyncTxLocyBuilder` | — |
| `timeout(seconds: float) -> AsyncTxLocyBuilder` | — |
| `max_iterations(n: int) -> AsyncTxLocyBuilder` | — |
| `with_config(config: dict[str, Any] | LocyConfig) -> AsyncTxLocyBuilder` | — |
| `cancellation_token(token: CancellationToken) -> AsyncTxLocyBuilder` | — |
| `async run() -> LocyResult` | — |
| `async profile() -> tuple[LocyResult, LocyProfileOutput]` | — |

---

## `AsyncTxQueryBuilder`

Async fluent builder for read queries within a transaction.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> AsyncTxQueryBuilder` | — |
| `timeout(seconds: float) -> AsyncTxQueryBuilder` | — |
| `cancellation_token(token: CancellationToken) -> AsyncTxQueryBuilder` | — |
| `async fetch_all() -> QueryResult` | — |
| `async fetch_one() -> dict[str, Any] | None` | — |
| `async execute() -> ExecuteResult` | — |
| `async cursor() -> AsyncQueryCursor` | — |

---

## `AsyncUni`

The main asynchronous Uni database interface.

| Signature | Description |
|---|---|
| `async open(path: str) -> AsyncUni` *(static)* | — |
| `async temporary() -> AsyncUni` *(static)* | — |
| `async create(path: str) -> AsyncUni` *(static)* | — |
| `async open_existing(path: str) -> AsyncUni` *(static)* | — |
| `async in_memory() -> AsyncUni` *(static)* | — |
| `builder() -> AsyncUniBuilder` *(static)* | — |
| `session() -> AsyncSession` | — |
| `session_template() -> AsyncSessionTemplateBuilder` | — |
| `async load_rhai_plugin(script: str, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `async load_wasm_component(wasm_bytes: bytes, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `async load_wasm_extism(wasm_bytes: bytes, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `schema() -> AsyncSchemaBuilder` | — |
| `async label_exists(name: str) -> bool` | — |
| `async edge_type_exists(name: str) -> bool` | — |
| `async list_labels() -> list[str]` | — |
| `async list_edge_types() -> list[str]` | — |
| `async get_label_info(name: str) -> LabelInfo | None` | — |
| `async get_edge_type_info(name: str) -> EdgeTypeInfo | None` | — |
| `async load_schema(path: str) -> None` | — |
| `async save_schema(path: str) -> None` | — |
| `rules() -> AsyncRuleRegistry` | — |
| `xervo() -> AsyncXervo` | — |
| `compaction() -> AsyncCompaction` | — |
| `indexes() -> AsyncIndexes` | — |
| `uri() -> str` *(property)* | — |
| `async flush() -> None` | — |
| `async create_snapshot(name: str) -> str` | — |
| `async list_snapshots() -> list[SnapshotInfo]` | — |
| `async restore_snapshot(snapshot_id: str) -> None` | — |
| `async list_forks() -> list[ForkInfo]` | — |
| `async fork_info(name: str) -> ForkInfo | None` | — |
| `async drop_fork(name: str) -> None` | — |
| `async drop_fork_cascade(name: str) -> None` | — |
| `async tag_fork(fork_name: str, tag: str) -> None` | — |
| `async untag_fork(fork_name: str, tag: str) -> None` | — |
| `async list_fork_tags(fork_name: str) -> list[str]` | — |
| `async diff_fork_primary(fork_name: str) -> ForkDiff` | — |
| `async diff_forks(a: str, b: str) -> ForkDiff` | — |
| `async promote_from_fork(fork_name: str, patterns: list[PromotePattern]) -> PromoteReport` | — |
| `async promote_from_fork_with_options(fork_name: str, patterns: list[PromotePattern], options: PromoteOptions) -> PromoteReport` | — |
| `metrics() -> DatabaseMetrics` | — |
| `config() -> dict[str, Any]` | — |
| `write_lease() -> WriteLease | None` | — |
| `async shutdown() -> None` | — |

---

## `AsyncUniBuilder`

Async builder for creating and configuring an AsyncUni instance.

| Signature | Description |
|---|---|
| `open(path: str) -> AsyncUniBuilder` *(static)* | — |
| `open_existing(path: str) -> AsyncUniBuilder` *(static)* | — |
| `create(path: str) -> AsyncUniBuilder` *(static)* | — |
| `temporary() -> AsyncUniBuilder` *(static)* | — |
| `in_memory() -> AsyncUniBuilder` *(static)* | — |
| `hybrid(local_path: str, remote_url: str) -> AsyncUniBuilder` | — |
| `cache_size(bytes: int) -> AsyncUniBuilder` | — |
| `parallelism(n: int) -> AsyncUniBuilder` | — |
| `schema_file(path: str) -> AsyncUniBuilder` | — |
| `xervo_catalog_from_str(json: str) -> AsyncUniBuilder` | — |
| `xervo_catalog_from_file(path: str) -> AsyncUniBuilder` | — |
| `xervo_runtime(runtime: ModelRuntime) -> AsyncUniBuilder` | — |
| `cloud_config(config: dict[str, Any]) -> AsyncUniBuilder` | — |
| `config(config: dict[str, Any]) -> AsyncUniBuilder` | — |
| `batch_size(size: int) -> AsyncUniBuilder` | — |
| `wal_enabled(enabled: bool) -> AsyncUniBuilder` | — |
| `read_only() -> AsyncUniBuilder` | — |
| `skip_invalid_locy_rules(skip: bool) -> AsyncUniBuilder` | — |
| `write_lease(lease: WriteLease) -> AsyncUniBuilder` | — |
| `strict_schema(enabled: bool) -> AsyncUniBuilder` | — |
| `max_forks(cap: int | None) -> AsyncUniBuilder` | — |
| `fork_default_ttl(ttl: timedelta | None) -> AsyncUniBuilder` | — |
| `fork_sweeper_interval(interval: timedelta) -> AsyncUniBuilder` | — |
| `disable_fork_sweeper(disabled: bool) -> AsyncUniBuilder` | — |
| `async build() -> AsyncUni` | — |

---

## `AsyncXervo`

Async facade for embedding and text generation.

| Signature | Description |
|---|---|
| `is_available() -> bool` | — |
| `raw_runtime() -> ModelRuntime | None` | — |
| `async prefetch(aliases: list[str]) -> None` | — |
| `async prefetch_all() -> None` | — |
| `async embed(alias: str, texts: list[str]) -> list[list[float]]` | — |
| `async generate(alias: str, messages: list[Message | dict[str, Any]], max_tokens: int | None=None, temperature: float | None=None, top_p: float | None=None) -> GenerationResult` | — |
| `async generate_text(alias: str, prompt: str, max_tokens: int | None=None, temperature: float | None=None, top_p: float | None=None) -> GenerationResult` | — |

---

## `Btic`

Bitemporal interval `[lo, hi)` (ms since epoch) with granularity/certainty.

| Signature | Description |
|---|---|
| `from_raw(lo: int, hi: int, meta: int) -> Btic` *(static)* | — |
| `lo() -> int` *(property)* | — |
| `hi() -> int` *(property)* | — |
| `meta() -> int` *(property)* | — |
| `lo_granularity() -> str` *(property)* | — |
| `hi_granularity() -> str` *(property)* | — |
| `lo_certainty() -> str` *(property)* | — |
| `hi_certainty() -> str` *(property)* | — |
| `duration_ms() -> int | None` *(property)* | — |
| `is_instant() -> bool` *(property)* | — |
| `is_unbounded() -> bool` *(property)* | — |
| `is_finite() -> bool` *(property)* | — |
| `contains_point(point_ms: int) -> bool` | — |
| `overlaps(other: Btic) -> bool` | — |
| `contains(other: Btic) -> bool` | — |
| `before(other: Btic) -> bool` | — |
| `after(other: Btic) -> bool` | — |
| `meets(other: Btic) -> bool` | — |
| `adjacent(other: Btic) -> bool` | — |
| `disjoint(other: Btic) -> bool` | — |
| `intersection(other: Btic) -> Btic | None` | — |
| `span(other: Btic) -> Btic` | — |
| `gap(other: Btic) -> Btic | None` | — |

---

## `BulkProgress`

Progress callback data during bulk loading.

**Attributes**

| Name | Type |
|---|---|
| `phase` | `str` |
| `rows_processed` | `int` |
| `total_rows` | `int | None` |
| `current_label` | `str | None` |
| `elapsed_secs` | `float` |

---

## `BulkStats`

Statistics from a bulk loading operation.

**Attributes**

| Name | Type |
|---|---|
| `vertices_inserted` | `int` |
| `edges_inserted` | `int` |
| `indexes_rebuilt` | `int` |
| `duration_secs` | `float` |
| `index_build_duration_secs` | `float` |
| `index_task_ids` | `list[str]` |
| `indexes_pending` | `bool` |

---

## `BulkWriter`

High-performance bulk data loader.

| Signature | Description |
|---|---|
| `insert_vertices(label: str, vertices: list[dict[str, Any]]) -> list[int]` | — |
| `insert_edges(edge_type: str, edges: list[tuple[int, int, dict[str, Any]]]) -> None` | — |
| `stats() -> BulkStats` | — |
| `touched_labels() -> list[str]` | — |
| `touched_edge_types() -> list[str]` | — |
| `commit() -> BulkStats` | — |
| `abort() -> None` | — |
| `__enter__() -> BulkWriter` | — |
| `__exit__(exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> bool` | — |

---

## `Calibrator`

Probability calibration function (e.g. Platt scaling) for Locy scores.

| Signature | Description |
|---|---|
| `method() -> str` *(property)* | — |
| `apply(raw: float) -> float` | — |
| `apply_batch(raws: list[float]) -> list[float]` | — |

---

## `CancellationToken`

A cooperative cancellation token for long-running operations.

| Signature | Description |
|---|---|
| `cancel() -> None` | — |
| `is_cancelled() -> bool` | — |

---

## `CommitHookContext`

Context passed to session hooks before/after transaction commit.

**Attributes**

| Name | Type |
|---|---|
| `session_id` | `str` |
| `tx_id` | `str` |
| `mutation_count` | `int` |

---

## `CommitNotification`

A commit notification describing the effects of a committed transaction.

**Attributes**

| Name | Type |
|---|---|
| `version` | `int` |
| `mutation_count` | `int` |
| `labels_affected` | `list[str]` |
| `edge_types_affected` | `list[str]` |
| `rules_promoted` | `int` |
| `timestamp` | `str` |
| `tx_id` | `str` |
| `session_id` | `str` |
| `causal_version` | `int` |

---

## `CommitResult`

Result of committing a transaction.

| Signature | Description |
|---|---|
| `version_gap() -> int` | — |

**Attributes**

| Name | Type |
|---|---|
| `mutations_committed` | `int` |
| `rules_promoted` | `int` |
| `version` | `int` |
| `started_at_version` | `int` |
| `wal_lsn` | `int` |
| `duration_secs` | `float` |
| `rule_promotion_errors` | `list[RulePromotionError]` |

---

## `CommitStream`

A synchronous iterator over commit notifications.

| Signature | Description |
|---|---|
| `close() -> None` | — |
| `__enter__() -> CommitStream` | — |
| `__exit__(exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> bool` | — |

---

## `Compaction`

Facade for compaction operations (sync).

| Signature | Description |
|---|---|
| `compact(name: str) -> CompactionStats` | — |
| `wait() -> None` | — |

---

## `CompactionStats`

What a compaction operation actually did.

**Attributes**

| Name | Type |
|---|---|
| `tables_optimized` | `int` |
| `fragments_removed` | `int` |
| `fragments_added` | `int` |
| `files_removed` | `int` |
| `files_added` | `int` |
| `bytes_reclaimed` | `int` |
| `duration_secs` | `float` |
| `semantic_passes` | `int` |
| `crdt_merges` | `int` |

---

## `CompiledProgram`

A compiled Locy program ready for evaluation.

| Signature | Description |
|---|---|
| `num_strata() -> int` *(property)* | — |
| `num_rules() -> int` *(property)* | — |
| `rule_names() -> list[str]` *(property)* | — |

---

## `ConflictPolicy` — extends `Enum`

How a baseline-aware promote resolves concurrent divergent edits.

---

## `ConstraintInfo`

Information about a constraint.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `constraint_type` | `str` |
| `properties` | `list[str]` |
| `enabled` | `bool` |

---

## `CrdtType`

CRDT type for conflict-free replicated data.

| Signature | Description |
|---|---|
| `G_COUNTER() -> CrdtType` *(static)* | — |
| `G_SET() -> CrdtType` *(static)* | — |
| `OR_SET() -> CrdtType` *(static)* | — |
| `LWW_REGISTER() -> CrdtType` *(static)* | — |
| `LWW_MAP() -> CrdtType` *(static)* | — |
| `RGA() -> CrdtType` *(static)* | — |
| `VECTOR_CLOCK() -> CrdtType` *(static)* | — |
| `VC_REGISTER() -> CrdtType` *(static)* | — |

---

## `CypherCommandResult`

Result from a Locy CYPHER command.

| Signature | Description |
|---|---|
| `command_type() -> str` *(property)* | — |
| `rows() -> list[dict[str, Any]]` *(property)* | — |
| `__getitem__(key: str) -> Any` | — |

---

## `DataType`

Data type for schema property definitions.

| Signature | Description |
|---|---|
| `STRING() -> DataType` *(static)* | — |
| `INT32() -> DataType` *(static)* | — |
| `INT64() -> DataType` *(static)* | — |
| `FLOAT32() -> DataType` *(static)* | — |
| `FLOAT64() -> DataType` *(static)* | — |
| `BOOL() -> DataType` *(static)* | — |
| `TIMESTAMP() -> DataType` *(static)* | — |
| `DATE() -> DataType` *(static)* | — |
| `TIME() -> DataType` *(static)* | — |
| `DATETIME() -> DataType` *(static)* | — |
| `DURATION() -> DataType` *(static)* | — |
| `JSON() -> DataType` *(static)* | — |
| `BTIC() -> DataType` *(static)* | — |
| `BYTES() -> DataType` *(static)* | — |
| `vector(dimensions: int) -> DataType` *(static)* | — |
| `sparse_vector(dimensions: int) -> DataType` *(static)* | — |
| `list(element_type: DataType) -> DataType` *(static)* | — |
| `map(key_type: DataType, value_type: DataType) -> DataType` *(static)* | — |
| `crdt(crdt_type: CrdtType) -> DataType` *(static)* | — |

---

## `DatabaseMetrics`

Database-wide metrics snapshot.

**Attributes**

| Name | Type |
|---|---|
| `l0_mutation_count` | `int` |
| `l0_estimated_size_bytes` | `int` |
| `schema_version` | `int` |
| `uptime_secs` | `float` |
| `active_sessions` | `int` |
| `l1_run_count` | `int` |
| `write_throttle_pressure` | `float` |
| `compaction_in_progress` | `bool` |
| `wal_size_bytes` | `int` |
| `wal_lsn` | `int` |
| `total_queries` | `int` |
| `total_commits` | `int` |

---

## `DeriveCommandResult`

Result from a Locy DERIVE command.

| Signature | Description |
|---|---|
| `command_type() -> str` *(property)* | — |
| `__getitem__(key: str) -> Any` | — |

**Attributes**

| Name | Type |
|---|---|
| `affected` | `int` |

---

## `DerivedFactSet`

Opaque wrapper around a Locy-derived fact set.

| Signature | Description |
|---|---|
| `evaluated_at_version() -> int` *(property)* | — |
| `vertex_count() -> int` *(property)* | — |
| `edge_count() -> int` *(property)* | — |
| `fact_count() -> int` *(property)* | — |
| `vertices() -> dict[str, list[dict[str, Any]]]` *(property)* | — |
| `edges() -> list[dict[str, Any]]` *(property)* | — |
| `is_empty() -> bool` | — |

---

## `DiffEdge`

An edge row from one side of a fork diff.

**Attributes**

| Name | Type |
|---|---|
| `edge_type` | `str` |
| `edge_uid` | `str` |
| `src_uid` | `str` |
| `dst_uid` | `str` |
| `properties` | `dict[str, Any]` |

---

## `DiffVertex`

A vertex row from one side of a fork diff.

**Attributes**

| Name | Type |
|---|---|
| `label` | `str` |
| `uid` | `str` |
| `vid` | `int | None` |
| `properties` | `dict[str, Any]` |

---

## `Edge`

A graph edge (relationship) returned from a Cypher query.

| Signature | Description |
|---|---|
| `id() -> Eid` *(property)* | — |
| `element_id() -> Eid` *(property)* | — |
| `type() -> str` *(property)* | — |
| `start_id() -> Vid` *(property)* | — |
| `end_id() -> Vid` *(property)* | — |
| `properties() -> dict[str, Any]` *(property)* | — |
| `get(key: str, default: Any=None) -> Any` | — |
| `keys() -> list[str]` | — |
| `values() -> list[Any]` | — |
| `items() -> list[tuple[str, Any]]` | — |
| `__getitem__(key: str) -> Any` | — |

---

## `EdgeDiff`

The edge side of a `ForkDiff`.

| Signature | Description |
|---|---|
| `is_empty() -> bool` | — |
| `total_rows() -> int` | — |

**Attributes**

| Name | Type |
|---|---|
| `added` | `list[DiffEdge]` |
| `deleted` | `list[DiffEdge]` |
| `changed` | `list[EdgePropertyChange]` |

---

## `EdgePropertyChange`

An edge's property changes (paired by src_uid, dst_uid, edge_type).

**Attributes**

| Name | Type |
|---|---|
| `edge_type` | `str` |
| `src_uid` | `str` |
| `dst_uid` | `str` |
| `changes` | `list[PropertyChange]` |

---

## `EdgeTypeBuilder`

Builder for defining an edge type.

| Signature | Description |
|---|---|
| `property(name: str, data_type: str | DataType, *, description: str | None=None) -> EdgeTypeBuilder` | — |
| `property_nullable(name: str, data_type: str | DataType, *, description: str | None=None) -> EdgeTypeBuilder` | — |
| `done() -> SchemaBuilder` | — |
| `apply() -> None` | — |

---

## `EdgeTypeInfo`

Information about an edge type.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `count` | `int` |
| `source_labels` | `list[str]` |
| `target_labels` | `list[str]` |
| `properties` | `list[PropertyInfo]` |
| `indexes` | `list[IndexInfo]` |
| `constraints` | `list[ConstraintInfo]` |
| `description` | `str | None` |

---

## `Eid`

Edge identifier (64-bit sequential ID).

| Signature | Description |
|---|---|
| `as_int() -> int` | — |

---

## `ExecuteResult`

Result of a ``transaction.execute()`` call.

**Attributes**

| Name | Type |
|---|---|
| `affected_rows` | `int` |
| `nodes_created` | `int` |
| `nodes_deleted` | `int` |
| `relationships_created` | `int` |
| `relationships_deleted` | `int` |
| `properties_set` | `int` |
| `labels_added` | `int` |
| `labels_removed` | `int` |
| `metrics` | `dict[str, Any]` |

---

## `ExplainCommandResult`

Result from a Locy EXPLAIN command.

| Signature | Description |
|---|---|
| `command_type() -> str` *(property)* | — |
| `tree() -> Any` *(property)* | — |
| `__getitem__(key: str) -> Any` | — |

---

## `ExplainOutput`

Typed output from ``session.explain()``.

**Attributes**

| Name | Type |
|---|---|
| `plan_text` | `str` |
| `warnings` | `list[str]` |
| `cost_estimates` | `Any` |
| `index_usage` | `Any` |
| `suggestions` | `Any` |

---

## `ForkBuilder`

Sync builder returned by `Session.fork(name)`. Drive via `.build()`.

| Signature | Description |
|---|---|
| `new_() -> ForkBuilder` | Require fresh creation; errors with `UniForkAlreadyExistsError`. |
| `ttl(ttl: timedelta) -> ForkBuilder` | Stamp a wall-clock TTL on the fork. |
| `build() -> Session` | Drive the open-or-create flow and return a forked Session. |

---

## `ForkDiff`

Structural delta between two fork views, or a fork and primary.

| Signature | Description |
|---|---|
| `is_empty() -> bool` | — |
| `total_rows() -> int` | — |
| `invert() -> ForkDiff` | — |

**Attributes**

| Name | Type |
|---|---|
| `vertices` | `VertexDiff` |
| `edges` | `EdgeDiff` |

---

## `ForkId`

Stable ULID-backed identifier for a fork.

| Signature | Description |
|---|---|
| `parse(s: str) -> ForkId` *(static)* | — |

---

## `ForkInfo`

Registry record for a single fork.

**Attributes**

| Name | Type |
|---|---|
| `id` | `ForkId` |
| `name` | `str` |
| `parent_fork_id` | `ForkId | None` |
| `parent_snapshot_id` | `str` |
| `created_at` | `datetime` |
| `ttl_expires_at` | `datetime | None` |
| `schema_version_at_creation` | `int` |
| `datasets` | `dict[str, str]` |
| `status` | `ForkStatus` |

---

## `ForkSchemaBuilder`

Sync builder returned by `Session.fork_schema()`. Drive via `.apply()`.

| Signature | Description |
|---|---|
| `label(name: str, description: str | None=None) -> ForkSchemaBuilder` | — |
| `edge_type(name: str, from_labels: list[str], to_labels: list[str], description: str | None=None) -> ForkSchemaBuilder` | — |
| `apply() -> None` | — |

---

## `ForkStatus` — extends `Enum`

Lifecycle status of a fork in the registry.

---

## `GenerationResult`

Result of a Xervo generation call.

**Attributes**

| Name | Type |
|---|---|
| `text` | `str` |
| `usage` | `TokenUsage | None` |

---

## `HookContext`

Context passed to session hooks before/after query execution.

**Attributes**

| Name | Type |
|---|---|
| `session_id` | `str` |
| `query_text` | `str` |
| `query_type` | `str` |

---

## `IndexDefinitionInfo`

Definition of an index in the schema.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `index_type` | `str` |
| `label` | `str` |
| `properties` | `list[str]` |
| `state` | `str` |

---

## `IndexInfo`

Information about an index.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `index_type` | `str` |
| `properties` | `list[str]` |
| `status` | `str` |

---

## `IndexRebuildTaskInfo`

Status of a background index rebuild task.

**Attributes**

| Name | Type |
|---|---|
| `id` | `str` |
| `label` | `str` |
| `status` | `str` |
| `created_at` | `str` |
| `started_at` | `str | None` |
| `completed_at` | `str | None` |
| `error` | `str | None` |
| `retry_count` | `int` |

---

## `Indexes`

Facade for index management (sync).

| Signature | Description |
|---|---|
| `list(label: str | None=None) -> builtins.list[IndexDefinitionInfo]` | — |
| `rebuild(label: str, background: bool=False) -> str | None` | — |
| `rebuild_status() -> builtins.list[IndexRebuildTaskInfo]` | — |
| `retry_failed() -> builtins.list[str]` | — |

---

## `LabelBuilder`

Builder for defining a vertex label.

| Signature | Description |
|---|---|
| `property(name: str, data_type: str | DataType, *, description: str | None=None) -> LabelBuilder` | — |
| `property_nullable(name: str, data_type: str | DataType, *, description: str | None=None) -> LabelBuilder` | — |
| `vector(name: str, dimensions: int) -> LabelBuilder` | — |
| `index(property: str, index_type: str | dict[str, Any]) -> LabelBuilder` | Add an index. ``index_type`` is a name (``"btree"``, ``"vector"``, |
| `done() -> SchemaBuilder` | — |
| `apply() -> None` | — |

---

## `LabelInfo`

Information about a vertex label.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `count` | `int` |
| `properties` | `list[PropertyInfo]` |
| `indexes` | `list[IndexInfo]` |
| `constraints` | `list[ConstraintInfo]` |
| `description` | `str | None` |

---

## `LocyConfig`

Configuration for Locy program evaluation.

| Signature | Description |
|---|---|
| `max_iterations() -> int` *(property)* | — |
| `timeout_secs() -> float` *(property)* | — |
| `max_explain_depth() -> int` *(property)* | — |
| `max_slg_depth() -> int` *(property)* | — |
| `max_abduce_candidates() -> int` *(property)* | — |
| `max_abduce_results() -> int` *(property)* | — |
| `max_derived_bytes() -> int` *(property)* | — |
| `deterministic_best_by() -> bool` *(property)* | — |
| `strict_probability_domain() -> bool` *(property)* | — |
| `probability_epsilon() -> float` *(property)* | — |
| `exact_probability() -> bool` *(property)* | — |
| `max_bdd_variables() -> int` *(property)* | — |
| `top_k_proofs() -> int` *(property)* | — |
| `top_k_proofs_training() -> int | None` *(property)* | — |
| `classifier_aliases() -> list[str]` *(property)* | — |
| `register_classifier(alias: str, classifier: Callable[..., Any]) -> None` | — |

---

## `LocyExplainOutput`

Typed output from ``session.locy_with(program).explain()``.

**Attributes**

| Name | Type |
|---|---|
| `plan_text` | `str` |
| `strata_count` | `int` |
| `rule_names` | `list[str]` |
| `has_recursive_strata` | `bool` |
| `warnings` | `list[str]` |
| `command_count` | `int` |

---

## `LocyProfileOutput`

Structured execution profile from ``session.locy_with(...).profile()``.

**Attributes**

| Name | Type |
|---|---|
| `total_time_ms` | `float` |
| `peak_memory_bytes` | `int` |
| `plan_text` | `str` |
| `strata` | `list[dict[str, Any]]` |

---

## `LocyResult`

Result of a Locy program evaluation.

| Signature | Description |
|---|---|
| `has_warning(code: str) -> bool` | — |
| `warnings_list() -> Any` | — |
| `derived_facts(rule: str) -> list[dict[str, Any]] | None` | — |
| `rows() -> list[dict[str, Any]] | None` | — |
| `columns() -> list[str] | None` | — |
| `iterations() -> int` *(property)* | — |
| `timed_out() -> bool` *(property)* | — |
| `incomplete() -> bool` *(property)* | — |

**Attributes**

| Name | Type |
|---|---|
| `derived` | `Any` |
| `stats` | `Any` |
| `command_results` | `Any` |
| `warnings` | `Any` |
| `compile_warnings` | `list[dict[str, str]]` |
| `approximate_groups` | `Any` |
| `derived_fact_set` | `Any` |

---

## `LocyStats`

Statistics from a Locy program evaluation.

**Attributes**

| Name | Type |
|---|---|
| `strata_evaluated` | `int` |
| `total_iterations` | `int` |
| `derived_nodes` | `int` |
| `derived_edges` | `int` |
| `evaluation_time_secs` | `float` |
| `queries_executed` | `int` |
| `mutations_executed` | `int` |
| `peak_memory_bytes` | `int` |

---

## `Message`

A message in a conversation (role + text content).

| Signature | Description |
|---|---|
| `user(text: str) -> Message` *(static)* | — |
| `assistant(text: str) -> Message` *(static)* | — |
| `system(text: str) -> Message` *(static)* | — |

**Attributes**

| Name | Type |
|---|---|
| `role` | `str` |
| `content` | `str` |

---

## `ModelRuntime`

An opaque handle to a Xervo model runtime.

| Signature | Description |
|---|---|
| `from_catalog_str(json: str) -> ModelRuntime` *(static)* | — |
| `from_catalog_file(path: str) -> ModelRuntime` *(static)* | — |
| `async from_catalog_str_async(json: str) -> ModelRuntime` *(static)* | — |
| `async from_catalog_file_async(path: str) -> ModelRuntime` *(static)* | — |
| `contains_alias(alias: str) -> bool` | — |

---

## `Node`

A graph node returned from a Cypher query.

| Signature | Description |
|---|---|
| `id() -> Vid` *(property)* | — |
| `element_id() -> Vid` *(property)* | — |
| `labels() -> list[str]` *(property)* | — |
| `properties() -> dict[str, Any]` *(property)* | — |
| `get(key: str, default: Any=None) -> Any` | — |
| `keys() -> list[str]` | — |
| `values() -> list[Any]` | — |
| `items() -> list[tuple[str, Any]]` | — |
| `__getitem__(key: str) -> Any` | — |

---

## `Params`

Session-scoped parameter store, returned by ``Session.params()``.

| Signature | Description |
|---|---|
| `set(key: str, value: Any) -> None` | — |
| `get(key: str) -> Any | None` | — |
| `unset(key: str) -> Any | None` | — |
| `get_all() -> dict[str, Any]` | — |
| `set_all(params: dict[str, Any]) -> None` | — |

---

## `Path`

A graph path (alternating sequence of nodes and edges).

| Signature | Description |
|---|---|
| `nodes() -> list[Node]` *(property)* | — |
| `edges() -> list[Edge]` *(property)* | — |
| `start() -> Node | None` *(property)* | — |
| `end() -> Node | None` *(property)* | — |
| `is_empty() -> bool` | — |
| `__getitem__(idx: int) -> Node | Edge` | — |

---

## `PreparedLocy`

A prepared Locy program that can be executed multiple times.

| Signature | Description |
|---|---|
| `execute(params: dict[str, Any] | None=None) -> LocyResult` | — |
| `program_text() -> str` | — |
| `bind() -> PreparedLocyBinder` | — |

---

## `PreparedLocyBinder`

A fluent binder for executing a prepared Locy program.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> PreparedLocyBinder` | — |
| `execute() -> LocyResult` | — |

---

## `PreparedQueryBinder`

A fluent binder for executing a prepared Cypher query.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> PreparedQueryBinder` | — |
| `execute() -> QueryResult` | — |

---

## `ProfileOutput`

Typed output from ``session.profile()``.

**Attributes**

| Name | Type |
|---|---|
| `total_time_ms` | `int` |
| `peak_memory_bytes` | `int` |
| `plan_text` | `str` |
| `operators` | `Any` |

---

## `PromoteOptions`

Options for `Uni.promote_from_fork_with_options`.

**Attributes**

| Name | Type |
|---|---|
| `upsert` | `bool` |
| `delete_promotion` | `bool` |

---

## `PromotePattern`

Selector for which rows `Uni.promote_from_fork` copies up.

| Signature | Description |
|---|---|
| `label(cls, label: str, where_clause: str | None=None) -> PromotePattern` | — |
| `edge_type(cls, edge_type: str, where_clause: str | None=None) -> PromotePattern` | — |

**Attributes**

| Name | Type |
|---|---|
| `kind` | `str` |

---

## `PromoteReport`

Outcome counts from `Uni.promote_from_fork`.

**Attributes**

| Name | Type |
|---|---|
| `vertices_inserted` | `int` |
| `vertices_updated` | `int` |
| `vertices_skipped_no_op` | `int` |
| `vertices_inserted_unverified` | `int` |
| `vertices_deleted` | `int` |
| `vertices_skipped_no_ext_id_for_delete` | `int` |
| `vertices_conflicting` | `int` |
| `vertices_skipped_uid_conflict` | `int` |
| `vertices_skipped_no_uid` | `int` |
| `edges_inserted` | `int` |
| `edges_skipped` | `int` |
| `edges_skipped_duplicate` | `int` |
| `edges_skipped_no_endpoint` | `int` |
| `per_pattern_inserted` | `list[int]` |

---

## `PropertyChange`

A single property's before/after pair within a diff.

**Attributes**

| Name | Type |
|---|---|
| `key` | `str` |
| `before` | `Any | None` |
| `after` | `Any | None` |

---

## `PropertyInfo`

Information about a property.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `data_type` | `str` |
| `nullable` | `bool` |
| `is_indexed` | `bool` |
| `description` | `str | None` |

---

## `PyPreparedQuery`

A prepared Cypher query that can be executed multiple times.

| Signature | Description |
|---|---|
| `execute(params: dict[str, Any] | None=None) -> QueryResult` | — |
| `query_text() -> str` | — |
| `bind() -> PreparedQueryBinder` | — |

---

## `QueryCommandResult`

Result from a Locy QUERY command.

| Signature | Description |
|---|---|
| `command_type() -> str` *(property)* | — |
| `rows() -> list[dict[str, Any]]` *(property)* | — |
| `__getitem__(key: str) -> Any` | — |

---

## `QueryCursor`

Streaming cursor for large result sets.

| Signature | Description |
|---|---|
| `columns() -> list[str]` *(property)* | — |
| `fetch_one() -> dict[str, Any] | None` | — |
| `fetch_many(n: int) -> list[dict[str, Any]]` | — |
| `fetch_all() -> list[dict[str, Any]]` | — |
| `close() -> None` | — |
| `__enter__() -> QueryCursor` | — |
| `__exit__(exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> bool` | — |

---

## `QueryMetrics`

Query performance metrics returned with every query result.

**Attributes**

| Name | Type |
|---|---|
| `parse_time_ms` | `float` |
| `plan_time_ms` | `float` |
| `exec_time_ms` | `float` |
| `total_time_ms` | `float` |
| `rows_returned` | `int` |
| `rows_scanned` | `int` |
| `bytes_read` | `int` |
| `plan_cache_hit` | `bool` |
| `l0_reads` | `int` |
| `storage_reads` | `int` |
| `cache_hits` | `int` |
| `branch_scans` | `int` |
| `snapshot_reads` | `int` |
| `index_scans` | `int` |
| `index_comparisons` | `int` |
| `scans_reported` | `int` |
| `vector_index_scans` | `int` |
| `fts_index_scans` | `int` |
| `searches_reported` | `int` |

---

## `QueryResult`

Rich query result with rows, metrics, warnings, and column names.

| Signature | Description |
|---|---|
| `rows() -> list[Row]` *(property)* | — |
| `__getitem__(idx: int) -> Row` | — |

**Attributes**

| Name | Type |
|---|---|
| `metrics` | `QueryMetrics` |
| `warnings` | `list[QueryWarning]` |
| `columns` | `list[str]` |

---

## `QueryType`

Query type discriminator for hook contexts.

| Signature | Description |
|---|---|
| `CYPHER() -> str` *(static)* | — |
| `LOCY() -> str` *(static)* | — |
| `EXECUTE() -> str` *(static)* | — |

---

## `QueryWarning`

A query warning emitted during execution (e.g., missing index).

**Attributes**

| Name | Type |
|---|---|
| `code` | `str` |
| `message` | `str` |

---

## `Row`

A query result row with named columns.

| Signature | Description |
|---|---|
| `columns() -> list[str]` *(property)* | — |
| `get(column: str) -> Any` | — |
| `to_dict() -> dict[str, Any]` | — |
| `__getitem__(key: str | int) -> Any` | — |

---

## `RuleInfo`

Metadata about a registered Locy rule.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `clause_count` | `int` |
| `is_recursive` | `bool` |

---

## `RulePromotionError`

A rule promotion error from a transaction commit.

**Attributes**

| Name | Type |
|---|---|
| `rule_text` | `str` |
| `error` | `str` |

---

## `RuleRegistry`

Facade for managing pre-compiled Locy rules.

| Signature | Description |
|---|---|
| `register(program: str) -> None` | — |
| `remove(name: str) -> bool` | — |
| `list() -> list[str]` | — |
| `get(name: str) -> RuleInfo | None` | — |
| `clear() -> None` | — |
| `count() -> int` | — |

---

## `Schema`

A read-only snapshot of the database schema.

| Signature | Description |
|---|---|
| `version() -> int` *(property)* | — |
| `label_names() -> list[str]` *(property)* | — |
| `edge_type_names() -> list[str]` *(property)* | — |
| `label_count() -> int` *(property)* | — |
| `edge_type_count() -> int` *(property)* | — |
| `label_info(name: str) -> LabelInfo | None` | — |

---

## `SchemaBuilder`

Builder for defining database schema.

| Signature | Description |
|---|---|
| `current() -> dict[str, Any]` | — |
| `current_typed() -> Schema` | — |
| `label(name: str, *, description: str | None=None) -> LabelBuilder` | — |
| `edge_type(name: str, from_labels: list[str], to_labels: list[str], *, description: str | None=None) -> EdgeTypeBuilder` | — |
| `apply() -> None` | — |

---

## `Session`

A query session with scoped variables.

| Signature | Description |
|---|---|
| `params() -> Params` | — |
| `query(cypher: str, params: dict[str, Any] | None=None) -> QueryResult` | — |
| `query_with(cypher: str) -> SessionQueryBuilder` | — |
| `locy(program: str, params: dict[str, Any] | None=None) -> LocyResult` | — |
| `locy_with(program: str) -> SessionLocyBuilder` | — |
| `rules() -> RuleRegistry` | — |
| `compile_locy(program: str) -> CompiledProgram` | — |
| `prepare(cypher: str) -> PreparedQuery` | — |
| `prepare_locy(program: str) -> PreparedLocy` | — |
| `tx() -> Transaction` | — |
| `tx_with() -> TransactionBuilder` | — |
| `fork(name: str) -> ForkBuilder` | — |
| `fork_schema() -> ForkSchemaBuilder` | — |
| `is_forked() -> bool` | — |
| `flush() -> None` | — |
| `pin_to_version(snapshot_id: str) -> None` | — |
| `pin_to_timestamp(epoch_secs: float) -> None` | — |
| `refresh() -> None` | — |
| `is_pinned() -> bool` | — |
| `add_hook(hook: Any) -> None` | — |
| `remove_hook(name: str) -> bool` | — |
| `list_hooks() -> list[str]` | — |
| `clear_hooks() -> None` | — |
| `watch() -> CommitStream` | — |
| `watch_with() -> WatchBuilder` | — |
| `cancel() -> None` | — |
| `cancellation_token() -> CancellationToken` | — |
| `id() -> str` | — |
| `capabilities() -> SessionCapabilities` | — |
| `metrics() -> SessionMetrics` | — |
| `set_plugin_id(plugin_id: str) -> None` | — |
| `set_plugin_version(version: str) -> None` | — |
| `load_python_plugin(module_src: str, module_name: str, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `finalize_plugin(plugin_id: str, version: str | None=None, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `scalar_fn(name: str, args: Any, returns: str, vectorized: bool=False, determinism: str='pure') -> Callable[..., Any]` | — |
| `aggregate_fn(name: str, args: Any, returns: str, determinism: str='pure') -> Callable[..., Any]` | — |
| `procedure(name: str, args: Any, yields: Any, mode: str='read') -> Callable[..., Any]` | — |
| `__enter__() -> Session` | — |
| `__exit__(exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> bool` | — |

---

## `SessionCapabilities`

Session capabilities snapshot.

**Attributes**

| Name | Type |
|---|---|
| `can_write` | `bool` |
| `can_pin` | `bool` |
| `isolation` | `str` |
| `has_notifications` | `bool` |
| `write_lease` | `str | None` |

---

## `SessionLocyBuilder`

Fluent builder for Locy evaluation on a session.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> SessionLocyBuilder` | — |
| `params(params: dict[str, Any]) -> SessionLocyBuilder` | — |
| `timeout(seconds: float) -> SessionLocyBuilder` | — |
| `max_iterations(n: int) -> SessionLocyBuilder` | — |
| `with_config(config: dict[str, Any] | LocyConfig) -> SessionLocyBuilder` | — |
| `cancellation_token(token: CancellationToken) -> SessionLocyBuilder` | — |
| `run() -> LocyResult` | — |
| `explain() -> LocyExplainOutput` | — |
| `profile() -> tuple[LocyResult, LocyProfileOutput]` | — |

---

## `SessionMetrics`

Metrics for a session's lifetime.

**Attributes**

| Name | Type |
|---|---|
| `session_id` | `str` |
| `active_since_secs` | `float` |
| `queries_executed` | `int` |
| `locy_evaluations` | `int` |
| `total_query_time_secs` | `float` |
| `transactions_committed` | `int` |
| `transactions_rolled_back` | `int` |
| `total_rows_returned` | `int` |
| `total_rows_scanned` | `int` |
| `plan_cache_hits` | `int` |
| `plan_cache_misses` | `int` |
| `plan_cache_size` | `int` |

---

## `SessionQueryBuilder`

Fluent builder for parameterized read queries on a session.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> SessionQueryBuilder` | — |
| `params(params: dict[str, Any]) -> SessionQueryBuilder` | — |
| `timeout(seconds: float) -> SessionQueryBuilder` | — |
| `max_memory(bytes: int) -> SessionQueryBuilder` | — |
| `cancellation_token(token: CancellationToken) -> SessionQueryBuilder` | — |
| `fetch_all() -> QueryResult` | — |
| `fetch_one() -> dict[str, Any] | None` | — |
| `cursor() -> QueryCursor` | — |
| `explain() -> ExplainOutput` | — |
| `profile() -> tuple[QueryResult, ProfileOutput]` | — |

---

## `SessionTemplate`

A pre-configured session factory.

| Signature | Description |
|---|---|
| `create() -> Session` | — |

---

## `SessionTemplateBuilder`

Builder for pre-configured session templates.

| Signature | Description |
|---|---|
| `param(key: str, value: Any) -> SessionTemplateBuilder` | — |
| `rules(program: str) -> SessionTemplateBuilder` | — |
| `hook(hook: Any) -> SessionTemplateBuilder` | — |
| `query_timeout(seconds: float) -> SessionTemplateBuilder` | — |
| `transaction_timeout(seconds: float) -> SessionTemplateBuilder` | — |
| `build() -> SessionTemplate` | — |

---

## `SnapshotInfo`

Information about a database snapshot.

**Attributes**

| Name | Type |
|---|---|
| `snapshot_id` | `str` |
| `name` | `str | None` |
| `created_at` | `str` |
| `version_hwm` | `int` |

---

## `SparseVector`

Learned-sparse vector (term-id -> weight) for SPLADE/BGE-M3 retrieval.

| Signature | Description |
|---|---|
| `from_dict(mapping: dict[int, float]) -> SparseVector` *(static)* | — |
| `indices() -> list[int]` *(property)* | — |
| `values() -> list[float]` *(property)* | — |
| `to_dict() -> dict[int, float]` | — |

---

## `StreamingAppender`

Streaming data appender for a single label.

| Signature | Description |
|---|---|
| `append(properties: dict[str, Any]) -> None` | — |
| `write_batch(batch: Any) -> None` | — |
| `finish() -> BulkStats` | — |
| `abort() -> None` | — |
| `buffered_count() -> int` | — |
| `__enter__() -> StreamingAppender` | — |
| `__exit__(exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> bool` | — |

---

## `TokenUsage`

Token usage statistics from a generation call.

**Attributes**

| Name | Type |
|---|---|
| `prompt_tokens` | `int` |
| `completion_tokens` | `int` |
| `total_tokens` | `int` |

---

## `Transaction`

A database transaction with ACID guarantees.

| Signature | Description |
|---|---|
| `query(cypher: str, params: dict[str, Any] | None=None) -> QueryResult` | — |
| `query_with(cypher: str) -> TxQueryBuilder` | — |
| `execute(cypher: str, params: dict[str, Any] | None=None) -> ExecuteResult` | — |
| `execute_with(cypher: str) -> TxExecuteBuilder` | — |
| `locy(program: str, params: dict[str, Any] | None=None) -> LocyResult` | — |
| `locy_with(program: str) -> TxLocyBuilder` | — |
| `apply(derived: DerivedFactSet) -> ApplyResult` | — |
| `apply_with(derived: DerivedFactSet) -> ApplyBuilder` | — |
| `rules() -> RuleRegistry` | — |
| `prepare(cypher: str) -> PreparedQuery` | — |
| `prepare_locy(program: str) -> PreparedLocy` | — |
| `commit() -> CommitResult` | — |
| `rollback() -> None` | — |
| `id() -> str` | — |
| `started_at_version() -> int` | — |
| `is_dirty() -> bool` | — |
| `is_completed() -> bool` | — |
| `cancel() -> None` | — |
| `cancellation_token() -> CancellationToken` | — |
| `bulk_writer() -> TxBulkWriterBuilder` | — |
| `appender(label: str) -> StreamingAppender` | — |
| `appender_builder(label: str) -> TxAppenderBuilder` | — |
| `__enter__() -> Transaction` | — |
| `__exit__(exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> bool` | — |

---

## `TransactionBuilder`

Builder for creating a transaction with options.

| Signature | Description |
|---|---|
| `timeout(seconds: float) -> TransactionBuilder` | — |
| `isolation(level: str) -> TransactionBuilder` | — |
| `start() -> Transaction` | — |

---

## `TxAppenderBuilder`

Builder for configuring a StreamingAppender within a transaction.

| Signature | Description |
|---|---|
| `batch_size(size: int) -> TxAppenderBuilder` | — |
| `defer_vector_indexes(defer: bool) -> TxAppenderBuilder` | — |
| `max_buffer_size_bytes(size: int) -> TxAppenderBuilder` | — |
| `build() -> StreamingAppender` | — |

---

## `TxBulkWriterBuilder`

Builder for configuring bulk data loading within a transaction.

| Signature | Description |
|---|---|
| `defer_vector_indexes(defer: bool) -> TxBulkWriterBuilder` | — |
| `defer_scalar_indexes(defer: bool) -> TxBulkWriterBuilder` | — |
| `batch_size(size: int) -> TxBulkWriterBuilder` | — |
| `async_indexes(async_: bool) -> TxBulkWriterBuilder` | — |
| `validate_constraints(validate: bool) -> TxBulkWriterBuilder` | — |
| `max_buffer_size_bytes(size: int) -> TxBulkWriterBuilder` | — |
| `on_progress(callback: Callable[[BulkProgress], None]) -> TxBulkWriterBuilder` | — |
| `build() -> BulkWriter` | — |

---

## `TxExecuteBuilder`

Fluent builder for mutations within a transaction.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> TxExecuteBuilder` | — |
| `timeout(seconds: float) -> TxExecuteBuilder` | — |
| `run() -> ExecuteResult` | — |
| `profile() -> tuple[ExecuteResult, ProfileOutput]` | — |

---

## `TxLocyBuilder`

Fluent builder for Locy evaluation within a transaction.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> TxLocyBuilder` | — |
| `timeout(seconds: float) -> TxLocyBuilder` | — |
| `max_iterations(n: int) -> TxLocyBuilder` | — |
| `with_config(config: dict[str, Any] | LocyConfig) -> TxLocyBuilder` | — |
| `cancellation_token(token: CancellationToken) -> TxLocyBuilder` | — |
| `run() -> LocyResult` | — |
| `profile() -> tuple[LocyResult, LocyProfileOutput]` | — |

---

## `TxQueryBuilder`

Fluent builder for read queries within a transaction.

| Signature | Description |
|---|---|
| `param(name: str, value: Any) -> TxQueryBuilder` | — |
| `timeout(seconds: float) -> TxQueryBuilder` | — |
| `cancellation_token(token: CancellationToken) -> TxQueryBuilder` | — |
| `fetch_all() -> QueryResult` | — |
| `fetch_one() -> dict[str, Any] | None` | — |
| `execute() -> ExecuteResult` | — |
| `cursor() -> QueryCursor` | — |

---

## `Uni`

The main synchronous Uni database interface.

| Signature | Description |
|---|---|
| `open(path: str) -> Uni` *(static)* | — |
| `temporary() -> Uni` *(static)* | — |
| `in_memory() -> Uni` *(static)* | — |
| `create(path: str) -> Uni` *(static)* | — |
| `open_existing(path: str) -> Uni` *(static)* | — |
| `builder() -> UniBuilder` *(static)* | — |
| `session() -> Session` | — |
| `session_template() -> SessionTemplateBuilder` | — |
| `schema() -> SchemaBuilder` | — |
| `label_exists(name: str) -> bool` | — |
| `edge_type_exists(name: str) -> bool` | — |
| `list_labels() -> list[str]` | — |
| `list_edge_types() -> list[str]` | — |
| `get_label_info(name: str) -> LabelInfo | None` | — |
| `get_edge_type_info(name: str) -> EdgeTypeInfo | None` | — |
| `load_schema(path: str) -> None` | — |
| `save_schema(path: str) -> None` | — |
| `rules() -> RuleRegistry` | — |
| `xervo() -> Xervo` | — |
| `compaction() -> Compaction` | — |
| `indexes() -> Indexes` | — |
| `uri() -> str` *(property)* | — |
| `flush() -> None` | — |
| `create_snapshot(name: str) -> str` | — |
| `list_snapshots() -> list[SnapshotInfo]` | — |
| `restore_snapshot(snapshot_id: str) -> None` | — |
| `list_forks() -> list[ForkInfo]` | — |
| `fork_info(name: str) -> ForkInfo | None` | — |
| `drop_fork(name: str) -> None` | — |
| `drop_fork_cascade(name: str) -> None` | — |
| `tag_fork(fork_name: str, tag: str) -> None` | — |
| `untag_fork(fork_name: str, tag: str) -> None` | — |
| `list_fork_tags(fork_name: str) -> list[str]` | — |
| `diff_fork_primary(fork_name: str) -> ForkDiff` | — |
| `diff_forks(a: str, b: str) -> ForkDiff` | — |
| `promote_from_fork(fork_name: str, patterns: list[PromotePattern]) -> PromoteReport` | — |
| `promote_from_fork_with_options(fork_name: str, patterns: list[PromotePattern], options: PromoteOptions) -> PromoteReport` | — |
| `metrics() -> DatabaseMetrics` | — |
| `config() -> dict[str, Any]` | — |
| `write_lease() -> WriteLease | None` | — |
| `shutdown() -> None` | — |
| `load_rhai_plugin(script: str, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `load_wasm_component(wasm_bytes: bytes, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `load_wasm_extism(wasm_bytes: bytes, grants: list[str] | None=None) -> dict[str, Any]` | — |
| `__enter__() -> Uni` | — |
| `__exit__(exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> bool` | — |

---

## `UniBuilder`

Builder for creating or opening a Uni database.

| Signature | Description |
|---|---|
| `open(path: str) -> UniBuilder` *(static)* | — |
| `create(path: str) -> UniBuilder` *(static)* | — |
| `open_existing(path: str) -> UniBuilder` *(static)* | — |
| `temporary() -> UniBuilder` *(static)* | — |
| `in_memory() -> UniBuilder` *(static)* | — |
| `hybrid(local_path: str, remote_url: str) -> UniBuilder` | — |
| `cache_size(bytes: int) -> UniBuilder` | — |
| `parallelism(n: int) -> UniBuilder` | — |
| `schema_file(path: str) -> UniBuilder` | — |
| `xervo_catalog_from_str(json: str) -> UniBuilder` | — |
| `xervo_catalog_from_file(path: str) -> UniBuilder` | — |
| `xervo_runtime(runtime: ModelRuntime) -> UniBuilder` | — |
| `cloud_config(config: dict[str, Any]) -> UniBuilder` | — |
| `config(config: dict[str, Any]) -> UniBuilder` | — |
| `batch_size(size: int) -> UniBuilder` | — |
| `wal_enabled(enabled: bool) -> UniBuilder` | — |
| `read_only() -> UniBuilder` | — |
| `skip_invalid_locy_rules(skip: bool) -> UniBuilder` | — |
| `write_lease(lease: WriteLease) -> UniBuilder` | — |
| `strict_schema(enabled: bool) -> UniBuilder` | — |
| `max_forks(cap: int | None) -> UniBuilder` | — |
| `fork_default_ttl(ttl: timedelta | None) -> UniBuilder` | — |
| `fork_sweeper_interval(interval: timedelta) -> UniBuilder` | — |
| `disable_fork_sweeper(disabled: bool) -> UniBuilder` | — |
| `build() -> Uni` | — |

---

## `UniCancelledError` — extends `UniError`

Operation was cancelled.

---

## `UniCommitTimeoutError` — extends `UniError`

Transaction commit timed out waiting for the writer lock.

---

## `UniConstraintConflictError` — extends `UniError`

Commit-time uniqueness race (e.g. concurrent MERGE on the same key).

---

## `UniConstraintError` — extends `UniError`

Constraint violation.

---

## `UniDatabaseLockedError` — extends `UniError`

Database is locked by another process.

---

## `UniEdgeTypeAlreadyExistsError` — extends `UniError`

Edge type already exists in schema.

---

## `UniEdgeTypeNotFoundError` — extends `UniError`

Edge type not found in schema.

---

## `UniError` — extends `Exception`

Base exception for all Uni database errors.

---

## `UniForkAlreadyExistsError` — extends `UniError`

`Session.fork(name).new_()` called against an existing fork.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |

---

## `UniForkBudgetExceededError` — extends `UniError`

`Session.fork(name)` refused because `max_forks` is at capacity.

**Attributes**

| Name | Type |
|---|---|
| `current` | `int` |
| `max` | `int` |

---

## `UniForkCorruptRegistryError` — extends `UniError`

Fork registry on disk is malformed.

---

## `UniForkHasChildrenError` — extends `UniError`

`drop_fork` refused because nested children exist.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `children` | `list[str]` |

---

## `UniForkInUseError` — extends `UniError`

Drop refused because forked sessions are still alive.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `holder_count` | `int` |

---

## `UniForkInflightTxError` — extends `UniError`

Drop refused because a transaction has uncommitted mutations.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |

---

## `UniForkLifecycleError` — extends `UniError`

A 2PC step on a fork lifecycle operation failed.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |
| `stage` | `str` |

---

## `UniForkNotFoundError` — extends `UniError`

Fork with the given name does not exist.

**Attributes**

| Name | Type |
|---|---|
| `name` | `str` |

---

## `UniForkSubtreeInUseError` — extends `UniError`

`drop_fork_cascade` refused because the subtree has live sessions / open tx.

**Attributes**

| Name | Type |
|---|---|
| `blockers` | `list[str]` |

---

## `UniHookRejectedError` — extends `UniError`

A session hook rejected the operation.

---

## `UniIOError` — extends `UniError`

I/O error.

---

## `UniId`

Universal content-addressed identifier (SHA3-256, multibase-encoded).

| Signature | Description |
|---|---|
| `to_multibase() -> str` | — |
| `as_bytes() -> bytes` | — |

---

## `UniIndexNotFoundError` — extends `UniError`

Index not found.

---

## `UniInternalError` — extends `UniError`

Internal error.

---

## `UniInvalidArgumentError` — extends `UniError`

Invalid argument.

---

## `UniInvalidIdentifierError` — extends `UniError`

Invalid identifier name.

---

## `UniLabelAlreadyExistsError` — extends `UniError`

Label already exists in schema.

---

## `UniLabelNotFoundError` — extends `UniError`

Label not found in schema.

---

## `UniLockTimeoutError` — extends `UniError`

Timed out waiting for a FOR UPDATE row lock. Retriable.

---

## `UniLocyCompileError` — extends `UniError`

Locy program compilation error.

---

## `UniLocyIncompleteError` — extends `UniError`

Locy evaluation stopped early (timeout or iteration/depth limit).

**Attributes**

| Name | Type |
|---|---|
| `reason` | `str` |
| `elapsed_ms` | `int` |
| `limit_ms` | `int` |
| `max_iterations` | `int` |
| `completed_strata` | `int` |
| `total_strata` | `int` |
| `incomplete_rules` | `list[str]` |
| `skipped_rules` | `list[str]` |
| `complement_rules_affected` | `list[str]` |

---

## `UniLocyRuntimeError` — extends `UniError`

Locy program runtime error.

---

## `UniMemoryLimitExceededError` — extends `UniError`

Query exceeded its memory limit.

---

## `UniNotFoundError` — extends `UniError`

Database path does not exist.

---

## `UniParseError` — extends `UniError`

Cypher or Locy parse error.

---

## `UniPermissionDeniedError` — extends `UniError`

Permission denied.

---

## `UniPropertyNotFoundError` — extends `UniError`

Property not found on entity.

---

## `UniQueryError` — extends `UniError`

Query execution error.

---

## `UniReadOnlyError` — extends `UniError`

Operation not supported on read-only database.

---

## `UniRuleConflictError` — extends `UniError`

Locy rule conflict during promotion.

---

## `UniSchemaError` — extends `UniError`

Schema definition or migration error.

---

## `UniSnapshotNotFoundError` — extends `UniError`

Snapshot not found.

---

## `UniStaleDerivedFactsError` — extends `UniError`

Derived facts are stale relative to the current database version.

---

## `UniStorageError` — extends `UniError`

Storage layer error.

---

## `UniTimeoutError` — extends `UniError`

Operation timed out.

---

## `UniTransactionAlreadyCompletedError` — extends `UniError`

Transaction has already been committed or rolled back.

---

## `UniTransactionConflictError` — extends `UniError`

Transaction serialization conflict.

---

## `UniTransactionError` — extends `UniError`

Transaction error.

---

## `UniTransactionExpiredError` — extends `UniError`

Transaction exceeded its deadline.

---

## `UniTypeError` — extends `UniError`

Type mismatch error.

---

## `UniWriteContextAlreadyActiveError` — extends `UniError`

A write context is already active on the session.

---

## `Value`

A tagged Uni value — opt-in wrapper for explicit type discrimination.

| Signature | Description |
|---|---|
| `null() -> Value` *(static)* | — |
| `bool(v: bool) -> Value` *(static)* | — |
| `int(v: int) -> Value` *(static)* | — |
| `float(v: float) -> Value` *(static)* | — |
| `string(v: str) -> Value` *(static)* | — |
| `bytes(v: bytes) -> Value` *(static)* | — |
| `vector(v: builtins.list[builtins.float]) -> Value` *(static)* | — |
| `binary_vector(v: builtins.bytes) -> Value` *(static)* | — |
| `btic(literal: str) -> Value` *(static)* | — |
| `type_name() -> str` *(property)* | — |
| `is_null() -> builtins.bool` | — |
| `is_bool() -> builtins.bool` | — |
| `is_int() -> builtins.bool` | — |
| `is_float() -> builtins.bool` | — |
| `is_string() -> builtins.bool` | — |
| `is_btic() -> builtins.bool` | — |
| `to_python() -> Any` | — |

---

## `VertexDiff`

The vertex side of a `ForkDiff`.

| Signature | Description |
|---|---|
| `is_empty() -> bool` | — |
| `total_rows() -> int` | — |

**Attributes**

| Name | Type |
|---|---|
| `added` | `list[DiffVertex]` |
| `deleted` | `list[DiffVertex]` |
| `changed` | `list[VertexPropertyChange]` |

---

## `VertexPropertyChange`

A vertex's property changes (paired by UID).

**Attributes**

| Name | Type |
|---|---|
| `label` | `str` |
| `uid` | `str` |
| `changes` | `list[PropertyChange]` |

---

## `Vid`

Vertex identifier (64-bit sequential ID).

| Signature | Description |
|---|---|
| `as_int() -> int` | — |

---

## `WatchBuilder`

Builder for configuring a commit watch stream.

| Signature | Description |
|---|---|
| `labels(labels: list[str]) -> WatchBuilder` | — |
| `edge_types(types: list[str]) -> WatchBuilder` | — |
| `debounce(seconds: float) -> WatchBuilder` | — |
| `exclude_session(session_id: str) -> WatchBuilder` | — |
| `build() -> CommitStream` | — |
| `build_async() -> AsyncCommitStream` | — |

---

## `WriteLease`

Write lease configuration for multi-agent coordination.

| Signature | Description |
|---|---|
| `LOCAL() -> WriteLease` *(static)* | — |
| `DYNAMODB(table: str) -> WriteLease` *(static)* | — |

---

## `Xervo`

Synchronous facade for embedding and text generation.

| Signature | Description |
|---|---|
| `is_available() -> bool` | — |
| `raw_runtime() -> ModelRuntime | None` | — |
| `prefetch(aliases: list[str]) -> None` | — |
| `prefetch_all() -> None` | — |
| `embed(alias: str, texts: list[str]) -> list[list[float]]` | — |
| `generate(alias: str, messages: list[Message | dict[str, Any]], max_tokens: int | None=None, temperature: float | None=None, top_p: float | None=None) -> GenerationResult` | — |
| `generate_text(alias: str, prompt: str, max_tokens: int | None=None, temperature: float | None=None, top_p: float | None=None) -> GenerationResult` | — |

---

