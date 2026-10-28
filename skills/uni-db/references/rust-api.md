# Uni Rust API Reference

**Crate:** `uni-db` | **Target:** current 5.0.0 source; selected API signatures below

---

## 1. Quick Start

```rust
use uni_db::{Uni, DataType, Value};

#[tokio::main]
async fn main() -> uni_db::Result<()> {
    let db = Uni::open("./my_db").build().await?;
    db.schema()
        .label("Person")
            .property("name", DataType::String)
            .property("age", DataType::Int64)
        .apply().await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
    tx.commit().await?;

    let result = session.query("MATCH (p:Person) RETURN p.name, p.age").await?;
    for row in result.rows() {
        println!("{}: {}", row.get::<String>("p.name")?, row.get::<i64>("p.age")?);
    }
    db.shutdown().await
}
```

---

## 2. Architecture Overview

| Scope | Created via | Purpose |
|---|---|---|
| `Uni` | `Uni::open()` etc. | Lifecycle, schema DDL, admin, facade accessors. NO direct query/mutation. |
| `Session` | `db.session()` | Read scope: query, locy, params, hooks, tx factory. Cheap (sync, no I/O). |
| `Transaction` | `session.tx()` | Write scope: execute, bulk, locy DERIVE, commit/rollback. Drop = auto-rollback. |

**Facade accessors** return borrowed views: `params()`, `rules()`, `compaction()`, `indexes()`, `functions()`, `xervo()`.

**Builder pattern** -- `*_with()` returns fluent builder; terminal method executes:

| Builder | Created By | Terminals |
|---|---|---|
| `QueryBuilder` | `session.query_with()` | `.fetch_all()`, `.fetch_one()`, `.cursor()`, `.explain()`, `.profile()` |
| `LocyBuilder` | `session.locy_with()` | `.run()`, `.explain()` |
| `TxQueryBuilder` | `tx.query_with()` | `.fetch_all()`, `.fetch_one()`, `.cursor()`, `.execute()` |
| `ExecuteBuilder` | `tx.execute_with()` | `.run()`, `.profile()` |
| `ApplyBuilder` | `tx.apply_with()` | `.run()` |
| `TxLocyBuilder` | `tx.locy_with()` | `.run()` |
| `TransactionBuilder` | `session.tx_with()` | `.start()` |
| `BulkWriterBuilder` | `tx.bulk_writer()` | `.build()` |
| `AppenderBuilder` | `tx.appender()` | `.build()` |
| `SchemaBuilder` | `db.schema()` | `.apply()` |

---

## 3. UniBuilder

```rust
impl Uni {
    pub fn open(uri: impl Into<String>) -> UniBuilder;      // open or create
    pub fn open_existing(uri: impl Into<String>) -> UniBuilder;
    pub fn create(uri: impl Into<String>) -> UniBuilder;     // error if exists
    pub fn temporary() -> UniBuilder;                        // auto-cleaned on drop
    pub fn in_memory() -> UniBuilder;                        // alias for temporary; uses a temporary directory
}

impl UniBuilder {
    pub fn config(mut self, config: UniConfig) -> Self;
    pub fn schema_file(mut self, path: impl AsRef<Path>) -> Self;
    pub fn xervo_catalog(mut self, catalog: Vec<ModelAliasSpec>) -> Self;
    pub fn remote_storage(mut self, remote_url: &str, config: CloudStorageConfig) -> Self;
    pub fn read_only(mut self) -> Self;
    pub fn write_lease(mut self, lease: WriteLease) -> Self;
    pub async fn build(self) -> Result<Uni>;
    pub fn build_sync(self) -> Result<Uni>;       // blocking, creates own runtime
}
```

---

## 4. Uni (Database Handle)

```rust
impl Uni {
    // Session factories
    pub fn session(&self) -> Session;
    pub fn session_template(&self) -> SessionTemplateBuilder;

    // Schema DDL
    pub fn schema(&self) -> SchemaBuilder<'_>;
    pub async fn load_schema(&self, path: impl AsRef<Path>) -> Result<()>;
    pub async fn save_schema(&self, path: impl AsRef<Path>) -> Result<()>;

    // Schema inspection
    pub async fn label_exists(&self, name: &str) -> Result<bool>;
    pub async fn edge_type_exists(&self, name: &str) -> Result<bool>;
    pub async fn list_labels(&self) -> Result<Vec<String>>;
    pub async fn list_edge_types(&self) -> Result<Vec<String>>;
    pub async fn get_label_info(&self, name: &str) -> Result<Option<LabelInfo>>;
    pub async fn get_edge_type_info(&self, name: &str) -> Result<Option<EdgeTypeInfo>>;

    // Facade accessors
    pub fn rules(&self) -> RuleRegistry<'_>;       // global rule registry
    pub fn compaction(&self) -> Compaction<'_>;
    pub fn indexes(&self) -> Indexes<'_>;
    pub fn functions(&self) -> Functions<'_>;
    pub fn xervo(&self) -> UniXervo;

    // Storage admin
    pub async fn flush(&self) -> Result<()>;
    pub async fn create_snapshot(&self, name: &str) -> Result<String>;
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotManifest>>;
    pub async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()>;

    // Metrics & config
    pub fn metrics(&self) -> DatabaseMetrics;
    pub fn config(&self) -> &UniConfig;

    // Lifecycle
    pub async fn shutdown(self) -> Result<()>;
}
```

---

## 5. Session

```rust
impl Session {
    pub fn params(&self) -> Params<'_>;

    // Cypher reads
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;
    pub fn query_with(&self, cypher: &str) -> QueryBuilder<'_>;

    // Locy
    pub async fn locy(&self, program: &str) -> Result<LocyResult>;
    pub fn locy_with(&self, program: &str) -> LocyBuilder<'_>;
    pub fn rules(&self) -> RuleRegistry<'_>;
    pub fn compile_locy(&self, program: &str) -> Result<CompiledProgram>;

    // Transaction factories
    pub async fn tx(&self) -> Result<Transaction>;
    pub fn tx_with(&self) -> TransactionBuilder<'_>;

    // Version pinning
    pub async fn pin_to_version(&mut self, snapshot_id: &str) -> Result<()>;
    pub async fn pin_to_timestamp(&mut self, ts: chrono::DateTime<chrono::Utc>) -> Result<()>;
    pub async fn refresh(&mut self) -> Result<()>;
    pub fn is_pinned(&self) -> bool;

    // Prepared statements
    pub async fn prepare(&self, cypher: &str) -> Result<PreparedQuery>;
    pub async fn prepare_locy(&self, program: &str) -> Result<PreparedLocy>;

    // Hooks (add_hook, remove_hook, list_hooks, clear_hooks)
    pub fn add_hook(&mut self, name: impl Into<String>, hook: impl SessionHook + 'static);
    pub fn remove_hook(&mut self, name: &str) -> bool;

    // Commit notifications
    pub fn watch(&self) -> CommitStream;           // all commits
    pub fn watch_with(&self) -> WatchBuilder;      // filtered/debounced

    // Observability & cancellation
    pub fn id(&self) -> &str;
    pub fn metrics(&self) -> SessionMetrics;
    pub fn cancel(&self);
}
```

---

## 6. Transaction

```rust
impl Transaction {
    // Reads (sees shared DB + uncommitted writes)
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;
    pub fn query_with(&self, cypher: &str) -> TxQueryBuilder<'_>;

    // Writes
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;
    pub fn execute_with(&self, cypher: &str) -> ExecuteBuilder<'_>;

    // Apply DerivedFactSet
    pub async fn apply(&self, derived: DerivedFactSet) -> Result<ApplyResult>;
    pub fn apply_with(&self, derived: DerivedFactSet) -> ApplyBuilder<'_>;

    // Bulk
    pub async fn bulk_insert_vertices(&self, label: &str, props: Vec<Properties>) -> Result<Vec<Vid>>;
    pub async fn bulk_insert_edges(&self, edge_type: &str, edges: Vec<(Vid, Vid, Properties)>) -> Result<()>;
    pub fn bulk_writer(&self) -> BulkWriterBuilder;
    pub fn appender(&self, label: &str) -> AppenderBuilder;

    // Locy (DERIVE auto-applies to tx's private L0)
    pub async fn locy(&self, program: &str) -> Result<LocyResult>;
    pub fn locy_with(&self, program: &str) -> TxLocyBuilder<'_>;
    pub fn rules(&self) -> RuleRegistry<'_>;

    // Lifecycle (Drop auto-rollbacks if dirty)
    pub async fn commit(mut self) -> Result<CommitResult>;
    pub fn rollback(mut self);
    pub fn is_dirty(&self) -> bool;
    pub fn id(&self) -> &str;
    pub fn started_at_version(&self) -> u64;
    pub fn cancel(&self);
}
```

---

## 7. Query & Execute Builders

### QueryBuilder (`session.query_with()`)

```rust
impl<'a> QueryBuilder<'a> {
    pub fn param<K: Into<String>, V: Into<Value>>(self, key: K, value: V) -> Self;
    pub fn params<'p>(self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self;
    pub fn timeout(self, duration: Duration) -> Self;
    pub fn max_memory(self, bytes: usize) -> Self;
    pub fn cancellation_token(self, token: CancellationToken) -> Self;
    // Terminals
    pub async fn fetch_all(self) -> Result<QueryResult>;
    pub async fn fetch_one(self) -> Result<Option<Row>>;
    pub async fn cursor(self) -> Result<QueryCursor>;
    pub async fn explain(self) -> Result<ExplainOutput>;
    pub async fn profile(self) -> Result<(QueryResult, ProfileOutput)>;
}
```

### LocyBuilder (`session.locy_with()`)

```rust
impl<'a> LocyBuilder<'a> {
    pub fn param(self, name: impl Into<String>, value: impl Into<Value>) -> Self;
    pub fn params<'p>(self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self;
    pub fn params_map(self, params: HashMap<String, Value>) -> Self;
    pub fn timeout(self, duration: Duration) -> Self;
    pub fn max_iterations(self, n: usize) -> Self;
    pub fn cancellation_token(self, token: CancellationToken) -> Self;
    pub fn with_config(self, config: LocyConfig) -> Self;
    pub async fn run(self) -> Result<LocyResult>;
    pub fn explain(self) -> Result<LocyExplainOutput>;  // synchronous
}
```

`.cancellation_token()` here races the token against the whole evaluation, so a long-running fixpoint is cancellable, not merely at statement boundaries. Cancelling yields `UniError::Cancelled`. The same holds for the enclosing scope: `Transaction::cancel()` reaches `tx.locy()` / `tx.locy_with()` and `tx.apply()` in addition to the transaction's Cypher statements.

### Other builders (tx-level)

- **`ExecuteBuilder`** (`tx.execute_with()`): `.param()`, `.params()`, `.timeout()` -> `.run()` -> `ExecuteResult` | `.profile()` -> `(ExecuteResult, ProfileOutput)`
- **`TxQueryBuilder`** (`tx.query_with()`): `.param()`, `.timeout()`, `.cancellation_token()` -> `.execute()` / `.fetch_all()` / `.fetch_one()` / `.cursor()`
- **`ApplyBuilder`** (`tx.apply_with()`): `.require_fresh()`, `.max_version_gap(n)` -> `.run()` -> `ApplyResult`
- **`TxLocyBuilder`** (`tx.locy_with()`): same as `LocyBuilder`; `.run()` auto-applies DERIVE to tx's L0
- **`TransactionBuilder`** (`session.tx_with()`): `.timeout(Duration)`, `.isolation(IsolationLevel)` -> `.start()` -> `Transaction`

---

## 8. Schema Types

```rust
impl<'a> SchemaBuilder<'a> {
    pub fn current(&self) -> Arc<Schema>;
    pub fn with_changes(self, changes: Vec<SchemaChange>) -> Self;
    pub fn label(self, name: &str) -> LabelBuilder<'a>;
    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilder<'a>;
    pub async fn apply(self) -> Result<()>;
}

impl<'a> LabelBuilder<'a> {
    pub fn property(self, name: &str, data_type: DataType) -> Self;
    pub fn property_nullable(self, name: &str, data_type: DataType) -> Self;
    pub fn vector(self, name: &str, dimensions: usize) -> Self;
    pub fn index(self, property: &str, index_type: IndexType) -> Self;
    pub fn done(self) -> SchemaBuilder<'a>;        // return to parent
    pub fn label(self, name: &str) -> LabelBuilder<'a>;   // chain next label
    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilder<'a>;
    pub async fn apply(self) -> Result<()>;        // shortcut
}

// EdgeTypeBuilder: same as LabelBuilder minus .vector()/.index(), plus same chaining/apply
```

### DataType

```rust
pub enum DataType {
    String, Int32, Int64, Float32, Float64, Bool,
    Timestamp, Date, Time, DateTime, Duration, CypherValue, Bytes,
    Point(PointType),              // Geographic | Cartesian2D | Cartesian3D
    Vector { dimensions: usize },
    Crdt(CrdtType),                // GCounter | GSet | ORSet | LWWRegister | LWWMap | Rga | VectorClock | VCRegister
    List(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
}
```

### IndexType

```rust
pub enum IndexType {
    Vector(VectorIndexCfg),  FullText,  Scalar(ScalarType),  Inverted(InvertedIndexConfig),
}
pub struct VectorIndexCfg {
    pub algorithm: VectorAlgo,  // Flat | IvfFlat | IvfPq | IvfSq | IvfRq | HnswFlat | Hnsw | HnswSq | HnswPq
    pub metric: VectorMetric,   // Cosine | L2 | Dot
    pub embedding: Option<EmbeddingCfg>,
}
pub enum ScalarType { BTree, Hash, Bitmap, LabelList }
```

### SchemaChange

```rust
pub enum SchemaChange {
    AddLabel { name },
    AddProperty { label_or_type, name, data_type: DataType, nullable: bool },
    AddIndex(IndexDefinition),
    AddEdgeType { name, from_labels: Vec<String>, to_labels: Vec<String> },
}
```

---

## 9. BulkWriter & StreamingAppender

### BulkWriterBuilder (`tx.bulk_writer()`)

```rust
impl BulkWriterBuilder {
    pub fn defer_vector_indexes(self, defer: bool) -> Self;   // default: true
    pub fn defer_scalar_indexes(self, defer: bool) -> Self;
    pub fn batch_size(self, size: usize) -> Self;             // default: 10,000
    pub fn on_progress<F: Fn(BulkProgress) + Send + 'static>(self, f: F) -> Self;
    pub fn async_indexes(self, async_: bool) -> Self;
    pub fn validate_constraints(self, validate: bool) -> Self;
    pub fn max_buffer_size_bytes(self, size: usize) -> Self;
    pub fn build(self) -> Result<BulkWriter>;
}
```

### BulkWriter

```rust
impl BulkWriter {
    pub async fn insert_vertices(&mut self, label: &str, vertices: impl IntoArrow) -> Result<Vec<Vid>>;
    pub async fn insert_edges(&mut self, edge_type: &str, edges: Vec<EdgeData>) -> Result<()>;
    pub async fn commit(mut self) -> Result<BulkStats>;
    pub async fn abort(mut self) -> Result<()>;
    pub fn stats(&self) -> &BulkStats;
}
```

**Gotcha:** BulkWriter bypasses normal isolation. Already-flushed batches cannot be rolled back.

### StreamingAppender (`tx.appender(label)`)

```rust
impl AppenderBuilder {
    pub fn batch_size(self, size: usize) -> Self;
    pub fn defer_vector_indexes(self, defer: bool) -> Self;
    pub fn max_buffer_size_bytes(self, size: usize) -> Self;
    pub fn build(self) -> Result<StreamingAppender>;
}
impl StreamingAppender {
    pub async fn append(&mut self, properties: impl Into<HashMap<String, Value>>) -> Result<()>;
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    pub async fn finish(mut self) -> Result<BulkStats>;
    pub fn abort(mut self);
    pub fn buffered_count(&self) -> usize;
}
```

**Supporting types:** `EdgeData { src_vid, dst_vid, properties }`, `BulkStats { vertices_inserted, edges_inserted, indexes_rebuilt, duration, index_build_duration, indexes_pending }`.

---

## 10. Facade Types

### Params (`session.params()`)

```rust
impl<'a> Params<'a> {
    pub fn set<K: Into<String>, V: Into<Value>>(&self, key: K, value: V);
    pub fn get(&self, key: &str) -> Option<Value>;
    pub fn unset(&self, key: &str) -> Option<Value>;
    pub fn get_all(&self) -> HashMap<String, Value>;
    pub fn set_all<I, K, V>(&self, params: I)
        where I: IntoIterator<Item = (K, V)>, K: Into<String>, V: Into<Value>;
}
```

### RuleRegistry (`db.rules()` / `session.rules()` / `tx.rules()`)

```rust
impl<'a> RuleRegistry<'a> {
    pub fn register(&self, program: &str) -> Result<()>;
    pub fn remove(&self, name: &str) -> Result<bool>;
    pub fn list(&self) -> Vec<String>;
    pub fn get(&self, name: &str) -> Option<RuleInfo>;   // { name, clause_count, is_recursive }
    pub fn clear(&self);
    pub fn count(&self) -> usize;
}
```

### Compaction (`db.compaction()`)

`.compact(name)` -> `CompactionStats`, `.wait()` -- waits for background tasks.

### Indexes (`db.indexes()`)

`.list(label: Option<&str>)`, `.rebuild(label, background)` -> `Option<task_id>`, `.rebuild_status()`, `.retry_failed()`.

### Functions (`db.functions()`)

`.register(name, |&[Value]| -> Result<Value>)`, `.remove(name)`, `.list()`.

### UniXervo (`db.xervo()`)

`.is_available()`, `.embed(alias, texts)` -> `Vec<Vec<f32>>`, `.generate(alias, messages, options)`, `.generate_text(alias, messages, options)`.

---

## 11. Result Types

### QueryResult / Row

```rust
impl QueryResult {
    pub fn columns(&self) -> &[String];
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn rows(&self) -> &[Row];
    pub fn into_rows(self) -> Vec<Row>;
    pub fn iter(&self) -> impl Iterator<Item = &Row>;
    pub fn warnings(&self) -> &[QueryWarning];
    pub fn metrics(&self) -> &QueryMetrics;
}
impl Row {
    pub fn get<T: FromValue>(&self, column: &str) -> Result<T>;
    pub fn get_idx<T: FromValue>(&self, index: usize) -> Result<T>;
    pub fn try_get<T: FromValue>(&self, column: &str) -> Option<T>;
    pub fn value(&self, column: &str) -> Option<&Value>;
    pub fn columns(&self) -> &[String];
    pub fn values(&self) -> &[Value];
    pub fn as_map(&self) -> HashMap<&str, &Value>;
    pub fn to_json(&self) -> serde_json::Value;
}
```

### Value / Node / Edge / Path

```rust
pub enum Value {
    Null, Bool(bool), Int(i64), Float(f64), String(String), Bytes(Vec<u8>),
    List(Vec<Value>), Map(HashMap<String, Value>),
    Node(Node), Edge(Edge), Path(Path), Vector(Vec<f32>), Temporal(TemporalValue),
}
pub struct Node { pub vid: Vid, pub labels: Vec<String>, pub properties: HashMap<String, Value> }
pub struct Edge { pub eid: Eid, pub edge_type: String, pub src: Vid, pub dst: Vid, pub properties: HashMap<String, Value> }
pub struct Path { pub nodes: Vec<Node>, pub edges: Vec<Edge> }
```

### ExecuteResult

```rust
impl ExecuteResult {
    pub fn affected_rows(&self) -> usize;
    pub fn nodes_created(&self) -> usize;       pub fn nodes_deleted(&self) -> usize;
    pub fn relationships_created(&self) -> usize;  pub fn relationships_deleted(&self) -> usize;
    pub fn properties_set(&self) -> usize;
    pub fn labels_added(&self) -> usize;        pub fn labels_removed(&self) -> usize;
    pub fn metrics(&self) -> &QueryMetrics;
}
```

### CommitResult / ApplyResult

```rust
pub struct CommitResult {
    pub mutations_committed: usize, pub rules_promoted: usize,
    pub version: u64, pub started_at_version: u64, pub wal_lsn: u64,
    pub duration: Duration, pub rule_promotion_errors: Vec<RulePromotionError>,
}
impl CommitResult { pub fn version_gap(&self) -> u64; }

pub struct ApplyResult { pub facts_applied: usize, pub version_gap: u64 }
```

### QueryMetrics / QueryCursor

`QueryMetrics` fields: `parse_time`, `plan_time`, `exec_time`, `total_time`, `rows_returned`, `rows_scanned`, `bytes_read`, `plan_cache_hit`, `l0_reads`, `storage_reads`, `cache_hits`.

`QueryCursor`: `.columns()`, `.next_batch()` -> `Option<Result<Vec<Row>>>`, `.collect_remaining()` -> `Result<Vec<Row>>`.

---

## 12. Locy Types

### LocyResult (API wrapper -- Deref to inner)

```rust
// API wrapper
impl LocyResult {
    pub fn metrics(&self) -> &QueryMetrics;
    pub fn derived(&self) -> Option<&DerivedFactSet>;
    pub fn into_inner(self) -> uni_locy::LocyResult;
    pub fn into_parts(self) -> (uni_locy::LocyResult, QueryMetrics);
}
// Inner (via Deref)
impl LocyResult {
    pub async fn derived_rules(&self) -> Result<&HashMap<String, Vec<FactRow>>>;
    pub async fn derived_rule(&self, rule: &str) -> Result<Option<&Vec<FactRow>>>;
    pub fn evaluated_rules(&self) -> &HashMap<String, Vec<FactRow>>;
    pub fn lazily_derived_rules(&self) -> &[String];
    pub fn rows(&self) -> Option<&Vec<FactRow>>;
    pub fn columns(&self) -> Option<Vec<String>>;
    pub fn stats(&self) -> &LocyStats;
    pub fn iterations(&self) -> usize;
    pub fn warnings(&self) -> &[RuntimeWarning];
    pub fn has_warning(&self, code: &RuntimeWarningCode) -> bool;
}
pub type FactRow = HashMap<String, Value>;
```

### DerivedFactSet

```rust
pub struct DerivedFactSet {
    pub vertices: HashMap<String, Vec<Properties>>,
    pub edges: Vec<DerivedEdge>,
    pub stats: LocyStats,
    pub evaluated_at_version: u64,
}
impl DerivedFactSet { pub fn fact_count(&self) -> usize; pub fn is_empty(&self) -> bool; }
```

### LocyStats

```rust
pub struct LocyStats {
    pub strata_evaluated: usize, pub total_iterations: usize,
    pub derived_nodes: usize, pub derived_edges: usize,
    pub evaluation_time: Duration, pub queries_executed: usize,
    pub mutations_executed: usize, pub peak_memory_bytes: usize,
}
```

### CommandResult

Variants: `Query(Vec<FactRow>)`, `Assume { results: Vec<CommandResult>, derived: HashMap<String, Vec<FactRow>> }`, `Explain(DerivationNode)`, `Abduce(AbductionResult)`, `Derive { affected }`, `Cypher(Vec<FactRow>)`. Accessors: `.as_explain()`, `.as_query()`, `.as_abduce()`, `.as_assume()`, `.rows()` (searches an `Assume` depth first).

### DerivationNode

Fields: `rule`, `clause_index`, `priority`, `bindings: HashMap<String, Value>`, `along_values`, `children: Vec<DerivationNode>`, `graph_fact`, `approximate`, `proof_probability`.

### RuntimeWarning

Fields: `code: RuntimeWarningCode`, `message`, `rule_name`, `variable_count`, `key_group`. Codes: `SharedProbabilisticDependency`, `BddLimitExceeded`, `CrossGroupCorrelationNotExact`.

---

## 13. Configuration

`partial_lance_writes` is a deprecated no-op. Defaults and field semantics are
documented in `crates/uni-common/src/config.rs` and the website configuration
reference. `Uni::in_memory()` is an alias for a temporary-directory database.

```rust
pub struct UniConfig {
    pub cache_size: usize,
    pub parallelism: usize,
    pub batch_size: usize,
    pub execution_batch_size: Option<usize>,
    pub max_frontier_size: usize,
    pub auto_flush_threshold: usize,
    pub commit_channel_capacity: usize,
    pub auto_flush_interval: Option<Duration>,
    pub auto_flush_min_mutations: usize,
    pub wal_enabled: bool,
    pub compaction: CompactionConfig,
    pub throttle: WriteThrottleConfig,
    pub file_sandbox: FileSandboxConfig,
    pub query_timeout: Duration,
    pub commit_timeout: Duration,
    pub max_query_memory: usize,
    pub regex_backtrack_limit: usize,
    pub max_transaction_memory: usize,
    pub max_compaction_rows: usize,
    pub max_recursive_cte_iterations: usize,
    pub vector_knn_default_k: usize,
    pub lance_metadata_cache_bytes: Option<usize>,
    pub lance_index_cache_bytes: Option<usize>,
    pub object_store: ObjectStoreConfig,
    pub index_rebuild: IndexRebuildConfig,
    pub strict_schema: bool,
    pub partial_lance_writes: bool,
    pub defer_embeddings: bool,
    pub fork_fragment_warn_threshold: usize,
    pub tx_id_reservoir_batch: usize,
    pub async_flush_enabled: bool,
    pub max_pending_flushes: usize,
    pub flush_stream_timeout: Duration,
    pub drop_fork_drain_timeout: Duration,
    pub flush_drain_timeout: Duration,
    pub max_forks: Option<usize>,
    pub fork_default_ttl: Option<Duration>,
    pub fork_sweeper_interval: Duration,
    pub disable_fork_sweeper: bool,
    pub fork_index_build_threshold: u64,
    pub fork_index_builder_interval: Duration,
    pub disable_fork_index_builder: bool,
    pub ssi_enabled: bool,
    pub ec_flush_interval: Duration,
    pub ec_flush_threshold: usize,
}
```

| Sub-config | Key fields |
|---|---|
| `CompactionConfig` | `enabled`, `max_l1_runs`, `max_l1_size_bytes`, `max_l1_age`, `check_interval`, `worker_threads` |
| `WriteThrottleConfig` | `soft_limit`, `hard_limit`, `base_delay` |
| `IndexRebuildConfig` | `max_retries`, `retry_delay`, `growth_trigger_ratio`, `max_index_age`, `auto_rebuild_enabled` |
| `ObjectStoreConfig` | `connect_timeout`, `read_timeout`, `write_timeout`, `max_retries` |
| `FileSandboxConfig` | `enabled`, `allowed_paths: Vec<PathBuf>` |
| `CloudStorageConfig` | `S3 { bucket, region, endpoint, ... }`, `Gcs { bucket, ... }`, `Azure { container, account, ... }` |

Convenience: `CloudStorageConfig::s3_from_env(bucket)`, `::gcs_from_env(bucket)`, `::azure_from_env(container)`.

---

## 14. Blocking API

Entry point: `UniSync::new(Uni::open("./db").build_sync()?)?` or `UniSync::in_memory()`.

| Async | Sync |
|---|---|
| `Uni` | `UniSync` |
| `Session` | `SessionSync<'a>` |
| `Transaction` | `TransactionSync<'a>` |
| `QueryBuilder` | `QueryBuilderSync<'s, 'a>` |
| `LocyBuilder` | `LocyBuilderSync<'s, 'a>` |
| `ExecuteBuilder` | `ExecuteBuilderSync<'t, 'a>` |
| `TxQueryBuilder` | `TxQueryBuilderSync<'t, 'a>` |
| `SchemaBuilder` / `LabelBuilder` / `EdgeTypeBuilder` | `*Sync<'a>` variants |

All sync types mirror their async counterpart; `async fn` becomes `fn`.

---

## 15. Error Types

```rust
#[non_exhaustive]
pub enum UniError {
    // Resource
    NotFound { path }, LabelNotFound { label }, EdgeTypeNotFound { edge_type },
    PropertyNotFound { property, entity_type, label }, IndexNotFound { index },
    SnapshotNotFound { snapshot_id },
    // Schema
    Schema { message }, LabelAlreadyExists { label }, EdgeTypeAlreadyExists { edge_type },
    InvalidIdentifier { name, reason },
    // Query
    Parse { message, position, line, column, context },
    Query { message, query }, Type { expected, actual }, InvalidArgument { arg, message },
    // Transaction
    Transaction { message }, TransactionConflict { message }, TransactionAlreadyCompleted,
    TransactionExpired { tx_id, hint }, CommitTimeout { tx_id, hint },
    // Access control
    ReadOnly { operation }, PermissionDenied { action }, DatabaseLocked,
    WriteContextAlreadyActive { session_id, hint },
    // Resource limits
    MemoryLimitExceeded { limit_bytes }, Timeout { timeout_ms }, Cancelled,
    // Locy
    StaleDerivedFacts { version_gap }, RuleConflict { rule_name },
    // Other
    HookRejected { message }, Constraint { message },
    Storage { message, source }, Io(std::io::Error), Internal(anyhow::Error),
}
```

---

## 16. Examples

### Schema + parameterized query

```rust
let db = Uni::open("./products").build().await?;
db.schema()
    .label("Product")
        .property("name", DataType::String)
        .property("price", DataType::Float64)
        .vector("embedding", 384)
        .index("name", IndexType::Scalar(ScalarType::BTree))
    .apply().await?;

let session = db.session();
let tx = session.tx().await?;
tx.execute_with("CREATE (:Product {name: $name, price: $price})")
    .param("name", "Widget").param("price", 9.99).run().await?;
tx.commit().await?;

let result = session.query_with("MATCH (p:Product) WHERE p.price < $max RETURN p.name, p.price")
    .param("max", 20.0).timeout(Duration::from_secs(5)).fetch_all().await?;
```

### Bulk loading

```rust
let tx = session.tx().await?;
let mut writer = tx.bulk_writer()
    .batch_size(50_000)
    .defer_vector_indexes(true)
    .on_progress(|p| eprintln!("Loaded {} rows", p.rows_processed))
    .build()?;
writer.insert_vertices("Product", rows).await?;
let stats = writer.commit().await?;
println!("Inserted {} vertices in {:?}", stats.vertices_inserted, stats.duration);
```

### Locy with session parameters

```rust
session.params().set("threshold", 0.8);
let result = session.locy_with(r#"
    CREATE RULE high_score AS
        MATCH (n:Product) WHERE n.score > $threshold
        YIELD KEY n
    QUERY high_score RETURN n
"#).timeout(Duration::from_secs(30)).max_iterations(100).run().await?;
for fact in result.rows().unwrap_or(&vec![]) {
    println!("{:?}", fact["n"]);
}
```
