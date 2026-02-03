# Rust API Reference

Uni provides a comprehensive Rust API for embedding the graph database directly into your application. This reference covers all public APIs.

## Quick Start

```rust
use uni_db::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Open or create database
    let db = Uni::open("./my-graph")
        .schema_file("schema.json")
        .build()
        .await?;

    // Run a query
    let results = db.query("MATCH (p:Paper) WHERE p.year > 2020 RETURN p.title LIMIT 10").await?;

    for row in results.iter() {
        let title: String = row.get("p.title")?;
        println!("{}", title);
    }

    Ok(())
}
```

---

## Module: `uni_db`

The main entry point for the database.

### Uni

```rust
/// Main database instance
pub struct Uni {
    // Internal state
}

impl Uni {
    /// Open or create a database at the given path or object store URI
    pub fn open(uri: impl Into<String>) -> UniBuilder;

    /// Create an in-memory database
    pub fn in_memory() -> UniBuilder;

    /// Get current configuration
    pub fn config(&self) -> &UniConfig;

    /// Get current schema (read-only snapshot)
    pub fn get_schema(&self) -> Schema;

    /// Flush uncommitted changes to storage
    pub async fn flush(&self) -> Result<()>;
}
```

### UniBuilder

```rust
/// Fluent builder for database instances
pub struct UniBuilder {
    // Internal state
}

impl UniBuilder {
    /// Set schema from JSON file
    pub fn schema_file(self, path: impl AsRef<Path>) -> Self;

    /// Set configuration
    pub fn config(self, config: UniConfig) -> Self;

    /// Set cache size in bytes
    pub fn cache_size(self, bytes: usize) -> Self;

    /// Set parallelism (worker threads)
    pub fn parallelism(self, n: usize) -> Self;

    /// Build the database instance (async)
    pub async fn build(self) -> Result<Uni>;

    /// Build the database instance (blocking)
    pub fn build_sync(self) -> Result<Uni>;
}

// Example
let db = Uni::open("./data")
    .schema_file("schema.json")
    .cache_size(2 * 1024 * 1024 * 1024)  // 2 GB
    .parallelism(8)
    .build()
    .await?;
```

---

## Queries

### Basic Queries

```rust
impl Uni {
    /// Execute a Cypher query
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Execute a query with parameters
    pub fn query_with(&self, cypher: &str) -> QueryBuilder<'_>;

    /// Execute a mutation (CREATE, SET, DELETE, MERGE)
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;

    /// Execute query returning a cursor for streaming results
    pub async fn query_cursor(&self, cypher: &str) -> Result<QueryCursor>;

    /// Explain a query plan without executing it
    pub async fn explain(&self, cypher: &str) -> Result<ExplainOutput>;

    /// Profile a query execution with timing information
    pub async fn profile(&self, cypher: &str) -> Result<(QueryResult, ProfileOutput)>;
}

// Simple query
let results = db.query("MATCH (p:Paper) RETURN p.title, p.year").await?;

// Query with parameters
let results = db.query_with("MATCH (p:Paper) WHERE p.year > $year RETURN p")
    .param("year", 2020)
    .fetch_all()
    .await?;

// Mutation
let result = db.execute("CREATE (p:Paper {title: 'New Paper', year: 2024})").await?;
println!("Created {} nodes", result.affected_rows);
```

### QueryBuilder

```rust
/// Builder for parameterized queries
pub struct QueryBuilder<'a> {
    // Internal state
}

impl<'a> QueryBuilder<'a> {
    /// Add a parameter
    pub fn param<V: Into<Value>>(self, name: &str, value: V) -> Self;

    /// Add multiple parameters
    pub fn params(self, params: HashMap<String, Value>) -> Self;

    /// Set maximum execution time for this query
    pub fn timeout(self, duration: Duration) -> Self;

    /// Set maximum memory per query in bytes
    pub fn max_memory(self, bytes: usize) -> Self;

    /// Execute the query and fetch all results into memory
    pub async fn fetch_all(self) -> Result<QueryResult>;

    /// Execute the query and return a cursor for streaming results
    pub async fn query_cursor(self) -> Result<QueryCursor>;
}

// Example with multiple parameters
let results = db.query_with(
    "MATCH (a:Author)-[:AUTHORED]->(p:Paper)
     WHERE a.name = $name AND p.year >= $min_year
     RETURN p.title, p.year"
)
    .param("name", "Jane Smith")
    .param("min_year", 2020)
    .fetch_all()
    .await?;
```

---

## Sessions

Sessions provide scoped context for multi-tenant and security-aware queries.

### SessionBuilder

```rust
impl Uni {
    /// Create a session builder with scoped variables
    pub fn session(&self) -> SessionBuilder<'_>;
}

/// Builder for creating query sessions
pub struct SessionBuilder<'a> {
    // Internal state
}

impl<'a> SessionBuilder<'a> {
    /// Set a session variable
    pub fn set<K: Into<String>, V: Into<Value>>(self, key: K, value: V) -> Self;

    /// Build the session (variables become immutable)
    pub fn build(self) -> Session<'a>;
}
```

### Session

```rust
/// A query session with scoped variables
pub struct Session<'a> {
    // Internal state
}

impl<'a> Session<'a> {
    /// Execute a query with session variables available
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Execute a query with additional parameters
    pub fn query_with(&self, cypher: &str) -> SessionQueryBuilder<'a, '_>;

    /// Execute a mutation
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;

    /// Get a session variable value
    pub fn get(&self, key: &str) -> Option<&Value>;
}
```

### Session Example

```rust
// Create session with tenant context
let session = db.session()
    .set("tenant_id", "acme-corp")
    .set("user_id", "user-123")
    .set("granted_tags", vec!["public", "team:eng"])
    .build();

// All queries automatically have access to $session.*
let results = session.query(
    "MATCH (d:Document)
     WHERE d.tenant_id = $session.tenant_id
     RETURN d.title"
).await?;

// Query with additional parameters
let results = session.query_with(
    "MATCH (d:Document)
     WHERE d.tenant_id = $session.tenant_id
       AND d.status = $status
     RETURN d"
)
    .param("status", "published")
    .execute()
    .await?;
```

---

## Schema Introspection

Query schema metadata and check existence of labels and edge types.

```rust
impl Uni {
    /// Check if a label exists in the schema
    pub async fn label_exists(&self, name: &str) -> Result<bool>;

    /// Check if an edge type exists in the schema
    pub async fn edge_type_exists(&self, name: &str) -> Result<bool>;

    /// Get all label names
    pub async fn list_labels(&self) -> Result<Vec<String>>;

    /// Get all edge type names
    pub async fn list_edge_types(&self) -> Result<Vec<String>>;

    /// Get detailed information about a label
    pub async fn get_label_info(&self, name: &str) -> Result<Option<LabelInfo>>;
}

/// Detailed label information
#[derive(Debug, Clone)]
pub struct LabelInfo {
    pub name: String,
    pub count: usize,
    pub properties: Vec<PropertyInfo>,
    pub indexes: Vec<IndexInfo>,
    pub constraints: Vec<ConstraintInfo>,
}

/// Property information
#[derive(Debug, Clone)]
pub struct PropertyInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_indexed: bool,
}

/// Index information
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub index_type: String,  // "VECTOR", "SCALAR", "FULLTEXT", "JSON_FTS"
    pub properties: Vec<String>,
    pub status: String,
}
```

### Schema Introspection Example

```rust
// Check label existence before creating
if !db.label_exists("Paper").await? {
    db.schema()
        .label("Paper")
            .property("title", DataType::String)
        .apply()
        .await?;
}

// List all labels
let labels = db.list_labels().await?;
println!("Labels: {:?}", labels);

// Get detailed label info
if let Some(info) = db.get_label_info("Paper").await? {
    println!("Label: {} ({} vertices)", info.name, info.count);
    for prop in &info.properties {
        println!("  - {} ({})", prop.name, prop.data_type);
    }
    for idx in &info.indexes {
        println!("  Index: {} ({}) on {:?}", idx.name, idx.index_type, idx.properties);
    }
}
```

---

## Compaction & Index Management

Control compaction and monitor index rebuild operations.

```rust
impl Uni {
    /// Manually trigger compaction for a specific label
    /// Merges L1 files into larger files for improved read performance
    pub async fn compact_label(&self, label: &str) -> Result<CompactionStats>;

    /// Manually trigger compaction for a specific edge type
    pub async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats>;

    /// Wait for any ongoing compaction to complete
    pub async fn wait_for_compaction(&self) -> Result<()>;

    /// Force rebuild indexes for a specific label
    /// If async_ is true, returns task ID for tracking; otherwise blocks until complete
    pub async fn rebuild_indexes(&self, label: &str, async_: bool) -> Result<Option<String>>;

    /// Get status of background index rebuild tasks
    pub async fn index_rebuild_status(&self) -> Result<Vec<IndexRebuildTask>>;

    /// Retry failed index rebuild tasks
    pub async fn retry_index_rebuilds(&self) -> Result<Vec<String>>;

    /// Check if an index is currently being rebuilt for a label
    pub async fn is_index_building(&self, label: &str) -> Result<bool>;
}

/// Compaction statistics
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    pub files_compacted: usize,
    pub rows_processed: usize,
    pub bytes_written: usize,
    pub duration: Duration,
}

/// Index rebuild task status
#[derive(Debug, Clone)]
pub struct IndexRebuildTask {
    pub id: String,
    pub label: String,
    pub status: IndexRebuildStatus,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub enum IndexRebuildStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}
```

### Compaction Example

```rust
// Manually compact a label after bulk operations
let stats = db.compact_label("Paper").await?;
println!(
    "Compacted {} files, {} rows in {:?}",
    stats.files_compacted, stats.rows_processed, stats.duration
);

// Wait for background compaction
db.wait_for_compaction().await?;
```

### Index Rebuild Example

```rust
// Rebuild indexes synchronously (blocks until complete)
db.rebuild_indexes("Paper", false).await?;

// Rebuild indexes asynchronously (returns immediately)
let task_id = db.rebuild_indexes("Paper", true).await?;
println!("Started index rebuild: {:?}", task_id);

// Monitor rebuild status
loop {
    let tasks = db.index_rebuild_status().await?;
    let building = tasks.iter().any(|t| matches!(t.status, IndexRebuildStatus::InProgress));
    if !building {
        break;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
}

// Check if specific label is building
if db.is_index_building("Paper").await? {
    println!("Index rebuild in progress...");
}

// Retry failed rebuilds
let retried = db.retry_index_rebuilds().await?;
if !retried.is_empty() {
    println!("Retrying {} failed tasks", retried.len());
}
```

---

## Bulk Loading

High-performance data loading with deferred indexing.

### BulkWriter

```rust
impl Uni {
    /// Create a bulk writer builder
    pub fn bulk_writer(&self) -> BulkWriterBuilder<'_>;
}

/// Builder for bulk write operations
pub struct BulkWriterBuilder<'a> {
    // Internal state
}

impl<'a> BulkWriterBuilder<'a> {
    /// Defer vector index updates until commit
    pub fn defer_vector_indexes(self, defer: bool) -> Self;

    /// Defer scalar index updates until commit
    pub fn defer_scalar_indexes(self, defer: bool) -> Self;

    /// Set batch size for flushing
    pub fn batch_size(self, size: usize) -> Self;

    /// Build indexes asynchronously after commit
    pub fn async_indexes(self, async_build: bool) -> Self;

    /// Set progress callback
    pub fn on_progress<F: Fn(BulkProgress) + Send + 'static>(self, f: F) -> Self;

    /// Build the bulk writer
    pub fn build(self) -> Result<BulkWriter<'a>>;
}

/// Bulk writer for high-performance data loading
pub struct BulkWriter<'a> {
    // Internal state
}

impl<'a> BulkWriter<'a> {
    /// Insert vertices in bulk
    pub async fn insert_vertices(
        &mut self,
        label: &str,
        vertices: Vec<HashMap<String, Value>>,
    ) -> Result<Vec<Vid>>;

    /// Insert edges in bulk
    pub async fn insert_edges(
        &mut self,
        edge_type: &str,
        edges: Vec<EdgeData>,
    ) -> Result<Vec<Eid>>;

    /// Commit all pending data and rebuild indexes
    pub async fn commit(self) -> Result<BulkStats>;

    /// Abort bulk operation, discarding uncommitted data
    pub async fn abort(self) -> Result<()>;
}

/// Bulk loading progress information
#[derive(Debug, Clone)]
pub struct BulkProgress {
    pub phase: BulkPhase,
    pub rows_processed: usize,
    pub total_rows: Option<usize>,
    pub current_label: Option<String>,
    pub elapsed: Duration,
}

/// Bulk loading phase
#[derive(Debug, Clone)]
pub enum BulkPhase {
    Inserting,
    RebuildingVectorIndex { label: String, property: String },
    RebuildingScalarIndex { label: String, property: String },
    UpdatingAdjacency,
    Finalizing,
}

/// Bulk loading statistics
#[derive(Debug, Clone, Default)]
pub struct BulkStats {
    pub vertices_inserted: usize,
    pub edges_inserted: usize,
    pub indexes_rebuilt: usize,
    pub duration: Duration,
    pub index_build_duration: Duration,
}
```

### BulkWriter Example

```rust
// Create bulk writer with deferred indexing
let mut bulk = db.bulk_writer()
    .defer_vector_indexes(true)
    .defer_scalar_indexes(true)
    .batch_size(50_000)
    .on_progress(|p| println!("{:?}: {} rows", p.phase, p.rows_processed))
    .build()?;

// Insert 100K vertices
let vertices: Vec<HashMap<String, Value>> = (0..100_000)
    .map(|i| {
        hashmap! {
            "name" => format!("item-{}", i).into(),
            "embedding" => random_vector(128).into(),
        }
    })
    .collect();

let vids = bulk.insert_vertices("Item", vertices).await?;
assert_eq!(vids.len(), 100_000);

// Commit and rebuild indexes
let stats = bulk.commit().await?;
println!("Loaded {} vertices, rebuilt {} indexes in {:?}",
    stats.vertices_inserted, stats.indexes_rebuilt, stats.duration);
```

---

## Snapshots

Read-only access to historical database states.

### Snapshot Management

```rust
impl Uni {
    /// Create a point-in-time snapshot (flushes current changes)
    /// Returns the snapshot ID
    pub async fn create_snapshot(&self, name: Option<&str>) -> Result<String>;

    /// Create a persisted named snapshot that can be retrieved later
    pub async fn create_named_snapshot(&self, name: &str) -> Result<String>;

    /// List all available snapshots
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>>;

    /// Open a read-only view at a specific snapshot ID
    pub async fn at_snapshot(&self, snapshot_id: &str) -> Result<Uni>;

    /// Open a read-only view at a named snapshot
    pub async fn open_named_snapshot(&self, name: &str) -> Result<Uni>;

    /// Restore database to a snapshot state
    /// Note: Requires restart or re-opening to fully take effect
    pub async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()>;
}

/// Snapshot metadata
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub id: String,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub version: u64,
}
```

### Snapshot Example

```rust
// Create a named snapshot before making changes
let snapshot_id = db.create_named_snapshot("before_migration").await?;
println!("Created snapshot: {}", snapshot_id);

// List available snapshots
let snapshots = db.list_snapshots().await?;
for snap in &snapshots {
    println!("{}: {} ({})", snap.id, snap.name.as_deref().unwrap_or("-"), snap.created_at);
}

// Open a read-only view at a named snapshot
let historical = db.open_named_snapshot("before_migration").await?;

// Query historical data
let old_results = historical.query("MATCH (n) RETURN count(n) AS c").await?;
println!("Count at snapshot: {}", old_results[0].get::<i64>("c")?);

// Writes fail on snapshot readers
let result = historical.execute("CREATE (n:Test)").await;
assert!(result.is_err()); // WriteOnReadOnly error

// Or open by snapshot ID
let historical2 = db.at_snapshot(&snapshots[0].id).await?;
```

### Snapshot Procedures

Snapshots can also be managed via Cypher:

```cypher
// Create a named snapshot
CALL uni.admin.snapshot.create('before_migration')
YIELD snapshot_id

// List snapshots
CALL uni.admin.snapshot.list()
YIELD snapshot_id, name, created_at, version_hwm, schema_version

// Restore to a snapshot
CALL uni.admin.snapshot.restore('before_migration')
YIELD status
```

---

## Transactions

```rust
impl Uni {
    /// Begin an explicit transaction
    pub async fn begin(&self) -> Result<Transaction<'_>>;

    /// Run a closure within a transaction
    pub async fn transaction<'a, F, T>(&'a self, f: F) -> Result<T>
    where
        F: for<'b> FnOnce(&'b mut Transaction<'a>) -> BoxFuture<'b, Result<T>>;
}

/// Active transaction handle
pub struct Transaction<'a> {
    // Internal state
}

impl<'a> Transaction<'a> {
    /// Execute a query within the transaction
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Execute a mutation within the transaction
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;

    /// Commit the transaction
    pub async fn commit(self) -> Result<()>;

    /// Rollback the transaction
    pub async fn rollback(self) -> Result<()>;
}
```

### Transaction Examples

```rust
// Explicit transaction
let tx = db.begin().await?;
tx.execute("CREATE (p:Paper {title: 'Paper 1'})").await?;
tx.execute("CREATE (p:Paper {title: 'Paper 2'})").await?;
tx.commit().await?;

// Closure-based transaction (auto-commit on success, rollback on error)
db.transaction(|tx| async move {
    tx.execute("CREATE (a:Author {name: 'Alice'})").await?;
    tx.execute("CREATE (a:Author {name: 'Bob'})").await?;
    Ok(())
}).await?;

// Transaction with rollback
let tx = db.begin().await?;
tx.execute("DELETE (p:Paper) WHERE p.year < 2000").await?;
// Changed our mind
tx.rollback().await?;
```

---

## Query Results

### QueryResult

```rust
/// Collection of result rows
pub struct QueryResult {
    // Internal state
}

impl QueryResult {
    /// Get column names
    pub fn columns(&self) -> &[String];

    /// Get number of rows
    pub fn len(&self) -> usize;

    /// Check if empty
    pub fn is_empty(&self) -> bool;

    /// Get rows as slice
    pub fn rows(&self) -> &[Row];

    /// Consume into owned rows
    pub fn into_rows(self) -> Vec<Row>;

    /// Iterate over rows
    pub fn iter(&self) -> impl Iterator<Item = &Row>;
}

// Implements IntoIterator
for row in results {
    // ...
}

// Or by reference
for row in &results {
    // ...
}
```

### Row

```rust
/// Single result row
pub struct Row {
    // Internal state
}

impl Row {
    /// Get typed value by column name
    pub fn get<T: FromValue>(&self, column: &str) -> Result<T>;

    /// Get typed value by index
    pub fn get_idx<T: FromValue>(&self, index: usize) -> Result<T>;

    /// Try to get value (returns None if missing or wrong type)
    pub fn try_get<T: FromValue>(&self, column: &str) -> Option<T>;

    /// Get raw Value by column name
    pub fn value(&self, column: &str) -> Option<&Value>;

    /// Convert to HashMap
    pub fn as_map(&self) -> HashMap<&str, &Value>;

    /// Convert to JSON
    pub fn to_json(&self) -> serde_json::Value;
}

// Index access
impl Index<usize> for Row {
    type Output = Value;
    fn index(&self, index: usize) -> &Value;
}

// Example
for row in &results {
    let title: String = row.get("p.title")?;
    let year: i32 = row.get("p.year")?;
    let citations: Option<i64> = row.try_get("p.citations");
    println!("{} ({}) - {:?} citations", title, year, citations);
}
```

### Node

```rust
/// Graph node returned from queries
pub struct Node {
    // Internal state
}

impl Node {
    /// Get vertex ID
    pub fn id(&self) -> Vid;

    /// Get label name
    pub fn label(&self) -> &str;

    /// Get all properties
    pub fn properties(&self) -> &HashMap<String, Value>;

    /// Get typed property value
    pub fn get<T: FromValue>(&self, property: &str) -> Result<T>;

    /// Try to get property (returns None if missing)
    pub fn try_get<T: FromValue>(&self, property: &str) -> Option<T>;
}

// Example: Query returns nodes
let results = db.query("MATCH (p:Paper) RETURN p").await?;
for row in &results {
    let node: Node = row.get("p")?;
    println!("Label: {}, ID: {}", node.label(), node.id());
    println!("Title: {}", node.get::<String>("title")?);
}
```

### Edge

```rust
/// Graph edge returned from queries
pub struct Edge {
    // Internal state
}

impl Edge {
    /// Get edge ID
    pub fn id(&self) -> Eid;

    /// Get edge type name
    pub fn edge_type(&self) -> &str;

    /// Get source vertex ID
    pub fn src(&self) -> Vid;

    /// Get destination vertex ID
    pub fn dst(&self) -> Vid;

    /// Get all properties
    pub fn properties(&self) -> &HashMap<String, Value>;

    /// Get typed property value
    pub fn get<T: FromValue>(&self, property: &str) -> Result<T>;
}

// Example
let results = db.query("MATCH (a:Author)-[r:AUTHORED]->(p:Paper) RETURN r").await?;
for row in &results {
    let edge: Edge = row.get("r")?;
    println!("Type: {}, From: {} To: {}", edge.edge_type(), edge.src(), edge.dst());
}
```

### Path

```rust
/// Graph path (sequence of nodes and edges)
pub struct Path {
    // Internal state
}

impl Path {
    /// Get all nodes in the path
    pub fn nodes(&self) -> &[Node];

    /// Get all edges in the path
    pub fn edges(&self) -> &[Edge];

    /// Get path length (number of edges)
    pub fn len(&self) -> usize;

    /// Check if path is empty
    pub fn is_empty(&self) -> bool;

    /// Get start node
    pub fn start(&self) -> &Node;

    /// Get end node
    pub fn end(&self) -> &Node;
}

// Example: Variable-length path query
let results = db.query(
    "MATCH path = (a:Paper)-[:CITES*1..3]->(b:Paper)
     WHERE a.title = 'Attention Is All You Need'
     RETURN path"
).await?;

for row in &results {
    let path: Path = row.get("path")?;
    println!("Path length: {} hops", path.len());
    println!("Start: {}", path.start().get::<String>("title")?);
    println!("End: {}", path.end().get::<String>("title")?);
}
```

### Value

```rust
/// Dynamic value type for properties and results
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Node(Node),
    Edge(Edge),
    Path(Path),
    Vector(Vec<f32>),
}

impl Value {
    // Type checking
    pub fn is_null(&self) -> bool;
    pub fn is_bool(&self) -> bool;
    pub fn is_int(&self) -> bool;
    pub fn is_float(&self) -> bool;
    pub fn is_string(&self) -> bool;
    pub fn is_list(&self) -> bool;
    pub fn is_map(&self) -> bool;
    pub fn is_node(&self) -> bool;
    pub fn is_edge(&self) -> bool;
    pub fn is_path(&self) -> bool;
    pub fn is_vector(&self) -> bool;

    // Accessors (return None if wrong type)
    pub fn as_bool(&self) -> Option<bool>;
    pub fn as_i64(&self) -> Option<i64>;
    pub fn as_f64(&self) -> Option<f64>;
    pub fn as_str(&self) -> Option<&str>;
    pub fn as_bytes(&self) -> Option<&[u8]>;
    pub fn as_list(&self) -> Option<&[Value]>;
    pub fn as_map(&self) -> Option<&HashMap<String, Value>>;
    pub fn as_node(&self) -> Option<&Node>;
    pub fn as_edge(&self) -> Option<&Edge>;
    pub fn as_path(&self) -> Option<&Path>;
    pub fn as_vector(&self) -> Option<&[f32]>;
}

// From implementations for common types
impl From<bool> for Value { ... }
impl From<i32> for Value { ... }
impl From<i64> for Value { ... }
impl From<f64> for Value { ... }
impl From<String> for Value { ... }
impl From<&str> for Value { ... }
impl From<Vec<f32>> for Value { ... }
```

### FromValue Trait

```rust
/// Trait for converting from Value
pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Result<Self>;
}

// Implemented for:
// - String, &str
// - bool
// - i32, i64, u32, u64
// - f32, f64
// - Vec<T> where T: FromValue
// - Option<T> where T: FromValue
// - Node, Edge, Path
// - Vid, Eid
// - Vec<f32> (vectors)
```

---

## Schema Management

### Schema Builder

```rust
impl Uni {
    /// Get schema builder for modifications
    pub fn schema(&self) -> SchemaBuilder<'_>;

    /// Load schema from file
    pub async fn load_schema(&self, path: impl AsRef<Path>) -> Result<()>;

    /// Save schema to file
    pub async fn save_schema(&self, path: impl AsRef<Path>) -> Result<()>;
}

/// Fluent schema builder
pub struct SchemaBuilder<'a> {
    // Internal state
}

impl<'a> SchemaBuilder<'a> {
    /// Add a new label (vertex type)
    pub fn label(self, name: &str) -> LabelBuilder<'a>;

    /// Add a new edge type
    pub fn edge_type(
        self,
        name: &str,
        from_labels: &[&str],
        to_labels: &[&str],
    ) -> EdgeTypeBuilder<'a>;

    /// Apply all schema changes
    pub async fn apply(self) -> Result<()>;
}
```

### LabelBuilder

```rust
/// Builder for label definitions
pub struct LabelBuilder<'a> {
    // Internal state
}

impl<'a> LabelBuilder<'a> {
    /// Add a required property
    pub fn property(self, name: &str, data_type: DataType) -> Self;

    /// Add a nullable property
    pub fn property_nullable(self, name: &str, data_type: DataType) -> Self;

    /// Add a vector property
    pub fn vector(self, name: &str, dimensions: usize) -> Self;

    /// Add an index on a property
    pub fn index(self, property: &str, index_type: IndexType) -> Self;

    /// Finish this label and return to SchemaBuilder
    pub fn done(self) -> SchemaBuilder<'a>;

    /// Chain to another label
    pub fn label(self, name: &str) -> LabelBuilder<'a>;

    /// Chain to an edge type
    pub fn edge_type(
        self,
        name: &str,
        from: &[&str],
        to: &[&str],
    ) -> EdgeTypeBuilder<'a>;

    /// Apply all schema changes
    pub async fn apply(self) -> Result<()>;
}
```

### EdgeTypeBuilder

```rust
/// Builder for edge type definitions
pub struct EdgeTypeBuilder<'a> {
    // Internal state
}

impl<'a> EdgeTypeBuilder<'a> {
    /// Add a required property
    pub fn property(self, name: &str, data_type: DataType) -> Self;

    /// Add a nullable property
    pub fn property_nullable(self, name: &str, data_type: DataType) -> Self;

    /// Finish and return to SchemaBuilder
    pub fn done(self) -> SchemaBuilder<'a>;

    /// Chain to a label
    pub fn label(self, name: &str) -> LabelBuilder<'a>;

    /// Chain to another edge type
    pub fn edge_type(
        self,
        name: &str,
        from: &[&str],
        to: &[&str],
    ) -> EdgeTypeBuilder<'a>;

    /// Apply all schema changes
    pub async fn apply(self) -> Result<()>;
}
```

### Schema Example

```rust
// Define schema using fluent API
db.schema()
    .label("Paper")
        .property("title", DataType::String)
        .property("year", DataType::Int32)
        .property_nullable("abstract", DataType::String)
        .vector("embedding", 768)
        .index("year", IndexType::Scalar(ScalarType::BTree))
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::Hnsw { m: 16, ef_construction: 200 },
            metric: VectorMetric::Cosine,
        }))
    .label("Author")
        .property("name", DataType::String)
        .property_nullable("email", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::BTree))
    .edge_type("AUTHORED", &["Author"], &["Paper"])
        .property_nullable("position", DataType::Int32)
    .edge_type("CITES", &["Paper"], &["Paper"])
    .apply()
    .await?;
```

### Schema Types

```rust
/// Property data type
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Timestamp,
    Json,
    Vector { dimensions: usize },
    Crdt(CrdtType),
}

/// CRDT type variants
#[derive(Clone, Debug, PartialEq)]
pub enum CrdtType {
    GCounter,
    GSet,
    ORSet,
    LWWRegister,
    LWWMap,
    Rga,
}

/// Index type configuration
#[derive(Clone, Debug)]
pub enum IndexType {
    Vector(VectorIndexCfg),
    FullText,
    Scalar(ScalarType),
}

/// Vector index configuration
#[derive(Clone, Debug)]
pub struct VectorIndexCfg {
    pub algorithm: VectorAlgo,
    pub metric: VectorMetric,
}

/// Vector index algorithms
#[derive(Clone, Debug)]
pub enum VectorAlgo {
    /// HNSW index (fast, high recall)
    Hnsw { m: u32, ef_construction: u32 },
    /// IVF-PQ index (memory efficient)
    IvfPq { partitions: u32, sub_vectors: u32 },
    /// Flat index (exact, slow)
    Flat,
}

/// Vector distance metrics
#[derive(Clone, Copy, Debug)]
pub enum VectorMetric {
    Cosine,
    L2,
    Dot,
}

/// Scalar index types
#[derive(Clone, Copy, Debug)]
pub enum ScalarType {
    BTree,   // Range queries
    Hash,    // Equality queries (planned)
    Bitmap,  // Low cardinality (planned)
}
```

Note: Scalar index variants other than `BTree` are accepted but currently map to BTree in the storage layer.

---

## Vector Search

### Basic Vector Search

```rust
impl Uni {
    /// Perform vector similarity search
    pub async fn vector_search(
        &self,
        label: &str,
        property: &str,
        query: Vec<f32>,
        k: usize,
    ) -> Result<Vec<VectorMatch>>;

    /// Vector search with options
    pub fn vector_search_with(
        &self,
        label: &str,
        property: &str,
        query: Vec<f32>,
    ) -> VectorSearchBuilder<'_>;
}

/// Vector search result
#[derive(Clone, Debug)]
pub struct VectorMatch {
    pub vid: Vid,
    pub distance: f32,
}
```

### VectorSearchBuilder

```rust
/// Fluent builder for vector searches
pub struct VectorSearchBuilder<'a> {
    // Internal state
}

impl<'a> VectorSearchBuilder<'a> {
    /// Set number of results to return
    pub fn k(self, k: usize) -> Self;

    /// Set distance threshold (filter out results above this)
    pub fn threshold(self, threshold: f32) -> Self;

    /// Add a filter predicate (Lance/DataFusion filter expression)
    pub fn filter(self, filter: &str) -> Self;

    /// Execute search and return matches
    pub async fn search(self) -> Result<Vec<VectorMatch>>;

    /// Execute search and fetch full nodes
    pub async fn fetch_nodes(self) -> Result<Vec<(Node, f32)>>;
}
```

### Vector Search Examples

```rust
// Simple vector search
let query_embedding = embedding_service.embed("machine learning")?;
let matches = db.vector_search("Paper", "embedding", query_embedding, 10).await?;

for m in matches {
    println!("VID: {}, Distance: {:.4}", m.vid, m.distance);
}

// Vector search with filters and fetch nodes
let results = db.vector_search_with("Paper", "embedding", query_embedding)
    .k(20)
    .threshold(0.5)
    .filter("year >= 2020")
    .fetch_nodes()
    .await?;

for (node, distance) in results {
    println!("{} ({:.4})", node.get::<String>("title")?, distance);
}

// Vector search via Cypher
let results = db.query_with(
    "CALL uni.vector.query('Paper', 'embedding', $vec, 10)
     YIELD node, distance
     WHERE node.year >= 2020
     RETURN node.title, distance
     ORDER BY distance"
)
    .param("vec", query_embedding)
    .fetch_all()
    .await?;
```

---

## Graph Algorithms

### AlgoBuilder

```rust
impl Uni {
    /// Access algorithm builder
    pub fn algo(&self) -> AlgoBuilder<'_>;
}

/// Builder for graph algorithms
pub struct AlgoBuilder<'a> {
    // Internal state
}

impl<'a> AlgoBuilder<'a> {
    /// PageRank centrality algorithm
    pub fn pagerank(self) -> PageRankBuilder<'a>;

    /// Weakly Connected Components
    pub fn wcc(self) -> WccBuilder<'a>;
}
```

### PageRank

```rust
/// PageRank algorithm builder
pub struct PageRankBuilder<'a> {
    // Internal state
}

impl<'a> PageRankBuilder<'a> {
    /// Filter to specific labels
    pub fn labels(self, labels: &[&str]) -> Self;

    /// Filter to specific edge types
    pub fn edge_types(self, types: &[&str]) -> Self;

    /// Set damping factor (default: 0.85)
    pub fn damping(self, d: f64) -> Self;

    /// Set maximum iterations (default: 20)
    pub fn max_iterations(self, n: usize) -> Self;

    /// Set convergence tolerance (default: 1e-6)
    pub fn tolerance(self, tol: f64) -> Self;

    /// Run the algorithm
    pub async fn run(self) -> Result<Vec<(Vid, f64)>>;
}

// Example
let rankings = db.algo()
    .pagerank()
    .labels(&["Paper"])
    .edge_types(&["CITES"])
    .damping(0.85)
    .max_iterations(50)
    .run()
    .await?;

for (vid, score) in rankings.iter().take(10) {
    println!("VID: {}, PageRank: {:.6}", vid, score);
}
```

### Weakly Connected Components

```rust
/// WCC algorithm builder
pub struct WccBuilder<'a> {
    // Internal state
}

impl<'a> WccBuilder<'a> {
    /// Filter to specific labels
    pub fn labels(self, labels: &[&str]) -> Self;

    /// Filter to specific edge types
    pub fn edge_types(self, types: &[&str]) -> Self;

    /// Run the algorithm
    pub async fn run(self) -> Result<Vec<(Vid, i64)>>;
}

// Example: Find connected components
let components = db.algo()
    .wcc()
    .labels(&["Paper", "Author"])
    .edge_types(&["AUTHORED", "CITES"])
    .run()
    .await?;

// Count component sizes
let mut component_sizes: HashMap<i64, usize> = HashMap::new();
for (_, component_id) in &components {
    *component_sizes.entry(*component_id).or_default() += 1;
}
println!("Found {} components", component_sizes.len());
```

### Algorithms via Cypher

```rust
// PageRank via Cypher CALL
let results = db.query(
    "CALL uni.algo.pageRank(['Paper'], ['CITES'], 0.85, 20, 1e-6)
     YIELD nodeId, score
     RETURN nodeId, score
     ORDER BY score DESC
     LIMIT 10"
).await?;

// Shortest path
let results = db.query_with(
    "CALL uni.algo.shortestPath($startVid, $endVid, ['CITES'])
     YIELD nodeIds, edgeIds, length
     RETURN nodeIds, edgeIds, length"
)
    .param("startVid", start_vid.as_u64() as i64)
    .param("endVid", end_vid.as_u64() as i64)
    .fetch_all()
    .await?;

// Louvain community detection
let results = db.query(
    "CALL uni.algo.louvain(['Paper'], ['CITES'])
     YIELD nodeId, communityId
     RETURN communityId, COUNT(*) AS size
     ORDER BY size DESC"
).await?;
```

---

## Core Types

### Vid (Vertex ID)

```rust
/// 64-bit vertex identifier
/// Encoding: label_id (16 bits) | local_offset (48 bits)
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Vid(u64);

impl Vid {
    /// Create from label ID and offset
    pub fn new(label_id: u16, local_offset: u64) -> Self;

    /// Extract label ID (upper 16 bits)
    pub fn label_id(&self) -> u16;

    /// Extract local offset (lower 48 bits)
    pub fn local_offset(&self) -> u64;

    /// Get raw u64 value
    pub fn as_u64(&self) -> u64;
}

impl From<u64> for Vid { ... }
impl Display for Vid { ... }  // Formats as "0x{:016x}"

// Example
let vid = Vid::new(1, 12345);
assert_eq!(vid.label_id(), 1);
assert_eq!(vid.local_offset(), 12345);
```

### Eid (Edge ID)

```rust
/// 64-bit edge identifier
/// Encoding: type_id (16 bits) | local_offset (48 bits)
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Eid(u64);

impl Eid {
    /// Create from edge type ID and offset
    pub fn new(type_id: u16, local_offset: u64) -> Self;

    /// Extract edge type ID
    pub fn type_id(&self) -> u16;

    /// Extract local offset
    pub fn local_offset(&self) -> u64;

    /// Get raw u64 value
    pub fn as_u64(&self) -> u64;
}

impl From<u64> for Eid { ... }
impl Display for Eid { ... }
```

### UniId

```rust
/// Content-addressed identifier (SHA3-256 hash)
/// Used for provenance tracking and distributed sync
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct UniId([u8; 32]);

impl UniId {
    /// Create from raw bytes
    pub fn from_bytes(bytes: [u8; 32]) -> Self;

    /// Compute from content
    pub fn compute(content: &[u8]) -> Self;

    /// Get as hex string
    pub fn to_hex(&self) -> String;

    /// Parse from hex string
    pub fn from_hex(s: &str) -> Result<Self>;

    /// Get raw bytes
    pub fn as_bytes(&self) -> &[u8; 32];
}
```

---

## Blocking API (UniSync)

For applications that cannot use async/await.

```rust
/// Blocking wrapper for Uni
pub struct UniSync {
    // Internal state
}

impl UniSync {
    /// Open database (blocking)
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;

    /// Create in-memory database (blocking)
    pub fn in_memory() -> Result<Self>;

    /// Execute query (blocking)
    pub fn query(&self, cypher: &str) -> Result<QueryResult>;

    /// Execute mutation (blocking)
    pub fn execute(&self, cypher: &str) -> Result<ExecuteResult>;

    /// Query with parameters
    pub fn query_with(&self, cypher: &str) -> QueryBuilderSync<'_>;

    /// Get current schema
    pub fn schema_meta(&self) -> Schema;

    /// Get schema builder
    pub fn schema(&self) -> SchemaBuilderSync<'_>;

    /// Begin transaction (blocking)
    pub fn begin(&self) -> Result<TransactionSync<'_>>;
}

/// Blocking transaction
pub struct TransactionSync<'a> {
    // Internal state
}

impl<'a> TransactionSync<'a> {
    pub fn query(&self, cypher: &str) -> Result<QueryResult>;
    pub fn execute(&self, cypher: &str) -> Result<ExecuteResult>;
    pub fn commit(self) -> Result<()>;
    pub fn rollback(self) -> Result<()>;
}

// Example
let db = UniSync::open("./data")?;
let results = db.query("MATCH (p:Paper) RETURN p.title LIMIT 10")?;
for row in &results {
    println!("{}", row.get::<String>("p.title")?);
}
```

---

## Configuration

```rust
/// Database configuration
#[derive(Clone, Debug)]
pub struct UniConfig {
    /// Cache size in bytes (default: 1 GB)
    pub cache_size: usize,

    /// Worker thread count (default: available cores)
    pub parallelism: usize,

    /// Batch size for vectorized execution (default: 1024)
    pub batch_size: usize,

    /// Maximum frontier size for traversals (default: 1M)
    pub max_frontier_size: usize,

    /// Auto-flush after this many mutations (default: 10K)
    pub auto_flush_threshold: usize,

    /// Enable write-ahead log (default: true)
    pub wal_enabled: bool,
}

impl Default for UniConfig {
    fn default() -> Self {
        Self {
            cache_size: 1024 * 1024 * 1024,  // 1 GB
            parallelism: num_cpus::get(),
            batch_size: 1024,
            max_frontier_size: 1_000_000,
            auto_flush_threshold: 10_000,
            wal_enabled: true,
        }
    }
}

// Example
let config = UniConfig {
    cache_size: 4 * 1024 * 1024 * 1024,  // 4 GB
    parallelism: 16,
    ..Default::default()
};

let db = Uni::open("./data")
    .config(config)
    .build()
    .await?;
```

---

## Error Handling

```rust
/// Main error type
#[derive(Debug, thiserror::Error)]
pub enum UniError {
    #[error("Database not found: {path}")]
    NotFound { path: PathBuf },

    #[error("Schema error: {message}")]
    Schema { message: String },

    #[error("Parse error: {message}")]
    Parse {
        message: String,
        position: Option<usize>,
        line: Option<usize>,
        column: Option<usize>,
        context: Option<String>,
    },

    #[error("Query error: {message}")]
    Query {
        message: String,
        query: Option<String>,
    },

    #[error("Transaction error: {message}")]
    Transaction { message: String },

    #[error("Transaction conflict: {message}")]
    TransactionConflict { message: String },

    #[error("Database is locked by another process")]
    DatabaseLocked,

    #[error("Query timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("Type error: expected {expected}, got {actual}")]
    Type { expected: String, actual: String },

    #[error("Constraint violation: {message}")]
    Constraint { message: String },

    #[error("Storage error: {message}")]
    Storage {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

/// Result type alias
pub type Result<T> = std::result::Result<T, UniError>;

// Error handling example
match db.query("INVALID QUERY").await {
    Ok(results) => { /* ... */ }
    Err(UniError::Parse { message, line, column, .. }) => {
        eprintln!("Parse error at {}:{}: {}", line.unwrap_or(0), column.unwrap_or(0), message);
    }
    Err(UniError::Query { message, query }) => {
        eprintln!("Query failed: {}", message);
    }
    Err(e) => {
        eprintln!("Error: {}", e);
    }
}
```

---

## Prelude

Convenient re-exports for common usage.

```rust
pub use crate::{
    Uni, UniBuilder, UniSync, UniConfig, UniError, Result,
    Transaction, QueryBuilder,
};
pub use crate::query::{
    QueryResult, Row, Node, Edge, Path, Value, FromValue, ExecuteResult,
};
pub use crate::core::{Vid, Eid, UniId};
pub use crate::schema::{
    Schema, DataType, CrdtType, IndexType, VectorIndexCfg,
    VectorAlgo, VectorMetric, ScalarType,
};
pub use crate::vector::VectorMatch;
```

---

## Complete Example

```rust
use uni_db::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create database with schema
    let db = Uni::open("./academic-graph")
        .cache_size(2 * 1024 * 1024 * 1024)
        .build()
        .await?;

    // Define schema
    db.schema()
        .label("Paper")
            .property("title", DataType::String)
            .property("year", DataType::Int32)
            .vector("embedding", 768)
            .index("year", IndexType::Scalar(ScalarType::BTree))
        .label("Author")
            .property("name", DataType::String)
        .edge_type("AUTHORED", &["Author"], &["Paper"])
        .edge_type("CITES", &["Paper"], &["Paper"])
        .apply()
        .await?;

    // Insert data in a transaction
    db.transaction(|tx| async move {
        tx.execute("CREATE (a:Author {name: 'Alice'})").await?;
        tx.execute("CREATE (p:Paper {title: 'Graph Databases', year: 2024})").await?;
        tx.execute(
            "MATCH (a:Author {name: 'Alice'}), (p:Paper {title: 'Graph Databases'})
             CREATE (a)-[:AUTHORED]->(p)"
        ).await?;
        Ok(())
    }).await?;

    // Query with parameters
    let results = db.query_with(
        "MATCH (a:Author)-[:AUTHORED]->(p:Paper)
         WHERE p.year >= $min_year
         RETURN a.name AS author, p.title AS paper, p.year AS year
         ORDER BY p.year DESC"
    )
        .param("min_year", 2020)
        .fetch_all()
        .await?;

    println!("Found {} results:", results.len());
    for row in &results {
        println!(
            "  {} wrote '{}' ({})",
            row.get::<String>("author")?,
            row.get::<String>("paper")?,
            row.get::<i32>("year")?
        );
    }

    // Run PageRank on citation network
    let rankings = db.algo()
        .pagerank()
        .labels(&["Paper"])
        .edge_types(&["CITES"])
        .run()
        .await?;

    println!("\nTop 5 papers by PageRank:");
    for (vid, score) in rankings.iter().take(5) {
        let result = db.query_with("MATCH (p:Paper) WHERE id(p) = $vid RETURN p.title")
            .param("vid", vid.as_u64() as i64)
            .fetch_all()
            .await?;
        if let Some(row) = result.rows().first() {
            println!("  {:.6}: {}", score, row.get::<String>("p.title")?);
        }
    }

    Ok(())
}
```

---

## API Gaps & Workarounds

The following features exist internally but have limited or no exposure through the public Rust API. This section documents what's available and workarounds where applicable.

### Feature Availability Summary

| Feature | Rust API | Cypher | Notes |
|---------|----------|--------|-------|
| Basic CRUD | ✅ | ✅ | `query()`, `execute()` |
| Parameterized queries | ✅ | ✅ | `query_with().param()` |
| Streaming queries | ✅ | — | `query_cursor()` for large result sets |
| Transactions | ✅ | ✅ | `begin()`, `transaction()` |
| Schema definition | ✅ | ✅ | `schema()` builder |
| Schema introspection | ✅ | ✅ | `list_labels()`, `get_label_info()`, `uni.schema.labels()` |
| Vector search | ✅ | ✅ | `vector_search()` + `uni.vector.query()` |
| PageRank / WCC | ✅ | ✅ | `algo().pagerank()` |
| Session variables | ✅ | ✅ | `session().set().build()` + `$session.*` |
| Bulk loading | ✅ | — | `bulk_writer()` with deferred indexing |
| Snapshots | ✅ | ✅ | `create_named_snapshot()`, `open_named_snapshot()`, `uni.admin.snapshot.*` |
| EXPLAIN/PROFILE | ✅ | ✅ | `explain()`, `profile()` |
| Compaction control | ✅ | — | `compact_label()`, `wait_for_compaction()` |
| Index management | ✅ | — | `rebuild_indexes()`, `index_rebuild_status()` |
| Temporal queries | — | ✅ | `uni.temporal.validAt()`, `VALID_AT` macro |
| Schema DDL procedures | — | ✅ | `uni.schema.createLabel()`, `uni.schema.createIndex()` |
| Import/Export | — | ✅ | Use `COPY` (see below) |
| Embedding generation | — | ✅ | Auto on CREATE with schema config |
| Other algorithms | — | ✅ | Use `CALL uni.algo.*` |
| CRDT operations | — | ✅ | Schema type + Cypher |
| S3/GCS storage | — | — | Filesystem assumptions in metadata ops |

### Batch Ingestion

No direct `put_batch()` method exists. Use Cypher `UNWIND` for batch operations:

```rust
// Batch insert via UNWIND
let papers = vec![
    json!({"title": "Paper 1", "year": 2024}),
    json!({"title": "Paper 2", "year": 2023}),
    json!({"title": "Paper 3", "year": 2024}),
];

db.query_with(
    "UNWIND $papers AS p
     CREATE (n:Paper {title: p.title, year: p.year})"
)
    .param("papers", papers)
    .fetch_all()
    .await?;
```

### Import/Export

No direct import/export methods. Use Cypher `COPY`:

```rust
// Import from CSV
db.execute("COPY Paper FROM 'papers.csv'").await?;

// Export to Parquet
db.execute("COPY Paper TO 'papers.parquet' WITH {format: 'parquet'}").await?;

// Export to CSV
db.execute("COPY Paper TO 'papers.csv' WITH {format: 'csv'}").await?;
```

### Graph Algorithms

Only PageRank and WCC have dedicated builder APIs. Other algorithms are accessible via Cypher:

```rust
// Louvain community detection
let results = db.query(
    "CALL uni.algo.louvain(['Paper'], ['CITES'])
     YIELD nodeId, communityId
     RETURN communityId, COUNT(*) AS size
     ORDER BY size DESC"
).await?;

// Shortest path
let results = db.query_with(
    "CALL uni.algo.shortestPath($start, $end, ['CITES'])
     YIELD nodeIds, edgeIds, length
     RETURN nodeIds, edgeIds, length"
)
    .param("start", start_vid.as_u64() as i64)
    .param("end", end_vid.as_u64() as i64)
    .fetch_all()
    .await?;

// Label propagation
let results = db.query(
    "CALL uni.algo.labelPropagation(['Paper'], ['CITES'])
     YIELD nodeId, communityId
     RETURN communityId, COUNT(*) AS size"
).await?;
```

### Embedding Generation

Embeddings are auto-generated on CREATE when configured in schema. No direct embedding API:

```rust
// Schema with embedding config (embeddings auto-generated on insert)
db.schema()
    .label("Paper")
        .property("title", DataType::String)
        .property("abstract", DataType::String)
        .vector("embedding", 384)  // Dimensions match model
    .apply()
    .await?;

// Embedding generated automatically from configured source properties
db.execute("CREATE (p:Paper {title: 'ML Paper', abstract: 'Deep learning...'})").await?;

// Query embeddings via vector search
let results = db.query_with(
    "CALL uni.vector.query('Paper', 'embedding', $query_vec, 10)
     YIELD node, distance
     RETURN node.title, distance"
)
    .param("query_vec", query_embedding)
    .fetch_all()
    .await?;
```

### CRDT Types

CRDTs can be defined in schema but manipulated only via Cypher:

```rust
// Define CRDT property
db.schema()
    .label("Counter")
        .property("value", DataType::Crdt(CrdtType::GCounter))
    .apply()
    .await?;

// Increment counter via Cypher
db.execute("MATCH (c:Counter {id: 'visits'}) SET c.value = crdt.increment(c.value, 1)").await?;

// Read counter
let results = db.query("MATCH (c:Counter {id: 'visits'}) RETURN crdt.value(c.value) AS count").await?;
```

### Storage Backend

Currently only local filesystem paths are supported. While Lance (the underlying storage) supports S3/GCS/Azure natively, Uni's metadata operations (schema, snapshots, WAL, ID allocation) use `std::fs` directly:

```rust
// Local storage (supported)
let db = Uni::open("./my-graph").build().await?;

// S3/GCS/Azure (NOT SUPPORTED)
// Blocked by filesystem assumptions in:
// - SchemaManager (fs::read_to_string, fs::write)
// - SnapshotManager (fs::create_dir_all, fs::write)
// - WriteAheadLog (File::open)
// - IdAllocator (fs::rename for atomic writes)
```

Supporting object stores would require abstracting these operations to use `object_store` crate throughout.

### Full-Text Search

FTS indexes and queries are fully supported:

```rust
// Create FTS index
db.execute("CREATE FULLTEXT INDEX bio_fts FOR (p:Person) ON EACH [p.bio]").await?;

// FTS queries with CONTAINS, STARTS WITH, ENDS WITH
let results = db.query(r#"
    MATCH (p:Person)
    WHERE p.bio CONTAINS 'machine learning'
    RETURN p.name, p.bio
"#).await?;

let results = db.query(r#"
    MATCH (p:Person)
    WHERE p.name STARTS WITH 'John'
    RETURN p.name
"#).await?;
```

### What's NOT Accessible

These internal features have no public access path:

- **WAL management**: Only `wal_enabled` config flag; no rotation/recovery API
- **ID allocation**: Internal `IdAllocator`; no public ID control
- **Subgraph extraction**: Internal `load_subgraph_cached()`; no public method
- **Property lazy-loading**: Internal `PropertyManager`; transparent to users

---

## Next Steps

- [Configuration Reference](configuration.md) — All configuration options
- [Cypher Querying](../guides/cypher-querying.md) — Query language guide
- [Vector Search](../guides/vector-search.md) — Embedding search patterns
- [Troubleshooting](troubleshooting.md) — Common issues and solutions
