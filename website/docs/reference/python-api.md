# Python API Reference

Uni provides full-featured Python bindings with type hints and comprehensive documentation.

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

## Quick Start

```python
import uni_db

# Open or create a database
db = uni_db.Database("/path/to/db")

# Or use the builder pattern
db = uni_db.DatabaseBuilder.open("/path/to/db").build()

# Create schema
db.create_label("Person")
db.add_property("Person", "name", "string", False)
db.add_property("Person", "age", "int", False)

# Insert data
db.query("CREATE (n:Person {name: 'Alice', age: 30})")
db.query("CREATE (n:Person {name: 'Bob', age: 25})")

# Query data
results = db.query("MATCH (n:Person) WHERE n.age > 20 RETURN n.name AS name")
for row in results:
    print(row["name"])
```

## Core Classes

### Database

The main database interface. Create using `Database(path)` or `DatabaseBuilder`.

```python
db = uni_db.Database("/path/to/db")

# Execute queries
results = db.query("MATCH (n) RETURN n LIMIT 10")
affected = db.execute("CREATE (n:Person {name: 'Alice'})")

# Parameterized queries
results = db.query(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    {"name": "Alice"}
)

# Or use QueryBuilder
builder = db.query_with("MATCH (n:Person) WHERE n.age > $min RETURN n")
builder.param("min", 21)
results = builder.fetch_all()
```

### DatabaseBuilder

Fluent builder for database configuration:

```python
# Create new database (fails if exists)
db = uni_db.DatabaseBuilder.create("/path/to/db").build()

# Open existing (fails if doesn't exist)
db = uni_db.DatabaseBuilder.open_existing("/path/to/db").build()

# Open or create
db = uni_db.DatabaseBuilder.open("/path/to/db").build()

# Temporary in-memory database
db = uni_db.DatabaseBuilder.temporary().build()

# With configuration
db = (
    uni_db.DatabaseBuilder.open("/path/to/db")
    .cache_size(1024 * 1024 * 100)  # 100 MB
    .parallelism(4)
    .build()
)
```

### SchemaBuilder

Fluent API for schema definition:

```python
schema = db.schema()
schema = schema.label("Person").property("name", "string").property("age", "int").done()
schema = schema.label("Company").property("name", "string").done()
schema = schema.edge_type("WORKS_AT", ["Person"], ["Company"]).property("since", "int").done()
schema.apply()
```

### Transaction

ACID transactions:

```python
tx = db.begin()
try:
    tx.query("CREATE (n:Person {name: 'Alice'})")
    tx.query("CREATE (n:Person {name: 'Bob'})")
    tx.commit()
except Exception:
    tx.rollback()
```

### Session

Scoped sessions with variables:

```python
session = db.session().set("user_id", 123).build()
results = session.query("MATCH (n:Person) RETURN n")
user_id = session.get("user_id")
```

### BulkWriter

High-performance bulk loading:

```python
writer = db.bulk_writer().batch_size(10000).build()

# Insert vertices
vids = writer.insert_vertices("Person", [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
])

# Insert edges
writer.insert_edges("KNOWS", [
    (vids[0], vids[1], {"since": 2020}),
])

# Commit and build indexes
stats = writer.commit()
print(f"Inserted {stats.vertices_inserted} vertices")
```

### VectorSearch

Vector similarity search:

```python
# Create vector index
db.add_property("Document", "embedding", "vector:128", False)
db.create_vector_index("Document", "embedding", "cosine")

# Search
query_vec = [0.1, 0.2, ...]  # 128 dimensions
matches = db.vector_search("Document", "embedding", query_vec, k=10)

for match in matches:
    print(f"VID: {match.vid}, Distance: {match.distance}")

# Builder pattern with threshold
matches = (
    db.vector_search_with("Document", "embedding", query_vec)
    .k(10)
    .threshold(0.5)
    .search()
)
```

## Data Types

Supported property data types:

| Type | Python | Description |
|------|--------|-------------|
| `string` | `str` | UTF-8 string |
| `int` | `int` | 64-bit integer |
| `float` | `float` | 64-bit float |
| `bool` | `bool` | Boolean |
| `vector:N` | `list[float]` | N-dimensional vector |

## Query Results

Query results are returned as `list[dict[str, Any]]`:

```python
results = db.query("MATCH (n:Person) RETURN n.name AS name, n.age AS age")
for row in results:
    print(f"Name: {row['name']}, Age: {row['age']}")
```

## EXPLAIN and PROFILE

Analyze query execution:

```python
# Get query plan without executing
plan = db.explain("MATCH (n:Person) RETURN n")
print(plan["plan_text"])
print(plan["cost_estimates"])

# Execute with profiling
results, profile = db.profile("MATCH (n:Person) RETURN n")
print(f"Total time: {profile['total_time_ms']}ms")
print(f"Peak memory: {profile['peak_memory_bytes']} bytes")
```

## Snapshots

Point-in-time snapshots:

```python
# Create snapshot
snapshot_id = db.create_snapshot("before_migration")

# List snapshots
for snap in db.list_snapshots():
    print(f"{snap.snapshot_id}: {snap.name} ({snap.vertex_count} vertices)")

# Open read-only view at snapshot
old_db = db.at_snapshot(snapshot_id)
results = old_db.query("MATCH (n) RETURN count(n)")

# Restore to snapshot
db.restore_snapshot(snapshot_id)
```

## Error Handling

The library raises standard Python exceptions:

- `RuntimeError`: Query execution errors
- `ValueError`: Invalid parameters
- `OSError`: Database I/O errors

```python
try:
    db.query("INVALID CYPHER")
except RuntimeError as e:
    print(f"Query error: {e}")
```

## Full API Documentation

See the [auto-generated pdoc documentation](../api/python/index.html) for complete API details.
