# Data Ingestion Guide

This guide covers all methods for getting data into Uni, from bulk imports to streaming writes and programmatic access.

## Overview

Uni supports multiple ingestion patterns:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION PATTERNS                              │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│    BULK IMPORT      │   STREAMING WRITE   │      PROGRAMMATIC API           │
├─────────────────────┼─────────────────────┼─────────────────────────────────┤
│ • JSONL files       │ • Real-time inserts │ • Rust crate API                │
│ • CLI import        │ • Embedded API      │ • Writer interface              │
│ • One-time load     │ • Continuous        │ • Fine-grained control          │
├─────────────────────┼─────────────────────┼─────────────────────────────────┤
│ Best for:           │ Best for:           │ Best for:                       │
│ Initial data load   │ Live applications   │ Custom pipelines                │
│ Batch migrations    │ Event streaming     │ Integration code                │
└─────────────────────┴─────────────────────┴─────────────────────────────────┘
```

---

## Bulk Import (CLI)

The fastest way to load large datasets.

### Input File Format

#### Vertices (JSONL)

Each line is a JSON object representing a **Paper** vertex for the built-in demo importer:

```json
{"vid": 0, "title": "Attention Is All You Need", "year": 2017, "citation_count": 3593, "embedding": [0.12, -0.34, ...]}
{"vid": 1, "title": "BERT", "year": 2018, "citation_count": 1021, "embedding": [0.08, -0.21, ...]}
```

**Required Fields:**
- `vid` (Integer): Pre-assigned vertex ID (64-bit)
- `title` (String)
- `year` (Integer)
- `citation_count` (Integer)
- `embedding` (Array[Float])

**Optional Fields:**
- Additional fields are ignored by the CLI importer

#### Edges (JSONL)

Each line is a JSON object representing a **CITES** edge:

```json
{"src_vid": 12, "dst_vid": 4}
{"src_vid": 25, "dst_vid": 4}
```

**Required Fields:**
- `src_vid` (Integer): Source vertex VID
- `dst_vid` (Integer): Destination vertex VID

**Optional Fields:**
- Additional fields are ignored by the CLI importer

### Running Import

**Basic Import (demo importer):**

```bash
uni import semantic-scholar \
    --papers ./data/papers.jsonl \
    --citations ./data/citations.jsonl \
    --output ./storage
```

**Note:** The CLI `import` command currently supports the built-in **Semantic Scholar demo format** (papers + citations with VIDs). It does **not** accept custom schemas, batch sizes, or incremental modes. For custom schemas or larger pipelines, use the Rust/Python APIs or Cypher ingestion.

### Import Options

| Option | Default | Description |
|--------|---------|-------------|
| `<name>` | Required | Dataset name (label only; used for logging) |
| `--papers` | Required | Path to papers JSONL file |
| `--citations` | Required | Path to citations JSONL file |
| `--output` | `./storage` | Output storage path |

### Import Process

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           IMPORT PIPELINE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   [1] SCHEMA SETUP                                                          │
│       └── Create Paper label, CITES edge type                                │
│                                                                             │
│   [2] PAPERS PASS                                                           │
│       ├── Stream papers JSONL                                               │
│       ├── Use provided VIDs (`vid`)                                         │
│       ├── Insert properties (title/year/citation_count/embedding)           │
│       └── Store schema-defined properties                                   │
│                                                                             │
│   [3] FLUSH PAPERS                                                          │
│       └── Persist vertex data to storage                                    │
│                                                                             │
│   [4] CITATIONS PASS                                                        │
│       ├── Stream citations JSONL                                            │
│       ├── Use provided VIDs (`src_vid`, `dst_vid`)                          │
│       └── Insert edges (CITES)                                              │
│                                                                             │
│   [5] FLUSH CITATIONS + SNAPSHOT                                            │
│       └── Persist edges, adjacency, and manifest                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Schema for the CLI Importer

The `uni import` command uses a **fixed schema** for the demo dataset:
- Label: `Paper` (document label) with properties `title`, `year`, `citation_count`, `embedding`
- Edge type: `CITES` (`Paper` → `Paper`)

The CLI importer **does not** infer or accept a custom schema. For custom schemas, use:
- **Cypher DDL** (`CREATE LABEL`, `CREATE EDGE TYPE`, `CREATE INDEX`), or
- **Rust/Python schema builders**, then ingest via `CREATE` or `BulkWriter`.

---

## Streaming Writes (Cypher)

For real-time applications, use CREATE statements.

### Creating Vertices

```cypher
// Single vertex
CREATE (p:Paper {
  title: 'New Research Paper',
  year: 2024,
  venue: 'ArXiv'
})
RETURN p

// With external ID
CREATE (p:Paper {
  id: 'paper_new_001',
  title: 'New Research'
})
```

### Creating Edges

```cypher
// Between existing vertices
MATCH (p:Paper {id: 'paper_001'}), (a:Author {id: 'author_001'})
CREATE (p)-[:AUTHORED_BY {position: 1}]->(a)

// Create both nodes and edge
CREATE (p:Paper {title: 'New Paper'})-[:AUTHORED_BY]->(a:Author {name: 'New Author'})
```

### Batch Creates

```cypher
// Multiple vertices
UNWIND $papers AS paper
CREATE (p:Paper {
  id: paper.id,
  title: paper.title,
  year: paper.year
})

// Multiple edges
UNWIND $edges AS edge
MATCH (src:Paper {id: edge.src}), (dst:Paper {id: edge.dst})
CREATE (src)-[:CITES]->(dst)
```

### HTTP API (Planned)

Uni does not expose an HTTP API in the current CLI. Use the CLI or embedded Rust/Python APIs for ingestion.

---

## Programmatic API (Rust)

For maximum control, use the Rust API directly.

### Bulk Loading with BulkWriter

The `BulkWriter` API provides high-performance bulk loading with deferred index building:

```rust
use std::collections::HashMap;

use serde_json::json;
use uni_db::{Result, Uni};
use uni_db::api::bulk::EdgeData;

#[tokio::main]
async fn main() -> Result<()> {
    let db = Uni::open("./my-graph").build().await?;

    // Create a bulk writer with deferred indexing
    let mut bulk = db.bulk_writer()
        .defer_vector_indexes(true)   // Defer vector index updates
        .defer_scalar_indexes(true)   // Defer scalar index updates
        .batch_size(10_000)           // Flush every 10K records
        .on_progress(|progress| {
            println!("{}: {} rows processed",
                progress.phase, progress.rows_processed);
        })
        .build()?;

    // Bulk insert vertices
    let vertices: Vec<HashMap<String, serde_json::Value>> = papers
        .iter()
        .map(|p| {
            let mut props = HashMap::new();
            props.insert("title".to_string(), json!(p.title));
            props.insert("year".to_string(), json!(p.year));
            props.insert("embedding".to_string(), json!(p.embedding));
            props
        })
        .collect();

    let vids = bulk.insert_vertices("Paper", vertices).await?;
    println!("Inserted {} vertices", vids.len());

    // Bulk insert edges
    let edges: Vec<EdgeData> = citations
        .iter()
        .map(|c| EdgeData::new(vid_map[&c.from], vid_map[&c.to], HashMap::new()))
        .collect();

    let eids = bulk.insert_edges("CITES", edges).await?;
    println!("Inserted {} edges", eids.len());

    // Commit and rebuild indexes
    let stats = bulk.commit().await?;
    println!("Bulk load complete:");
    println!("  Vertices: {}", stats.vertices_inserted);
    println!("  Edges: {}", stats.edges_inserted);
    println!("  Indexes rebuilt: {}", stats.indexes_rebuilt);
    println!("  Duration: {:?}", stats.duration);

    Ok(())
}
```

### BulkWriter Options

| Option | Description | Default |
|--------|-------------|---------|
| `defer_vector_indexes(bool)` | Defer vector index updates until commit | `true` |
| `defer_scalar_indexes(bool)` | Defer scalar index updates until commit | `true` |
| `batch_size(usize)` | Records per batch before flushing | `10_000` |
| `async_indexes(bool)` | Build indexes in background after commit | `false` |
| `validate_constraints(bool)` | Enforce NOT NULL / UNIQUE / CHECK during bulk load | `true` |
| `max_buffer_size_bytes(usize)` | Trigger checkpoint flush when buffer is large | `1_073_741_824` (1 GB) |
| `on_progress(callback)` | Progress callback for monitoring | None |

### Progress Monitoring

```rust
let mut bulk = db.bulk_writer()
    .on_progress(|progress| {
        match progress.phase {
            BulkPhase::Inserting => {
                println!("Inserting: {}/{}",
                    progress.rows_processed,
                    progress.total_rows.unwrap_or(0));
            }
            BulkPhase::RebuildingIndexes { label } => {
                println!("Rebuilding indexes for label: {}", label);
            }
            BulkPhase::Finalizing => {
                println!("Finalizing snapshot...");
            }
        }
    })
    .build()?;
```

### Aborting Bulk Operations

```rust
let mut bulk = db.bulk_writer().build()?;
bulk.insert_vertices("Paper", vertices).await?;

// Something went wrong - abort without committing
bulk.abort().await?;
// No data is persisted
```

### Performance Guidelines

| Dataset Size | Recommended Settings |
|--------------|---------------------|
| < 100K | `batch_size: 5_000`, `defer_*: false` |
| 100K - 1M | `batch_size: 10_000`, `defer_*: true` |
| 1M - 10M | `batch_size: 50_000`, `defer_*: true` |
| > 10M | `batch_size: 100_000`, `defer_*: true`, `async_indexes: true` |

---

### Low-Level Writer API (Advanced)

This is an internal, low-level API. Prefer **BulkWriter** unless you need direct control.

```rust
use std::path::Path;
use std::sync::Arc;

use uni_db::core::schema::SchemaManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let schema_manager = Arc::new(
        SchemaManager::load(Path::new("./storage/schema.json")).await?
    );

    let storage = Arc::new(
        StorageManager::new("./storage", schema_manager.clone()).await?
    );

    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0).await?;
    Ok(())
}
```

### Insert Vertices

```rust
use std::collections::HashMap;
use serde_json::json;

// Allocate VID (label_id is required but not encoded in VID)
let label_id = schema_manager.schema().label_id_by_name("Paper").unwrap();
let vid = writer.next_vid(label_id).await?;

let mut props: HashMap<String, serde_json::Value> = HashMap::new();
props.insert("title".to_string(), json!("New Paper"));
props.insert("year".to_string(), json!(2024));

writer.insert_vertex(vid, props).await?;
```

### Insert Edges

```rust
use std::collections::HashMap;
use serde_json::json;

let edge_type_id = schema_manager.schema().edge_type_id_by_name("CITES").unwrap();
let eid = writer.next_eid(edge_type_id).await?;

let mut props: HashMap<String, serde_json::Value> = HashMap::new();
props.insert("weight".to_string(), json!(0.95));

writer.insert_edge(src_vid, dst_vid, edge_type_id, eid, props).await?;
```

### Batch Inserts (Vertices)

```rust
use std::collections::HashMap;

let label_id = schema_manager.schema().label_id_by_name("Paper").unwrap();
let vids = writer.allocate_vids(label_id, papers.len()).await?;
let props_batch: Vec<HashMap<String, serde_json::Value>> = papers
    .iter()
    .map(build_properties)
    .collect();

writer
    .insert_vertices_batch(vids, props_batch, vec!["Paper".to_string()])
    .await?;
```

For edge batches, loop over `insert_edge` or use **BulkWriter**.

### Flush to Storage

```rust
// Manual flush (creates a snapshot)
writer.flush_to_l1(None).await?;
```

Auto-flush behavior is controlled via `UniConfig` when constructing a writer with `Writer::new_with_config` (`auto_flush_threshold`, `auto_flush_interval`, `auto_flush_min_mutations`).

---

## Data Transformation

**Note:** Field names depend on ingestion method.  
- For `uni import`, emit `vid` for papers and `src_vid`/`dst_vid` for citations.  
- For Cypher/BulkWriter pipelines, you can use your own `id` properties.

### Converting CSV to JSONL

```python
import csv
import json

# Convert CSV to JSONL
with open('papers.csv', 'r') as csv_file, open('papers.jsonl', 'w') as jsonl_file:
    reader = csv.DictReader(csv_file)
    for row in reader:
        # Transform as needed
        record = {
            'id': row['paper_id'],
            'title': row['title'],
            'year': int(row['year']),
            'venue': row['venue']
        }
        jsonl_file.write(json.dumps(record) + '\n')
```

### Adding Embeddings

```python
import json
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

with open('papers_raw.jsonl', 'r') as infile, open('papers.jsonl', 'w') as outfile:
    for line in infile:
        record = json.loads(line)
        # Generate embedding from title + abstract
        text = record['title'] + ' ' + record.get('abstract', '')
        embedding = model.encode(text).tolist()
        record['embedding'] = embedding
        outfile.write(json.dumps(record) + '\n')
```

### Extracting from Database

```python
import psycopg2
import json

conn = psycopg2.connect("postgresql://...")
cursor = conn.cursor()

# Export vertices
cursor.execute("SELECT id, title, year FROM papers")
with open('papers.jsonl', 'w') as f:
    for row in cursor:
        record = {'id': str(row[0]), 'title': row[1], 'year': row[2]}
        f.write(json.dumps(record) + '\n')

# Export edges
cursor.execute("SELECT citing_id, cited_id FROM citations")
with open('citations.jsonl', 'w') as f:
    for row in cursor:
        record = {'src': str(row[0]), 'dst': str(row[1])}
        f.write(json.dumps(record) + '\n')
```

---

## Incremental Updates

The CLI importer is **one-shot**. For incremental updates, use Cypher or the BulkWriter API.

### Delta Processing

```cypher
// Add new vertices
UNWIND $new_papers AS paper
MERGE (p:Paper {id: paper.id})
SET p.title = paper.title, p.year = paper.year

// Add new edges
UNWIND $new_edges AS edge
MATCH (src:Paper {id: edge.src}), (dst:Paper {id: edge.dst})
MERGE (src)-[:CITES]->(dst)
```

---

## Validation & Error Handling

### Constraint Validation

BulkWriter validates NOT NULL / UNIQUE / CHECK constraints by default. For trusted data sources, you can skip validation:

```rust
let bulk = db.bulk_writer()
    .validate_constraints(false)
    .build()?;
```

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Property type mismatch` | Wrong data type | Check schema types |
| `Unknown property` | Property not in schema | Add to schema or filter |
| `Vector dimension mismatch` | Wrong embedding size | Ensure consistent dimensions |
| `Unique constraint violation` | Duplicate key | Deduplicate source data or change constraint |
| `Missing required property` | Null in non-nullable | Fix source data |

---

## Performance Tips

### Large File Handling

```bash
# The CLI importer streams JSONL by default.
# For very large files, split and import into separate storage paths.
split -l 1000000 huge_file.jsonl chunk_
# Import chunks sequentially or in parallel (separate outputs)
```

### Memory Management

For BulkWriter pipelines, tune batch sizes and buffer limits:

```rust
let bulk = db.bulk_writer()
    .batch_size(5_000)
    .max_buffer_size_bytes(256 * 1024 * 1024) // 256 MB
    .build()?;
```

### Parallel Import

```bash
# Parallel imports are safe only when writing to separate storage paths
uni import shard1 --papers shard1.jsonl --citations shard1_edges.jsonl --output ./storage/shard1 &
uni import shard2 --papers shard2.jsonl --citations shard2_edges.jsonl --output ./storage/shard2 &
wait
```

---

## Next Steps

- [Schema Design](schema-design.md) — Best practices for schema definition
- [Vector Search](vector-search.md) — Working with embeddings
- [Performance Tuning](performance-tuning.md) — Optimization strategies
