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
    let session = db.session();
    let tx = session.tx().await?;

    // Create a bulk writer with deferred indexing
    let mut bulk = tx.bulk_writer()
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
| `skip_before_triggers(bool)` | Load even though a synchronous before-trigger watches what the load writes, without running it (see [Triggers and CDC](#triggers-cdc-and-the-enclosing-transaction)) | `false` |
| `on_progress(callback)` | Progress callback for monitoring | None |

### Progress Monitoring

```rust
let mut bulk = tx.bulk_writer()
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
let mut bulk = tx.bulk_writer().build()?;
bulk.insert_vertices("Paper", vertices).await?;

// Something went wrong - abort without committing
bulk.abort().await?;
// No data is persisted
```

### What `commit()` Returning Means

A bulk load is decided by one durable write inside `commit()`, its **commit
point**: the recovery marker that records every staged row. Nothing a load
writes is visible before it. The result of `commit()` says which side of the
commit point the load ended on, so it always tells you whether to retry:

| Result | Applied? | What to do |
|--------|----------|------------|
| `Ok(stats)` | Yes, every row visible | Nothing. `stats.warnings` lists later steps that failed without affecting the load (index builds, snapshot manifest, schema save, marker delete); each is completed by the next open or the index rebuild worker. |
| `Err(UniError::BulkCommitIncomplete { load_id, .. })` | Yes (committed) | **Do not retry.** Publishing could not be finished by `commit()` after bounded retries. A background task keeps retrying it, with backoff, until it completes or the database shuts down; otherwise the next open completes it. Each row lands exactly once, and until then none of the load is visible to readers of this database handle. |
| `Err(UniError::BulkCommitOutcomeUnknown { load_id, .. })` | Unknown | Writing the commit marker failed in a way that may still have written it. Reopen, then call `db.bulk_load_applied(load_id)` and retry only if it is `false`. |
| any other `Err` | No | Nothing of the load is visible, now or after a reopen, and its staged files are deleted. Retrying is safe. |

The typed errors arrive inside `commit()`'s `anyhow::Error`: match with
`err.downcast_ref::<UniError>()`. In Python they are
`UniBulkCommitIncompleteError` and `UniBulkCommitOutcomeUnknownError`, each with
`load_id` and `reason` attributes.

A failure in the publish step itself (after the commit point) is retried
in-process through the same exactly-once path recovery uses, so a transient
error there returns `Ok` with a warning rather than a half-visible load.

**Readers never see part of a load.** A load is published table by table
(the per-label tables, the shared vertex table, the edge table, then each
edge type's two direction tables). While that runs -- in a successful
`commit()`, or in the background after `BulkCommitIncomplete` -- every row of
the load is hidden from the database handle's readers, and it appears all at
once when the last table is published: a label scan and a label-less scan
agree, and no vertex appears without its edges or an edge in one direction
only. The guarantee covers the handle the load ran on (its sessions,
transactions and pinned snapshots). Another process, or a second handle
opened on the same path, reads the tables directly and can still see a load
part way through its publish.

A fork created while a load is being published waits for the publish to
finish (up to 30 seconds) and then includes all of the load. If the load stays
incomplete past that (its background re-drive keeps failing), creating the
fork fails with `UniError::ForkLifecycle` (stage `bulk_publish`) and creates
nothing; retry it.

**Declared edge endpoints.** `insert_edges` refuses an edge whose endpoint
lacks its type's declared label. An endpoint that existed before the load can
still lose that label (or be deleted) before the load is published, since the
load's edges are invisible until then. The load is applied regardless; the
edge type is then recorded unverified, so queries stop assuming its endpoints'
labels, and `stats.warnings` (or the log, for a load completed in the
background or at the next open) names it. Fix the endpoints and run
`CALL uni.admin.verifyEdgeEndpoints()` to restore it.

Bulk loading needs a storage backend that stages rows invisibly until
`commit()`, as the built-in Lance storage does; any other backend is refused
with `UniError::BulkLoadUnsupported` when the writer is built.

```rust
let mut bulk = tx.bulk_writer().build()?;
let load_id = bulk.load_id().to_string();   // also in BulkStats::load_id
bulk.insert_vertices("Paper", vertices).await?;
match bulk.commit().await {
    Ok(stats) => for w in &stats.warnings { tracing::warn!(%w) },
    Err(e) => match e.downcast_ref::<UniError>() {
        Some(UniError::BulkCommitIncomplete { .. }) => { /* committed: do not retry */ }
        Some(UniError::BulkCommitOutcomeUnknown { .. }) => {
            // after reopening: retry only if !db.bulk_load_applied(&load_id).await?
        }
        _ => { /* not applied: safe to retry */ }
    },
}
```

`Uni::bulk_load_applied(load_id)` answers from the load's marker while it
exists and from a small receipt (`catalog/bulk_loads_applied/{load_id}.json`,
one per load) afterwards.

A load on a fork behaves the same way and returns the same results. Its marker
and receipt live in the fork's own namespace (`catalog/forks/{fork_id}/`), a
load left incomplete is completed when the fork is next opened, and you ask
about it on a session of that fork: `fork.bulk_load_applied(load_id)`
(`Session.bulk_load_applied` / `AsyncSession.bulk_load_applied` in Python).
`Uni::bulk_load_applied` answers only for primary loads. Dropping the fork
removes its markers and receipts.

### Crashes During `commit()`

A crash before the commit point leaves nothing visible. A crash after it is
completed the next time the database is opened: every table the interrupted
commit did not reach is published then, each row exactly once. Recovery never
restores an older table version, so writes other sessions committed meanwhile,
including another bulk load, are kept. Several loads interrupted at once are
each completed. User indexes on the load's labels are marked `Stale` and rebuilt
by the background index rebuild.

A successful `commit()` publishes one table at a time (one Lance commit each).
While it runs, a concurrent reader can see the load in some tables and not yet
in others: measured, a label scan (`MATCH (n:Label)`) seeing the new vertices
while a label-less scan (`MATCH (n)`) did not, and edges traversable from the
source side before the destination side. Reads are consistent again once
`commit()` returns.

### Triggers, CDC and the Enclosing Transaction

**A bulk load commits on its own.** `tx.bulk_writer()` and `tx.appender()`
borrow the transaction only for its session write guard. `bulk.commit()` (or
`appender.finish()`) makes the load durable and visible by itself; committing
the transaction afterwards adds nothing to it, and rolling the transaction back
does not undo it.

**Events.** Once a load is visible, `commit()` announces it as one commit
notification (`session.watch()` sees it, with `tx_id` set to the load id and
`session_id` `"bulk-load"`) and emits a create event for every row:
`NODE_CREATE` per vertex and label, `EDGE_CREATE` per edge, in the same event
rows a transaction produces (see [Triggers & CDC](../features/triggers-cdc.md)).
The rows are read back from storage in chunks of at most 4096
(`uni_bulk::BULK_EVENT_CHUNK_ROWS`), so a load's events are never held in
memory at once:

- After-phase triggers (`AfterMutation`, `AfterCommit`) fire once per chunk.
- CDC receives the load as one announcement whose rows arrive as one or more
  `CdcBatch`es, each carrying the load's LSN range: `lsn_start` is the load's
  version, `lsn_end` the database version when the load became visible. Each
  stream is checkpointed once, after the last of them. While CDC has four
  chunks undelivered, `commit()` waits for it.
- `commit()` returns after the events are delivered (asynchronous triggers are
  only started by then). Index building comes after.

**Before-triggers refuse a load.** A synchronous `BeforeMutation` or
`BeforeCommit` trigger can veto a transaction, but a load's events exist only
once it is already visible. So a load that writes vertices under a label, or
edges of a type, that such a trigger subscribes to is refused with
`UniError::BulkLoadBlockedByTrigger` before those rows are staged; a trigger
registered while a load is running is caught by `commit()`, which then discards
the load. Either way nothing of the load becomes visible. The match is on event
kind (`NODE_CREATE` / `EDGE_CREATE`) and label or edge type only: a trigger
with a predicate counts as matching, since the predicate cannot be evaluated
before the rows exist. To load anyway, accepting that those triggers do not see
the load, build the writer with `.skip_before_triggers(true)` (the appender has
the same option); after-phase triggers and CDC still get the events.

**Cancelling `commit()`.** Once the load is visible, its events are delivered
by a background task that `commit()` waits for. Dropping the `commit()` future
does not stop that delivery: the events still reach every trigger and CDC
output. The dropped commit does skip the load's finalization (index builds,
snapshot), which the next open completes -- and that open delivers the load's
events a second time.

**Loads completed later.** A load whose publish `commit()` could not finish
(`BulkCommitIncomplete`) emits its announcement and events from the background
task once that task makes it visible. A load completed by recovery when the
database is next opened -- the process died after the load became visible and
before its events were delivered -- emits them during that open: the open
announces it before `build()` returns and streams its rows in the background.
They reach the triggers present during the open, which are the built-in ones and
those declared with `uni.plugin.declareTrigger`; a trigger or CDC output
registered after `build()` returns does not see them. The load's marker stays
until the delivery finishes, so a crash during it repeats the delivery at the
next open (consumers may see a load twice, never not at all). A fork's
recovered load fires triggers when the fork is next opened. A read-only open
delivers nothing and leaves the load to the next writable open.

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

## COPY TO and COPY FROM (Cypher)

`COPY` exports one label or edge type to a file and imports one back. The
format comes from `WITH {format: ...}` or the file extension (`.csv`,
`.parquet`); paths may be local or `s3://`, `gs://`, `az://` URLs, and local
paths are checked against `UniConfig::file_sandbox` when it is enabled.

```cypher
COPY Person TO '/data/people.parquet'
COPY KNOWS TO '/data/knows.csv' WITH {delimiter: ';'}
COPY Person FROM '/data/people.parquet'
COPY KNOWS FROM '/data/knows.csv' WITH {delimiter: ';'}
```

**File layout.** A vertex file has a `_vid` column; an edge file has `_eid`,
`_src` and `_dst`. Then comes one column per property: exactly the properties
`properties(n)` returns — declared and undeclared, flushed or not — taken as the
union over all exported entities and sorted by name. An entity without a
property has an empty cell (CSV) or a null (Parquet). Internal storage columns
never appear. Labels and edge types need not be declared.

**Import.** Identity columns are not stored: imported vertices get new ids. An
edge's endpoints are read from `_src`/`_dst` (or `src`/`dst` when the file has
no `_src`/`_dst`; override with `src_col`/`dst_col`), so an edge file imports
into the database whose vertices it references. An empty CSV cell is an absent
property.

**Transactions.** Inside a transaction (`tx.execute`, `session.run`'s
auto-commit) `COPY FROM` writes into the transaction like any other write: the
rows are visible to that transaction only until it commits, a rollback
discards them, and an import larger than `max_transaction_memory` fails with
the transaction memory error. Edges are checked against their type's declared
endpoint labels.

**Types.** Parquet keeps them: a declared property keeps its declared type, and
an undeclared column gets its values' shared type, or a tagged encoding the
import decodes back when the values are mixed or structured (lists, temporals).
CSV is text: on import a declared property is parsed as its declared type and an
undeclared one is read as a string. Use Parquet to round-trip undeclared
non-string properties.

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
let bulk = tx.bulk_writer()
    .validate_constraints(false)
    .build()?;
```

CHECK validation covers a single `property <op> literal` comparison (`=`, `!=`/`<>`, `>`, `<`, `>=`, `<=`) and is identical on the bulk and transactional write paths — both go through one evaluator. Spacing around the operator is irrelevant: `CHECK (age>=18)` and `CHECK (age >= 18)` are the same constraint. An expression that does not reduce to those three tokens is accepted and stored, but logs a warning and permits the write; one that does reduce to three tokens but whose operands cannot be ordered — a right-hand side naming a second property, say — fails the write with a comparison error rather than a constraint violation. Numeric comparisons — including `=` and `!=` — compare across `Int` and `Float`, so `CHECK (score = 5)` holds for a stored `5.0` whether the row arrives via `BulkWriter` or `tx.execute`.

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
let bulk = tx.bulk_writer()
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
