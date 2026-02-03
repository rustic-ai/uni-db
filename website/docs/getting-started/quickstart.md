# Quick Start

Get Uni running with real data in under 5 minutes. This guide walks you through building the project, importing a sample dataset, and executing your first graph queries.

## Prerequisites

Before starting, ensure you have:
- Uni installed ([Installation Guide](installation.md))
- At least 1 GB free disk space for sample data
- Terminal access

---

## Step 1: Build the Project

If you haven't already built Uni, compile it in release mode for optimal performance:

```bash
cd uni
cargo build --release
```

The binary will be available at `target/release/uni`. For convenience, you can add it to your PATH:

```bash
export PATH="$PWD/target/release:$PATH"
```

---

## Step 2: Explore the Demo Dataset

Uni includes a sample dataset based on academic papers and citations, perfect for learning graph queries.

### Dataset Overview

| Entity | Description | Sample Size |
|--------|-------------|-------------|
| **Papers** | Academic publications with titles, years, and embeddings | ~1,000 |
| **Citations** | Directed edges from citing to cited papers | ~5,000 |

### Data Format

**Vertices (`papers.jsonl`):**
```json
{"vid": 0, "title": "Attention Is All You Need", "year": 2017, "citation_count": 3593, "embedding": [0.12, -0.34, ...]}
{"vid": 1, "title": "BERT: Pre-training of Deep Bidirectional Transformers", "year": 2018, "citation_count": 1021, "embedding": [0.08, -0.21, ...]}
```

**Edges (`citations.jsonl`):**
```json
{"src_vid": 1, "dst_vid": 0}
{"src_vid": 2, "dst_vid": 0}
```

The `src_vid` paper cites the `dst_vid` paper. In graph terms: `(vid=1)-[:CITES]->(vid=0)`.

---

## Step 3: Import Data

Use the `import` command to ingest the sample dataset. This creates the demo schema (Paper/CITES), loads data, and persists it to storage.

```bash
uni import semantic-scholar \
    --papers demos/demo01/data/papers.jsonl \
    --citations demos/demo01/data/citations.jsonl \
    --output ./my-first-graph
```

### What Happens During Import

```
Initializing storage at ./my-first-graph
Loading papers from demos/demo01/data/papers.jsonl
Processed 1000 papers
Flushing papers...
Loading citations from demos/demo01/data/citations.jsonl
Processed 5000 citations
Flushing citations...
Import complete!
```

The import process:
1. **Schema Setup** — Creates `Paper` + `CITES`
2. **Load Papers** — Inserts properties using provided `vid` values
3. **Flush Papers** — Persists vertex data to storage
4. **Load Citations** — Inserts edges using `src_vid` / `dst_vid`
5. **Flush Citations** — Persists edges and adjacency

If you want vector indexes, create them after import:

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper) ON p.embedding
OPTIONS { type: 'hnsw' }
```

## Step 4: Use the Interactive REPL

The easiest way to explore Uni is via the interactive shell (REPL). It supports syntax highlighting, command history, and formatted table output.

Start the REPL by running `uni` without any arguments (or with the `repl` command):

```bash
uni repl --path ./my-first-graph
```

You should see the welcome message:

```text
Welcome to UniDB CLI
Type 'help' for commands or enter a Cypher query.
uni> 
```

### Try These Queries in the REPL:

**1. Count all papers:**
```cypher
MATCH (p:Paper) RETURN COUNT(p)
```

**2. Find papers cited by "Attention Is All You Need":**
```cypher
MATCH (source:Paper {title: 'Attention Is All You Need'})-[:CITES]->(cited)
RETURN cited.title, cited.year
ORDER BY cited.year DESC
```

**3. Clear the screen:**
```text
clear
```

**4. Exit the shell:**
```text
exit
```

---

## Step 5: Run One-off Queries

You can also run queries directly from your terminal using the `query` command:

```bash
uni query "MATCH (p:Paper) RETURN COUNT(p)" --path ./my-first-graph
```

---

## Step 6: Create New Data

Add new papers and citations with `CREATE`:

```bash
# Create a new paper
uni query "
    CREATE (p:Paper {
        id: 'new_paper_001',
        title: 'My Research Paper',
        year: 2024,
        venue: 'ArXiv'
    })
    RETURN p.title
" --path ./my-first-graph

# Create a citation edge
uni query "
    MATCH (citing:Paper), (cited:Paper)
    WHERE citing.id = 'new_paper_001'
      AND cited.title = 'Attention Is All You Need'
    CREATE (citing)-[:CITES]->(cited)
    RETURN citing.title, cited.title
" --path ./my-first-graph
```

---

## Understanding Query Execution

When you run a query, Uni:

```
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│   1. Parse     │ ──▶ │   2. Plan      │ ──▶ │   3. Execute   │
│    Cypher      │     │   Optimize     │     │   Vectorized   │
└────────────────┘     └────────────────┘     └────────────────┘
        │                      │                      │
        ▼                      ▼                      ▼
   "MATCH (n)..."      LogicalPlan with       Process batches
   becomes AST         filter pushdown        of 1024 rows
```

1. **Parse**: Cypher text → Abstract Syntax Tree
2. **Plan**: AST → Optimized Logical Plan (with predicate pushdown)
3. **Execute**: Vectorized operators process Arrow batches

---

## What's Next?

You've successfully:
- Imported graph data from JSONL files
- Traversed graph relationships with MATCH
- Filtered results with WHERE clauses
- Aggregated data with COUNT and ORDER BY
- Searched vectors with semantic similarity

### Continue Learning

| Topic | Description |
|-------|-------------|
| [CLI Reference](cli-reference.md) | All CLI commands and options |
| [Cypher Querying](../guides/cypher-querying.md) | Complete query language guide |
| [Vector Search](../guides/vector-search.md) | Deep dive into semantic search |
| [Data Ingestion](../guides/data-ingestion.md) | Import strategies and formats |
| [Architecture](../concepts/architecture.md) | Understand Uni's internals |

---

## Troubleshooting

### "No vertices found"

Ensure your JSONL files have the correct format:
```bash
head -1 papers.jsonl
# Should output valid JSON: {"id": "...", "title": "...", ...}
```

### "Property not found"

Check that query property names match your schema exactly (case-sensitive):
```bash
# Wrong: WHERE p.Title = '...'
# Right: WHERE p.title = '...'
```

### "Path does not exist"

Provide the full path to your storage directory:
```bash
# Use absolute paths for clarity
uni query "..." --path /home/user/my-first-graph
```

### Slow queries

For large datasets, ensure vector indexes are built:
```bash
uni query "SHOW INDEXES" --path ./my-first-graph
```
