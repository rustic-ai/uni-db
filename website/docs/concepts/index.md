# Core Concepts

Understand the fundamental concepts behind Uni's design and architecture.

<div class="feature-grid">

<div class="feature-card">

### [Architecture](architecture.md)
Layered design, component overview, and how the pieces fit together.

</div>

<div class="feature-card">

### [Data Model](data-model.md)
Vertices, edges, properties, labels, and the property graph model.

</div>

<div class="feature-card">

### [Identity Model](identity.md)
VID, EID, UniId, and how Uni identifies entities.

</div>

<div class="feature-card">

### [Indexing](indexing.md)
Vector, scalar (BTree), full-text, JSON FTS, and inverted indexes for fast queries.

</div>

<div class="feature-card">

### [Concurrency](concurrency.md)
Single-writer model, snapshot isolation, and concurrent readers.

</div>

</div>

## Overview

Uni is built on three core principles:

1. **Multi-Model Unification** — Graph, vector, document, and columnar workloads in one engine
2. **Embedded Simplicity** — No separate server, network hops, or operational overhead
3. **Cloud-Native Storage** — Object store persistence with local caching for performance

## Architecture at a Glance

```
┌─────────────────────────────────────────┐
│            Query Layer                   │
│   Parser → Planner → Executor           │
├─────────────────────────────────────────┤
│            Runtime Layer                 │
│   L0 Buffer | CSR Cache | Properties    │
├─────────────────────────────────────────┤
│            Storage Layer                 │
│   Lance Datasets | WAL | Indexes        │
├─────────────────────────────────────────┤
│            Object Store                  │
│      S3 | GCS | Azure | Local           │
└─────────────────────────────────────────┘
```

## Next Steps

Start with [Architecture](architecture.md) for a comprehensive system overview.
