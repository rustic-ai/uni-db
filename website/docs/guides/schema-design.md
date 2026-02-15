# Schema Design Guide

A well-designed schema is crucial for performance, maintainability, and query efficiency. This guide covers best practices for modeling your domain in Uni.

## Schema Design Principles

### 1. Model the Domain, Not the Queries

Design your schema around your domain entities, not specific queries:

```
✓ Good: Entities represent real-world concepts
  Paper, Author, Venue, Citation

✗ Bad: Entities shaped by specific queries
  PaperWithAuthorNames, RecentPapersByVenue
```

### 2. Use Labels for Types, Not States

Labels define entity types, not transient states:

```
✓ Good: Labels are stable types
  :Paper, :Author, :Venue

✗ Bad: Labels represent changeable states
  :PublishedPaper, :DraftPaper, :RetractedPaper
  (Use a "status" property instead)
```

### 3. Relationships Are First-Class

Graph relationships are powerful—use them:

```
✓ Good: Relationships as edges
  (paper)-[:AUTHORED_BY]->(author)
  (paper)-[:CITES]->(cited)

✗ Bad: Relationships as properties
  Paper { author_ids: ["a1", "a2"] }
```

### 4. Keep Vertices Focused

Each vertex should represent one cohesive entity:

```
✓ Good: Focused vertex
  Paper { title, year, abstract }

✗ Bad: Kitchen sink vertex
  Paper { title, year, author_name, venue_name, citation_count }
  (author_name, venue_name should be separate vertices)
```

---

## Labels

### Naming Conventions

| Convention | Example | Rationale |
|------------|---------|-----------|
| Singular nouns | `:Paper` | Represents one entity |
| PascalCase | `:ResearchPaper` | Standard convention |
| Descriptive | `:AcademicPaper` | Clear meaning |
| Avoid abbreviations | `:Organization` not `:Org` | Readable |

### Label Granularity

**Too Few Labels:**
```
// All entities in one label - hard to query efficiently
:Entity { type: "paper", ... }
:Entity { type: "author", ... }
```

**Too Many Labels:**
```
// Fragmented - complex schema, poor caching
:NeurIPSPaper, :ICMLPaper, :ICLRPaper, :ArXivPaper
```

**Just Right:**
```
// Labels represent fundamental types
:Paper { venue: "NeurIPS" }  // venue is a property
:Author
:Venue
```

### Label Hierarchy Considerations

Uni supports multi-label vertices — each vertex can have one or more labels. The complete label set is stored as a `_labels: List<Utf8>` column in every per-label table. For modelling hierarchies you can use multi-labels directly, or consider these patterns:

```cypher
// Option 1: Multi-label vertex (preferred)
CREATE (p:Paper:ConferenceSubmission {title: "..."})

// Option 2: Property-based classification
CREATE (:Paper {paper_type: "research", venue_type: "conference"})

// Option 3: Composition via edges
CREATE (paper:Paper)-[:IN_CATEGORY]->(category:Category)
```

---

## Edge Types

### Naming Conventions

| Convention | Example | Rationale |
|------------|---------|-----------|
| UPPER_SNAKE_CASE | `:AUTHORED_BY` | Visually distinct from labels |
| Verb phrases | `:CITES`, `:BELONGS_TO` | Describes relationship |
| Past tense or present | `:WROTE` or `:WRITES` | Consistent style |
| Active voice | `:CITES` not `:CITED_BY` | Clear direction |

### Direction Semantics

Choose direction based on typical query patterns:

```
// Natural reading direction: subject -[verb]-> object
(paper)-[:CITES]->(cited_paper)      // Paper cites another paper
(paper)-[:AUTHORED_BY]->(author)     // Paper is authored by author
(author)-[:WORKS_AT]->(institution)  // Author works at institution

// Query from either direction
MATCH (a:Author)<-[:AUTHORED_BY]-(p:Paper)  // Find author's papers
MATCH (p:Paper)-[:AUTHORED_BY]->(a:Author)  // Find paper's authors
```

### Edge Properties

Use edge properties sparingly for relationship metadata:

```json
{
  "edge_types": {
    "AUTHORED_BY": {
      "id": 1,
      "src_labels": ["Paper"],
      "dst_labels": ["Author"]
    }
  },
  "properties": {
    "AUTHORED_BY": {
      "position": { "type": "Int32" },      // Author order
      "contribution": { "type": "String" }   // Role: "lead", "contributor"
    }
  }
}
```

**When to Use Edge Properties:**
- Relationship metadata (timestamps, weights, roles)
- Data specific to the relationship, not the connected vertices

**When to Avoid Edge Properties:**
- Frequently updated data (edges are immutable)
- Large data (embeddings, documents)

---

## Property Design

### Data Type Selection

| Data Type | Use Case | Example |
|-----------|----------|---------|
| `String` | Text, identifiers | title, name, doi |
| `Int32` | Small integers | year, count |
| `Int64` | Large integers | timestamp_ms, big_count |
| `Float64` | Decimal values | price, score |
| `Bool` | Flags | is_published, is_retracted |
| `Timestamp` | Date/time | created_at, published_at |
| `Vector` | Embeddings | embedding, image_vector |
| `Json` | Semi-structured | metadata, config |

### Nullability

Be intentional about nullable properties:

```json
{
  "Paper": {
    // Required: every paper has these
    "title": { "type": "String", "nullable": false },

    // Optional: not all papers have these
    "abstract": { "type": "String", "nullable": true },
    "doi": { "type": "String", "nullable": true }
  }
}
```

### Property Naming

| Convention | Example | Notes |
|------------|---------|-------|
| snake_case | `created_at` | Consistent with JSON |
| Descriptive | `citation_count` not `cc` | Self-documenting |
| No prefixes | `title` not `paper_title` | Label provides context |

### Avoid Property Bloat

```
✓ Good: Focused properties
  Paper { title, year, venue, abstract, doi }

✗ Bad: Everything on one vertex
  Paper {
    title, year, venue, abstract, doi,
    author_names,        // Should be vertex + edge
    all_citations,       // Should be edges
    raw_pdf_bytes,       // Too large
    processing_status    // Transient state
  }
```

---

## Vector Properties

### Dimension Planning

Vector dimensions are immutable after schema creation:

```json
{
  "embedding": {
    "type": "Vector",
    "dimensions": 768  // Cannot change later
  }
}
```

**Choosing Dimensions:**

| Model Family | Typical Dimensions | Notes |
|--------------|-------------------|-------|
| Sentence Transformers | 384-768 | General text |
| OpenAI embeddings | 1536-3072 | Commercial |
| CLIP | 512-768 | Multimodal |
| Custom | Varies | Match your model |

### Multiple Embeddings

For different embedding types, use separate properties:

```json
{
  "Paper": {
    "title_embedding": { "type": "Vector", "dimensions": 384 },
    "abstract_embedding": { "type": "Vector", "dimensions": 768 },
    "figure_embedding": { "type": "Vector", "dimensions": 512 }
  }
}
```

### Embedding Versioning

When upgrading embedding models:

```json
{
  "Paper": {
    // Current
    "embedding": { "type": "Vector", "dimensions": 768 },

    // Legacy (deprecated)
    "embedding_v1": { "type": "Vector", "dimensions": 384 }
  }
}
```

---

## Schemaless Properties (Overflow)

### Overview

Uni supports **schemaless properties** - properties not defined in the schema that can still be stored and queried. These properties are automatically stored in an `overflow_json` column and are queryable via automatic query rewriting.

### When to Use Schemaless Properties

**Ideal Use Cases:**
- 🔄 Rapidly evolving schemas during development
- 🧪 Prototyping and exploratory data analysis
- 📝 User-defined metadata fields
- 🎯 Optional/rare properties (< 10% of vertices have them)
- 🌊 Variable property sets (different vertices have different properties)

**Avoid for:**
- ❌ Frequently queried core properties
- ❌ Properties needing indexes
- ❌ Properties used in aggregations/sorting
- ❌ Performance-critical query paths

### Creating Schemaless Labels

#### Pure Schemaless (No Schema Properties)

```rust
// Create label with NO property definitions
db.schema().label("Document").apply().await?;

// Create with arbitrary properties
db.execute("CREATE (:Document {
    title: 'Research Paper',
    author: 'Alice',
    tags: ['ml', 'nlp'],
    year: 2024,
    conference: 'NeurIPS'
})").await?;

// Query works normally (automatic rewriting)
let results = db.query("
    MATCH (d:Document)
    WHERE d.author = 'Alice' AND d.year > 2023
    RETURN d.title, d.conference
").await?;
```

#### Mixed Schema + Schemaless

```rust
// Define core properties in schema
db.schema()
    .label("Person")
    .property("name", DataType::String)    // Schema property (fast)
    .property("email", DataType::String)   // Schema property (fast)
    .apply().await?;

// Create with schema + overflow properties
db.execute("CREATE (:Person {
    name: 'Bob',           -- Schema (typed column)
    email: 'bob@x.com',    -- Schema (typed column)
    city: 'NYC',           -- Overflow (overflow_json)
    github: 'bob123',      -- Overflow (overflow_json)
    verified: true         -- Overflow (overflow_json)
})").await?;
```

### Storage and Performance

#### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                      PROPERTY STORAGE                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Properties defined in schema:                                  │
│  ┌──────────────┬──────────────┬───────────────────────────┐   │
│  │ name         │ email        │ ...                       │   │
│  │ (String col) │ (String col) │                           │   │
│  └──────────────┴──────────────┴───────────────────────────┘   │
│                   Typed Arrow Columns                           │
│                   - Fast filtering/sorting                      │
│                   - Type-specific compression                   │
│                   - Indexable                                   │
│                                                                 │
│  Properties NOT in schema:                                      │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ overflow_json (LargeBinary column)                      │    │
│  │                                                         │    │
│  │ JSONB binary blob:                                      │    │
│  │ {                                                       │    │
│  │   "city": "NYC",                                        │    │
│  │   "github": "bob123",                                   │    │
│  │   "verified": true                                      │    │
│  │ }                                                       │    │
│  └────────────────────────────────────────────────────────┘    │
│                   JSONB Binary Format                           │
│                   - Queryable via rewriting                     │
│                   - PostgreSQL-compatible                       │
│                   - No schema migration needed                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Performance Comparison

| Operation | Schema Properties | Overflow Properties |
|-----------|------------------|---------------------|
| **WHERE filtering** | ⚡ ~2-3ms (columnar) | 🟡 ~5-10ms (JSONB parse) |
| **ORDER BY** | ⚡ Fast (native sort) | 🟡 Slower (extract + sort) |
| **Aggregations** | ⚡ Fast (columnar) | 🟡 Slower (extract + aggregate) |
| **Compression** | ⚡ 5-20x (type-specific) | 🔴 ~2x (JSONB binary) |
| **Indexing** | ✅ Supported | ❌ Not supported |
| **Schema changes** | ⚠️ Migration required | ✅ No migration |

### Query Rewriting

Queries on overflow properties are automatically rewritten to use JSONB functions:

```cypher
-- Original query
MATCH (p:Person) WHERE p.city = 'NYC' RETURN p.github

-- Automatically rewritten to (transparent to user):
MATCH (p:Person)
WHERE json_get_string(p.overflow_json, 'city') = 'NYC'
RETURN json_get_string(p.overflow_json, 'github')
```

**Supported Functions:**
- `json_get_string(overflow_json, 'key')` - Extract string
- `json_get_int(overflow_json, 'key')` - Extract integer
- `json_get_float(overflow_json, 'key')` - Extract float
- `json_get_bool(overflow_json, 'key')` - Extract boolean

### Design Guidelines

#### Good: Core Properties in Schema, Optional in Overflow

```rust
db.schema()
    .label("Product")
    // Core properties everyone queries
    .property("name", DataType::String)
    .property("price", DataType::Float64)
    .property("category", DataType::String)
    .apply().await?;

// Create with overflow for optional metadata
db.execute("CREATE (:Product {
    name: 'Widget',
    price: 19.99,
    category: 'electronics',
    // Overflow properties:
    manufacturer: 'ACME',
    warranty_months: 24,
    color: 'blue'
})").await?;
```

#### Bad: Frequently-Queried Properties in Overflow

```rust
// Don't do this!
db.schema().label("Product").apply().await?;  // No properties

// All properties in overflow (slow queries!)
db.execute("CREATE (:Product {
    name: 'Widget',     -- Should be schema property!
    price: 19.99,       -- Should be schema property!
    category: 'electronics'  -- Should be schema property!
})").await?;

// This query will be slow (JSONB parsing for every row)
db.query("
    MATCH (p:Product)
    WHERE p.price < 50 AND p.category = 'electronics'
    ORDER BY p.price
").await?;
```

### Migration from Overflow to Schema

When an overflow property becomes frequently queried, migrate it to schema:

```rust
// Step 1: Add property to schema
db.schema()
    .label("Product")
    .property("manufacturer", DataType::String)  // Promote from overflow
    .apply().await?;

// Step 2: Backfill existing data (one-time operation)
db.execute("
    MATCH (p:Product)
    WHERE p.manufacturer IS NOT NULL
    SET p.manufacturer = p.manufacturer
").await?;

// Future writes automatically use typed column
```

### Edge Overflow Properties

Edges also support overflow properties:

```rust
db.schema()
    .label("Person")
    .edge_type("KNOWS", &["Person"], &["Person"])
    // Only 'since' in schema
    .apply().await?;

db.schema()
    .properties("KNOWS")
    .property("since", DataType::Int)
    .apply().await?;

// Create with overflow edge properties
db.execute("CREATE
    (a:Person {name: 'Alice'})-[:KNOWS {
        since: 2020,         -- Schema property
        context: 'work',     -- Overflow property
        strength: 0.9        -- Overflow property
    }]->(b:Person {name: 'Bob'})
").await?;
```

### Null Handling

Overflow properties handle nulls gracefully:

```cypher
-- Explicit null
CREATE (:Person {name: 'Alice', city: null})

-- Missing property (implicit null)
CREATE (:Person {name: 'Bob'})

-- Both return null for city
MATCH (p:Person) RETURN p.city
```

---

## Index Planning

### Index Strategy

Plan indexes based on query patterns:

```cypher
// Vector index for similarity search
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper) ON p.embedding
OPTIONS { type: "hnsw" }

// Scalar index for frequent filters
CREATE INDEX paper_year FOR (p:Paper) ON (p.year)

// Scalar index for unique lookups
CREATE INDEX paper_doi FOR (p:Paper) ON (p.doi)
```

DDL selects the vector algorithm only; for metric choice or tuning, use the Rust/Python schema builders.

### Index Selection Guidelines

| Query Pattern | Index Type | Example |
|---------------|------------|---------|
| `WHERE x = 5` | BTree | Year, ID |
| `WHERE x > 5` | BTree | Year ranges |
| `WHERE x IN [...]` | BTree | Categories |
| Vector similarity | HNSW / IVF_PQ | Embeddings |
| Text search | Full-text | Title, abstract |

---

## Schema Evolution

### Adding Properties

Safe operation—existing data gets NULL:

```json
// Before
{ "Paper": { "title": "String" } }

// After (add new property)
{ "Paper": {
    "title": "String",
    "citation_count": { "type": "Int32", "nullable": true }  // New
}}
```

### Deprecating Properties

Use state markers for gradual removal:

```json
{
  "Paper": {
    "old_field": {
      "type": "String",
      "state": "deprecated",
      "deprecated_since": "2024-01-01",
      "migration_hint": "Use new_field instead"
    },
    "new_field": { "type": "String" }
  }
}
```

### Adding Labels/Edge Types

Safe operation—new types get new ID ranges:

```json
// Add new label
{
  "labels": {
    "Paper": { "id": 1 },
    "Preprint": { "id": 2 }  // New label
  }
}
```

### Breaking Changes (Avoid)

These require data migration:
- Changing property types
- Changing vector dimensions
- Renaming labels (ID is fixed)
- Changing edge type direction semantics

---

## Example Schemas

> These JSON snippets are **conceptual** and meant for design discussions.  
> The on-disk `schema.json` format includes additional metadata fields and enum casing.  
> Use Cypher DDL or the Rust/Python schema builders for real schema creation.

### Academic Papers

```json
{
  "schema_version": 1,

  "labels": {
    "Paper": { "id": 1 },
    "Author": { "id": 2 },
    "Venue": { "id": 3 },
    "Institution": { "id": 4 }
  },

  "edge_types": {
    "CITES": { "id": 1, "src_labels": ["Paper"], "dst_labels": ["Paper"] },
    "AUTHORED_BY": { "id": 2, "src_labels": ["Paper"], "dst_labels": ["Author"] },
    "PUBLISHED_IN": { "id": 3, "src_labels": ["Paper"], "dst_labels": ["Venue"] },
    "AFFILIATED_WITH": { "id": 4, "src_labels": ["Author"], "dst_labels": ["Institution"] }
  },

  "properties": {
    "Paper": {
      "title": { "type": "String", "nullable": false },
      "abstract": { "type": "String", "nullable": true },
      "year": { "type": "Int32", "nullable": false },
      "doi": { "type": "String", "nullable": true },
      "embedding": { "type": "Vector", "dimensions": 768 }
    },
    "Author": {
      "name": { "type": "String", "nullable": false },
      "email": { "type": "String", "nullable": true },
      "orcid": { "type": "String", "nullable": true }
    },
    "Venue": {
      "name": { "type": "String", "nullable": false },
      "type": { "type": "String", "nullable": true }
    },
    "AUTHORED_BY": {
      "position": { "type": "Int32", "nullable": true }
    }
  }
}
```

### E-Commerce

```json
{
  "schema_version": 1,

  "labels": {
    "User": { "id": 1 },
    "Product": { "id": 2 },
    "Category": { "id": 3 },
    "Order": { "id": 4 }
  },

  "edge_types": {
    "PURCHASED": { "id": 1, "src_labels": ["User"], "dst_labels": ["Product"] },
    "VIEWED": { "id": 2, "src_labels": ["User"], "dst_labels": ["Product"] },
    "IN_CATEGORY": { "id": 3, "src_labels": ["Product"], "dst_labels": ["Category"] },
    "ORDERED": { "id": 4, "src_labels": ["Order"], "dst_labels": ["Product"] },
    "PLACED_BY": { "id": 5, "src_labels": ["Order"], "dst_labels": ["User"] }
  },

  "properties": {
    "User": {
      "email": { "type": "String", "nullable": false },
      "name": { "type": "String", "nullable": true },
      "preference_embedding": { "type": "Vector", "dimensions": 128 }
    },
    "Product": {
      "name": { "type": "String", "nullable": false },
      "description": { "type": "String", "nullable": true },
      "price": { "type": "Float64", "nullable": false },
      "embedding": { "type": "Vector", "dimensions": 384 }
    },
    "PURCHASED": {
      "quantity": { "type": "Int32", "nullable": false },
      "timestamp": { "type": "Timestamp", "nullable": false }
    }
  }
}
```

---

## Schema Validation Checklist

Before deploying your schema:

- [ ] All labels use singular PascalCase nouns
- [ ] All edge types use UPPER_SNAKE_CASE verbs
- [ ] All properties use snake_case
- [ ] Required properties are marked `nullable: false`
- [ ] Vector dimensions match your embedding model
- [ ] Edge type constraints match your domain rules
- [ ] Indexes planned for common query patterns
- [ ] No circular dependencies or overly complex relationships
- [ ] Schema version tracked for evolution

---

## Next Steps

- [Data Ingestion](data-ingestion.md) — Import data with your schema
- [Indexing](../concepts/indexing.md) — Configure indexes
- [Cypher Querying](cypher-querying.md) — Query your schema
