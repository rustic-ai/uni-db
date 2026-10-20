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
| `Btic` | Temporal intervals with granularity | valid_period, event_when |
| `Vector` | Embeddings | embedding, image_vector |
| `List<T>` | Typed arrays | tags (`List<String>`), scores (`List<Float64>`) |
| `List<Vector>` | Multi-vector (ColBERT / late-interaction) | tokens (`List<Vector(96)>`) |
| `Map<K, V>` | Typed string-keyed maps | scores (`Map<String, Float64>`) |
| `Json` | Semi-structured | metadata, config |
| `Bytes` | Raw binary blobs | image, audio, pdf, model_weights |

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

**Reserved names.** A few names belong to the engine. A write that sets a
property named `overflow_json`, `_vid`, `_uid`, `_eid`, `_labels`,
`_label`, `_type`, `_type_name`, `_src`, `_dst`, `_src_vid`, `_dst_vid`,
`_all_props`, `_score`, `_fwd`, `_deleted`, `_version`, `_created_at`,
`_updated_at`, Lance's row columns (`_rowid`, `_rowaddr`, `_rowoffset`,
`_row_created_at_version`, `_row_last_updated_at_version`), `__set_struct__`
or `__with_passthrough__`, or one starting with `_gen_`, `__fde_` or
`__alias_of__` — through `CREATE`, `SET`, `MERGE`, bulk insert, the bulk
writer or the Python API — fails with
`UniError::InvalidIdentifier` (Python: `UniInvalidIdentifierError`), declared
label or not. Any other name is yours, including one that starts with `_`: an
undeclared `_source` or `__tag` property reads back like any other, through
`n._source`, `keys(n)`, `properties(n)`, `n {.*}`, the returned node and Locy.
Declaring a property cannot use a leading `_` at all, nor `ext_id`, `eid`,
`src_vid`, `dst_vid` or `op`. `ext_id` is the vertex's external id; `eid`,
`src_vid`, `dst_vid`, `op`, `type`, `labels` and `props_json` work as
undeclared properties.

**Characters a declared property cannot use.** A declared property is a storage
column, and a column name cannot contain `.` or a backtick: declaring one fails
with a schema error (through `CREATE LABEL`, `CREATE EDGE TYPE`,
`ALTER … ADD PROPERTY`, rename, and the schema builder). Such a name still works
as an undeclared property, written with backticks in Cypher (`` n.`a.b` ``,
`` n.`has``tick` ``). Spaces, keywords and other punctuation are fine in a
declared name.

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

The declared dimension is enforced at write time (since 2.5.0): a wrong-length vector fails with a `TypeError` naming the declared and actual lengths, and an auto-embed model whose output width differs from the declared dimension errors, naming the embedding alias. Re-applying an identical schema is idempotent, but re-declaring an existing property with a different type or dimension (e.g. `VECTOR(4)` → `VECTOR(8)`) raises a schema conflict error (Python: `UniSchemaError`) — use a new property name (see [Embedding Versioning](#embedding-versioning)) or migrate the data.

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

### Multi-Vector (ColBERT) Properties

For late-interaction retrieval, store **many vectors per row** (one per token) in a `List<Vector(dim)>` property. Multi-vectors must be schema-declared — they cannot live on a schemaless property:

```cypher
CREATE LABEL Document (
  title  STRING,
  tokens LIST<VECTOR(96)>   // per-token (ColBERT) vectors
)
```

Pair it with a MUVERA index for fast first-stage retrieval, then exact MaxSim re-ranking. See [Vector Search — Multi-Vector](../features/vector-search.md#multi-vector-search-colbert-late-interaction).

### Typed Collection Properties

Beyond vectors, properties can declare typed collections — `List<T>` and string-keyed `Map<K, V>`, including nested forms:

```cypher
CREATE LABEL Document (
  tags     LIST<STRING>,             // typed list
  scores   MAP<STRING, FLOAT>,       // typed map
  sections MAP<STRING, LIST<INT>>    // nested
)
```

Use a typed collection when the shape is known (it stores compactly and validates on write); fall back to `Json` for ad-hoc, varying structures.

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
let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Document {
    title: 'Research Paper',
    author: 'Alice',
    tags: ['ml', 'nlp'],
    year: 2024,
    conference: 'NeurIPS'
})").await?;
tx.commit().await?;

// Query works normally (automatic rewriting)
let results = session.query("
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
let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Person {
    name: 'Bob',           // Schema (typed column)
    email: 'bob@x.com',    // Schema (typed column)
    city: 'NYC',           // Overflow (overflow_json)
    github: 'bob123',      // Overflow (overflow_json)
    verified: true         // Overflow (overflow_json)
})").await?;
tx.commit().await?;
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
// Original query
MATCH (p:Person) WHERE p.city = 'NYC' RETURN p.github

// Automatically rewritten to (transparent to user):
MATCH (p:Person)
WHERE json_get_string(p.overflow_json, 'city') = 'NYC'
RETURN json_get_string(p.overflow_json, 'github')
```

**Supported Functions:**
- `json_get_string(overflow_json, 'key')` - Extract string
- `json_get_int(overflow_json, 'key')` - Extract integer
- `json_get_float(overflow_json, 'key')` - Extract float
- `json_get_bool(overflow_json, 'key')` - Extract boolean

### A Declaration Applies Only to Its Own Label

Declaring `price: FLOAT` on `Product` types `price` on `Product` vertices only.
The same name on another label, or on a relationship type that does not
declare it, keeps whatever value was written: `MATCH (o:Offer) RETURN o.price`
returns the integer or string stored there, in a `CALL { ... }` subquery and a
Locy rule as much as in a plain `MATCH`. A relationship matched over several
types (`[e]`, `[e:A|B]`) takes a declared type only when every one of those
types declares it the same way. A `UNION` whose branches return different
types for one column (a typed property and an untyped one, or `1` and `'a'`)
returns each row's own value.

### Enforcing Strict Schema

If you want to disable schemaless labels and edge types entirely, enable `strict_schema`:

=== "Rust"
    ```rust
    let config = UniConfig { strict_schema: true, ..UniConfig::default() };
    let db = Uni::in_memory().config(config).build().await?;
    ```

=== "Python"
    ```python
    db = uni_db.UniBuilder.in_memory().strict_schema(True).build()
    ```

With `strict_schema` enabled:

- `CREATE (:UndeclaredLabel {...})` → error
- `CREATE (a)-[:UNDECLARED_TYPE]->(b)` → error
- `MERGE (:UndeclaredLabel {...})` → error
- Unknown **properties** are still accepted (stored in overflow) — only labels and edge types are enforced

This is useful in production to catch typos and enforce schema-first discipline. Error messages include the undeclared name and suggest using `db.schema()` to declare it.

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
let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Product {
    name: 'Widget',
    price: 19.99,
    category: 'electronics',
    // Overflow properties:
    manufacturer: 'ACME',
    warranty_months: 24,
    color: 'blue'
})").await?;
tx.commit().await?;
```

#### Bad: Frequently-Queried Properties in Overflow

```rust
// Don't do this!
db.schema().label("Product").apply().await?;  // No properties

// All properties in overflow (slow queries!)
let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Product {
    name: 'Widget',     // Should be schema property!
    price: 19.99,       // Should be schema property!
    category: 'electronics'  // Should be schema property!
})").await?;
tx.commit().await?;

// This query will be slow (JSONB parsing for every row)
session.query("
    MATCH (p:Product)
    WHERE p.price < 50 AND p.category = 'electronics'
    RETURN p
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
let tx = session.tx().await?;
tx.execute("
    MATCH (p:Product)
    WHERE p.manufacturer IS NOT NULL
    SET p.manufacturer = p.manufacturer
").await?;
tx.commit().await?;

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
    .property("since", DataType::Int32)
    .apply().await?;

// Create with overflow edge properties
let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE
    (a:Person {name: 'Alice'})-[:KNOWS {
        since: 2020,         // Schema property
        context: 'work',     // Overflow property
        strength: 0.9        // Overflow property
    }]->(b:Person {name: 'Bob'})
").await?;
tx.commit().await?;
```

### Null Handling

Overflow properties handle nulls gracefully:

```cypher
// Explicit null
CREATE (:Person {name: 'Alice', city: null})

// Missing property (implicit null)
CREATE (:Person {name: 'Bob'})

// Both return null for city
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

Safe operation—existing rows read NULL for the new property:

```json
// Before
{ "Paper": { "title": "String" } }

// After (add new property)
{ "Paper": {
    "title": "String",
    "citation_count": { "type": "Int32", "nullable": true }  // New
}}
```

This works whether the label is empty or already holds millions of rows, and
**no data is rewritten**: the column is added as a storage-metadata change, so
the cost does not grow with table size. Open forks keep working too—there is no
need to drop and recreate them.

Three behaviours worth knowing:

**A `NOT NULL` property on a populated label is recorded as nullable.** The
builder's `property()` defaults to `NOT NULL`, but rows that already exist have
no value for a property you are only now declaring, so the declaration is
recorded as nullable and a warning is logged. `NOT NULL` is still enforced on
every subsequent write. Use `property_nullable()` to say so explicitly and skip
the warning.

**Declaring a property that already has schemaless data keeps its values.**
Properties you write without declaring are stored in an overflow blob (see
[Schemaless Properties](#schemaless-properties-overflow)). Declaring such a
property promotes those values into the typed column, so reads keep returning
them:

```cypher
// citation_count is not declared, so it is stored in the overflow blob
CREATE (:Paper {title: 'A', citation_count: 12})

// declaring it later promotes the stored values into the typed column
ALTER LABEL Paper ADD PROPERTY citation_count INT32

// still 12
MATCH (p:Paper) RETURN p.citation_count
```

This promotion reads and rewrites the label's table once, so it is the one
schema change whose cost scales with data size. Declaring properties up front
avoids it entirely.

**Dropping a property is safe too**, and equally does not rewrite data. The
column stays on disk, unread, until the next compaction.

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
- Changing property types (re-declaring an existing property with a different type raises a schema conflict error — Python: `UniSchemaError`)
- Changing vector dimensions (same schema conflict error — declare a new property instead)
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
