# uni-pydantic Design (Current)

## Overview

uni-pydantic is a Pydantic v2-based OGM for the Uni graph database. It provides
schema-from-models generation, a type-safe query builder, and a session API for
CRUD operations. The implementation is synchronous (matching the current
uni_db bindings) and focuses on additive schema sync.

## Current Implementation

### Core Models

- `UniNode` and `UniEdge` are Pydantic v2 models with dirty tracking and
  session-aware identity fields (`_vid`, `_uid`).
- `Vector[N]` is supported as a custom type for fixed-dimension embeddings.
- Relationship fields are declared with `Relationship(...)` and exposed through
  `RelationshipDescriptor` for lazy loading.

### Schema Generation

- `SchemaGenerator` registers node/edge models, builds a `DatabaseSchema`, and
  applies it to `uni_db.Database`.
- Schema sync is additive-only: labels and properties are created if missing.
- Index creation is supported for:
  - Scalar indexes (`btree`, `hash`) via `create_scalar_index`.
  - Vector indexes via `create_vector_index` (metric honored if provided).
- Edge type creation is supported; edge property creation is not yet applied.

### Session & CRUD

- `UniSession` provides:
  - `register`, `sync_schema`, `add`, `add_all`, `commit`, `delete`, `refresh`.
  - `begin()` and context-managed `transaction()` using `uni_db.Transaction`.
- `commit()` writes pending inserts/updates/deletes and calls `db.flush()`.

### Query DSL

- `QueryBuilder` supports:
  - `filter`, `order_by`, `limit`, `skip`, `distinct`.
  - `traverse` and `eager_load` (relationship traversal / prefetch).
  - `vector_search` using `CALL uni.vector.query`.
- Queries are compiled to Cypher and executed via `uni_db.Database.query` or
  `uni_db.Database.execute`.

### Relationships

- Lazy loading uses `MATCH` + relationship pattern and requires an attached
  session. If `_label` is present in results, mapping to registered models is
  attempted; otherwise, raw values are returned.
- Eager loading uses a batch query and caches the raw result on each entity.

### Lifecycle Hooks

- Supported hook decorators: `before_create`, `after_create`, `before_update`,
  `after_update`, `before_delete`, `after_delete`, `before_load`, `after_load`.

## Data Type Mapping (Python -> Uni)

- `str` -> `string`
- `int` -> `int64`
- `float` -> `float64`
- `bool` -> `bool`
- `datetime` -> `datetime`
- `date` -> `date`
- `time` -> `time`
- `timedelta` -> `duration`
- `bytes` -> `bytes`
- `dict` -> `json`
- `list[T]` -> `list<T>`
- `Vector[N]` -> `vector:N`
- `T | None` -> nullable

## Known Gaps / Not Yet Implemented

- `Field(unique=True)` is recorded but not emitted as a constraint or index.
- `Field(index="fulltext")` is recorded, but schema sync does not create
  fulltext indexes yet.
- `Field(generated=...)` is not applied during schema sync or runtime.
- Edge property creation in `sync_schema()` is a TODO.
- Relationship `edge_model` and `cascade_delete` are not enforced.
- Eager loading caches raw results; typed mapping is best-effort and depends on
  `_label` being returned by the database.
- No async API is provided.

## Package Layout

```
uni-pydantic/
├── pyproject.toml
├── src/uni_pydantic/
│   ├── __init__.py
│   ├── base.py
│   ├── exceptions.py
│   ├── fields.py
│   ├── hooks.py
│   ├── query.py
│   ├── schema.py
│   ├── session.py
│   └── types.py
└── tests/
```
