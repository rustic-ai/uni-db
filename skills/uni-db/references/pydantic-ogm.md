# uni-pydantic OGM Reference

**Package:** `uni-pydantic` v1.0.0 — Pydantic v2 Object-Graph Mapping for Uni.

---

## 1. Quick Start

```python
from uni_db import Uni
from uni_pydantic import UniNode, UniEdge, UniSession, Field, Relationship, Vector, before_create
from datetime import date

class Person(UniNode):
    name: str = Field(index="btree")
    age: int | None = None
    email: str = Field(unique=True, index="btree")
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

class Knows(UniEdge):
    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person
    since: date

db = Uni.temporary()
session = UniSession(db)
session.register(Person, Knows)
session.sync_schema()

alice = Person(name="Alice", age=30, email="alice@example.com")
bob = Person(name="Bob", age=25, email="bob@example.com")
session.add_all([alice, bob])
session.commit()
session.create_edge(alice, "FRIEND_OF", bob, Knows(since=date.today()))
session.commit()

adults = session.query(Person).filter(Person.age >= 18).order_by(Person.name).all()
```

---

## 2. UniNode

Base class for graph vertices. Extends Pydantic `BaseModel`.

| Class Attribute | Type | Default |
|---|---|---|
| `__label__` | `str` | Class name |
| `__relationships__` | `dict[str, RelationshipConfig]` | Auto-populated |

| Instance Property | Type |
|---|---|
| `vid` | `int \| None` -- DB-assigned vertex ID |
| `uid` | `str \| None` -- Content-addressed identifier |
| `is_persisted` | `bool` |
| `is_dirty` | `bool` |

```python
# Class methods
@classmethod get_property_fields(cls) -> dict[str, FieldInfo]
@classmethod get_relationship_fields(cls) -> dict[str, RelationshipConfig]
@classmethod from_properties(cls, props: dict, *, vid=None, uid=None, session=None) -> UniNode

# Instance methods
def to_properties(self) -> dict[str, Any]
```

---

## 3. UniEdge

Base class for graph edges with properties.

| Class Attribute | Type | Description |
|---|---|---|
| `__edge_type__` | `str` | Edge type name (defaults to class name) |
| `__from__` | `type[UniNode] \| tuple[type[UniNode], ...]` | Source node type(s) |
| `__to__` | `type[UniNode] \| tuple[type[UniNode], ...]` | Target node type(s) |

| Instance Property | Type |
|---|---|
| `eid` | `int \| None` -- DB-assigned edge ID |
| `src_vid` | `int \| None` |
| `dst_vid` | `int \| None` |
| `is_persisted` | `bool` |

```python
# Class methods
@classmethod get_from_labels(cls) -> list[str]
@classmethod get_to_labels(cls) -> list[str]
@classmethod get_property_fields(cls) -> dict[str, FieldInfo]
@classmethod from_properties(cls, props: dict, *, eid=None, src_vid=None, dst_vid=None, session=None) -> UniEdge
@classmethod from_edge_result(cls, data: dict, *, session=None) -> UniEdge

# Instance methods
def to_properties(self) -> dict[str, Any]
```

---

## 4. Field Configuration

```python
def Field(
    default=..., *, default_factory=None, alias=None, title=None,
    description=None, examples=None, exclude=False, json_schema_extra=None,
    # Uni-specific:
    index: IndexType | None = None,
    unique: bool = False,
    tokenizer: str | None = None,
    metric: VectorMetric | None = None,
    generated: str | None = None,
) -> Any
```

### Uni-Specific Parameters

| Parameter | Type | Description |
|---|---|---|
| `index` | `"btree" \| "hash" \| "fulltext" \| "vector" \| "sparse"` | Index type to create |
| `unique` | `bool` | Unique constraint (recorded, not yet enforced) |
| `tokenizer` | `str` | Tokenizer for fulltext index |
| `metric` | `"l2" \| "cosine" \| "dot"` | Distance metric for vector index |
| `generated` | `str` | Computed property expression (recorded, not yet applied) |

### Type Aliases

```python
IndexType  = Literal["btree", "hash", "fulltext", "vector", "sparse"]
Direction  = Literal["outgoing", "incoming", "both"]
VectorMetric = Literal["l2", "cosine", "dot"]
```

`FieldConfig` is a dataclass mirroring these parameters. Extract with `get_field_config(field_info) -> FieldConfig | None`.

---

## 5. Relationships

```python
def Relationship(
    edge_type: str, *,
    direction: Direction = "outgoing",
    edge_model: type[UniEdge] | None = None,
    eager: bool = False,
    cascade_delete: bool = False,
) -> Any
```

| Parameter | Type | Description |
|---|---|---|
| `edge_type` | `str` | Edge type name |
| `direction` | `"outgoing" \| "incoming" \| "both"` | Traversal direction |
| `edge_model` | `type[UniEdge] \| None` | Edge model for typed properties |
| `eager` | `bool` | Eager-load by default |
| `cascade_delete` | `bool` | Delete edges when node deleted |

**Example:**

```python
class Person(UniNode):
    follows: list["Person"] = Relationship("FOLLOWS")                          # outgoing
    followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")  # incoming
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")      # bidirectional
    manager: "Person | None" = Relationship("REPORTS_TO")                      # single optional
    friendships: list[tuple["Person", FriendshipEdge]] = Relationship(
        "FRIEND_OF", edge_model=FriendshipEdge,
    )
```

---

## 6. Vector Type

Fixed-dimension vectors for embeddings. Validated at assignment time.

```python
class Vector(Generic[N]):
    def __init__(self, values: list[float]) -> None
    @property values -> list[float]
    def __len__(self) -> int
```

**Usage:**

```python
class Document(UniNode):
    embedding: Vector[1536]                            # Required, auto-indexed
    summary_vec: Vector[768] | None = None             # Optional
    custom_vec: Vector[384] = Field(metric="cosine")   # Custom metric
```

At runtime, `Vector[N]` stores `list[float]` and validates `len(values) == N`. Schema sync auto-creates a vector index.

---

## 7. UniSession / AsyncUniSession

### UniSession (Sync)

```python
class UniSession:
    def __init__(self, db: uni_db.Uni) -> None
    def __enter__(self) -> UniSession
    def __exit__(...) -> None
    def close(self) -> None
    @property db -> uni_db.Uni

    # Registration & Schema
    def register(self, *models: type[UniNode] | type[UniEdge]) -> None
    def sync_schema(self) -> None

    # Query
    def query(self, model: type[NodeT]) -> QueryBuilder[NodeT]

    # CRUD
    def add(self, entity: UniNode) -> None
    def add_all(self, entities: Sequence[UniNode]) -> None
    def delete(self, entity: UniNode) -> None
    def get(self, model: type[NodeT], vid=None, uid=None, **kwargs) -> NodeT | None
    def refresh(self, entity: UniNode) -> None

    # Commit / Rollback
    def commit(self) -> None
    def rollback(self) -> None

    # Transactions
    @contextmanager
    def transaction(self) -> Iterator[UniTransaction]
    def begin(self) -> UniTransaction

    # Edge Operations
    def create_edge(self, source, edge_type: str, target, properties=None) -> None
    def delete_edge(self, source, edge_type: str, target) -> int
    def update_edge(self, source, edge_type: str, target, properties: dict) -> int
    def get_edge(self, source, edge_type: str, target, edge_model=None) -> list

    # Bulk
    def bulk_add(self, entities: Sequence[UniNode]) -> list[int]

    # Raw Cypher & Locy
    def cypher(self, query: str, params=None, result_type=None) -> list
    def locy(self, program: str, params=None) -> uni_db.LocyResult

    # Profiling
    def explain(self, cypher: str) -> uni_db.ExplainOutput
    def profile(self, cypher: str) -> tuple[uni_db.QueryResult, uni_db.ProfileOutput]

    # Schema Persistence
    def save_schema(self, path: str) -> None
    def load_schema(self, path: str) -> None
```

**Key behaviors:** `add()` stages only; `commit()` persists creates + auto-detected updates + deletes. `get(Model, vid=..., email=...)` looks up by VID, UID, or keyword property. `bulk_add()` uses `tx.bulk_writer()` for throughput. `cypher(query, result_type=Person)` hydrates raw Cypher results.

### AsyncUniSession

Same API; `register`, `add`, `add_all`, `delete`, `query`, `close` remain **sync**. Everything else is `async`. Constructor takes `uni_db.AsyncUni`. Returns `AsyncQueryBuilder` from `query()`.

---

## 8. UniTransaction / AsyncUniTransaction

### UniTransaction (Sync)

```python
class UniTransaction:
    def __init__(self, session: UniSession) -> None
    def __enter__(self) -> UniTransaction
    def __exit__(...) -> None       # auto-commits on success, rolls back on exception
    def add(self, entity: UniNode) -> None
    def create_edge(self, source, edge_type: str, target, properties=None, **kwargs) -> None
    def commit(self) -> None
    def rollback(self) -> None
```

### AsyncUniTransaction

Same methods; `add` and `create_edge` are sync (staging only); `commit` and `rollback` are `async`.

**Example:**

```python
with session.transaction() as tx:
    tx.add(Person(name="Alice"))
    tx.add(Person(name="Bob"))
    tx.create_edge(alice, "FRIEND_OF", bob)
# auto-commits on exit; rolls back on exception
```

---

## 9. QueryBuilder / AsyncQueryBuilder

Immutable, type-safe query builder. Each method returns a new instance.

### Builder Methods (sync, return new builder)

```python
class QueryBuilder(Generic[NodeT]):
    def filter(self, expr: FilterExpr) -> QueryBuilder[NodeT]
    def filter_by(self, **kwargs) -> QueryBuilder[NodeT]
    def order_by(self, prop: PropertyProxy | str, descending=False) -> QueryBuilder[NodeT]
    def limit(self, n: int) -> QueryBuilder[NodeT]
    def skip(self, n: int) -> QueryBuilder[NodeT]
    def distinct(self) -> QueryBuilder[NodeT]
    def traverse(self, relationship, target_model=None) -> QueryBuilder[NodeT]
    def eager_load(self, *relationships) -> QueryBuilder[NodeT]
    def vector_search(self, prop, query_vector: list[float], k=10,
                      threshold=None, pre_filter=None) -> QueryBuilder[NodeT]
    def sparse_search(self, prop, query, k=10,
                      threshold=None, pre_filter=None) -> QueryBuilder[NodeT]
    def hybrid_search(self, *, vector=None, fts=None, sparse=None,
                      query_text=None, k=10, method="rrf",
                      weights=None, alpha=None, rrf_k=None,
                      over_fetch=None, filter=None) -> QueryBuilder[NodeT]
    def timeout(self, seconds: float) -> QueryBuilder[NodeT]
    def max_memory(self, bytes_: int) -> QueryBuilder[NodeT]
```

`sparse_search` `query` is a `SparseVector`, `dict[int, float]`, or `(indices, values)` pair.
`hybrid_search` wraps the engine's `uni.search` (three-way fused dense + FTS + sparse); each of
`vector` / `fts` accepts a `(property, query)` tuple or a bare property (`PropertyProxy` | `str`,
which auto-embeds from the shared text), and `sparse` is a `(property, query)` tuple. Supply at
least one source. A single `query_text` (from `fts`'s text or the `query_text=` kwarg) drives both
FTS and dense auto-embed. `method="weighted"` takes `weights=[vector, fts, sparse]` (3-way) or
`alpha=` (2-way). See [vector-hybrid-search.md](vector-hybrid-search.md) §5 for the fusion engine.

### Terminal Methods

| Method | Returns | Notes |
|---|---|---|
| `all()` | `list[NodeT]` | All matching results |
| `first()` | `NodeT \| None` | First result or None |
| `one()` | `NodeT` | Exactly one result; raises `QueryError` if != 1 |
| `count()` | `int` | Count of matching results |
| `exists()` | `bool` | Whether any match exists |
| `delete()` | `int` | DETACH DELETE, returns count |
| `update(**kwargs)` | `int` | SET properties, returns count |

### Search Scores

Results from `vector_search` / `sparse_search` / `hybrid_search` carry a `.search_scores` sidecar
(a `SearchScores`) on each hydrated node; it is `None` for ordinary (non-search) queries. Kept off
the model's own fields, so a user field literally named `score` never collides.

| Attribute | Meaning |
|---|---|
| `score` | Primary score used for ordering: fused (hybrid) or per-source (single-source) |
| `vector` / `fts` / `sparse` | Per-arm branch scores (populated when that arm contributed, else `None`) |
| `rerank` | Second-stage reranker score, when a reranker ran |
| `distance` | Raw dense distance (vector arm) |

```python
for doc in hits:
    print(doc.title, doc.search_scores.score, doc.search_scores.vector)
```

### AsyncQueryBuilder

Builder methods are identical (sync). Terminal methods are `async`.

**Example:**

```python
results = (
    session.query(Person)
    .filter(Person.age >= 18)
    .filter(Person.name.starts_with("A"))
    .order_by(Person.name)
    .skip(20).limit(10)
    .all()
)

similar = (
    session.query(Document)
    .vector_search(Document.embedding, query_vec, k=5, threshold=0.8)
    .all()
)

# Three-way fused hybrid (dense + FTS + sparse); scores via .search_scores
hits = (
    session.query(Document)
    .hybrid_search(
        vector=("embedding", query_vec),
        fts=("content", "quarterly revenue"),
        sparse=("splade", sparse_vec),
        method="rrf",
        k=5,
    )
    .all()
)
top = hits[0].search_scores.score  # fused score

deleted = session.query(Person).filter(Person.age < 18).delete()
updated = session.query(Person).filter(Person.age < 18).update(status="minor")
```

---

## 10. Filter Expressions

### PropertyProxy Operators

Access at class level (e.g., `Person.age`) to get a `PropertyProxy` that supports:

| Operator | Cypher | Example |
|---|---|---|
| `==` | `=` | `Person.name == "Alice"` |
| `!=` | `<>` | `Person.name != "Bob"` |
| `<` | `<` | `Person.age < 30` |
| `<=` | `<=` | `Person.age <= 30` |
| `>` | `>` | `Person.age > 18` |
| `>=` | `>=` | `Person.age >= 18` |
| `.in_(list)` | `IN` | `Person.age.in_([25, 30])` |
| `.not_in(list)` | `NOT IN` | `Person.age.not_in([25, 30])` |
| `.like(pattern)` | `=~` | `Person.name.like("A.*")` |
| `.starts_with(s)` | `STARTS WITH` | `Person.name.starts_with("A")` |
| `.ends_with(s)` | `ENDS WITH` | `Person.name.ends_with("z")` |
| `.contains(s)` | `CONTAINS` | `Person.name.contains("li")` |
| `.is_null()` | `IS NULL` | `Person.email.is_null()` |
| `.is_not_null()` | `IS NOT NULL` | `Person.email.is_not_null()` |

Each operator returns a `FilterExpr(property_name, op, value)` for use with `.filter()`.

---

## 11. Schema Generation

```python
class SchemaGenerator:
    def __init__(self) -> None
    def register_node(self, model: type[UniNode]) -> None
    def register_edge(self, model: type[UniEdge]) -> None
    def register(self, *models: type[UniNode] | type[UniEdge]) -> None
    def generate(self) -> DatabaseSchema
    def apply_to_database(self, db: uni_db.Uni) -> None
    async def async_apply_to_database(self, db: uni_db.AsyncUni) -> None

def generate_schema(*models) -> DatabaseSchema   # convenience
```

Schema sync flow: `generate()` -> iterate labels/edges (skip existing, create new) -> `builder.apply()` -> second pass for vector/fulltext indexes.

### Gotchas

- **Additive-only** -- existing labels/edge types are skipped entirely (no new properties added)
- **Unique constraints** -- recorded in `FieldConfig` but **not created** in `apply_to_database()`
- **Generated properties** -- `Field(generated=...)` stored but **not applied**
- **Fulltext/vector index errors** -- silently swallowed (`try/except pass`)

**Preferred usage:** `session.register(...)` then `session.sync_schema()` (calls `SchemaGenerator` internally).

---

## 12. Lifecycle Hooks

Decorators for model lifecycle events on `UniNode`/`UniEdge` subclasses:

| Decorator | When |
|---|---|
| `@before_create` | Before insert (not yet persisted) |
| `@after_create` | After insert (vid/eid assigned) |
| `@before_update` | Before update (dirty fields detected) |
| `@after_update` | After update committed |
| `@before_delete` | Before delete |
| `@after_delete` | After delete committed |
| `@before_load` | Before hydration from DB result |
| `@after_load` | After hydration from DB result |

Utility: `get_hooks(model, hook_type) -> list[Callable]`, `run_hooks(entity, hook_type)`, `run_class_hooks(model, hook_type)`.

**Example:**

```python
class User(UniNode):
    name: str
    created_at: datetime | None = None

    @before_create
    def set_timestamps(self):
        self.created_at = datetime.now()

    @before_delete
    def guard_admin(self):
        if self.name == "admin":
            raise ValueError("Cannot delete admin user")
```

---

## 13. Type Mapping

| Python Type | Uni DataType | Notes |
|---|---|---|
| `str` | `string` | UTF-8 |
| `int` | `int` | 64-bit |
| `float` | `float` | 64-bit |
| `bool` | `bool` | |
| `datetime` | `datetime` | |
| `date` | `date` | |
| `time` | `time` | |
| `timedelta` | `duration` | |
| `bytes` | `bytes` | |
| `dict` | `json` | |
| `list[T]` | `list:T` | Typed list |
| `Vector[N]` | `vector:N` | N-dimensional vector |
| `T \| None` | `T` + nullable | Optional field |

Utilities: `python_type_to_uni(type_hint, *, nullable=False) -> (str, bool)`, `uni_to_python_type(str) -> type`, `get_vector_dimensions(type_hint) -> int|None`, `python_to_db_value(value, type_hint) -> Any`, `db_to_python_value(value, type_hint) -> Any`.

---

## 14. Database Builders

### UniDatabase (Sync)

```python
class UniDatabase:
    @classmethod def open(cls, path: str) -> UniDatabase
    @classmethod def create(cls, path: str) -> UniDatabase
    @classmethod def open_existing(cls, path: str) -> UniDatabase
    @classmethod def temporary(cls) -> UniDatabase
    @classmethod def in_memory(cls) -> UniDatabase
    def cache_size(self, bytes_: int) -> UniDatabase
    def parallelism(self, n: int) -> UniDatabase
    def build(self) -> uni_db.Uni
```

### AsyncUniDatabase

```python
class AsyncUniDatabase:
    @classmethod def open(cls, path: str) -> AsyncUniDatabase
    @classmethod def temporary(cls) -> AsyncUniDatabase
    @classmethod def in_memory(cls) -> AsyncUniDatabase
    def cache_size(self, bytes_: int) -> AsyncUniDatabase
    def parallelism(self, n: int) -> AsyncUniDatabase
    async def build(self) -> uni_db.AsyncUni
```

**Example:**

```python
db = UniDatabase.open("./my_db").cache_size(256 * 1024 * 1024).build()
```

---

## 15. Examples

### RAG with Vector Search

```python
from uni_pydantic import UniNode, UniSession, Field, Vector
from uni_db import Uni

class Document(UniNode):
    title: str = Field(index="btree")
    content: str = Field(index="fulltext", tokenizer="standard")
    embedding: Vector[1536] = Field(metric="cosine")

db = Uni.temporary()
with UniSession(db) as session:
    session.register(Document)
    session.sync_schema()
    doc = Document(title="Guide", content="...", embedding=Vector([0.1] * 1536))
    session.add(doc)
    session.commit()

    results = (
        session.query(Document)
        .vector_search(Document.embedding, [0.2] * 1536, k=5, threshold=0.8)
        .all()
    )
```

### RAG with Hybrid Search

```python
from uni_pydantic import UniNode, UniSession, Field, Vector, SparseVector

class Document(UniNode):
    title: str = Field(index="btree")
    content: str = Field(index="fulltext", tokenizer="standard")
    embedding: Vector[1536] = Field(metric="cosine")
    splade: SparseVector[30522]                      # learned-sparse (SPLADE) arm

with UniSession(db) as session:
    session.register(Document)
    session.sync_schema()
    # ... add documents ...

    hits = (
        session.query(Document)
        .hybrid_search(
            vector=("embedding", query_vec),          # precomputed dense (or bare "embedding" to auto-embed)
            fts=("content", "quarterly revenue"),      # FTS query text (also the auto-embed text)
            sparse=("splade", SparseVector.from_dict({1: 1.4, 7: 0.9})),
            method="rrf",                              # or "weighted" + weights=[v, f, s] / alpha=
            k=10,
        )
        .all()
    )
    for doc in hits:
        s = doc.search_scores                          # fused .score + per-arm .vector/.fts/.sparse
        print(doc.title, s.score, s.vector, s.fts, s.sparse)
```

---

## Exceptions

All inherit from `UniPydanticError`.

| Exception | Trigger |
|---|---|
| `SchemaError` | Model missing `__label__` or `__edge_type__` |
| `TypeMappingError` | Unsupported Python type in model field |
| `ValidationError` | Model validation failure |
| `NotRegisteredError` | Model not registered with `session.register()` |
| `NotPersisted` | `refresh()` or edge op on entity without VID |
| `NotTrackedError` | Operating on entity not tracked by session |
| `TransactionError` | Transaction state violation (double commit, etc.) |
| `QueryError` | Query failure or `one()` with != 1 result |
| `CypherInjectionError` | Property name fails validation |
| `LazyLoadError` | Accessing relationship without active session |
| `BulkLoadError` | Failure during `bulk_add()` |
