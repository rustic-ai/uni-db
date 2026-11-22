# Pydantic OGM Guide

This guide shows you how to use **uni-pydantic**, a Pydantic-based Object-Graph Mapping (OGM) library that provides type-safe Python models for working with Uni graph databases.

## Why Use an OGM?

While you can always use raw Cypher queries with `uni_db`, an OGM offers:

- **Type Safety**: Full IDE autocomplete and type checking
- **Less Boilerplate**: Define models once, use them everywhere
- **Validation**: Pydantic validates your data automatically
- **Schema Generation**: Auto-create database schema from models
- **Cleaner Code**: Object-oriented API vs string-based queries

## Installation

```bash
pip install uni-pydantic
```

## Defining Models

### Nodes

Create node models by subclassing `UniNode`:

```python
from uni_pydantic import UniNode, Field, Relationship, Vector

class Person(UniNode):
    # Optional: set label name (defaults to class name)
    __label__ = "Person"

    # Required property
    name: str

    # Optional property with default
    age: int | None = None

    # Indexed property
    email: str = Field(index="btree", unique=True)

    # Fulltext searchable
    bio: str | None = Field(default=None, index="fulltext")

    # Vector for embeddings
    embedding: Vector[768] = Field(metric="cosine")

    # Relationships (covered below)
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
```

### Edges

Define edge models for edges with properties:

```python
from uni_pydantic import UniEdge
from datetime import date

class FriendshipEdge(UniEdge):
    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person

    since: date
    strength: float = 1.0
```

### Relationships

Declare relationships using `Relationship()`:

```python
class Person(UniNode):
    # Outgoing: Person -[:FOLLOWS]-> Person
    following: list["Person"] = Relationship("FOLLOWS", direction="outgoing")

    # Incoming: Person <-[:FOLLOWS]- Person
    followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

    # Both directions
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

    # Single optional relationship
    employer: "Company | None" = Relationship("WORKS_AT")
```

## Setting Up a Session

Connect to the database and register your models:

```python
from uni_pydantic import UniSession
import uni_db

# Open database
db = uni_db.Uni.open("./my_graph")

# Create session
session = UniSession(db)

# Register all models
session.register(Person, Company, FriendshipEdge)

# Create schema in database (labels, properties, indexes)
session.sync_schema()
```

## CRUD Operations

### Create

```python
# Create a node
alice = Person(name="Alice", age=30, email="alice@example.com")
session.add(alice)
session.commit()

# alice.vid is now populated
print(f"Created Alice with vid={alice.vid}")

# Bulk create
users = [Person(name=f"User{i}", email=f"user{i}@example.com") for i in range(100)]
session.add_all(users)
session.commit()
```

### Read

```python
# By vertex ID
person = session.get(Person, vid=12345)

# By unique property
person = session.get(Person, email="alice@example.com")

# Query builder (more below)
people = session.query(Person).filter(Person.age >= 18).all()
```

### Update

```python
# Modify and commit - changes are auto-detected
alice.age = 31
alice.bio = "Software engineer"
session.commit()
```

### Delete

```python
session.delete(alice)
session.commit()
```

## Query Builder

The query builder provides a type-safe, fluent API for building queries.

### Basic Queries

```python
# Get all
all_people = session.query(Person).all()

# Get first match
alice = session.query(Person).filter(Person.name == "Alice").first()

# Count
adult_count = session.query(Person).filter(Person.age >= 18).count()

# Check existence
exists = session.query(Person).filter(Person.email == "test@test.com").exists()
```

### Filtering

```python
# Comparison operators
adults = session.query(Person).filter(Person.age >= 18).all()
not_bob = session.query(Person).filter(Person.name != "Bob").all()

# Null checks
with_bio = session.query(Person).filter(Person.bio.is_not_null()).all()

# String matching
a_names = session.query(Person).filter(Person.name.starts_with("A")).all()
smiths = session.query(Person).filter(Person.name.ends_with("Smith")).all()

# Collection membership
specific = session.query(Person).filter(Person.age.in_([25, 30, 35])).all()

# Chain multiple filters (AND)
results = (
    session.query(Person)
    .filter(Person.age >= 21)
    .filter(Person.email.is_not_null())
    .all()
)
```

### Ordering and Pagination

```python
# Order by
sorted_people = (
    session.query(Person)
    .order_by(Person.name)
    .all()
)

# Descending order
newest_first = (
    session.query(Person)
    .order_by(Person.created_at, descending=True)
    .all()
)

# Pagination
page_2 = (
    session.query(Person)
    .order_by(Person.name)
    .skip(20)    # Skip first 20
    .limit(10)   # Take 10
    .all()
)
```

### Bulk Operations

```python
# Delete all matching
deleted_count = (
    session.query(Person)
    .filter(Person.age < 18)
    .delete()
)

# Update all matching
updated_count = (
    session.query(Person)
    .filter(Person.age.is_null())
    .update(age=0)
)
```

### Vector, Sparse & Hybrid Search

The query builder wraps uni's retrieval procedures, so you stay in typed, model-bound code instead
of hand-writing Cypher.

```python
# Dense vector search
similar = (
    session.query(Document)
    .vector_search("embedding", query_vec, k=5, threshold=0.8)
    .all()
)

# Learned-sparse (SPLADE) search
# query is a SparseVector, dict[int, float], or (indices, values) pair
sparse_hits = (
    session.query(Document)
    .sparse_search("splade", {1: 1.4, 7: 0.9}, k=10)
    .all()
)

# Three-way fused hybrid search (dense + full-text + sparse)
hits = (
    session.query(Document)
    .hybrid_search(
        vector=("embedding", query_vec),       # (property, precomputed vec); bare "embedding" auto-embeds
        fts=("content", "quarterly revenue"),   # (property, full-text query text)
        sparse=("splade", sparse_vec),          # (property, sparse query)
        method="rrf",                           # or "weighted" with weights=[v, f, s] / alpha=
        k=10,
    )
    .all()
)
```

`pre_filter` scopes a vector or sparse search **before** the top-`k` cut: only rows that pass it
are ranked. It is a predicate over the model's stored columns by bare name. `.filter()` is
different: it trims the `k` rows the search already returned, so it can leave fewer than `k`.

```python
adults = (
    session.query(Document)
    .vector_search("embedding", query_vec, k=5, pre_filter="year >= 2020")
    .all()
)
```

Every result from these three methods carries a `.search_scores` sidecar:

```python
for doc in hits:
    s = doc.search_scores             # SearchScores (None for ordinary queries)
    print(doc.title, s.score)         # fused score used for ordering
    print(s.vector, s.fts, s.sparse)  # per-arm branch scores (None when that arm is off)
```

!!! note "Under the hood"
    `hybrid_search()` wraps the [`uni.search`](../features/hybrid-search.md) procedure. Supply at
    least one of `vector` / `fts` / `sparse`; a single shared query text (from `fts`'s text or the
    `query_text=` kwarg) drives both full-text matching and dense auto-embed. See
    [Hybrid Search](../features/hybrid-search.md) for fusion methods (RRF vs weighted) and score
    transparency.

## Creating Edges

```python
# Create edge with properties dict
session.create_edge(alice, "FRIEND_OF", bob, {"since": 2020, "strength": 0.9})

# Create edge with edge model
friendship = FriendshipEdge(since=date.today(), strength=0.8)
session.create_edge(alice, "FRIEND_OF", charlie, friendship)

session.commit()
```

## Raw Cypher

When you need features not yet in the query builder, use raw Cypher:

```python
# Simple query
results = session.cypher(
    "MATCH (p:Person) WHERE p.age > $age RETURN p.name as name, p.age as age",
    params={"age": 25}
)
for row in results:
    print(f"{row['name']}: {row['age']}")

# With type mapping (returns Person instances)
people = session.cypher(
    "MATCH (p:Person)-[:FRIEND_OF]-(friend) WHERE p.name = $name RETURN friend",
    params={"name": "Alice"},
    result_type=Person
)
```

## Transactions

```python
# Context manager (recommended)
with session.transaction() as tx:
    alice = Person(name="Alice", email="alice@example.com")
    bob = Person(name="Bob", email="bob@example.com")
    tx.add(alice)
    tx.add(bob)
    # Commits automatically, rolls back on exception

# Manual transaction
tx = session.begin()
try:
    tx.add(Person(name="Charlie", email="charlie@example.com"))
    tx.commit()
except Exception:
    tx.rollback()
    raise
```

## Lifecycle Hooks

Add custom logic at various points in the entity lifecycle:

```python
from uni_pydantic import before_create, after_create, before_update
from datetime import datetime

class Person(UniNode):
    name: str
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @before_create
    def set_timestamps(self):
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

    @after_create
    def log_creation(self):
        print(f"Created person: {self.name}")

    @before_update
    def update_timestamp(self):
        self.updated_at = datetime.now()
```

## Working with Vectors

```python
from uni_pydantic import Vector

class Document(UniNode):
    title: str
    content: str
    embedding: Vector[1536]  # OpenAI ada-002 dimensions

# Create with embedding
doc = Document(
    title="My Document",
    content="...",
    embedding=[0.1, 0.2, ...]  # 1536 floats
)
session.add(doc)
session.commit()

# Vector search via Cypher
similar_docs = session.cypher(
    """
    MATCH (d:Document)
    WHERE similar_to(d.embedding, $query_vec) > 0.8
    RETURN d.title as title
    """,
    params={"query_vec": query_embedding}
)
```

### Multi-vector (ColBERT) and typed maps

Use `list[Vector[N]]` for late-interaction (ColBERT) token embeddings, and `dict[str, V]` for typed maps:

```python
from uni_pydantic import Vector

class Document(UniNode):
    title: str
    tokens: list[Vector[96]]          # multi-vector -> list:vector:96
    section_scores: dict[str, float] = {}   # typed map -> map:string:float64

doc = Document(
    title="My Document",
    tokens=[Vector[96]([0.1] * 96), Vector[96]([0.2] * 96)],
    section_scores={"intro": 0.9, "body": 0.7},
)
session.add(doc)
session.commit()
```

A MUVERA / MaxSim index on the `tokens` field is created via Cypher DDL or the imperative schema API (not via `Field()`); see [Vector Search — Multi-Vector](../features/vector-search.md#multi-vector-search-colbert-late-interaction).

## Best Practices

### 1. Define Models in Separate Module

```python
# models.py
from uni_pydantic import UniNode, UniEdge, Field, Relationship

class User(UniNode):
    ...

class Post(UniNode):
    ...
```

### 2. Use Type Hints Consistently

```python
# Good
friends: list["Person"] = Relationship("FRIEND_OF")
manager: "Person | None" = Relationship("REPORTS_TO")

# Avoid
friends = Relationship("FRIEND_OF")  # No type hint
```

### 3. Batch Operations

```python
# Good - single commit
session.add_all([user1, user2, user3])
session.commit()

# Avoid - multiple commits
session.add(user1)
session.commit()
session.add(user2)
session.commit()
```

### 4. Use Transactions for Related Operations

```python
# Persist the nodes first: an edge joins nodes that are already saved
session.add_all([alice, bob, charlie])
session.commit()

with session.transaction() as tx:
    tx.create_edge(alice, "FRIEND_OF", bob)
    tx.create_edge(alice, "FRIEND_OF", charlie)
    # All or nothing
```

## Next Steps

- [Pydantic OGM Reference](../reference/pydantic-ogm.md) - Complete API documentation
- [Hybrid Search](../features/hybrid-search.md) - Fusion methods behind `hybrid_search()`
- [Example Notebooks](../examples/index.md) - Interactive examples with uni-pydantic
- [Schema Design](schema-design.md) - Best practices for graph modeling
