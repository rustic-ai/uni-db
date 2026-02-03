# uni-pydantic

Pydantic-based OGM (Object-Graph Mapping) for the [Uni Graph Database](https://github.com/rustic-ai/uni).

## Features

- **Type Safety**: Full IDE autocomplete and type checking for graph operations
- **Pydantic v2**: Leverage Pydantic's powerful validation, computed fields, and serialization
- **Schema-from-Models**: Auto-generate Uni schema from Pydantic model definitions
- **First-Class Relationships**: Edges as declarative fields with lazy/eager loading
- **Query DSL**: Type-safe query builder with raw Cypher escape hatch
- **Vector Search**: Built-in support for vector similarity search

## Installation

```bash
pip install uni-pydantic
```

Or with Poetry:

```bash
poetry add uni-pydantic
```

## Quick Start

```python
from datetime import date
from uni_db import Database
from uni_pydantic import UniNode, UniEdge, UniSession, Field, Relationship, Vector

# Define your models
class Person(UniNode):
    __label__ = "Person"

    name: str
    age: int | None = None
    email: str = Field(unique=True)
    bio: str = Field(index="fulltext")
    embedding: Vector[1536]

    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
    works_at: "Company | None" = Relationship("WORKS_AT")


class Company(UniNode):
    __label__ = "Company"

    name: str = Field(index="btree", unique=True)
    founded: date | None = None

    employees: list[Person] = Relationship("WORKS_AT", direction="incoming")


class FriendshipEdge(UniEdge):
    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person

    since: date
    strength: float = 1.0


# Connect and sync schema
db = Database("./my_graph")
session = UniSession(db)
session.register(Person, Company, FriendshipEdge)
session.sync_schema()

# Create nodes
alice = Person(name="Alice", age=30, email="alice@example.com")
session.add(alice)
session.commit()

# Query with type safety
adults = (
    session.query(Person)
    .filter(Person.age >= 18)
    .order_by(Person.name)
    .limit(10)
    .all()
)

# Vector search
similar_people = (
    session.query(Person)
    .vector_search(Person.embedding, query_vector, k=10)
    .all()
)
```

## Model Definition

### Nodes

```python
from uni_pydantic import UniNode, Field, Vector

class Article(UniNode):
    __label__ = "Article"  # Optional, defaults to class name

    # Required field
    title: str

    # Optional field
    published_at: datetime | None = None

    # Indexed field
    slug: str = Field(index="btree", unique=True)

    # Fulltext search
    content: str = Field(index="fulltext", tokenizer="standard")

    # Vector field (auto-indexed)
    embedding: Vector[768] = Field(metric="cosine")

    # Default values
    views: int = Field(default=0)
    tags: list[str] = Field(default_factory=list)
```

### Edges

```python
from uni_pydantic import UniEdge

class AuthoredEdge(UniEdge):
    __edge_type__ = "AUTHORED"
    __from__ = Person
    __to__ = Article

    role: str = "primary"
    contribution_pct: float = 100.0
```

### Relationships

```python
from uni_pydantic import Relationship

class Person(UniNode):
    # Outgoing (default)
    follows: list["Person"] = Relationship("FOLLOWS")

    # Incoming
    followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

    # Bidirectional
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

    # Single optional
    manager: "Person | None" = Relationship("REPORTS_TO")

    # With edge properties
    friendships: list[tuple["Person", FriendshipEdge]] = Relationship(
        "FRIEND_OF",
        edge_model=FriendshipEdge
    )
```

## Query DSL

```python
# Simple filter
people = session.query(Person).filter(Person.name == "Alice").all()

# Chained filters
results = (
    session.query(Person)
    .filter(Person.age >= 18)
    .filter(Person.email.is_not_null())
    .order_by(Person.name, descending=True)
    .limit(10)
    .skip(20)
    .all()
)

# Filter methods
Person.name.starts_with("A")
Person.name.ends_with("son")
Person.name.contains("lic")
Person.age.in_([25, 30, 35])
Person.email.is_null()

# Relationship traversal
friends = (
    session.query(Person)
    .filter(Person.name == "Alice")
    .traverse(Person.friends)
    .all()
)

# Eager loading
people = (
    session.query(Person)
    .eager_load(Person.friends, Person.works_at)
    .all()
)

# Vector search
similar = (
    session.query(Person)
    .vector_search(Person.embedding, query_vec, k=10, threshold=0.8)
    .all()
)
```

## CRUD Operations

```python
# Create
person = Person(name="Bob", age=25)
session.add(person)
session.commit()

# Read
person = session.get(Person, vid=12345)
person = session.get(Person, email="bob@example.com")

# Update
person.age = 26
session.commit()

# Delete
session.delete(person)
session.commit()

# Bulk operations
people = [Person(name=f"User{i}") for i in range(1000)]
session.add_all(people)
session.commit()
```

## Transactions

```python
# Context manager (auto-commit/rollback)
with session.transaction() as tx:
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    tx.add(alice)
    tx.add(bob)
    tx.create_edge(alice, "FRIEND_OF", bob)

# Manual control
tx = session.begin()
try:
    tx.add(Person(name="Charlie"))
    tx.commit()
except Exception:
    tx.rollback()
```

## Lifecycle Hooks

```python
from uni_pydantic import before_create, after_create, before_update

class Person(UniNode):
    name: str
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @before_create
    def set_created_at(self):
        self.created_at = datetime.now()

    @after_create
    def log_creation(self):
        print(f"Created: {self.name}")

    @before_update
    def set_updated_at(self):
        self.updated_at = datetime.now()
```

## Raw Cypher

```python
# Execute raw Cypher with type mapping
results = session.cypher(
    "MATCH (p:Person)-[:FRIEND_OF]->(f:Person) WHERE p.name = $name RETURN f",
    params={"name": "Alice"},
    result_type=Person
)
```

## Type Mapping

| Python Type | Uni DataType |
|-------------|--------------|
| `str` | String |
| `int` | Int64 |
| `float` | Float64 |
| `bool` | Bool |
| `datetime` | DateTime |
| `date` | Date |
| `time` | Time |
| `bytes` | Bytes |
| `dict` | Json |
| `list[T]` | List(T) |
| `Vector[N]` | Vector{N} |
| `T \| None` | T (nullable) |

## License

Apache-2.0
