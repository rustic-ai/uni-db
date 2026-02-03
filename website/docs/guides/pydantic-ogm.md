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
db = uni_db.Database("./my_graph")

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
    .filter(Person.status == "pending")
    .update(status="approved")
)
```

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
    WHERE vector_similarity(d.embedding, $query_vec) > 0.8
    RETURN d.title as title
    """,
    params={"query_vec": query_embedding}
)
```

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
with session.transaction() as tx:
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    tx.add(alice)
    tx.add(bob)
    tx.create_edge(alice, "FRIEND_OF", bob)
    # All or nothing
```

## Next Steps

- [Pydantic OGM Reference](../reference/pydantic-ogm.md) - Complete API documentation
- [Example Notebooks](../examples/index.md) - Interactive examples with uni-pydantic
- [Schema Design](schema-design.md) - Best practices for graph modeling
