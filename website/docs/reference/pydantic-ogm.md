# Pydantic OGM Reference

**uni-pydantic** is a Pydantic-based Object-Graph Mapping (OGM) library for Uni. It provides type-safe Python models for graph nodes and edges with full IDE autocomplete support.

## Installation

```bash
pip install uni-pydantic
```

Or install from source:

```bash
cd bindings/uni-pydantic
poetry install
```

## Quick Start

```python
from uni_pydantic import UniNode, UniEdge, UniSession, Field, Relationship, Vector
import uni_db

# Define your models
class Person(UniNode):
    __label__ = "Person"

    name: str
    age: int | None = None
    email: str = Field(unique=True, index="btree")

class Knows(UniEdge):
    __edge_type__ = "KNOWS"
    __from__ = Person
    __to__ = Person

    since: int

# Connect and sync schema
db = uni_db.Database("./my_graph")
session = UniSession(db)
session.register(Person, Knows)
session.sync_schema()

# Create data with type safety
alice = Person(name="Alice", age=30, email="alice@example.com")
bob = Person(name="Bob", age=25, email="bob@example.com")

session.add_all([alice, bob])
session.commit()

# Query with type-safe builder
adults = session.query(Person).filter(Person.age >= 18).all()
```

---

## Core Classes

### UniNode

Base class for graph vertices. Subclass to define your node types.

```python
class Person(UniNode):
    """A person in the social graph."""
    __label__ = "Person"  # Optional, defaults to class name

    # Properties with types
    name: str                           # Required string
    age: int | None = None              # Optional int
    email: str = Field(unique=True)     # Unique constraint
    bio: str = Field(index="fulltext")  # Fulltext indexed
    embedding: Vector[768]              # Vector with auto-index

    # Relationships (lazy-loaded)
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
    company: "Company | None" = Relationship("WORKS_AT")
```

**Class Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `__label__` | `str` | Vertex label name (defaults to class name) |
| `__relationships__` | `dict` | Auto-populated relationship configs |

**Instance Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `vid` | `int \| None` | Vertex ID assigned by database |
| `uid` | `str \| None` | Content-addressed unique identifier |
| `is_persisted` | `bool` | Whether saved to database |
| `is_dirty` | `bool` | Whether has unsaved changes |

**Methods:**

```python
# Convert to property dict for storage
props = person.to_properties()

# Create from property dict
person = Person.from_properties({"name": "Alice", "age": 30}, vid=123)

# Get property fields (excluding relationships)
fields = Person.get_property_fields()

# Get relationship configurations
rels = Person.get_relationship_fields()
```

### UniEdge

Base class for graph edges with properties.

```python
class FriendshipEdge(UniEdge):
    """Friendship with properties."""
    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person

    since: date
    strength: float = 1.0
```

**Class Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `__edge_type__` | `str` | Edge type name |
| `__from__` | `type \| tuple` | Source node type(s) |
| `__to__` | `type \| tuple` | Target node type(s) |

**Instance Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `eid` | `int \| None` | Edge ID assigned by database |
| `src_vid` | `int \| None` | Source vertex ID |
| `dst_vid` | `int \| None` | Destination vertex ID |
| `is_persisted` | `bool` | Whether saved to database |

---

## Field Configuration

The `Field()` function extends Pydantic's Field with graph database options.

```python
from uni_pydantic import Field

class Article(UniNode):
    # Index types
    title: str = Field(index="btree")           # B-tree index
    slug: str = Field(index="btree", unique=True) # BTree index + unique
    content: str = Field(index="fulltext", tokenizer="standard")

    # Vector field with metric
    embedding: Vector[768] = Field(metric="cosine")

    # Default values
    views: int = Field(default=0)
    tags: list[str] = Field(default_factory=list)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `index` | `"btree" \| "hash" \| "fulltext" \| "vector"` | Index type to create |
| `unique` | `bool` | Create unique constraint |
| `tokenizer` | `str` | Tokenizer for fulltext index |
| `metric` | `"l2" \| "cosine" \| "dot"` | Distance metric for vector index |
| `generated` | `str` | Expression for computed property |

Plus all standard Pydantic Field options (`default`, `default_factory`, `alias`, `description`, etc.).

**Note:** Scalar indexes are currently built as BTree under the hood; `hash` is accepted for forward compatibility.

---

## Relationships

Declare relationships between nodes using `Relationship()`.

```python
from uni_pydantic import Relationship

class Person(UniNode):
    # Outgoing relationship (default)
    follows: list["Person"] = Relationship("FOLLOWS")

    # Incoming relationship
    followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

    # Bidirectional
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

    # Single optional relationship
    manager: "Person | None" = Relationship("REPORTS_TO")

    # With edge model for properties
    friendships: list[tuple["Person", FriendshipEdge]] = Relationship(
        "FRIEND_OF",
        edge_model=FriendshipEdge
    )
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `edge_type` | `str` | Edge type name (e.g., "FOLLOWS") |
| `direction` | `"outgoing" \| "incoming" \| "both"` | Traversal direction |
| `edge_model` | `type[UniEdge]` | Optional edge model for properties |
| `eager` | `bool` | Eager-load by default |
| `cascade_delete` | `bool` | Delete edges when node deleted |

---

## Vector Type

Fixed-dimension vectors for embeddings.

```python
from uni_pydantic import Vector

class Document(UniNode):
    # 1536-dimensional vector (OpenAI ada-002)
    embedding: Vector[1536]

    # With custom metric
    embedding: Vector[768] = Field(metric="cosine")
```

At runtime, vectors are stored as `list[float]`. The dimension is validated on assignment.

---

## UniSession

Session for database operations.

```python
from uni_pydantic import UniSession
import uni_db

db = uni_db.Database("./my_graph")
session = UniSession(db)
```

### Model Registration

```python
# Register models for schema generation
session.register(Person, Company, FriendshipEdge)

# Sync schema to database (creates labels, properties, indexes)
session.sync_schema()
```

### CRUD Operations

```python
# Create
alice = Person(name="Alice", age=30)
session.add(alice)
session.commit()

# Read by ID
person = session.get(Person, vid=12345)

# Read by property
person = session.get(Person, email="alice@example.com")

# Update
alice.age = 31
session.commit()  # Auto-detects dirty fields

# Delete
session.delete(alice)
session.commit()

# Bulk create
people = [Person(name=f"User{i}") for i in range(1000)]
session.add_all(people)
session.commit()

# Refresh from database
session.refresh(alice)
```

### Creating Edges

```python
# Create edge between nodes
session.create_edge(alice, "FRIEND_OF", bob, {"since": 2020})

# With edge model
friendship = FriendshipEdge(since=date.today(), strength=0.9)
session.create_edge(alice, "FRIEND_OF", bob, friendship)
```

### Raw Cypher

```python
# Execute raw Cypher
results = session.cypher(
    "MATCH (p:Person) WHERE p.age > $age RETURN p.name as name",
    params={"age": 18}
)

# With result type mapping
people = session.cypher(
    "MATCH (p:Person) RETURN p",
    result_type=Person
)
```

---

## Query Builder

Type-safe query construction with method chaining.

```python
# Basic query
people = session.query(Person).all()

# Filtering
adults = (
    session.query(Person)
    .filter(Person.age >= 18)
    .filter(Person.email.is_not_null())
    .all()
)

# Ordering and pagination
page = (
    session.query(Person)
    .order_by(Person.name)
    .skip(20)
    .limit(10)
    .all()
)

# Get first/one
first = session.query(Person).filter(Person.name == "Alice").first()

# Count
count = session.query(Person).filter(Person.age >= 18).count()

# Check existence
exists = session.query(Person).filter(Person.email == "test@example.com").exists()
```

### Filter Operators

```python
# Comparison
Person.age == 30
Person.age != 30
Person.age < 30
Person.age <= 30
Person.age > 30
Person.age >= 30

# Null checks
Person.email.is_null()
Person.email.is_not_null()

# String operations
Person.name.like("Al%")
Person.name.starts_with("Al")
Person.name.ends_with("ice")
Person.name.contains("lic")

# Collection membership
Person.age.in_([25, 30, 35])
Person.age.not_in([25, 30, 35])
```

### Bulk Operations

```python
# Bulk delete
deleted = session.query(Person).filter(Person.age < 18).delete()

# Bulk update
updated = session.query(Person).filter(Person.age < 18).update(status="minor")
```

---

## Transactions

```python
# Context manager (auto-commit/rollback)
with session.transaction() as tx:
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    tx.add(alice)
    tx.add(bob)
    tx.create_edge(alice, "FRIEND_OF", bob)
    # Auto-commits on success, rolls back on exception

# Manual control
tx = session.begin()
try:
    tx.add(Person(name="Charlie"))
    tx.commit()
except Exception:
    tx.rollback()
```

---

## Lifecycle Hooks

Decorators for model lifecycle events.

```python
from uni_pydantic import before_create, after_create, before_update, before_delete
from datetime import datetime

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

    @before_delete
    def check_deletion(self):
        if self.name == "Admin":
            raise ValueError("Cannot delete admin user")
```

**Available Hooks:**

| Hook | Timing |
|------|--------|
| `@before_create` | Before insert |
| `@after_create` | After insert |
| `@before_update` | Before update |
| `@after_update` | After update |
| `@before_delete` | Before delete |
| `@after_delete` | After delete |
| `@before_load` | Before hydration from DB |
| `@after_load` | After hydration from DB |

---

## Type Mapping

Automatic conversion between Python and Uni types:

| Python Type | Uni DataType | Notes |
|-------------|--------------|-------|
| `str` | `string` | UTF-8 string |
| `int` | `int` | 64-bit integer |
| `float` | `float` | 64-bit float |
| `bool` | `bool` | Boolean |
| `datetime` | `datetime` | Date and time |
| `date` | `date` | Date only |
| `time` | `time` | Time only |
| `timedelta` | `duration` | Time duration |
| `bytes` | `bytes` | Binary data |
| `dict` | `json` | JSON object |
| `list[T]` | `list:T` | Typed list |
| `Vector[N]` | `vector:N` | N-dim vector |
| `T \| None` | `T` + nullable | Optional field |

---

## Exceptions

```python
from uni_pydantic import (
    UniPydanticError,    # Base exception
    SchemaError,         # Schema definition errors
    QueryError,          # Query building/execution errors
    SessionError,        # Session state errors
    NotPersisted,        # Entity not saved
    LazyLoadError,       # Relationship loading without session
    TransactionError,    # Transaction state errors
    TypeMappingError,    # Unsupported type conversion
)
```

---

## Complete Example

```python
from datetime import date
from uni_pydantic import (
    UniNode, UniEdge, UniSession,
    Field, Relationship, Vector,
    before_create
)
import uni_db

# Models
class User(UniNode):
    __label__ = "User"

    email: str = Field(unique=True, index="btree")
    name: str = Field(index="btree")
    bio: str | None = Field(default=None, index="fulltext")
    embedding: Vector[384] | None = None
    created_at: date | None = None

    posts: list["Post"] = Relationship("AUTHORED", direction="outgoing")
    followers: list["User"] = Relationship("FOLLOWS", direction="incoming")
    following: list["User"] = Relationship("FOLLOWS", direction="outgoing")

    @before_create
    def set_created(self):
        self.created_at = date.today()

class Post(UniNode):
    __label__ = "Post"

    title: str
    content: str = Field(index="fulltext")
    embedding: Vector[384]

    author: User | None = Relationship("AUTHORED", direction="incoming")

class Follows(UniEdge):
    __edge_type__ = "FOLLOWS"
    __from__ = User
    __to__ = User

    since: date

# Usage
db = uni_db.Database("./social_graph")
session = UniSession(db)
session.register(User, Post, Follows)
session.sync_schema()

# Create users
alice = User(email="alice@example.com", name="Alice")
bob = User(email="bob@example.com", name="Bob")
session.add_all([alice, bob])
session.commit()

# Create follow relationship
session.create_edge(alice, "FOLLOWS", bob, Follows(since=date.today()))
session.commit()

# Query
popular_users = (
    session.query(User)
    .filter(User.name.starts_with("A"))
    .order_by(User.created_at, descending=True)
    .limit(10)
    .all()
)
```
