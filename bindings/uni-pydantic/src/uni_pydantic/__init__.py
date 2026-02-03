# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""
uni-pydantic: Pydantic-based OGM for Uni Graph Database.

This package provides a type-safe Object-Graph Mapping layer on top of
the Uni graph database, using Pydantic v2 for model definitions.

Example:
    >>> from uni_db import Database
    >>> from uni_pydantic import UniNode, UniSession, Field, Relationship, Vector
    >>>
    >>> class Person(UniNode):
    ...     name: str
    ...     age: int | None = None
    ...     email: str = Field(unique=True)
    ...     embedding: Vector[1536]
    ...     friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
    >>>
    >>> db = Database("./my_graph")
    >>> session = UniSession(db)
    >>> session.register(Person)
    >>> session.sync_schema()
    >>>
    >>> alice = Person(name="Alice", age=30, email="alice@example.com")
    >>> session.add(alice)
    >>> session.commit()
    >>>
    >>> # Query with type safety
    >>> adults = session.query(Person).filter(Person.age >= 18).all()
"""

__version__ = "0.1.0"

# Base classes
# Async support
from .async_query import AsyncQueryBuilder
from .async_session import AsyncUniSession, AsyncUniTransaction
from .base import UniEdge, UniNode

# Database wrappers
from .database import AsyncUniDatabase, UniDatabase

# Exceptions
from .exceptions import (
    BulkLoadError,
    CypherInjectionError,
    LazyLoadError,
    NotPersisted,
    NotRegisteredError,
    NotTrackedError,
    QueryError,
    RelationshipError,
    SchemaError,
    SessionError,
    TransactionError,
    TypeMappingError,
    UniPydanticError,
    ValidationError,
)

# Field configuration
from .fields import (
    Direction,
    Field,
    FieldConfig,
    IndexType,
    Relationship,
    RelationshipConfig,
    RelationshipDescriptor,
    VectorMetric,
    get_field_config,
)

# Lifecycle hooks
from .hooks import (
    after_create,
    after_delete,
    after_load,
    after_update,
    before_create,
    before_delete,
    before_load,
    before_update,
)

# Query builder
from .query import (
    FilterExpr,
    FilterOp,
    ModelProxy,
    OrderByClause,
    PropertyProxy,
    QueryBuilder,
    TraversalStep,
    VectorSearchConfig,
)

# Schema generation
from .schema import (
    DatabaseSchema,
    EdgeTypeSchema,
    LabelSchema,
    PropertySchema,
    SchemaGenerator,
    generate_schema,
)

# Session management
from .session import UniSession, UniTransaction

# Type utilities
from .types import (
    DATETIME_TYPES,
    Vector,
    db_to_python_value,
    get_vector_dimensions,
    is_list_type,
    is_optional,
    python_to_db_value,
    python_type_to_uni,
    uni_to_python_type,
    unwrap_annotated,
)

__all__ = [
    # Version
    "__version__",
    # Base classes
    "UniNode",
    "UniEdge",
    # Session
    "UniSession",
    "UniTransaction",
    # Async Session
    "AsyncUniSession",
    "AsyncUniTransaction",
    # Fields
    "Field",
    "FieldConfig",
    "Relationship",
    "RelationshipConfig",
    "RelationshipDescriptor",
    "get_field_config",
    "IndexType",
    "Direction",
    "VectorMetric",
    # Types
    "Vector",
    "python_type_to_uni",
    "uni_to_python_type",
    "get_vector_dimensions",
    "is_optional",
    "is_list_type",
    "unwrap_annotated",
    "python_to_db_value",
    "db_to_python_value",
    "DATETIME_TYPES",
    # Query
    "QueryBuilder",
    "AsyncQueryBuilder",
    "FilterExpr",
    "FilterOp",
    "PropertyProxy",
    "ModelProxy",
    "OrderByClause",
    "TraversalStep",
    "VectorSearchConfig",
    # Schema
    "SchemaGenerator",
    "DatabaseSchema",
    "LabelSchema",
    "EdgeTypeSchema",
    "PropertySchema",
    "generate_schema",
    # Database
    "UniDatabase",
    "AsyncUniDatabase",
    # Hooks
    "before_create",
    "after_create",
    "before_update",
    "after_update",
    "before_delete",
    "after_delete",
    "before_load",
    "after_load",
    # Exceptions
    "UniPydanticError",
    "SchemaError",
    "TypeMappingError",
    "ValidationError",
    "SessionError",
    "NotRegisteredError",
    "NotPersisted",
    "NotTrackedError",
    "TransactionError",
    "QueryError",
    "RelationshipError",
    "LazyLoadError",
    "BulkLoadError",
    "CypherInjectionError",
]
