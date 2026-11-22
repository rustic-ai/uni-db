# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""
uni-pydantic: Pydantic-based OGM for Uni Graph Database.

This package provides a type-safe Object-Graph Mapping layer on top of
the Uni graph database, using Pydantic v2 for model definitions.

Example:
    >>> from uni_db import Uni
    >>> from uni_pydantic import UniNode, UniSession, Field, Relationship, Vector
    >>>
    >>> class Person(UniNode):
    ...     name: str
    ...     age: int | None = None
    ...     email: str = Field(unique=True)
    ...     embedding: Vector[1536]
    ...     friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
    >>>
    >>> db = Uni.open("./my_graph")
    >>> session = UniSession(db)
    >>> session.register(Person)
    >>> session.sync_schema()
    >>>
    >>> alice = Person(
    ...     name="Alice", age=30, email="alice@example.com", embedding=[0.0] * 1536
    ... )
    >>> session.add(alice)
    >>> session.commit()
    >>>
    >>> # Query with type safety
    >>> adults = session.query(Person).filter(Person.age >= 18).all()
"""

from pathlib import Path

# Base classes
# Async support
from .async_query import AsyncQueryBuilder
from .async_session import AsyncUniSession, AsyncUniTransaction
from .base import SearchScores, UniEdge, UniNode

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
    HybridSearchConfig,
    ModelProxy,
    OrderByClause,
    PropertyProxy,
    QueryBuilder,
    SparseSearchConfig,
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
    Btic,
    SparseVector,
    Vector,
    db_to_python_value,
    get_sparse_vector_dimensions,
    get_vector_dimensions,
    is_list_type,
    is_optional,
    python_to_db_value,
    python_type_to_uni,
    uni_to_python_type,
    unwrap_annotated,
)


def _package_version() -> str:
    """Read the version from installed package metadata.

    Derived rather than hand-written: a literal here is a second source of truth
    beside ``pyproject.toml`` and drifts silently, because only ``pyproject.toml``
    is checked at release time. It had drifted to ``2.5.0`` against a ``3.3.0``
    package, which would have shipped a wrong ``__version__`` to every consumer.

    Falls back to reading ``pyproject.toml`` for a source checkout that has not
    been installed, and finally to ``"0.0.0"`` so an import never fails over a
    version string.
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("uni-pydantic")
    except PackageNotFoundError:
        pass

    try:
        import tomllib

        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        with pyproject.open("rb") as fh:
            return str(tomllib.load(fh)["project"]["version"])
    except Exception:  # noqa: BLE001 - a version string must never break import
        return "0.0.0"


__version__ = _package_version()


__all__ = [
    # Version
    "__version__",
    # Base classes
    "UniNode",
    "UniEdge",
    "SearchScores",
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
    "Btic",
    "Vector",
    "SparseVector",
    "python_type_to_uni",
    "uni_to_python_type",
    "get_vector_dimensions",
    "get_sparse_vector_dimensions",
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
    "SparseSearchConfig",
    "HybridSearchConfig",
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
