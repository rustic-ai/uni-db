# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
"""
Type stubs for the Uni Graph Database Python bindings.

Uni is an embedded, multi-model graph database with OpenCypher queries,
and columnar analytics.
"""

from __future__ import annotations

from typing import Any

# =============================================================================
# Data Classes
# =============================================================================

class PropertyInfo:
    """Information about a property."""

    name: str
    """Property name."""

    data_type: str
    """Data type (e.g., 'String', 'Int64', 'Vector{128}')."""

    nullable: bool
    """Whether null values are allowed."""

    is_indexed: bool
    """Whether an index exists on this property."""

class IndexInfo:
    """Information about an index."""

    name: str
    """Index name."""

    index_type: str
    """Index type (e.g., 'btree', 'hash', 'vector')."""

    properties: list[str]
    """Properties covered by this index."""

    status: str
    """Index status (e.g., 'Ready', 'Building')."""

class ConstraintInfo:
    """Information about a constraint."""

    name: str
    """Constraint name."""

    constraint_type: str
    """Constraint type."""

    properties: list[str]
    """Properties covered by this constraint."""

    enabled: bool
    """Whether the constraint is enabled."""

class LabelInfo:
    """Information about a vertex label."""

    name: str
    """Label name."""

    count: int
    """Approximate vertex count."""

    properties: list[PropertyInfo]
    """List of properties defined on this label."""

    indexes: list[IndexInfo]
    """List of indexes on this label."""

    constraints: list[ConstraintInfo]
    """List of constraints on this label."""

class BulkStats:
    """Statistics from a bulk loading operation."""

    vertices_inserted: int
    """Number of vertices inserted."""

    edges_inserted: int
    """Number of edges inserted."""

    indexes_rebuilt: int
    """Number of indexes rebuilt."""

    duration_secs: float
    """Total duration in seconds."""

    index_build_duration_secs: float
    """Duration spent building indexes in seconds."""

    indexes_pending: bool
    """Whether indexes are still building in background."""

class BulkProgress:
    """Progress callback data during bulk loading."""

    phase: str
    """Current phase of bulk loading."""

    rows_processed: int
    """Number of rows processed so far."""

    total_rows: int | None
    """Total rows if known."""

    current_label: str | None
    """Current label being processed."""

# =============================================================================
# Builder Classes
# =============================================================================

class DatabaseBuilder:
    """Builder for creating or opening a Uni database."""

    @staticmethod
    def open(path: str) -> DatabaseBuilder: ...
    @staticmethod
    def create(path: str) -> DatabaseBuilder: ...
    @staticmethod
    def open_existing(path: str) -> DatabaseBuilder: ...
    @staticmethod
    def temporary() -> DatabaseBuilder: ...
    @staticmethod
    def in_memory() -> DatabaseBuilder: ...
    def hybrid(self, local_path: str, remote_url: str) -> DatabaseBuilder: ...
    def cache_size(self, bytes: int) -> DatabaseBuilder: ...
    def parallelism(self, n: int) -> DatabaseBuilder: ...
    def build(self) -> Database: ...

class QueryBuilder:
    """Builder for parameterized queries."""

    def param(self, name: str, value: Any) -> QueryBuilder: ...
    def params(self, params: dict[str, Any]) -> None: ...
    def timeout(self, seconds: float) -> QueryBuilder: ...
    def max_memory(self, bytes: int) -> QueryBuilder: ...
    def fetch_all(self) -> list[dict[str, Any]]: ...

class SchemaBuilder:
    """Builder for defining database schema."""

    def label(self, name: str) -> LabelBuilder: ...
    def edge_type(
        self, name: str, from_labels: list[str], to_labels: list[str]
    ) -> EdgeTypeBuilder: ...
    def apply(self) -> None: ...

class LabelBuilder:
    """Builder for defining a vertex label."""

    def property(self, name: str, data_type: str) -> LabelBuilder: ...
    def property_nullable(self, name: str, data_type: str) -> LabelBuilder: ...
    def vector(self, name: str, dimensions: int) -> LabelBuilder: ...
    def index(self, property: str, index_type: str) -> LabelBuilder: ...
    def done(self) -> SchemaBuilder: ...
    def apply(self) -> None: ...

class EdgeTypeBuilder:
    """Builder for defining an edge type."""

    def property(self, name: str, data_type: str) -> EdgeTypeBuilder: ...
    def property_nullable(self, name: str, data_type: str) -> EdgeTypeBuilder: ...
    def done(self) -> SchemaBuilder: ...
    def apply(self) -> None: ...

class SessionBuilder:
    """Builder for creating query sessions."""

    def set(self, key: str, value: Any) -> None: ...
    def build(self) -> Session: ...

class Session:
    """A query session with scoped variables."""

    def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]: ...
    def execute(self, cypher: str) -> int: ...
    def get(self, key: str) -> Any | None: ...

class BulkWriterBuilder:
    """Builder for bulk data loading."""

    def defer_vector_indexes(self, defer: bool) -> BulkWriterBuilder: ...
    def defer_scalar_indexes(self, defer: bool) -> BulkWriterBuilder: ...
    def batch_size(self, size: int) -> BulkWriterBuilder: ...
    def async_indexes(self, async_: bool) -> BulkWriterBuilder: ...
    def build(self) -> BulkWriter: ...

class BulkWriter:
    """High-performance bulk data loader."""

    def insert_vertices(
        self, label: str, vertices: list[dict[str, Any]]
    ) -> list[int]: ...
    def insert_edges(
        self,
        edge_type: str,
        edges: list[tuple[int, int, dict[str, Any]]],
    ) -> None: ...
    def commit(self) -> BulkStats: ...
    def abort(self) -> None: ...

# =============================================================================
# Transaction
# =============================================================================

class Transaction:
    """A database transaction."""

    def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

# =============================================================================
# Database (Sync)
# =============================================================================

class Database:
    """
    The main synchronous Uni database interface.

    Example:
        >>> db = Database("/path/to/db")
        >>> db.create_label("Person")
        >>> db.query("CREATE (n:Person {name: 'Alice'})")
        >>> results = db.query("MATCH (n:Person) RETURN n.name AS name")
    """

    def __init__(self, path: str) -> None: ...

    # Query Methods
    def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]: ...
    def execute(self, cypher: str, params: dict[str, Any] | None = None) -> int: ...
    def query_with(self, cypher: str) -> QueryBuilder: ...
    def explain(self, cypher: str) -> dict[str, Any]: ...
    def profile(self, cypher: str) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    # Transaction Methods
    def begin(self) -> Transaction: ...
    def flush(self) -> None: ...

    # Session Methods
    def session(self) -> SessionBuilder: ...

    # Schema Methods
    def schema(self) -> SchemaBuilder: ...
    def create_label(self, name: str) -> int: ...
    def create_edge_type(
        self,
        name: str,
        from_labels: list[str] | None = None,
        to_labels: list[str] | None = None,
    ) -> int: ...
    def add_property(
        self, label_or_type: str, name: str, data_type: str, nullable: bool
    ) -> None: ...
    def label_exists(self, name: str) -> bool: ...
    def edge_type_exists(self, name: str) -> bool: ...
    def list_labels(self) -> list[str]: ...
    def list_edge_types(self) -> list[str]: ...
    def get_label_info(self, name: str) -> LabelInfo | None: ...
    def get_schema(self) -> dict[str, Any]: ...
    def load_schema(self, path: str) -> None: ...
    def save_schema(self, path: str) -> None: ...

    # Index Methods
    def create_scalar_index(
        self, label: str, property: str, index_type: str
    ) -> None: ...
    def create_vector_index(self, label: str, property: str, metric: str) -> None: ...
    # Bulk Loading Methods
    def bulk_writer(self) -> BulkWriterBuilder: ...
    def bulk_insert_vertices(
        self, label: str, vertices: list[dict[str, Any]]
    ) -> list[int]: ...
    def bulk_insert_edges(
        self,
        edge_type: str,
        edges: list[tuple[int, int, dict[str, Any]]],
    ) -> None: ...

# =============================================================================
# Async API
# =============================================================================

class AsyncDatabaseBuilder:
    """Async builder for creating and configuring an AsyncDatabase instance."""

    @staticmethod
    def open(path: str) -> AsyncDatabaseBuilder: ...
    @staticmethod
    def open_existing(path: str) -> AsyncDatabaseBuilder: ...
    @staticmethod
    def create(path: str) -> AsyncDatabaseBuilder: ...
    @staticmethod
    def temporary() -> AsyncDatabaseBuilder: ...
    @staticmethod
    def in_memory() -> AsyncDatabaseBuilder: ...
    def hybrid(self, local_path: str, remote_url: str) -> AsyncDatabaseBuilder: ...
    def cache_size(self, bytes: int) -> AsyncDatabaseBuilder: ...
    def parallelism(self, n: int) -> AsyncDatabaseBuilder: ...
    async def build(self) -> AsyncDatabase: ...

class AsyncDatabase:
    """
    The main asynchronous Uni database interface.

    Example:
        >>> db = await AsyncDatabase.open("/path/to/db")
        >>> await db.create_label("Person")
        >>> await db.execute("CREATE (n:Person {name: 'Alice'})")
        >>> results = await db.query("MATCH (n:Person) RETURN n.name AS name")
    """

    @staticmethod
    async def open(path: str) -> AsyncDatabase: ...
    @staticmethod
    async def temporary() -> AsyncDatabase: ...
    @staticmethod
    def builder() -> AsyncDatabaseBuilder: ...

    # Query Methods
    async def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]: ...
    async def execute(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> int: ...
    async def explain(self, cypher: str) -> dict[str, Any]: ...
    async def profile(
        self, cypher: str
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...
    async def flush(self) -> None: ...
    def query_with(self, cypher: str) -> AsyncQueryBuilder: ...

    # Transaction Methods
    async def begin(self) -> AsyncTransaction: ...

    # Session Methods
    def session(self) -> AsyncSessionBuilder: ...

    # Schema Methods
    def schema(self) -> AsyncSchemaBuilder: ...
    async def create_label(self, name: str) -> int: ...
    async def create_edge_type(
        self,
        name: str,
        from_labels: list[str] | None = None,
        to_labels: list[str] | None = None,
    ) -> int: ...
    async def add_property(
        self, label_or_type: str, name: str, data_type: str, nullable: bool
    ) -> None: ...
    async def label_exists(self, name: str) -> bool: ...
    async def edge_type_exists(self, name: str) -> bool: ...
    async def list_labels(self) -> list[str]: ...
    async def list_edge_types(self) -> list[str]: ...
    async def get_label_info(self, name: str) -> LabelInfo | None: ...
    def get_schema(self) -> dict[str, Any]: ...
    async def load_schema(self, path: str) -> None: ...
    async def save_schema(self, path: str) -> None: ...

    # Index Methods
    async def create_scalar_index(
        self, label: str, property: str, index_type: str
    ) -> None: ...
    async def create_vector_index(
        self, label: str, property: str, metric: str
    ) -> None: ...
    # Bulk Loading Methods
    def bulk_writer(self) -> AsyncBulkWriterBuilder: ...
    async def bulk_insert_vertices(
        self, label: str, vertices: list[dict[str, Any]]
    ) -> list[int]: ...
    async def bulk_insert_edges(
        self,
        edge_type: str,
        edges: list[tuple[int, int, dict[str, Any]]],
    ) -> None: ...

class AsyncTransaction:
    """An async database transaction with context manager support."""

    async def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def __aenter__(self) -> AsyncTransaction: ...
    async def __aexit__(
        self, exc_type: type | None, exc_val: Any, exc_tb: Any
    ) -> bool: ...

class AsyncSessionBuilder:
    """Builder for async sessions."""

    def set(self, key: str, value: Any) -> None: ...
    def build(self) -> AsyncSession: ...

class AsyncSession:
    """An async query session with scoped variables."""

    async def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]: ...
    async def execute(self, cypher: str) -> int: ...
    def get(self, key: str) -> Any | None: ...

class AsyncBulkWriterBuilder:
    """Builder for async bulk writer."""

    def defer_vector_indexes(self, defer: bool) -> AsyncBulkWriterBuilder: ...
    def defer_scalar_indexes(self, defer: bool) -> AsyncBulkWriterBuilder: ...
    def batch_size(self, size: int) -> AsyncBulkWriterBuilder: ...
    def async_indexes(self, async_: bool) -> AsyncBulkWriterBuilder: ...
    def build(self) -> AsyncBulkWriter: ...

class AsyncBulkWriter:
    """Async bulk writer for high-throughput data ingestion."""

    async def insert_vertices(
        self, label: str, vertices: list[dict[str, Any]]
    ) -> list[int]: ...
    async def insert_edges(
        self,
        edge_type: str,
        edges: list[tuple[int, int, dict[str, Any]]],
    ) -> None: ...
    async def commit(self) -> BulkStats: ...
    def abort(self) -> None: ...

class AsyncQueryBuilder:
    """Async builder for parameterized queries."""

    def param(self, name: str, value: Any) -> AsyncQueryBuilder: ...
    def params(self, params: dict[str, Any]) -> None: ...
    def timeout(self, seconds: float) -> AsyncQueryBuilder: ...
    def max_memory(self, bytes: int) -> AsyncQueryBuilder: ...
    async def run(self) -> list[dict[str, Any]]: ...

class AsyncSchemaBuilder:
    """Async builder for defining database schema."""

    def label(self, name: str) -> AsyncLabelBuilder: ...
    def edge_type(
        self, name: str, from_labels: list[str], to_labels: list[str]
    ) -> AsyncEdgeTypeBuilder: ...
    async def apply(self) -> None: ...

class AsyncLabelBuilder:
    """Async builder for defining a vertex label."""

    def property(self, name: str, data_type: str) -> AsyncLabelBuilder: ...
    def property_nullable(self, name: str, data_type: str) -> AsyncLabelBuilder: ...
    def vector(self, name: str, dimensions: int) -> AsyncLabelBuilder: ...
    def index(self, property: str, index_type: str) -> AsyncLabelBuilder: ...
    def done(self) -> AsyncSchemaBuilder: ...
    async def apply(self) -> None: ...

class AsyncEdgeTypeBuilder:
    """Async builder for defining an edge type."""

    def property(self, name: str, data_type: str) -> AsyncEdgeTypeBuilder: ...
    def property_nullable(self, name: str, data_type: str) -> AsyncEdgeTypeBuilder: ...
    def done(self) -> AsyncSchemaBuilder: ...
    async def apply(self) -> None: ...
