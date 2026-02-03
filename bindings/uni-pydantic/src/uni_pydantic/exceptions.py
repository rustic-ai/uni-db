# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Custom exceptions for uni-pydantic."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .base import UniEdge, UniNode


class UniPydanticError(Exception):
    """Base exception for all uni-pydantic errors."""


class SchemaError(UniPydanticError):
    """Error related to schema definition or generation."""

    def __init__(self, message: str, model: type | None = None) -> None:
        self.model = model
        super().__init__(message)


class TypeMappingError(SchemaError):
    """Error mapping Python type to Uni DataType."""

    def __init__(self, python_type: Any, message: str | None = None) -> None:
        self.python_type = python_type
        msg = message or f"Cannot map Python type {python_type!r} to Uni DataType"
        super().__init__(msg)


class ValidationError(UniPydanticError):
    """Validation error for model instances."""


class SessionError(UniPydanticError):
    """Error related to session operations."""


class NotRegisteredError(SessionError):
    """Model type not registered with session."""

    def __init__(self, model: type[UniNode] | type[UniEdge]) -> None:
        self.model = model
        super().__init__(
            f"Model {model.__name__!r} is not registered with this session. "
            f"Call session.register({model.__name__}) first."
        )


class NotPersisted(SessionError):
    """Operation requires a persisted entity."""

    def __init__(self, entity: UniNode | UniEdge) -> None:
        self.entity = entity
        super().__init__(
            f"Entity {entity!r} is not persisted. Call session.add() and commit() first."
        )


class NotTrackedError(SessionError):
    """Entity is not tracked by this session."""


class TransactionError(SessionError):
    """Error related to transaction operations."""


class QueryError(UniPydanticError):
    """Error executing a query."""


class RelationshipError(UniPydanticError):
    """Error related to relationship operations."""


class LazyLoadError(RelationshipError):
    """Error lazy-loading a relationship."""

    def __init__(self, field_name: str, reason: str) -> None:
        self.field_name = field_name
        super().__init__(f"Cannot lazy-load relationship '{field_name}': {reason}")


class BulkLoadError(UniPydanticError):
    """Error during bulk loading operations."""


class CypherInjectionError(QueryError):
    """Property name validation failure — potential Cypher injection."""

    def __init__(self, name: str, reason: str | None = None) -> None:
        self.name = name
        msg = reason or f"Invalid property name {name!r}: possible Cypher injection"
        super().__init__(msg)
