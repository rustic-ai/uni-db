# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Field definitions and configuration for uni-pydantic models."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    cast,
    overload,
)

from pydantic.fields import FieldInfo

if TYPE_CHECKING:
    from .base import UniEdge, UniNode

T = TypeVar("T")
NodeT = TypeVar("NodeT", bound="UniNode")
EdgeT = TypeVar("EdgeT", bound="UniEdge")

# Valid index types
IndexType = Literal["btree", "hash", "fulltext", "vector"]

# Valid relationship directions
Direction = Literal["outgoing", "incoming", "both"]

# Valid vector metrics
VectorMetric = Literal["l2", "cosine", "dot"]


@dataclass
class FieldConfig:
    """Configuration for a uni-pydantic field."""

    # Index configuration
    index: IndexType | None = None
    unique: bool = False

    # Fulltext index options
    tokenizer: str | None = None

    # Vector index options
    metric: VectorMetric | None = None

    # Generated/computed property
    generated: str | None = None

    # Pydantic field options (passed through)
    default: Any = dataclass_field(default_factory=lambda: ...)
    default_factory: Callable[[], Any] | None = None
    alias: str | None = None
    title: str | None = None
    description: str | None = None
    examples: list[Any] | None = None
    exclude: bool = False
    json_schema_extra: dict[str, Any] | None = None


def Field(
    default: Any = ...,
    *,
    default_factory: Callable[[], Any] | None = None,
    alias: str | None = None,
    title: str | None = None,
    description: str | None = None,
    examples: list[Any] | None = None,
    exclude: bool = False,
    json_schema_extra: dict[str, Any] | None = None,
    # Uni-specific options
    index: IndexType | None = None,
    unique: bool = False,
    tokenizer: str | None = None,
    metric: VectorMetric | None = None,
    generated: str | None = None,
) -> Any:
    """
    Create a field with uni-pydantic configuration.

    This extends Pydantic's Field with graph database options.

    Args:
        default: Default value for the field.
        default_factory: Factory function for default value.
        alias: Field alias for serialization.
        title: Human-readable title.
        description: Field description.
        examples: Example values.
        exclude: Exclude from serialization.
        json_schema_extra: Extra JSON schema properties.
        index: Index type ("btree", "hash", "fulltext", "vector").
        unique: Whether to create a unique constraint.
        tokenizer: Tokenizer for fulltext index (default: "standard").
        metric: Distance metric for vector index ("l2", "cosine", "dot").
        generated: Expression for generated/computed property.

    Returns:
        A Pydantic FieldInfo with uni-pydantic metadata attached.

    Examples:
        >>> class Person(UniNode):
        ...     name: str = Field(index="btree")
        ...     email: str = Field(unique=True)
        ...     bio: str = Field(index="fulltext", tokenizer="standard")
        ...     embedding: Vector[768] = Field(metric="cosine")
    """
    # Default tokenizer for fulltext indexes
    if index == "fulltext" and tokenizer is None:
        tokenizer = "standard"

    # Store uni config in json_schema_extra
    uni_config = FieldConfig(
        index=index,
        unique=unique,
        tokenizer=tokenizer,
        metric=metric,
        generated=generated,
        default=default,
        default_factory=default_factory,
        alias=alias,
        title=title,
        description=description,
        examples=examples,
        exclude=exclude,
        json_schema_extra=json_schema_extra,
    )

    # Merge uni config into json_schema_extra
    extra = json_schema_extra or {}
    extra["uni_config"] = uni_config

    # Create Pydantic FieldInfo
    from pydantic.fields import FieldInfo as PydanticFieldInfo

    if default_factory is not None:
        return PydanticFieldInfo(
            default_factory=default_factory,
            alias=alias,
            title=title,
            description=description,
            examples=examples,
            exclude=exclude,
            json_schema_extra=extra,
        )
    elif default is not ...:
        return PydanticFieldInfo(
            default=default,
            alias=alias,
            title=title,
            description=description,
            examples=examples,
            exclude=exclude,
            json_schema_extra=extra,
        )
    else:
        return PydanticFieldInfo(
            alias=alias,
            title=title,
            description=description,
            examples=examples,
            exclude=exclude,
            json_schema_extra=extra,
        )


def get_field_config(field_info: FieldInfo) -> FieldConfig | None:
    """Extract uni-pydantic config from a Pydantic FieldInfo."""
    extra = field_info.json_schema_extra
    if isinstance(extra, dict):
        config = extra.get("uni_config")
        if isinstance(config, FieldConfig):
            return config
    return None


@dataclass
class RelationshipConfig:
    """Configuration for a relationship field."""

    edge_type: str
    direction: Direction = "outgoing"
    edge_model: type[UniEdge] | None = None
    eager: bool = False
    cascade_delete: bool = False


class RelationshipDescriptor(Generic[NodeT]):
    """
    Descriptor for relationship fields that enables lazy loading.

    When accessed on an instance, it returns the related nodes.
    When accessed on the class, it returns the descriptor for query building.
    """

    def __init__(
        self,
        config: RelationshipConfig,
        field_name: str,
        target_type: type[NodeT] | str | None = None,
        is_list: bool = True,
    ) -> None:
        self.config = config
        self.field_name = field_name
        self.target_type = target_type
        self.is_list = is_list
        self._cache_attr = f"_rel_cache_{field_name}"

    def __set_name__(self, owner: type, name: str) -> None:
        self.field_name = name
        self._cache_attr = f"_rel_cache_{name}"

    @overload
    def __get__(
        self, obj: None, objtype: type[NodeT]
    ) -> RelationshipDescriptor[NodeT]: ...

    @overload
    def __get__(
        self, obj: NodeT, objtype: type[NodeT] | None = None
    ) -> list[NodeT] | NodeT | None: ...

    def __get__(
        self, obj: NodeT | None, objtype: type[NodeT] | None = None
    ) -> RelationshipDescriptor[NodeT] | list[NodeT] | NodeT | None:
        if obj is None:
            # Class-level access returns the descriptor
            return self

        # Instance-level access - check cache first
        if hasattr(obj, self._cache_attr):
            cached = getattr(obj, self._cache_attr)
            return cast("list[NodeT] | NodeT | None", cached)

        # Check if we have a session for lazy loading
        session = getattr(obj, "_session", None)
        if session is None:
            from .exceptions import LazyLoadError

            raise LazyLoadError(
                self.field_name,
                "No session attached. Use session.get() or enable eager loading.",
            )

        # Lazy load the relationship
        result = session._load_relationship(obj, self)

        # Cache the result
        setattr(obj, self._cache_attr, result)
        return cast("list[NodeT] | NodeT | None", result)

    def __set__(self, obj: NodeT, value: list[NodeT] | NodeT | None) -> None:
        # Allow setting the cached value (e.g., during eager loading)
        setattr(obj, self._cache_attr, value)

    def __repr__(self) -> str:
        return f"Relationship({self.config.edge_type!r}, direction={self.config.direction!r})"


# Sentinel value to detect unset relationship defaults
_RELATIONSHIP_UNSET = object()


def Relationship(
    edge_type: str,
    *,
    direction: Direction = "outgoing",
    edge_model: type[UniEdge] | None = None,
    eager: bool = False,
    cascade_delete: bool = False,
) -> Any:
    """
    Declare a relationship to another node type.

    Relationships are lazy-loaded by default. Use eager=True or
    query.eager_load() to load them with the parent query.

    Args:
        edge_type: The edge type name (e.g., "FRIEND_OF", "WORKS_AT").
        direction: Relationship direction:
            - "outgoing": Follow edges from this node (default)
            - "incoming": Follow edges to this node
            - "both": Follow edges in both directions
        edge_model: Optional UniEdge subclass for typed edge properties.
        eager: Whether to eager-load this relationship by default.
        cascade_delete: Whether to delete related edges when this node is deleted.

    Returns:
        A RelationshipDescriptor that will be processed during model creation.

    Examples:
        >>> class Person(UniNode):
        ...     # Outgoing relationship (default)
        ...     follows: list["Person"] = Relationship("FOLLOWS")
        ...
        ...     # Incoming relationship
        ...     followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")
        ...
        ...     # Single optional relationship
        ...     manager: "Person | None" = Relationship("REPORTS_TO")
        ...
        ...     # Relationship with edge properties
        ...     friendships: list[tuple["Person", FriendshipEdge]] = Relationship(
        ...         "FRIEND_OF",
        ...         edge_model=FriendshipEdge
        ...     )
    """
    config = RelationshipConfig(
        edge_type=edge_type,
        direction=direction,
        edge_model=edge_model,
        eager=eager,
        cascade_delete=cascade_delete,
    )
    # Return a marker that will be processed by the metaclass
    return _RelationshipMarker(config)


@dataclass
class _RelationshipMarker:
    """Marker class to identify relationship fields before model creation."""

    config: RelationshipConfig

    def __repr__(self) -> str:
        return f"Relationship({self.config.edge_type!r})"
