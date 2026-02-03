# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Base classes for uni-pydantic models."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    get_type_hints,
)

from pydantic import BaseModel, ConfigDict, PrivateAttr
from pydantic.fields import FieldInfo

from .fields import (
    RelationshipConfig,
    RelationshipDescriptor,
    _RelationshipMarker,
)
from .types import (
    db_to_python_value,
    is_list_type,
    is_optional,
    python_to_db_value,
)

if TYPE_CHECKING:
    from .query import PropertyProxy
    from .session import UniSession


def _model_to_properties(
    model_instance: BaseModel,
    property_fields: dict[str, Any],
) -> dict[str, Any]:
    """Convert a model's property fields to a database-ready dictionary.

    Shared implementation for both UniNode.to_properties() and UniEdge.to_properties().
    """
    try:
        hints = get_type_hints(type(model_instance))
    except Exception:
        hints = {}

    props: dict[str, Any] = {}
    for name in property_fields:
        value = getattr(model_instance, name)
        type_hint = hints.get(name)
        if type_hint is not None:
            value = python_to_db_value(value, type_hint)
        elif hasattr(value, "values") and hasattr(type(value), "__dimensions__"):
            # Fallback for Vector instances when type hints unavailable
            value = value.values
        props[name] = value
    return props


def _convert_db_values(
    data: dict[str, Any],
    model_class: type[BaseModel],
) -> dict[str, Any]:
    """Convert database values back to Python types using model type hints.

    Shared implementation for both UniNode.from_properties() and UniEdge.from_properties().
    """
    try:
        hints = get_type_hints(model_class)
    except Exception:
        hints = {}

    return {
        key: db_to_python_value(value, hints[key]) if key in hints else value
        for key, value in data.items()
    }


class UniModelMeta(type(BaseModel)):  # type: ignore[misc]
    """Metaclass for UniNode and UniEdge that processes relationship fields."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type[Any]:
        # Process relationship markers in annotations
        annotations = namespace.get("__annotations__", {})
        relationships: dict[str, RelationshipConfig] = {}

        # Collect relationship markers and convert to descriptors
        for field_name, annotation in list(annotations.items()):
            default = namespace.get(field_name)
            if isinstance(default, _RelationshipMarker):
                # Determine if it's a list or single relationship
                is_opt, inner = is_optional(annotation)
                is_lst, elem_type = is_list_type(inner if is_opt else annotation)

                # Store relationship config
                relationships[field_name] = default.config

                # Remove from namespace and annotations to exclude from Pydantic validation
                if field_name in namespace:
                    del namespace[field_name]
                # Also remove from annotations so Pydantic doesn't treat it as a required field
                del annotations[field_name]

        # Create the class
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Store relationship configs on the class
        existing_rels = getattr(cls, "__relationships__", {})
        cls.__relationships__ = {**existing_rels, **relationships}

        # Add relationship descriptors
        for field_name, config in relationships.items():
            annotation = annotations.get(field_name)
            is_opt, inner = is_optional(annotation) if annotation else (False, None)
            is_lst, _ = (
                is_list_type(inner if is_opt else annotation)
                if annotation
                else (False, None)
            )

            descriptor: RelationshipDescriptor[Any] = RelationshipDescriptor(
                config=config,
                field_name=field_name,
                is_list=is_lst,
            )
            setattr(cls, field_name, descriptor)

        return cls  # type: ignore[no-any-return]

    def __getattr__(cls, name: str) -> PropertyProxy[Any] | Any:
        """Enable filter DSL: Person.age >= 18 returns a FilterExpr."""
        # Only trigger for UniNode/UniEdge subclasses, not the base classes themselves
        if name.startswith("_"):
            raise AttributeError(name)

        # Check if it's a model field
        model_fields = getattr(cls, "model_fields", None)
        if model_fields and name in model_fields:
            from .query import PropertyProxy

            return PropertyProxy(name, cls)

        # Check if it's a relationship
        relationships = getattr(cls, "__relationships__", None)
        if relationships and name in relationships:
            return relationships[name]

        raise AttributeError(f"type object {cls.__name__!r} has no attribute {name!r}")


class UniNode(BaseModel, metaclass=UniModelMeta):
    """
    Base class for graph node models.

    Subclass this to define your node types. Each UniNode subclass
    represents a vertex label in the graph database.

    Attributes:
        __label__: The vertex label name. Defaults to the class name.
        __relationships__: Dictionary of relationship configurations.

    Private Attributes:
        _vid: The vertex ID assigned by the database.
        _uid: The unique identifier (content-addressed hash).
        _session: Reference to the owning session.
        _dirty: Set of modified field names.

    Example:
        >>> class Person(UniNode):
        ...     __label__ = "Person"
        ...
        ...     name: str
        ...     age: int | None = None
        ...     email: str = Field(unique=True)
        ...
        ...     friends: list["Person"] = Relationship("FRIEND_OF", direction="both")
    """

    model_config = ConfigDict(
        # Allow extra fields for future extensibility
        extra="forbid",
        # Validate on assignment for dirty tracking
        validate_assignment=True,
        # Allow arbitrary types (for Vector, etc.)
        arbitrary_types_allowed=True,
        # Use enum values
        use_enum_values=True,
    )

    # Class-level configuration
    __label__: ClassVar[str] = ""
    __relationships__: ClassVar[dict[str, RelationshipConfig]] = {}

    # Private attributes for session tracking
    _vid: int | None = PrivateAttr(default=None)
    _uid: str | None = PrivateAttr(default=None)
    _session: UniSession | None = PrivateAttr(default=None)
    _dirty: set[str] = PrivateAttr(default_factory=set)
    _is_new: bool = PrivateAttr(default=True)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Set default label to class name if not specified
        if not cls.__label__:
            cls.__label__ = cls.__name__

    def model_post_init(self, __context: Any) -> None:
        """Clear dirty tracking after construction."""
        super().model_post_init(__context)
        self._dirty = set()

    @property
    def vid(self) -> int | None:
        """The vertex ID assigned by the database."""
        return self._vid

    @property
    def uid(self) -> str | None:
        """The unique identifier (content-addressed hash)."""
        return self._uid

    @property
    def is_persisted(self) -> bool:
        """Whether this node has been saved to the database."""
        return self._vid is not None

    @property
    def is_dirty(self) -> bool:
        """Whether this node has unsaved changes."""
        return bool(self._dirty)

    def __setattr__(self, name: str, value: Any) -> None:
        # Track dirty fields (but not private attributes)
        if not name.startswith("_") and hasattr(self, "_dirty"):
            self._dirty.add(name)
        super().__setattr__(name, value)

    def _mark_clean(self) -> None:
        """Mark all fields as clean (called after commit)."""
        self._dirty.clear()
        self._is_new = False

    def _attach_session(
        self, session: UniSession, vid: int, uid: str | None = None
    ) -> None:
        """Attach this node to a session with its database IDs."""
        self._session = session
        self._vid = vid
        self._uid = uid
        self._is_new = False

    @classmethod
    def get_property_fields(cls) -> dict[str, FieldInfo]:
        """Get all property fields (excluding relationships)."""
        return {
            name: info
            for name, info in cls.model_fields.items()
            if name not in cls.__relationships__
        }

    @classmethod
    def get_relationship_fields(cls) -> dict[str, RelationshipConfig]:
        """Get all relationship field configurations."""
        return cls.__relationships__

    def to_properties(self) -> dict[str, Any]:
        """Convert to a property dictionary for database storage.

        Uses python_to_db_value() for type conversion. Includes None
        explicitly so null-outs work.
        """
        return _model_to_properties(self, self.get_property_fields())

    @classmethod
    def from_properties(
        cls,
        props: dict[str, Any],
        *,
        vid: int | None = None,
        uid: str | None = None,
        session: UniSession | None = None,
    ) -> UniNode:
        """Create an instance from a property dictionary.

        Accepts _id (string->int vid) and _label from uni-db node dicts.
        Does not mutate the input dict.
        """
        data = dict(props)

        raw_id = data.pop("_id", None)
        if raw_id is not None and vid is None:
            vid = int(raw_id) if not isinstance(raw_id, int) else raw_id
        data.pop("_label", None)

        converted = _convert_db_values(data, cls)

        instance = cls.model_validate(converted)
        if vid is not None:
            instance._vid = vid
        if uid is not None:
            instance._uid = uid
        if session is not None:
            instance._session = session
        instance._is_new = vid is None
        instance._dirty = set()
        return instance

    def __repr__(self) -> str:
        vid_str = f"vid={self._vid}" if self._vid else "unsaved"
        return f"{self.__class__.__name__}({vid_str}, {super().__repr__()})"


class UniEdge(BaseModel, metaclass=UniModelMeta):
    """
    Base class for graph edge models with properties.

    Subclass this to define edge types with typed properties.
    Edges represent relationships between nodes.

    Attributes:
        __edge_type__: The edge type name.
        __from__: The source node type(s).
        __to__: The target node type(s).

    Private Attributes:
        _eid: The edge ID assigned by the database.
        _src_vid: The source vertex ID.
        _dst_vid: The destination vertex ID.
        _session: Reference to the owning session.

    Example:
        >>> class FriendshipEdge(UniEdge):
        ...     __edge_type__ = "FRIEND_OF"
        ...     __from__ = Person
        ...     __to__ = Person
        ...
        ...     since: date
        ...     strength: float = 1.0
    """

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        arbitrary_types_allowed=True,
        use_enum_values=True,
    )

    # Class-level configuration
    __edge_type__: ClassVar[str] = ""
    __from__: ClassVar[type[UniNode] | tuple[type[UniNode], ...] | None] = None
    __to__: ClassVar[type[UniNode] | tuple[type[UniNode], ...] | None] = None
    __relationships__: ClassVar[dict[str, RelationshipConfig]] = {}

    # Private attributes
    _eid: int | None = PrivateAttr(default=None)
    _src_vid: int | None = PrivateAttr(default=None)
    _dst_vid: int | None = PrivateAttr(default=None)
    _session: UniSession | None = PrivateAttr(default=None)
    _is_new: bool = PrivateAttr(default=True)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Set default edge type to class name if not specified
        if not cls.__edge_type__:
            cls.__edge_type__ = cls.__name__

    @property
    def eid(self) -> int | None:
        """The edge ID assigned by the database."""
        return self._eid

    @property
    def src_vid(self) -> int | None:
        """The source vertex ID."""
        return self._src_vid

    @property
    def dst_vid(self) -> int | None:
        """The destination vertex ID."""
        return self._dst_vid

    @property
    def is_persisted(self) -> bool:
        """Whether this edge has been saved to the database."""
        return self._eid is not None

    def _attach(
        self,
        session: UniSession,
        eid: int,
        src_vid: int,
        dst_vid: int,
    ) -> None:
        """Attach this edge to a session with its database IDs."""
        self._session = session
        self._eid = eid
        self._src_vid = src_vid
        self._dst_vid = dst_vid
        self._is_new = False

    @classmethod
    def get_from_labels(cls) -> list[str]:
        """Get the source label names."""
        if cls.__from__ is None:
            return []
        if isinstance(cls.__from__, tuple):
            return [n.__label__ for n in cls.__from__]
        return [cls.__from__.__label__]

    @classmethod
    def get_to_labels(cls) -> list[str]:
        """Get the target label names."""
        if cls.__to__ is None:
            return []
        if isinstance(cls.__to__, tuple):
            return [n.__label__ for n in cls.__to__]
        return [cls.__to__.__label__]

    @classmethod
    def get_property_fields(cls) -> dict[str, FieldInfo]:
        """Get all property fields."""
        return dict(cls.model_fields)

    def to_properties(self) -> dict[str, Any]:
        """Convert to a property dictionary for database storage."""
        return _model_to_properties(self, self.get_property_fields())

    @classmethod
    def from_properties(
        cls,
        props: dict[str, Any],
        *,
        eid: int | None = None,
        src_vid: int | None = None,
        dst_vid: int | None = None,
        session: UniSession | None = None,
    ) -> UniEdge:
        """Create an instance from a property dictionary.

        Accepts _id, _type, _src, _dst from uni-db edge dicts.
        Does not mutate the input dict.
        """
        data = dict(props)

        raw_id = data.pop("_id", None)
        if raw_id is not None and eid is None:
            eid = int(raw_id) if not isinstance(raw_id, int) else raw_id
        data.pop("_type", None)
        raw_src = data.pop("_src", None)
        if raw_src is not None and src_vid is None:
            src_vid = int(raw_src) if not isinstance(raw_src, int) else raw_src
        raw_dst = data.pop("_dst", None)
        if raw_dst is not None and dst_vid is None:
            dst_vid = int(raw_dst) if not isinstance(raw_dst, int) else raw_dst

        converted = _convert_db_values(data, cls)

        instance = cls.model_validate(converted)
        if eid is not None:
            instance._eid = eid
        if src_vid is not None:
            instance._src_vid = src_vid
        if dst_vid is not None:
            instance._dst_vid = dst_vid
        if session is not None:
            instance._session = session
        instance._is_new = eid is None
        return instance

    @classmethod
    def from_edge_result(
        cls,
        data: dict[str, Any],
        *,
        session: UniSession | None = None,
    ) -> UniEdge:
        """Create an instance from a uni-db edge result dict.

        Convenience method that handles _id, _type, _src, _dst keys.
        """
        return cls.from_properties(data, session=session)

    def __repr__(self) -> str:
        eid_str = f"eid={self._eid}" if self._eid else "unsaved"
        return f"{self.__class__.__name__}({eid_str}, {super().__repr__()})"
