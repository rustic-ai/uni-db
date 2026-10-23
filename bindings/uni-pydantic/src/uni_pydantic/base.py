# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Base classes for uni-pydantic models."""

from __future__ import annotations

import ast
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    ForwardRef,
    TypeVar,
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
    from pydantic._internal._model_construction import ModelMetaclass

    from .async_session import AsyncUniSession
    from .query import PropertyProxy
    from .session import UniSession
else:
    # pydantic's metaclass is private API; name it only through the public
    # ``BaseModel`` at runtime. Under TYPE_CHECKING the private import above
    # gives mypy the real base, so class attributes such as ``__name__`` and
    # ``__mro__`` resolve through ``type`` instead of our ``__getattr__``.
    ModelMetaclass = type(BaseModel)


def _resolve_type_hints(cls: type) -> dict[str, Any]:
    """Resolve a model's type hints, tolerating TYPE_CHECKING-only names.

    ``UniNode``/``UniEdge`` annotate private attrs like ``_session: UniSession``
    whose types are imported only under ``TYPE_CHECKING`` (to avoid a circular
    import). At runtime ``get_type_hints`` would raise ``NameError`` on those
    names and wipe out *all* hints — silently disabling per-field value
    conversion (``Vector``/``Btic`` → db value), so those values reach the Rust
    layer unconverted and are rejected. Supplying the TYPE_CHECKING names via
    ``localns`` lets resolution succeed; a still-unresolvable model degrades to
    ``{}`` exactly as before.
    """
    localns: dict[str, Any] = {}
    try:
        from .session import UniSession

        localns["UniSession"] = UniSession
    except Exception:
        pass
    try:
        from .async_session import AsyncUniSession

        localns["AsyncUniSession"] = AsyncUniSession
    except Exception:
        pass
    try:
        from .query import PropertyProxy

        localns["PropertyProxy"] = PropertyProxy
    except Exception:
        pass
    try:
        return get_type_hints(cls, localns=localns)
    except Exception:
        return {}


def _class_namespace_annotations(namespace: dict[str, Any]) -> dict[str, Any]:
    """Return the annotations declared in a class body, from a metaclass.

    Before Python 3.14 -- and on 3.14 when the module uses
    ``from __future__ import annotations`` -- the compiler stores an eager
    ``__annotations__`` dict in the class namespace. Under PEP 649/749 (3.14+)
    a class body without the future import instead carries only a lazy annotate
    function (``__annotate__``/``__annotate_func__``), so a plain
    ``namespace.get("__annotations__", {})`` returns ``{}`` and every annotation
    is invisible. That is evaluated here in ``FORWARDREF`` format, which never
    raises on names defined later in the module (they come back as
    ``ForwardRef`` objects) -- the same format pydantic uses for this step.
    """
    if "__annotations__" in namespace:
        annotations: dict[str, Any] = namespace["__annotations__"]
        return annotations
    if sys.version_info >= (3, 14):
        import annotationlib

        annotate = annotationlib.get_annotate_from_class_namespace(namespace)
        if annotate is not None:
            return dict(
                annotationlib.call_annotate_function(
                    annotate, annotationlib.Format.FORWARDREF
                )
            )
    return {}


def _exclude_lazy_annotations(
    namespace: dict[str, Any], excluded: frozenset[str]
) -> Callable[[type], None]:
    """Hide ``excluded`` names from a 3.14+ class's lazy annotate function.

    The metaclass removes each relationship's annotation so pydantic does not
    treat the relationship as a field. With an eager ``__annotations__`` dict
    that is a ``del``; with a PEP 649 annotate function there is no dict to
    edit, so the function is replaced by a wrapper that drops those names from
    whatever the original returns. Every reader -- pydantic's metaclass, which
    calls the annotate function from the namespace, and
    ``annotationlib.get_annotations``/``typing.get_type_hints`` on the finished
    class -- then sees the filtered mapping, and evaluation stays lazy, so a
    property annotation that names a class defined later in the module still
    resolves the same way it would without this metaclass.

    Returns a callback that records the created class as the owner passed to
    the original annotate function (used to resolve class-scoped names in
    ``ForwardRef`` evaluation).
    """
    import annotationlib

    original = annotationlib.get_annotate_from_class_namespace(namespace)
    if original is None:
        # Only called for a namespace whose relationships were read from its
        # lazy annotations, which therefore has an annotate function.
        raise TypeError("class namespace has no annotate function to filter")
    owner: list[type] = []

    def __annotate__(format: int, /) -> dict[str, Any]:
        if format == annotationlib.Format.VALUE_WITH_FAKE_GLOBALS:
            # Only meaningful for compiler-generated functions; the protocol
            # says to refuse it, and VALUE/FORWARDREF/STRING are served below.
            raise NotImplementedError(format)
        result = annotationlib.call_annotate_function(
            original,
            annotationlib.Format(format),
            owner=owner[0] if owner else None,
        )
        return {k: v for k, v in result.items() if k not in excluded}

    key = "__annotate__" if "__annotate__" in namespace else "__annotate_func__"
    namespace[key] = __annotate__
    return owner.append


def _string_annotation_shape(source: str) -> tuple[bool, Any]:
    """Shape of a relationship annotation known only as source text.

    Annotations arrive unevaluated as strings under
    ``from __future__ import annotations`` (``"list[Book]"``), when quoted
    (``"Bio | None"``), and -- via ``ForwardRef`` -- for names not yet defined
    under Python 3.14's lazy annotations. The target model usually is not
    defined yet either, so the text cannot be evaluated; its shape is read from
    the syntax instead. Recognized: ``list[X]``/``List[X]``, and optionality as
    ``X | None``, ``Optional[X]`` or ``Union[X, None]``, nested either way. The
    target is returned as source text (``"Book"``), which schema generation
    matches against registered models. Unparseable text is returned as-is,
    as a to-one target.
    """
    try:
        node = ast.parse(source.strip(), mode="eval").body
    except SyntaxError:
        return False, source

    def is_none(n: ast.expr) -> bool:
        return isinstance(n, ast.Constant) and n.value is None

    def name_of(n: ast.expr) -> str | None:
        if isinstance(n, ast.Name):
            return n.id
        if isinstance(n, ast.Attribute):  # typing.List, t.Optional, ...
            return n.attr
        return None

    def strip_optional(n: ast.expr) -> ast.expr:
        if isinstance(n, ast.BinOp) and isinstance(n.op, ast.BitOr):
            if is_none(n.right):
                return strip_optional(n.left)
            if is_none(n.left):
                return strip_optional(n.right)
        if isinstance(n, ast.Subscript):
            wrapper = name_of(n.value)
            if wrapper == "Optional":
                return strip_optional(n.slice)
            if wrapper == "Union" and isinstance(n.slice, ast.Tuple):
                members = [e for e in n.slice.elts if not is_none(e)]
                if len(members) == 1 and len(members) < len(n.slice.elts):
                    return strip_optional(members[0])
        return n

    def text_of(n: ast.expr) -> str:
        # A nested quoted name (`list["Book"]` under the future import) is a
        # string constant; its value, not its repr, is the target.
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            return n.value
        return ast.unparse(n)

    node = strip_optional(node)
    if isinstance(node, ast.Subscript) and name_of(node.value) in ("list", "List"):
        return True, text_of(strip_optional(node.slice))
    return False, text_of(node)


def _relationship_shape(annotation: Any) -> tuple[bool, Any]:
    """Return ``(is_list, target)`` for a relationship field's annotation.

    ``annotation`` is whatever the class namespace yields: an evaluated type
    (``list[Book]``), a string (quoted, or every annotation under
    ``from __future__ import annotations``), or a ``ForwardRef`` (Python 3.14's
    lazy annotations evaluate an undefined name to one -- ``Bio | None`` becomes
    ``ForwardRef('Bio | None')`` -- and ``typing.List["Book"]`` holds one on any
    version). Strings and forward references are read syntactically by
    `_string_annotation_shape`; an evaluated type is unwrapped with the typing
    helpers, and a forward reference left inside it is reduced the same way.
    """
    if isinstance(annotation, ForwardRef):
        annotation = annotation.__forward_arg__
    if isinstance(annotation, str):
        return _string_annotation_shape(annotation)
    is_opt, inner = is_optional(annotation)
    if is_opt:
        annotation = inner
    is_lst, elem_type = is_list_type(annotation)
    if is_lst:
        if isinstance(elem_type, (str, ForwardRef)):
            return True, _relationship_shape(elem_type)[1]
        return True, elem_type
    if isinstance(annotation, (str, ForwardRef)):
        return _relationship_shape(annotation)
    return False, annotation


def _model_to_properties(
    model_instance: BaseModel,
    property_fields: dict[str, Any],
) -> dict[str, Any]:
    """Convert a model's property fields to a database-ready dictionary.

    Shared implementation for both UniNode.to_properties() and UniEdge.to_properties().
    """
    hints = _resolve_type_hints(type(model_instance))

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
    hints = _resolve_type_hints(model_class)

    return {
        key: db_to_python_value(value, hints[key]) if key in hints else value
        for key, value in data.items()
    }


class UniModelMeta(ModelMetaclass):
    """Metaclass for UniNode and UniEdge that processes relationship fields."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type[Any]:
        # Process relationship markers in annotations. On Python 3.14+ (PEP 649)
        # the namespace may hold only a lazy annotate function rather than an
        # `__annotations__` dict; see `_class_namespace_annotations`.
        eager_annotations = "__annotations__" in namespace
        annotations = _class_namespace_annotations(namespace)
        relationships: dict[str, RelationshipConfig] = {}
        # Shape of each relationship, captured *before* its annotation is
        # deleted below. The descriptor loop further down used to re-read
        # `annotations[field_name]`, which by then no longer existed -- so
        # `is_list` was always False and `target_type` always None for every
        # relationship ever declared. A `list[Book]` field was indistinguishable
        # from a to-one, and the target model was simply unavailable.
        rel_shapes: dict[str, tuple[bool, Any]] = {}

        # Collect relationship markers and convert to descriptors
        for field_name, annotation in list(annotations.items()):
            default = namespace.get(field_name)
            if isinstance(default, _RelationshipMarker):
                # Determine if it's a list or single relationship
                rel_shapes[field_name] = _relationship_shape(annotation)

                # Store relationship config
                relationships[field_name] = default.config

                # Remove from namespace and annotations to exclude from Pydantic validation
                if field_name in namespace:
                    del namespace[field_name]
                # Also remove from annotations so Pydantic doesn't treat it as a
                # required field. `annotations` is the namespace's own dict only
                # on the eager path; the lazy path is filtered below.
                if eager_annotations:
                    del annotations[field_name]

        set_annotate_owner: Callable[[type], None] | None = None
        if relationships and not eager_annotations:
            set_annotate_owner = _exclude_lazy_annotations(
                namespace, frozenset(relationships)
            )

        # Create the class
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if set_annotate_owner is not None:
            set_annotate_owner(cls)

        # Store relationship configs on the class
        existing_rels = getattr(cls, "__relationships__", {})
        setattr(cls, "__relationships__", {**existing_rels, **relationships})

        # Add relationship descriptors
        for field_name, config in relationships.items():
            is_lst, target = rel_shapes.get(field_name, (False, None))

            descriptor: RelationshipDescriptor[Any] = RelationshipDescriptor(
                config=config,
                field_name=field_name,
                target_type=target,
                is_list=is_lst,
            )
            setattr(cls, field_name, descriptor)

        return cls

    def __getattr__(cls, name: str) -> PropertyProxy[Any] | Any:
        """Enable filter DSL: Person.age >= 18 returns a FilterExpr."""
        # Only trigger for UniNode/UniEdge subclasses, not the base classes themselves
        if name.startswith("_"):
            raise AttributeError(name)

        # Check if it's a model field
        model_fields = getattr(cls, "model_fields", None)
        if (
            model_fields
            and name in model_fields
            and issubclass(cls, (UniNode, UniEdge))
        ):
            from .query import PropertyProxy

            return PropertyProxy(name, cls)

        # Check if it's a relationship
        relationships = getattr(cls, "__relationships__", None)
        if relationships and name in relationships:
            return relationships[name]

        raise AttributeError(f"type object {cls.__name__!r} has no attribute {name!r}")


@dataclass(frozen=True)
class SearchScores:
    """Relevance scores attached to a hydrated search result.

    Populated on instances returned by the search builders
    (``vector_search`` / ``sparse_search`` / ``hybrid_search``) and reachable
    via :attr:`UniNode.search_scores`. Kept in a sidecar (rather than as a model
    field) so it never collides with a user field named ``score`` and survives
    hydration under ``extra="forbid"``.

    ``score`` is the primary score used for ordering (the fused score for
    hybrid; the per-source score for single-source search). The per-arm fields
    are populated only when that retrieval source contributed, ``None`` otherwise.
    """

    score: float
    vector: float | None = None
    fts: float | None = None
    sparse: float | None = None
    rerank: float | None = None
    distance: float | None = None


# `from_properties` returns the class it is called on (`Person.from_properties`
# gives a `Person`), which these typevars express on Python 3.10.
_NodeSelfT = TypeVar("_NodeSelfT", bound="UniNode")
_EdgeSelfT = TypeVar("_EdgeSelfT", bound="UniEdge")


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
    _session: UniSession | AsyncUniSession | None = PrivateAttr(default=None)
    _dirty: set[str] = PrivateAttr(default_factory=set)
    _is_new: bool = PrivateAttr(default=True)
    _scores: SearchScores | None = PrivateAttr(default=None)

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

    @property
    def search_scores(self) -> SearchScores | None:
        """Relevance scores, if this instance came from a search builder.

        ``None`` for instances from ordinary queries. See :class:`SearchScores`.
        """
        return self._scores

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
        self, session: UniSession | AsyncUniSession, vid: int, uid: str | None = None
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
        cls: type[_NodeSelfT],
        props: dict[str, Any],
        *,
        vid: int | None = None,
        uid: str | None = None,
        session: UniSession | AsyncUniSession | None = None,
    ) -> _NodeSelfT:
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
        vid_str = f"vid={self._vid}" if self._vid is not None else "unsaved"
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
    _session: UniSession | AsyncUniSession | None = PrivateAttr(default=None)
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
        session: UniSession | AsyncUniSession,
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
        cls: type[_EdgeSelfT],
        props: dict[str, Any],
        *,
        eid: int | None = None,
        src_vid: int | None = None,
        dst_vid: int | None = None,
        session: UniSession | AsyncUniSession | None = None,
    ) -> _EdgeSelfT:
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
        session: UniSession | AsyncUniSession | None = None,
    ) -> UniEdge:
        """Create an instance from a uni-db edge result dict.

        Convenience method that handles _id, _type, _src, _dst keys.
        """
        return cls.from_properties(data, session=session)

    def __repr__(self) -> str:
        eid_str = f"eid={self._eid}" if self._eid is not None else "unsaved"
        return f"{self.__class__.__name__}({eid_str}, {super().__repr__()})"
