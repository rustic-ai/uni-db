# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Type mapping between Python/Pydantic types and Uni DataTypes."""

from __future__ import annotations

import types
from datetime import date, datetime, time, timedelta
from typing import (
    Annotated,
    Any,
    ForwardRef,
    Generic,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

from .exceptions import TypeMappingError

# Import the Rust BTIC binding (optional — allows pydantic layer to work
# for type annotations even when the Rust extension is not installed).
try:
    from uni_db import Btic as _RustBtic

    _PyBtic: type[_RustBtic] | None = _RustBtic
except ImportError:
    _PyBtic = None

# Import the Rust SparseVector binding (optional, like Btic above).
try:
    from uni_db import SparseVector as _RustSparseVector

    _PySparseVector: type[_RustSparseVector] | None = _RustSparseVector
except ImportError:
    _PySparseVector = None

# Type variable for vector dimensions
N = TypeVar("N", bound=int)


class VectorMeta(type):
    """Metaclass for Vector type that supports subscripting with dimensions."""

    _cache: dict[int, type[Vector[Any]]] = {}

    def __getitem__(cls, dimensions: int) -> type[Vector[Any]]:
        if not isinstance(dimensions, int) or dimensions <= 0:
            raise TypeError(
                f"Vector dimensions must be a positive integer, got {dimensions}"
            )

        if dimensions in cls._cache:
            return cls._cache[dimensions]

        # Create a new class for this dimension
        new_cls = type(
            f"Vector[{dimensions}]",
            (Vector,),
            {"__dimensions__": dimensions, "__origin__": Vector},
        )
        cls._cache[dimensions] = new_cls
        return new_cls


class Vector(Generic[N], metaclass=VectorMeta):
    """
    A vector type with fixed dimensions for embeddings.

    Usage:
        embedding: Vector[1536]  # 1536-dimensional vector

    At runtime, vectors are stored as list[float].
    """

    __dimensions__: int = 0
    __origin__: type | None = None

    def __init__(self, values: list[float]) -> None:
        expected = self.__class__.__dimensions__
        if expected > 0 and len(values) != expected:
            raise ValueError(f"Vector expects {expected} dimensions, got {len(values)}")
        self._values = values

    @property
    def values(self) -> list[float]:
        return self._values

    def __repr__(self) -> str:
        dims = self.__class__.__dimensions__
        return (
            f"Vector[{dims}]({self._values[:3]}...)"
            if len(self._values) > 3
            else f"Vector[{dims}]({self._values})"
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Vector):
            return self._values == other._values
        if isinstance(other, list):
            return self._values == other
        return False

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self._values)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Make Vector compatible with Pydantic v2."""
        dimensions = getattr(source_type, "__dimensions__", 0)
        vec_cls = source_type if dimensions > 0 else cls

        def validate_vector(v: Any) -> Vector:  # type: ignore[type-arg]
            if isinstance(v, Vector):
                if dimensions > 0 and len(v) != dimensions:
                    raise ValueError(
                        f"Vector expects {dimensions} dimensions, got {len(v)}"
                    )
                return v
            if isinstance(v, list):
                if dimensions > 0 and len(v) != dimensions:
                    raise ValueError(
                        f"Vector expects {dimensions} dimensions, got {len(v)}"
                    )
                return vec_cls([float(x) for x in v])
            raise TypeError(f"Expected list or Vector, got {type(v)}")

        return core_schema.no_info_plain_validator_function(
            validate_vector,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: v.values if isinstance(v, Vector) else list(v),
                info_arg=False,
            ),
        )


def vector_class(dimensions: int) -> type[Vector[Any]]:
    """Return the ``Vector[dimensions]`` class for a dimension known only at runtime.

    ``Vector[1536]`` in an annotation is a literal subscript; with a variable
    (``Vector[dims]``) a type checker reads it as an invalid type expression.
    This calls the same ``VectorMeta.__getitem__`` (and its class cache).
    """
    return VectorMeta.__getitem__(Vector, dimensions)


def get_vector_dimensions(type_hint: Any) -> int | None:
    """Extract vector dimensions from a Vector[N] type hint."""
    if hasattr(type_hint, "__dimensions__"):
        dims: int = type_hint.__dimensions__
        return dims
    origin = get_origin(type_hint)
    if origin is Vector:
        args = get_args(type_hint)
        if args and isinstance(args[0], int):
            return args[0]
    return None


class SparseVectorMeta(type):
    """Metaclass for SparseVector enabling subscripting with a vocabulary size."""

    _cache: dict[int, type[SparseVector[Any]]] = {}

    def __getitem__(cls, dimensions: int) -> type[SparseVector[Any]]:
        if not isinstance(dimensions, int) or dimensions <= 0:
            raise TypeError(
                f"SparseVector dimensions must be a positive integer, got {dimensions}"
            )

        if dimensions in cls._cache:
            return cls._cache[dimensions]

        # A distinct attribute name (`__sparse_dimensions__`, not the dense
        # `__dimensions__`) keeps `get_vector_dimensions` from misclassifying a
        # sparse field as a dense vector.
        new_cls = type(
            f"SparseVector[{dimensions}]",
            (SparseVector,),
            {"__sparse_dimensions__": dimensions, "__origin__": SparseVector},
        )
        cls._cache[dimensions] = new_cls
        return new_cls


class SparseVector(Generic[N], metaclass=SparseVectorMeta):
    """
    A learned-sparse (SPLADE / BGE-M3) vector over a fixed-size vocabulary.

    Usage:
        terms: SparseVector[30522]  # SPLADE head over a 30522-term BERT vocab

    At runtime, holds parallel ``indices`` (term ids) and ``values`` (weights).
    Accepts a ``dict[int, float]`` of term id -> weight, a ``uni_db.SparseVector``,
    or an existing instance; ingestion serializes to the typed Rust binding when
    available, otherwise to an ``{"indices": [...], "values": [...]}`` mapping.
    """

    __sparse_dimensions__: int = 0
    __origin__: type | None = None

    def __init__(self, indices: list[int], values: list[float]) -> None:
        if len(indices) != len(values):
            raise ValueError(
                f"SparseVector indices/values length mismatch: "
                f"{len(indices)} vs {len(values)}"
            )
        self._indices = [int(i) for i in indices]
        self._values = [float(v) for v in values]

    @property
    def indices(self) -> list[int]:
        return self._indices

    @property
    def values(self) -> list[float]:
        return self._values

    @classmethod
    def from_dict(cls, mapping: dict[int, float]) -> SparseVector[Any]:
        """Build from a ``{term_id: weight}`` mapping (sorted by term id)."""
        items = sorted(mapping.items())
        return cls([k for k, _ in items], [v for _, v in items])

    def __repr__(self) -> str:
        dims = self.__class__.__sparse_dimensions__
        return f"SparseVector[{dims}](indices={self._indices}, values={self._values})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SparseVector):
            return self._indices == other._indices and self._values == other._values
        return False

    def __len__(self) -> int:
        return len(self._indices)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Make SparseVector compatible with Pydantic v2."""
        dimensions = getattr(source_type, "__sparse_dimensions__", 0)
        sv_cls = source_type if dimensions > 0 else cls

        def validate_sparse(v: Any) -> SparseVector:  # type: ignore[type-arg]
            if isinstance(v, SparseVector):
                return v
            if _PySparseVector is not None and isinstance(v, _PySparseVector):
                return sv_cls(list(v.indices), list(v.values))
            if isinstance(v, dict):
                return sv_cls.from_dict(v)
            if isinstance(v, (tuple, list)) and len(v) == 2:
                return sv_cls(list(v[0]), list(v[1]))
            raise TypeError(
                f"Expected SparseVector, dict, or (indices, values), got {type(v)}"
            )

        def serialize_sparse(v: SparseVector) -> Any:  # type: ignore[type-arg]
            if _PySparseVector is not None:
                return _PySparseVector(v.indices, v.values)
            return {"indices": v.indices, "values": v.values}

        return core_schema.no_info_plain_validator_function(
            validate_sparse,
            serialization=core_schema.plain_serializer_function_ser_schema(
                serialize_sparse,
                info_arg=False,
            ),
        )


def get_sparse_vector_dimensions(type_hint: Any) -> int | None:
    """Extract the vocabulary size from a SparseVector[N] type hint."""
    if hasattr(type_hint, "__sparse_dimensions__"):
        dims: int = type_hint.__sparse_dimensions__
        return dims
    origin = get_origin(type_hint)
    if origin is SparseVector:
        args = get_args(type_hint)
        if args and isinstance(args[0], int):
            return args[0]
    return None


class Btic:
    """A BTIC temporal interval value for Uni graph database.

    Construct from an ISO 8601-inspired string literal::

        Btic("1985")
        Btic("1985-03/2024-06")
        Btic("~1985")           # approximate certainty
        Btic("2020-03/")        # ongoing (unbounded hi)

    Use as a Pydantic model field type::

        class Event(UniNode):
            when: Btic
    """

    def __init__(self, value: str | object) -> None:
        if _PyBtic is None:
            raise ImportError("uni_db is required for Btic type")
        if isinstance(value, str):
            self._inner = _PyBtic(value)
        elif _PyBtic is not None and isinstance(value, _PyBtic):
            self._inner = value
        elif isinstance(value, Btic):
            self._inner = value._inner
        else:
            raise TypeError(f"Expected str or Btic, got {type(value)}")

    @property
    def lo(self) -> int:
        """Lower bound in milliseconds since epoch."""
        return self._inner.lo

    @property
    def hi(self) -> int:
        """Upper bound in milliseconds since epoch."""
        return self._inner.hi

    @property
    def meta(self) -> int:
        """Raw 64-bit metadata word."""
        return self._inner.meta

    @property
    def lo_granularity(self) -> str:
        """Lower bound granularity name."""
        return self._inner.lo_granularity

    @property
    def hi_granularity(self) -> str:
        """Upper bound granularity name."""
        return self._inner.hi_granularity

    @property
    def lo_certainty(self) -> str:
        """Lower bound certainty name."""
        return self._inner.lo_certainty

    @property
    def hi_certainty(self) -> str:
        """Upper bound certainty name."""
        return self._inner.hi_certainty

    @property
    def duration_ms(self) -> int | None:
        """Duration in milliseconds, or None if unbounded."""
        return self._inner.duration_ms

    @property
    def is_instant(self) -> bool:
        """True if the interval is exactly 1 millisecond wide."""
        return self._inner.is_instant

    @property
    def is_unbounded(self) -> bool:
        """True if either bound is infinite."""
        return self._inner.is_unbounded

    @property
    def is_finite(self) -> bool:
        """True if both bounds are finite."""
        return self._inner.is_finite

    def __repr__(self) -> str:
        return f'Btic("{self._inner}")'

    def __str__(self) -> str:
        return str(self._inner)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Btic):
            return self._inner == other._inner
        return False

    def __hash__(self) -> int:
        return hash(self._inner)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Make Btic compatible with Pydantic v2."""

        def validate_btic(v: Any) -> Btic:
            if isinstance(v, Btic):
                return v
            if isinstance(v, str):
                return Btic(v)
            if _PyBtic is not None and isinstance(v, _PyBtic):
                return Btic(v)
            raise TypeError(f"Expected str or Btic, got {type(v)}")

        return core_schema.no_info_plain_validator_function(
            validate_btic,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: str(v._inner) if isinstance(v, Btic) else str(v),
                info_arg=False,
            ),
        )


def is_optional(type_hint: Any) -> tuple[bool, Any]:
    """
    Check if a type hint is Optional (T | None).

    Returns:
        Tuple of (is_optional, inner_type)
    """
    origin = get_origin(type_hint)

    # Handle Union types (including T | None which is Union[T, None])
    if origin is Union:
        args = get_args(type_hint)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1 and type(None) in args:
            return True, non_none_args[0]

    # Python 3.10+ uses types.UnionType for X | Y syntax
    if isinstance(type_hint, types.UnionType):
        args = get_args(type_hint)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1 and type(None) in args:
            return True, non_none_args[0]

    return False, type_hint


def is_list_type(type_hint: Any) -> tuple[bool, Any | None]:
    """
    Check if a type hint is a list type.

    Returns:
        Tuple of (is_list, element_type)
    """
    origin = get_origin(type_hint)
    if origin is list:
        args = get_args(type_hint)
        return True, args[0] if args else Any
    return False, None


def unwrap_annotated(type_hint: Any) -> tuple[Any, tuple[Any, ...]]:
    """
    Unwrap an Annotated type.

    Returns:
        Tuple of (base_type, metadata_tuple)
    """
    origin = get_origin(type_hint)
    if origin is Annotated:
        args = get_args(type_hint)
        return args[0], args[1:]
    return type_hint, ()


# Datetime types that need special round-trip handling
DATETIME_TYPES: set[type] = {datetime, date, time, timedelta, Btic}


def python_to_db_value(value: Any, type_hint: Any) -> Any:
    """Convert a Python value to a database-compatible value.

    Passes datetime/date/time/timedelta through to the Rust layer which
    converts them to proper Value::Temporal types. Converts Vector to
    list[float] and passes through everything else.
    """
    if value is None:
        return None

    # list[Vector[N]] → list[list[float]] (multi-vector / ColBERT)
    if isinstance(value, list) and value and isinstance(value[0], Vector):
        return [v.values if isinstance(v, Vector) else v for v in value]

    # Vector → list[float]
    if isinstance(value, Vector):
        return value.values

    # Btic → unwrap to the Rust PyBtic for py_object_to_value
    if isinstance(value, Btic):
        return value._inner

    # datetime/date/time/timedelta pass through — the Rust py_object_to_value
    # handles conversion to Value::Temporal with proper type information.
    return value


def db_to_python_value(value: Any, type_hint: Any) -> Any:
    """Convert a database value back to a Python value.

    The Rust layer now returns proper Python datetime/date/time objects
    via Value::Temporal, so in most cases values pass through directly.
    """
    if value is None:
        return None

    # Unwrap Optional
    _, inner = is_optional(type_hint)
    if inner is not type_hint:
        type_hint = inner

    # Unwrap Annotated
    type_hint, _ = unwrap_annotated(type_hint)

    # If value is already the right Python type, pass through
    if type_hint is datetime and isinstance(value, datetime):
        return value
    if type_hint is date and isinstance(value, date):
        return value
    if type_hint is time and isinstance(value, time):
        return value
    if type_hint is timedelta and isinstance(value, timedelta):
        return value

    # Btic — wrap Rust PyBtic in the pydantic Btic wrapper
    if type_hint is Btic and _PyBtic is not None and isinstance(value, _PyBtic):
        return Btic(value)

    # Handle struct dict from Arrow deserialization (e.g. datetime struct)
    if type_hint is datetime and isinstance(value, dict):
        nanos = value.get("nanos_since_epoch")
        if nanos is not None:
            return datetime.fromtimestamp(nanos / 1_000_000_000)
        return None

    # list[Vector[N]] fields: list[list[float]] → list[Vector[N]] (multi-vector)
    is_lst, elem_type = is_list_type(type_hint)
    if is_lst:
        elem_dims = get_vector_dimensions(elem_type)
        if elem_dims is not None and isinstance(value, list):
            elem_cls = vector_class(elem_dims)
            return [elem_cls(v) if isinstance(v, list) else v for v in value]

    # Vector fields: list[float] → Vector
    dims = get_vector_dimensions(type_hint)
    if dims is not None and isinstance(value, list):
        return vector_class(dims)(value)

    return value


# Mapping from Python types to Uni DataType strings
TYPE_MAP: dict[type, str] = {
    str: "string",
    int: "int64",
    float: "float64",
    bool: "bool",
    datetime: "datetime",
    date: "date",
    time: "time",
    timedelta: "duration",
    bytes: "bytes",  # raw binary, stored as DataType::Bytes (LargeBinary)
    dict: "json",
    Btic: "btic",
}


def python_type_to_uni(type_hint: Any, *, nullable: bool = False) -> tuple[str, bool]:
    """
    Convert a Python type hint to a Uni DataType string.

    Args:
        type_hint: The Python type hint to convert.
        nullable: Whether the field is explicitly nullable.

    Returns:
        Tuple of (uni_data_type, is_nullable)

    Raises:
        TypeMappingError: If the type cannot be mapped.
    """
    # Unwrap Annotated if present
    type_hint, _ = unwrap_annotated(type_hint)

    # Check for optional (T | None)
    is_opt, inner_type = is_optional(type_hint)
    if is_opt:
        uni_type, _ = python_type_to_uni(inner_type)
        return uni_type, True

    # Check for SparseVector types (before dense Vector: distinct marker attr).
    sparse_dims = get_sparse_vector_dimensions(type_hint)
    if sparse_dims is not None:
        return f"sparse_vector:{sparse_dims}", nullable

    # Check for Vector types
    dims = get_vector_dimensions(type_hint)
    if dims is not None:
        return f"vector:{dims}", nullable

    # Check for list types
    is_lst, elem_type = is_list_type(type_hint)
    if is_lst:
        if elem_type in (str, int, float, bool):
            # Simple list types
            elem_uni = TYPE_MAP.get(elem_type, "string")
            return f"list:{elem_uni}", nullable
        # list[Vector[N]] (multi-vector / ColBERT) -> list:vector:N
        elem_dims = get_vector_dimensions(elem_type)
        if elem_dims is not None:
            return f"list:vector:{elem_dims}", nullable
        # Complex list types stored as JSON
        return "json", nullable

    # Direct type mapping
    if type_hint in TYPE_MAP:
        return TYPE_MAP[type_hint], nullable

    # Handle generic dict types -> typed MAP<STRING, V> when the key is `str` and the
    # value type maps to a concrete Uni type; otherwise schemaless JSON. Keys must be
    # `str` because the storage value model (Value::Map) is string-keyed.
    origin = get_origin(type_hint)
    if origin is dict:
        args = get_args(type_hint)
        if len(args) == 2 and args[0] is str:
            try:
                val_uni, _ = python_type_to_uni(args[1])
            except TypeMappingError:
                val_uni = "json"
            # Recurse handles nested values: dict[str, list[int]] -> map:string:list:int64,
            # dict[str, Vector[N]] -> map:string:vector:N, dict[str, dict[str,int]] -> nested.
            if val_uni != "json":
                return f"map:string:{val_uni}", nullable
        return "json", nullable

    # Handle forward references: a quoted annotation (a plain string) or a
    # ``ForwardRef`` -- which is what pydantic leaves on an unresolved field, and
    # what Python 3.14's lazy annotations (PEP 649) produce for a name that is
    # not yet defined. Both carry the same source text; neither can be resolved
    # here, so report the reference itself rather than an opaque repr.
    if isinstance(type_hint, (str, ForwardRef)):
        ref = type_hint if isinstance(type_hint, str) else type_hint.__forward_arg__
        raise TypeMappingError(
            type_hint,
            f"Cannot resolve forward reference {ref!r}. "
            "Ensure the referenced class is defined before schema sync.",
        )

    raise TypeMappingError(type_hint)


def uni_to_python_type(uni_type: str) -> type:
    """
    Convert a Uni DataType string to a Python type.

    Args:
        uni_type: The Uni data type string.

    Returns:
        The corresponding Python type.
    """
    # Reverse mapping — manually constructed to avoid bytes overwriting str for "string"
    _REVERSE_MAP: dict[str, type] = {
        "string": str,
        "int64": int,
        "float64": float,
        "bool": bool,
        "datetime": datetime,
        "date": date,
        "time": time,
        "duration": timedelta,
        "json": dict,
        "btic": Btic,
        "bytes": bytes,
    }

    # Handle vector types
    if uni_type.startswith("vector:"):
        return list  # Vectors are stored as list[float]

    # Handle list types
    if uni_type.startswith("list:"):
        return list

    return _REVERSE_MAP.get(uni_type.lower(), str)
