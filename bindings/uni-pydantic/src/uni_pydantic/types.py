# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Type mapping between Python/Pydantic types and Uni DataTypes."""

from __future__ import annotations

import types
from datetime import date, datetime, time, timedelta
from typing import (
    Annotated,
    Any,
    Generic,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

from .exceptions import TypeMappingError

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
DATETIME_TYPES: set[type] = {datetime, date, time, timedelta}


def python_to_db_value(value: Any, type_hint: Any) -> Any:
    """Convert a Python value to a database-compatible value.

    Passes datetime/date/time/timedelta through to the Rust layer which
    converts them to proper Value::Temporal types. Converts Vector to
    list[float] and passes through everything else.
    """
    if value is None:
        return None

    # Vector → list[float]
    if isinstance(value, Vector):
        return value.values

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

    # Handle struct dict from Arrow deserialization (e.g. datetime struct)
    if type_hint is datetime and isinstance(value, dict):
        nanos = value.get("nanos_since_epoch")
        if nanos is not None:
            return datetime.fromtimestamp(nanos / 1_000_000_000)
        return None

    # Vector fields: list[float] → Vector
    dims = get_vector_dimensions(type_hint)
    if dims is not None and isinstance(value, list):
        vec_cls = Vector[dims]
        return vec_cls(value)

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
    bytes: "string",  # uni-db maps bytes→String
    dict: "json",
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
        # Complex list types stored as JSON
        return "json", nullable

    # Direct type mapping
    if type_hint in TYPE_MAP:
        return TYPE_MAP[type_hint], nullable

    # Handle generic dict types
    origin = get_origin(type_hint)
    if origin is dict:
        return "json", nullable

    # Handle forward references (strings)
    if isinstance(type_hint, str):
        # This is a forward reference, can't resolve here
        raise TypeMappingError(
            type_hint,
            f"Cannot resolve forward reference {type_hint!r}. "
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
    }

    # Handle vector types
    if uni_type.startswith("vector:"):
        return list  # Vectors are stored as list[float]

    # Handle list types
    if uni_type.startswith("list:"):
        return list

    return _REVERSE_MAP.get(uni_type.lower(), str)
