# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for type mapping utilities."""

from datetime import date, datetime, time, timedelta
from typing import Annotated

import pytest

from uni_pydantic import (
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
from uni_pydantic.exceptions import TypeMappingError


class TestVector:
    """Tests for Vector type."""

    def test_vector_subscript(self):
        """Test Vector[N] subscripting."""
        VecType = Vector[128]
        assert VecType.__dimensions__ == 128

    def test_vector_subscript_caching(self):
        """Test that Vector[N] returns cached type."""
        assert Vector[256] is Vector[256]

    def test_vector_invalid_dimensions(self):
        """Test Vector with invalid dimensions."""
        with pytest.raises(TypeError):
            Vector["not_an_int"]  # type: ignore

        with pytest.raises(TypeError):
            Vector[-1]

        with pytest.raises(TypeError):
            Vector[0]

    def test_vector_instance_creation(self):
        """Test creating Vector instances."""
        VecType = Vector[3]
        vec = VecType([1.0, 2.0, 3.0])
        assert vec.values == [1.0, 2.0, 3.0]
        assert len(vec) == 3

    def test_vector_dimension_validation(self):
        """Test Vector dimension validation."""
        VecType = Vector[3]
        with pytest.raises(ValueError):
            VecType([1.0, 2.0])  # Wrong dimension

    def test_vector_equality(self):
        """Test Vector equality."""
        VecType = Vector[3]
        vec1 = VecType([1.0, 2.0, 3.0])
        vec2 = VecType([1.0, 2.0, 3.0])
        vec3 = VecType([1.0, 2.0, 4.0])

        assert vec1 == vec2
        assert vec1 != vec3
        assert vec1 == [1.0, 2.0, 3.0]

    def test_vector_isinstance(self):
        """Test that Pydantic schema returns Vector instances."""
        from uni_pydantic import UniNode

        class Doc(UniNode):
            title: str
            embedding: Vector[3]

        doc = Doc(title="test", embedding=[1.0, 2.0, 3.0])
        assert isinstance(doc.embedding, Vector)
        assert doc.embedding.values == [1.0, 2.0, 3.0]

    def test_vector_from_vector_instance(self):
        """Test constructing model with Vector instance."""
        from uni_pydantic import UniNode

        class Doc(UniNode):
            title: str
            embedding: Vector[3]

        vec = Vector[3]([1.0, 2.0, 3.0])
        doc = Doc(title="test", embedding=vec)
        assert isinstance(doc.embedding, Vector)
        assert doc.embedding.values == [1.0, 2.0, 3.0]


class TestGetVectorDimensions:
    """Tests for get_vector_dimensions."""

    def test_vector_type(self):
        """Test extracting dimensions from Vector type."""
        assert get_vector_dimensions(Vector[128]) == 128
        assert get_vector_dimensions(Vector[1536]) == 1536

    def test_non_vector_type(self):
        """Test non-vector types return None."""
        assert get_vector_dimensions(str) is None
        assert get_vector_dimensions(int) is None
        assert get_vector_dimensions(list[float]) is None


class TestIsOptional:
    """Tests for is_optional."""

    def test_optional_types(self):
        """Test detecting Optional types."""
        is_opt, inner = is_optional(str | None)
        assert is_opt is True
        assert inner is str

        is_opt, inner = is_optional(int | None)
        assert is_opt is True
        assert inner is int

    def test_non_optional_types(self):
        """Test non-optional types."""
        is_opt, inner = is_optional(str)
        assert is_opt is False
        assert inner is str

        is_opt, inner = is_optional(int)
        assert is_opt is False
        assert inner is int

    def test_union_types(self):
        """Test union types that aren't Optional."""
        is_opt, inner = is_optional(str | int)
        assert is_opt is False


class TestIsListType:
    """Tests for is_list_type."""

    def test_list_types(self):
        """Test detecting list types."""
        is_lst, elem = is_list_type(list[str])
        assert is_lst is True
        assert elem is str

        is_lst, elem = is_list_type(list[int])
        assert is_lst is True
        assert elem is int

    def test_non_list_types(self):
        """Test non-list types."""
        is_lst, elem = is_list_type(str)
        assert is_lst is False
        assert elem is None


class TestUnwrapAnnotated:
    """Tests for unwrap_annotated."""

    def test_annotated_type(self):
        """Test unwrapping Annotated types."""
        base, metadata = unwrap_annotated(Annotated[str, "metadata"])
        assert base is str
        assert metadata == ("metadata",)

    def test_non_annotated_type(self):
        """Test non-Annotated types."""
        base, metadata = unwrap_annotated(str)
        assert base is str
        assert metadata == ()


class TestPythonTypeToUni:
    """Tests for python_type_to_uni."""

    def test_basic_types(self):
        """Test basic type mapping (canonical uni-db names)."""
        assert python_type_to_uni(str) == ("string", False)
        assert python_type_to_uni(int) == ("int64", False)
        assert python_type_to_uni(float) == ("float64", False)
        assert python_type_to_uni(bool) == ("bool", False)
        assert python_type_to_uni(bytes) == ("string", False)  # bytes→string

    def test_datetime_types(self):
        """Test datetime type mapping."""
        assert python_type_to_uni(datetime) == ("datetime", False)
        assert python_type_to_uni(date) == ("date", False)
        assert python_type_to_uni(time) == ("time", False)
        assert python_type_to_uni(timedelta) == ("duration", False)

    def test_optional_types(self):
        """Test optional type mapping."""
        assert python_type_to_uni(str | None) == ("string", True)
        assert python_type_to_uni(int | None) == ("int64", True)

    def test_list_types(self):
        """Test list type mapping."""
        assert python_type_to_uni(list[str]) == ("list:string", False)
        assert python_type_to_uni(list[int]) == ("list:int64", False)

    def test_dict_type(self):
        """Test dict type mapping."""
        assert python_type_to_uni(dict) == ("json", False)
        assert python_type_to_uni(dict[str, int]) == ("json", False)

    def test_vector_type(self):
        """Test vector type mapping."""
        assert python_type_to_uni(Vector[128]) == ("vector:128", False)
        assert python_type_to_uni(Vector[1536] | None) == ("vector:1536", True)

    def test_unsupported_type(self):
        """Test unsupported type raises error."""

        class CustomClass:
            pass

        with pytest.raises(TypeMappingError):
            python_type_to_uni(CustomClass)


class TestUniToPythonType:
    """Tests for uni_to_python_type."""

    def test_basic_types(self):
        """Test basic type reverse mapping."""
        assert uni_to_python_type("string") is str
        assert uni_to_python_type("int64") is int
        assert uni_to_python_type("float64") is float
        assert uni_to_python_type("bool") is bool

    def test_vector_type(self):
        """Test vector type reverse mapping."""
        assert uni_to_python_type("vector:128") is list

    def test_list_type(self):
        """Test list type reverse mapping."""
        assert uni_to_python_type("list:string") is list


class TestPythonToDbValue:
    """Tests for python_to_db_value conversion."""

    def test_datetime_passthrough(self):
        """Test datetime passes through to Rust layer."""
        dt = datetime(2020, 1, 1, 0, 0, 0)
        result = python_to_db_value(dt, datetime)
        assert isinstance(result, datetime)
        assert result == dt
        # db_to_python_value passes through datetime objects
        rt = db_to_python_value(result, datetime)
        assert rt == dt

    def test_date_passthrough(self):
        """Test date passes through to Rust layer."""
        d = date(2020, 1, 1)
        result = python_to_db_value(d, date)
        assert isinstance(result, date)
        assert result == d
        rt = db_to_python_value(result, date)
        assert rt == d

    def test_time_passthrough(self):
        """Test time passes through to Rust layer."""
        t = time(12, 30, 45, 123456)
        result = python_to_db_value(t, time)
        assert isinstance(result, time)
        assert result == t
        rt = db_to_python_value(result, time)
        assert rt == t

    def test_timedelta_passthrough(self):
        """Test timedelta passes through to Rust layer."""
        td = timedelta(days=1, hours=2, minutes=3)
        result = python_to_db_value(td, timedelta)
        assert isinstance(result, timedelta)
        assert result == td
        rt = db_to_python_value(result, timedelta)
        assert rt == td

    def test_none_passthrough(self):
        """Test None passes through."""
        assert python_to_db_value(None, datetime) is None
        assert db_to_python_value(None, datetime) is None

    def test_vector_to_list(self):
        """Test Vector → list[float]."""
        vec = Vector[3]([1.0, 2.0, 3.0])
        result = python_to_db_value(vec, Vector[3])
        assert result == [1.0, 2.0, 3.0]

    def test_string_passthrough(self):
        """Test string passes through."""
        assert python_to_db_value("hello", str) == "hello"
        assert db_to_python_value("hello", str) == "hello"

    def test_db_to_python_vector(self):
        """Test list[float] → Vector for vector type hints."""
        result = db_to_python_value([1.0, 2.0, 3.0], Vector[3])
        assert isinstance(result, Vector)
        assert result.values == [1.0, 2.0, 3.0]
