# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for Query DSL builder."""

from unittest.mock import MagicMock

import pytest

from uni_pydantic import (
    FilterExpr,
    FilterOp,
    PropertyProxy,
    QueryBuilder,
    UniNode,
)
from uni_pydantic.exceptions import CypherInjectionError


class Person(UniNode):
    """Test model for queries."""

    name: str
    age: int | None = None
    email: str | None = None
    active: bool = True


class TestPropertyProxy:
    """Tests for PropertyProxy filter expressions."""

    def test_equality(self):
        """Test equality filter."""
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy == "Alice"
        assert isinstance(expr, FilterExpr)
        assert expr.property_name == "name"
        assert expr.op == FilterOp.EQ
        assert expr.value == "Alice"

    def test_inequality(self):
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy != "Bob"
        assert expr.op == FilterOp.NE

    def test_less_than(self):
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy < 30
        assert expr.op == FilterOp.LT

    def test_less_than_or_equal(self):
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy <= 30
        assert expr.op == FilterOp.LE

    def test_greater_than(self):
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy > 18
        assert expr.op == FilterOp.GT

    def test_greater_than_or_equal(self):
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy >= 18
        assert expr.op == FilterOp.GE

    def test_in_list(self):
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy.in_([25, 30, 35])
        assert expr.op == FilterOp.IN
        assert expr.value == [25, 30, 35]

    def test_not_in_list(self):
        proxy = PropertyProxy[int]("age", Person)
        expr = proxy.not_in([25, 30])
        assert expr.op == FilterOp.NOT_IN

    def test_like_pattern(self):
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy.like("^A.*")
        assert expr.op == FilterOp.LIKE

    def test_is_null(self):
        proxy = PropertyProxy[str]("email", Person)
        expr = proxy.is_null()
        assert expr.op == FilterOp.IS_NULL

    def test_is_not_null(self):
        proxy = PropertyProxy[str]("email", Person)
        expr = proxy.is_not_null()
        assert expr.op == FilterOp.IS_NOT_NULL

    def test_starts_with(self):
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy.starts_with("Al")
        assert expr.op == FilterOp.STARTS_WITH

    def test_ends_with(self):
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy.ends_with("ice")
        assert expr.op == FilterOp.ENDS_WITH

    def test_contains(self):
        proxy = PropertyProxy[str]("name", Person)
        expr = proxy.contains("lic")
        assert expr.op == FilterOp.CONTAINS


class TestFilterExpr:
    """Tests for FilterExpr Cypher generation."""

    def test_equality_cypher(self):
        expr = FilterExpr("name", FilterOp.EQ, "Alice")
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.name = $p1"
        assert params == {"p1": "Alice"}

    def test_comparison_cypher(self):
        expr = FilterExpr("age", FilterOp.GE, 18)
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.age >= $p1"
        assert params == {"p1": 18}

    def test_in_cypher(self):
        expr = FilterExpr("age", FilterOp.IN, [25, 30])
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.age IN $p1"
        assert params == {"p1": [25, 30]}

    def test_is_null_cypher(self):
        expr = FilterExpr("email", FilterOp.IS_NULL)
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.email IS NULL"
        assert params == {}

    def test_starts_with_cypher(self):
        expr = FilterExpr("name", FilterOp.STARTS_WITH, "Al")
        cypher, params = expr.to_cypher("n", "p1")
        assert cypher == "n.name STARTS WITH $p1"
        assert params == {"p1": "Al"}


class TestQueryBuilderImmutability:
    """Tests for QueryBuilder immutability."""

    def _make_builder(self) -> QueryBuilder[Person]:
        session = MagicMock()
        return QueryBuilder(session, Person)

    def test_filter_returns_new_builder(self):
        """Test that filter() returns a new builder, not self."""
        q1 = self._make_builder()
        expr = FilterExpr("name", FilterOp.EQ, "Alice")
        q2 = q1.filter(expr)
        assert q1 is not q2
        assert len(q1._filters) == 0
        assert len(q2._filters) == 1

    def test_filter_by_returns_new_builder(self):
        q1 = self._make_builder()
        q2 = q1.filter_by(name="Alice")
        assert q1 is not q2
        assert len(q1._filters) == 0
        assert len(q2._filters) == 1

    def test_limit_returns_new_builder(self):
        q1 = self._make_builder()
        q2 = q1.limit(10)
        assert q1 is not q2
        assert q1._limit is None
        assert q2._limit == 10

    def test_skip_returns_new_builder(self):
        q1 = self._make_builder()
        q2 = q1.skip(5)
        assert q1 is not q2
        assert q1._skip is None
        assert q2._skip == 5

    def test_order_by_returns_new_builder(self):
        q1 = self._make_builder()
        q2 = q1.order_by("name")
        assert q1 is not q2
        assert len(q1._order_by) == 0
        assert len(q2._order_by) == 1

    def test_distinct_returns_new_builder(self):
        q1 = self._make_builder()
        q2 = q1.distinct()
        assert q1 is not q2
        assert q1._distinct is False
        assert q2._distinct is True

    def test_timeout_returns_new_builder(self):
        q1 = self._make_builder()
        q2 = q1.timeout(10.0)
        assert q1 is not q2
        assert q1._timeout is None
        assert q2._timeout == 10.0

    def test_max_memory_returns_new_builder(self):
        q1 = self._make_builder()
        q2 = q1.max_memory(1024)
        assert q1 is not q2
        assert q1._max_memory is None
        assert q2._max_memory == 1024

    def test_chaining_preserves_immutability(self):
        """Test that chaining multiple methods preserves immutability."""
        q1 = self._make_builder()
        q2 = (
            q1.filter(FilterExpr("name", FilterOp.EQ, "Alice"))
            .order_by("age")
            .limit(10)
            .skip(5)
        )
        assert q1 is not q2
        assert len(q1._filters) == 0
        assert q1._limit is None
        assert len(q2._filters) == 1
        assert q2._limit == 10


class TestQueryBuilderCypherGeneration:
    """Unit tests for Cypher generation."""

    def _make_builder(self) -> QueryBuilder[Person]:
        session = MagicMock()
        return QueryBuilder(session, Person)

    def test_basic_match(self):
        q = self._make_builder()
        cypher, params = q._build_cypher()
        assert "MATCH (n:Person)" in cypher
        assert "properties(n) AS _props" in cypher
        assert "id(n) AS _vid" in cypher
        assert params == {}

    def test_filter_match(self):
        q = self._make_builder().filter(FilterExpr("name", FilterOp.EQ, "Alice"))
        cypher, params = q._build_cypher()
        assert "MATCH (n:Person)" in cypher
        assert "WHERE n.name = $p1" in cypher
        assert "properties(n) AS _props" in cypher
        assert params == {"p1": "Alice"}

    def test_limit_skip(self):
        q = self._make_builder().limit(10).skip(5)
        cypher, _ = q._build_cypher()
        assert "SKIP 5" in cypher
        assert "LIMIT 10" in cypher

    def test_order_by(self):
        q = self._make_builder().order_by("name", descending=True)
        cypher, _ = q._build_cypher()
        assert "ORDER BY n.name DESC" in cypher

    def test_distinct(self):
        q = self._make_builder().distinct()
        cypher, _ = q._build_cypher()
        assert "RETURN DISTINCT properties(n)" in cypher

    def test_vector_search_cypher(self):
        q = self._make_builder().vector_search("embedding", [1.0, 2.0], k=5)
        cypher, params = q._build_cypher()
        assert "uni.vector.query" in cypher
        assert "YIELD node, distance, score" in cypher
        assert "RETURN node AS n" in cypher
        assert params["query_vec"] == [1.0, 2.0]

    def test_vector_search_with_threshold(self):
        q = self._make_builder().vector_search(
            "embedding", [1.0, 2.0], k=5, threshold=0.5
        )
        cypher, _ = q._build_cypher()
        assert "distance <= 0.5" in cypher


class TestPropertyValidation:
    """Tests for property name validation."""

    def test_valid_property(self):
        from uni_pydantic.query import _validate_property

        # Should not raise
        _validate_property("name", Person)
        _validate_property("age", Person)

    def test_invalid_property_name_format(self):
        from uni_pydantic.query import _validate_property

        with pytest.raises(CypherInjectionError):
            _validate_property("n.name; DROP", Person)

    def test_property_not_on_model(self):
        from uni_pydantic.query import _validate_property

        with pytest.raises(CypherInjectionError):
            _validate_property("nonexistent", Person)

    def test_system_property_always_valid(self):
        from uni_pydantic.query import _validate_property

        # System properties should pass without model
        _validate_property("_id")
        _validate_property("_label")

    def test_filter_by_validates_properties(self):
        q = QueryBuilder(MagicMock(), Person)
        with pytest.raises(CypherInjectionError):
            q.filter_by(injection_field="value")


class TestQueryBuilderUnit:
    """Unit tests for QueryBuilder (no database)."""

    def test_filter_expr_creation(self):
        name_filter = PropertyProxy[str]("name", Person) == "Alice"
        age_filter = PropertyProxy[int]("age", Person) >= 18

        assert name_filter.property_name == "name"
        assert age_filter.property_name == "age"

    def test_multiple_filters(self):
        filters = [
            PropertyProxy[str]("name", Person) == "Alice",
            PropertyProxy[int]("age", Person) >= 18,
            PropertyProxy[str]("email", Person).is_not_null(),
        ]

        assert len(filters) == 3
        assert all(isinstance(f, FilterExpr) for f in filters)
