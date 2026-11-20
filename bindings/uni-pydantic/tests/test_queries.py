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
from uni_pydantic.exceptions import CypherInjectionError, QueryError


class Person(UniNode):
    """Test model for queries."""

    name: str
    age: int | None = None
    email: str | None = None
    active: bool = True


class Doc(UniNode):
    """Test model with a user field literally named ``score`` (collision test)."""

    title: str
    score: float


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
        # Must return the properties()/id()/labels() triple so rows hydrate;
        # `node AS n` would silently yield zero rows (regression guard).
        assert "RETURN properties(node) AS _props" in cypher
        assert "labels(node) AS _labels, distance, score ORDER BY distance" in cypher
        assert "node AS n" not in cypher
        assert params["query_vec"] == [1.0, 2.0]

    def test_vector_search_with_threshold(self):
        q = self._make_builder().vector_search(
            "embedding", [1.0, 2.0], k=5, threshold=0.5
        )
        cypher, _ = q._build_cypher()
        assert "distance <= 0.5" in cypher

    def test_sparse_search_cypher_from_dict(self):
        q = self._make_builder().sparse_search("emb", {5: 2.0, 1: 1.0}, k=5)
        cypher, params = q._build_cypher()
        assert "uni.sparse.query" in cypher
        assert "YIELD node, score" in cypher
        assert (
            "RETURN properties(node) AS _props, id(node) AS _vid, "
            "labels(node) AS _labels, score ORDER BY score DESC" in cypher
        )
        assert "node AS n" not in cypher
        # dict canonicalized to sorted parallel arrays.
        assert params["sparse_q"] == {"indices": [1, 5], "values": [1.0, 2.0]}

    def test_sparse_search_cypher_from_pair(self):
        q = self._make_builder().sparse_search("emb", ([1, 5], [1.0, 2.0]), k=3)
        cypher, params = q._build_cypher()
        assert "uni.sparse.query('Person', 'emb', $sparse_q, 3)" in cypher
        assert params["sparse_q"] == {"indices": [1, 5], "values": [1.0, 2.0]}

    def test_sparse_search_with_threshold(self):
        q = self._make_builder().sparse_search("emb", {1: 1.0}, k=5, threshold=0.5)
        cypher, _ = q._build_cypher()
        # Sparse score is a dot product → threshold is a lower bound.
        assert "score >= 0.5" in cypher

    def test_vector_search_pre_filter_is_passed_to_the_procedure(self):
        q = self._make_builder().vector_search(
            "embedding", [1.0, 2.0], k=5, pre_filter="age >= 18"
        )
        cypher, params = q._build_cypher()
        assert (
            "uni.vector.query('Person', 'embedding', $query_vec, 5, $pre_filter)"
            in cypher
        )
        assert params["pre_filter"] == "age >= 18"

    def test_sparse_search_pre_filter_is_passed_to_the_procedure(self):
        q = self._make_builder().sparse_search(
            "emb", {1: 1.0}, k=5, pre_filter="age >= 18"
        )
        cypher, params = q._build_cypher()
        assert "uni.sparse.query('Person', 'emb', $sparse_q, 5, $pre_filter)" in cypher
        assert params["pre_filter"] == "age >= 18"

    def test_search_without_pre_filter_passes_no_filter_argument(self):
        cypher, params = (
            self._make_builder().sparse_search("emb", {1: 1.0}, k=5)._build_cypher()
        )
        assert "uni.sparse.query('Person', 'emb', $sparse_q, 5)" in cypher
        assert "pre_filter" not in params

        cypher, params = (
            self._make_builder().vector_search("embedding", [1.0], k=5)._build_cypher()
        )
        assert "uni.vector.query('Person', 'embedding', $query_vec, 5)" in cypher
        assert "pre_filter" not in params

    def test_vector_search_rejects_bad_property(self):
        with pytest.raises(CypherInjectionError):
            self._make_builder().vector_search("bad;name", [1.0])

    def test_sparse_search_rejects_bad_property(self):
        with pytest.raises(CypherInjectionError):
            self._make_builder().sparse_search("bad;name", {1: 1.0})


class TestHybridSearchCypher:
    """Unit tests for hybrid_search() Cypher generation."""

    def _make_builder(self) -> QueryBuilder[Person]:
        return QueryBuilder(MagicMock(), Person)

    def test_three_way_precomputed_rrf(self):
        q = self._make_builder().hybrid_search(
            vector=("embedding", [1.0, 2.0]),
            fts=("name", "hello world"),
            sparse=("emb", {5: 2.0, 1: 1.0}),
            method="rrf",
            k=10,
        )
        cypher, params = q._build_cypher()
        assert "CALL uni.search('Person'," in cypher
        assert "vector: 'embedding'" in cypher
        assert "fts: 'name'" in cypher
        assert "sparse: 'emb'" in cypher
        assert "YIELD node, score, vector_score, fts_score, sparse_score" in cypher
        assert "RETURN properties(node) AS _props" in cypher
        assert "node AS n" not in cypher
        assert "method: 'rrf'" in cypher
        assert "sparse_query: $sparse_q" in cypher
        assert "ORDER BY score DESC" in cypher
        assert params["qtext"] == "hello world"
        assert params["qvec"] == [1.0, 2.0]
        assert params["sparse_q"] == {"indices": [1, 5], "values": [1.0, 2.0]}

    def test_two_way_weighted_alpha(self):
        q = self._make_builder().hybrid_search(
            vector=("embedding", [1.0]),
            fts=("name", "q"),
            method="weighted",
            alpha=0.7,
        )
        cypher, _ = q._build_cypher()
        assert "method: 'weighted'" in cypher
        assert "alpha: 0.7" in cypher
        # No sparse arm: neither the properties key nor the option is emitted.
        assert "sparse: '" not in cypher
        assert "sparse_query" not in cypher

    def test_three_way_weights(self):
        q = self._make_builder().hybrid_search(
            vector=("embedding", [1.0]),
            fts=("name", "q"),
            sparse=("emb", {1: 1.0}),
            method="weighted",
            weights=[0.5, 0.3, 0.2],
        )
        cypher, _ = q._build_cypher()
        assert "weights: [0.5, 0.3, 0.2]" in cypher

    def test_dense_auto_embed(self):
        # Bare vector property + query_text ⇒ null query-vector positional.
        q = self._make_builder().hybrid_search(
            vector="embedding",
            query_text="find me",
        )
        cypher, params = q._build_cypher()
        assert "vector: 'embedding'" in cypher
        assert "$qtext, null, 10," in cypher
        assert params["qtext"] == "find me"
        assert "qvec" not in params

    def test_sparse_arm_requires_both(self):
        # sparse= present ⇒ both the map key AND options.sparse_query.
        q = self._make_builder().hybrid_search(sparse=("emb", {1: 1.0}))
        cypher, params = q._build_cypher()
        assert "sparse: 'emb'" in cypher
        assert "sparse_query: $sparse_q" in cypher
        assert params["sparse_q"] == {"indices": [1], "values": [1.0]}

    def test_filter_positional_bound(self):
        q = self._make_builder().hybrid_search(
            vector=("embedding", [1.0]),
            filter="node.active = true",
        )
        cypher, params = q._build_cypher()
        assert "$qtext, $qvec, 10, $filter," in cypher
        assert params["filter"] == "node.active = true"

    def test_no_sources_raises(self):
        with pytest.raises(QueryError):
            self._make_builder().hybrid_search(k=10)

    def test_bad_weights_length_raises(self):
        with pytest.raises(QueryError):
            self._make_builder().hybrid_search(
                vector=("embedding", [1.0]), weights=[0.5, 0.5]
            )

    def test_property_proxy_resolves(self):
        # PropertyProxy args resolve to bare property names.
        q = self._make_builder().hybrid_search(
            vector=(PropertyProxy("embedding", Person), [1.0]),
            fts=PropertyProxy("name", Person),
            query_text="q",
        )
        cypher, _ = q._build_cypher()
        assert "vector: 'embedding'" in cypher
        assert "fts: 'name'" in cypher

    def test_model_filter_chain_appends_where(self):
        # A model .filter() chain becomes a trailing WHERE over `node`.
        q = (
            self._make_builder()
            .hybrid_search(vector=("embedding", [1.0]))
            .filter(FilterExpr("age", FilterOp.GE, 18))
        )
        cypher, params = q._build_cypher()
        assert "WHERE node.age >= $p1" in cypher
        assert params["p1"] == 18

    def test_query_text_overrides_fts(self):
        q = self._make_builder().hybrid_search(
            fts=("name", "fts text"), query_text="override"
        )
        _, params = q._build_cypher()
        assert params["qtext"] == "override"

    def test_rrf_k_and_over_fetch_emitted(self):
        q = self._make_builder().hybrid_search(
            vector=("embedding", [1.0]), rrf_k=42, over_fetch=3.0
        )
        cypher, _ = q._build_cypher()
        assert "rrf_k: 42" in cypher
        assert "over_fetch: 3.0" in cypher

    def test_limit_emitted(self):
        q = self._make_builder().hybrid_search(vector=("embedding", [1.0])).limit(7)
        cypher, _ = q._build_cypher()
        assert "LIMIT 7" in cypher

    def test_weighted_three_sources_no_weights_ok(self):
        # Not an error: the engine falls back to equal thirds.
        q = self._make_builder().hybrid_search(
            vector=("embedding", [1.0]),
            fts=("name", "q"),
            sparse=("emb", {1: 1.0}),
            method="weighted",
        )
        cypher, _ = q._build_cypher()
        assert "method: 'weighted'" in cypher
        assert "weights:" not in cypher

    def test_injection_property_name_raises(self):
        with pytest.raises(CypherInjectionError):
            self._make_builder().hybrid_search(vector=("emb'} ) //", [1.0]))
        with pytest.raises(CypherInjectionError):
            self._make_builder().hybrid_search(fts=("bad name", "q"))
        with pytest.raises(CypherInjectionError):
            self._make_builder().hybrid_search(sparse=("bad;name", {1: 1.0}))

    def test_non_tuple_sparse_raises(self):
        with pytest.raises(QueryError):
            self._make_builder().hybrid_search(sparse="emb")

    def test_invalid_sparse_query_type_raises(self):
        with pytest.raises(TypeError):
            self._make_builder().hybrid_search(sparse=("emb", object()))

    def test_hybrid_search_returns_new_builder(self):
        q1 = self._make_builder()
        q2 = q1.hybrid_search(vector=("embedding", [1.0]))
        assert q1 is not q2
        assert q1._hybrid_search is None
        assert q2._hybrid_search is not None


def _search_session(rows: list[dict]) -> MagicMock:
    """A mock session that returns ``rows`` and hydrates via from_properties."""
    session = MagicMock()
    result = []
    for r in rows:
        m = MagicMock()
        m.to_dict.return_value = r
        result.append(m)
    session._db_session.query.return_value = result

    def _to_model(node_data, model):
        data = dict(node_data)
        vid = data.pop("_vid", None)
        data.pop("_label", None)
        return model.from_properties(data, vid=vid)

    session._result_to_model.side_effect = _to_model
    return session


class TestSearchScoreSurfacing:
    """Execution tests: search builders hydrate instances carrying .search_scores."""

    def test_hybrid_scores_surfaced(self):
        rows = [
            {
                "_props": {"name": "Alice"},
                "_vid": 1,
                "_labels": ["Person"],
                "score": 0.9,
                "vector_score": 0.8,
                "fts_score": 0.5,
                "sparse_score": 0.3,
            }
        ]
        results = (
            QueryBuilder(_search_session(rows), Person)
            .hybrid_search(
                vector=("embedding", [1.0]),
                fts=("name", "a"),
                sparse=("emb", {1: 1.0}),
            )
            .all()
        )
        assert len(results) == 1
        p = results[0]
        assert p.name == "Alice"
        assert p.search_scores is not None
        assert p.search_scores.score == 0.9
        assert p.search_scores.vector == 0.8
        assert p.search_scores.fts == 0.5
        assert p.search_scores.sparse == 0.3

    def test_vector_search_hydrates_with_scores(self):
        # Regression guard: the old `node AS n` RETURN would yield zero rows.
        rows = [
            {
                "_props": {"name": "Bob"},
                "_vid": 2,
                "_labels": ["Person"],
                "distance": 0.1,
                "score": 0.95,
            }
        ]
        results = (
            QueryBuilder(_search_session(rows), Person)
            .vector_search("embedding", [1.0], k=5)
            .all()
        )
        assert len(results) == 1
        assert results[0].name == "Bob"
        assert results[0].search_scores.score == 0.95
        assert results[0].search_scores.distance == 0.1

    def test_sparse_search_hydrates_with_scores(self):
        rows = [
            {
                "_props": {"name": "Cara"},
                "_vid": 3,
                "_labels": ["Person"],
                "score": 0.7,
            }
        ]
        results = (
            QueryBuilder(_search_session(rows), Person)
            .sparse_search("emb", {1: 1.0}, k=5)
            .all()
        )
        assert len(results) == 1
        assert results[0].search_scores.score == 0.7

    def test_score_field_no_collision(self):
        # A model with its own `score` field: model field intact, search score
        # reachable via .search_scores (the sidecar's reason to exist).
        rows = [
            {
                "_props": {"title": "Doc A", "score": 1.5},
                "_vid": 4,
                "_labels": ["Doc"],
                "score": 0.42,
                "vector_score": 0.4,
            }
        ]
        results = (
            QueryBuilder(_search_session(rows), Doc)
            .hybrid_search(vector=("embedding", [1.0]))
            .all()
        )
        assert len(results) == 1
        assert results[0].score == 1.5
        assert results[0].search_scores.score == 0.42

    def test_missing_arm_is_none(self):
        rows = [
            {
                "_props": {"name": "Dee"},
                "_vid": 5,
                "_labels": ["Person"],
                "score": 0.6,
            }
        ]
        results = (
            QueryBuilder(_search_session(rows), Person)
            .hybrid_search(vector=("embedding", [1.0]))
            .all()
        )
        assert results[0].search_scores.sparse is None
        assert results[0].search_scores.fts is None

    def test_non_search_query_has_no_scores(self):
        # A plain match query hydrates without a scores sidecar.
        rows = [{"_props": {"name": "Eve"}, "_vid": 6, "_labels": ["Person"]}]
        results = QueryBuilder(_search_session(rows), Person).all()
        assert len(results) == 1
        assert results[0].search_scores is None

    def test_empty_results(self):
        results = (
            QueryBuilder(_search_session([]), Person)
            .hybrid_search(vector=("embedding", [1.0]))
            .all()
        )
        assert results == []

    def test_first_carries_scores(self):
        rows = [
            {
                "_props": {"name": "Fay"},
                "_vid": 7,
                "_labels": ["Person"],
                "score": 0.5,
            }
        ]
        result = (
            QueryBuilder(_search_session(rows), Person)
            .hybrid_search(vector=("embedding", [1.0]))
            .first()
        )
        assert result is not None
        assert result.search_scores.score == 0.5


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
