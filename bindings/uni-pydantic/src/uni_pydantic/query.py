# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Query DSL builder for type-safe graph queries."""

from __future__ import annotations

import copy
import re
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    cast,
)

from pydantic import ValidationError

from .base import SearchScores, UniNode
from .exceptions import CypherInjectionError, QueryError
from .fields import RelationshipDescriptor

if TYPE_CHECKING:
    from .session import UniSession

NodeT = TypeVar("NodeT", bound=UniNode)
T = TypeVar("T")


def _edge_pattern(edge_type: str, direction: str) -> str:
    """Build a Cypher edge pattern from an edge type and direction."""
    if direction == "outgoing":
        return f"-[:{edge_type}]->"
    elif direction == "incoming":
        return f"<-[:{edge_type}]-"
    else:  # both
        return f"-[:{edge_type}]-"


def _node_return_clause(var: str, *, distinct: bool = False) -> str:
    """Build a RETURN clause that returns node properties, id, and labels.

    uni-db limitation: ``RETURN n`` with a WHERE clause returns None.
    Use ``properties(n)`` + ``id(n)`` + ``labels(n)`` instead.
    """
    prefix = "DISTINCT " if distinct else ""
    return (
        f"RETURN {prefix}properties({var}) AS _props, "
        f"id({var}) AS _vid, labels({var}) AS _labels"
    )


def _opt_float(value: Any) -> float | None:
    """Coerce an optional numeric column to ``float | None``."""
    return float(value) if value is not None else None


def _warn_unhydratable(
    model: type[Any],
    vid: int | None,
    exc: ValidationError,
) -> None:
    """Report a stored row that does not satisfy its model, then skip it.

    Skipping is the right behaviour -- one unmappable row should not fail a
    whole query -- but it used to happen in complete silence, so a result set
    that quietly lost rows was indistinguishable from one that matched fewer.
    Naming the label and vid makes the offending row findable.

    Only a genuine :class:`ValidationError` reaches here. Anything else raised
    during hydration is a defect in the mapping layer, not a property of the
    data, and is deliberately left to propagate.
    """
    label = getattr(model, "__label__", model.__name__)
    where = f"vid={vid}" if vid is not None else "no vid"
    warnings.warn(
        f"skipping {label} row ({where}): stored properties do not satisfy "
        f"{model.__name__}: {exc}",
        UserWarning,
        stacklevel=3,
    )


def _row_to_node_dict(row: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a properties()/id()/labels() result row into a node-like dict."""
    props = row.get("_props")
    if props is None:
        return None
    data = dict(props)
    vid = row.get("_vid")
    if vid is not None:
        data["_vid"] = vid
    labels = row.get("_labels")
    if labels:
        data["_label"] = labels[0] if isinstance(labels, list) else labels
    return data


# Valid property name pattern (alphanumeric + underscore, must start with letter/underscore)
_VALID_PROPERTY_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Known system properties that are always valid
_SYSTEM_PROPERTIES = {"_id", "_label", "_type", "_src", "_dst"}


def _validate_property(name: str, model: type[UniNode] | None = None) -> None:
    """Validate a property name to prevent Cypher injection.

    Raises CypherInjectionError for invalid names.
    """
    if name in _SYSTEM_PROPERTIES:
        return
    if not _VALID_PROPERTY_RE.match(name):
        raise CypherInjectionError(
            name, f"Invalid property name {name!r}: must be alphanumeric/underscore"
        )
    if model is not None and name not in model.model_fields:
        raise CypherInjectionError(
            name,
            f"Property {name!r} is not defined on model {model.__name__!r}",
        )


class FilterOp(Enum):
    """Filter operation types."""

    EQ = "="
    NE = "<>"
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "=~"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    STARTS_WITH = "STARTS WITH"
    ENDS_WITH = "ENDS WITH"
    CONTAINS = "CONTAINS"


@dataclass
class FilterExpr:
    """A filter expression for a query."""

    property_name: str
    op: FilterOp
    value: Any = None

    def to_cypher(self, node_var: str, param_name: str) -> tuple[str, dict[str, Any]]:
        """Convert to Cypher WHERE clause fragment."""
        prop = f"{node_var}.{self.property_name}"

        if self.op == FilterOp.IS_NULL:
            return f"{prop} IS NULL", {}
        elif self.op == FilterOp.IS_NOT_NULL:
            return f"{prop} IS NOT NULL", {}
        elif self.op == FilterOp.IN:
            return f"{prop} IN ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.NOT_IN:
            return f"NOT {prop} IN ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.LIKE:
            return f"{prop} =~ ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.STARTS_WITH:
            return f"{prop} STARTS WITH ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.ENDS_WITH:
            return f"{prop} ENDS WITH ${param_name}", {param_name: self.value}
        elif self.op == FilterOp.CONTAINS:
            return f"{prop} CONTAINS ${param_name}", {param_name: self.value}
        else:
            return f"{prop} {self.op.value} ${param_name}", {param_name: self.value}


class PropertyProxy(Generic[T]):
    """
    Proxy for model properties that enables filter expressions.

    Used in query builder to create type-safe filter conditions.

    Example:
        >>> query.filter(Person.age >= 18)
        >>> query.filter(Person.name.starts_with("A"))
    """

    def __init__(self, property_name: str, model: type[UniNode]) -> None:
        self._property_name = property_name
        self._model = model

    def __eq__(self, other: Any) -> FilterExpr:  # type: ignore[override]
        return FilterExpr(self._property_name, FilterOp.EQ, other)

    def __ne__(self, other: Any) -> FilterExpr:  # type: ignore[override]
        return FilterExpr(self._property_name, FilterOp.NE, other)

    def __lt__(self, other: Any) -> FilterExpr:
        return FilterExpr(self._property_name, FilterOp.LT, other)

    def __le__(self, other: Any) -> FilterExpr:
        return FilterExpr(self._property_name, FilterOp.LE, other)

    def __gt__(self, other: Any) -> FilterExpr:
        return FilterExpr(self._property_name, FilterOp.GT, other)

    def __ge__(self, other: Any) -> FilterExpr:
        return FilterExpr(self._property_name, FilterOp.GE, other)

    def in_(self, values: Sequence[T]) -> FilterExpr:
        """Check if value is in a list."""
        return FilterExpr(self._property_name, FilterOp.IN, list(values))

    def not_in(self, values: Sequence[T]) -> FilterExpr:
        """Check if value is not in a list."""
        return FilterExpr(self._property_name, FilterOp.NOT_IN, list(values))

    def like(self, pattern: str) -> FilterExpr:
        """Match a regex pattern against the whole value (Cypher ``=~``).

        The pattern must match the entire string, as in Neo4j: use
        ``".*foo.*"`` to find ``foo`` anywhere.
        """
        return FilterExpr(self._property_name, FilterOp.LIKE, pattern)

    def is_null(self) -> FilterExpr:
        """Check if value is null."""
        return FilterExpr(self._property_name, FilterOp.IS_NULL)

    def is_not_null(self) -> FilterExpr:
        """Check if value is not null."""
        return FilterExpr(self._property_name, FilterOp.IS_NOT_NULL)

    def starts_with(self, prefix: str) -> FilterExpr:
        """Check if string starts with prefix."""
        return FilterExpr(self._property_name, FilterOp.STARTS_WITH, prefix)

    def ends_with(self, suffix: str) -> FilterExpr:
        """Check if string ends with suffix."""
        return FilterExpr(self._property_name, FilterOp.ENDS_WITH, suffix)

    def contains(self, substring: str) -> FilterExpr:
        """Check if string contains substring."""
        return FilterExpr(self._property_name, FilterOp.CONTAINS, substring)


class ModelProxy(Generic[NodeT]):
    """
    Proxy for model classes that provides property proxies.

    Enables type-safe property access in query filters.

    Example:
        >>> Person.name  # Returns PropertyProxy for 'name'
        >>> query.filter(Person.age >= 18)
    """

    def __init__(self, model: type[NodeT]) -> None:
        self._model = model

    def __getattr__(self, name: str) -> PropertyProxy[Any]:
        if name.startswith("_"):
            raise AttributeError(name)
        return PropertyProxy(name, self._model)


@dataclass
class OrderByClause:
    """An ORDER BY clause."""

    property_name: str
    descending: bool = False


@dataclass
class TraversalStep:
    """A relationship traversal step."""

    edge_type: str
    direction: Literal["outgoing", "incoming", "both"]
    target_label: str | None = None


@dataclass
class VectorSearchConfig:
    """Configuration for vector similarity search."""

    property_name: str
    query_vector: list[float]
    k: int
    threshold: float | None = None
    pre_filter: str | None = None


@dataclass
class SparseSearchConfig:
    """Configuration for learned-sparse (SPLADE) similarity search."""

    property_name: str
    query_indices: list[int]
    query_values: list[float]
    k: int
    threshold: float | None = None
    pre_filter: str | None = None


@dataclass
class HybridSearchConfig:
    """Configuration for three-way fused hybrid search (``uni.search``).

    Each ``*_property`` is ``None`` when that retrieval arm is off. ``query_text``
    is the shared string that drives both FTS matching and dense auto-embed;
    ``query_vector`` holds a precomputed dense vector (``None`` ⇒ auto-embed from
    ``query_text``). ``sparse_query`` is the coerced ``(indices, values)`` pair
    (``None`` ⇒ sparse arm off).
    """

    query_text: str
    k: int
    vector_property: str | None = None
    fts_property: str | None = None
    sparse_property: str | None = None
    query_vector: list[float] | None = None
    sparse_query: tuple[list[int], list[float]] | None = None
    method: str = "rrf"
    alpha: float | None = None
    weights: list[float] | None = None
    rrf_k: int | None = None
    over_fetch: float | None = None
    filter: str | None = None


def _coerce_sparse_query(query: Any) -> tuple[list[int], list[float]]:
    """Normalize a sparse query into parallel ``(indices, values)`` lists.

    Accepts a ``SparseVector`` (OGM or Rust binding), a ``dict[int, float]``
    of term id -> weight, or an ``(indices, values)`` pair.
    """
    indices = getattr(query, "indices", None)
    values = getattr(query, "values", None)
    if indices is not None and values is not None:
        return [int(i) for i in indices], [float(v) for v in values]
    if isinstance(query, dict):
        items = sorted(query.items())
        return [int(k) for k, _ in items], [float(v) for _, v in items]
    if isinstance(query, (tuple, list)) and len(query) == 2:
        return [int(i) for i in query[0]], [float(v) for v in query[1]]
    raise TypeError(
        "sparse query must be a SparseVector, dict[int, float], or (indices, values)"
    )


SelfT = TypeVar("SelfT", bound="_QueryBuilderBase[Any]")


class _QueryBuilderBase(Generic[NodeT]):
    """Shared state, cloning, and Cypher-building logic for query builders.

    Both ``QueryBuilder`` (sync) and ``AsyncQueryBuilder`` (async) inherit
    from this class so that all Cypher construction is defined once.
    """

    _session: Any  # UniSession or AsyncUniSession
    _model: type[NodeT]
    _filters: list[FilterExpr]
    _order_by: list[OrderByClause]
    _limit: int | None
    _skip: int | None
    _distinct: bool
    _traversals: list[TraversalStep]
    _eager_load: list[str]
    _vector_search: VectorSearchConfig | None
    _sparse_search: SparseSearchConfig | None
    _hybrid_search: HybridSearchConfig | None
    _timeout: float | None
    _max_memory: int | None
    _param_counter: int

    def _init_state(self, session: Any, model: type[NodeT]) -> None:
        """Initialise builder state (called from subclass ``__init__``)."""
        self._session = session
        self._model = model
        self._filters = []
        self._order_by = []
        self._limit = None
        self._skip = None
        self._distinct = False
        self._traversals = []
        self._eager_load = []
        self._vector_search = None
        self._sparse_search = None
        self._hybrid_search = None
        self._timeout = None
        self._max_memory = None
        self._param_counter = 0

    def _clone(self: SelfT) -> SelfT:
        """Create a shallow copy of this builder with all state."""
        new = self.__class__.__new__(self.__class__)
        new._session = self._session
        new._model = self._model
        new._filters = list(self._filters)
        new._order_by = list(self._order_by)
        new._limit = self._limit
        new._skip = self._skip
        new._distinct = self._distinct
        new._traversals = list(self._traversals)
        new._eager_load = list(self._eager_load)
        new._vector_search = copy.copy(self._vector_search)
        new._sparse_search = copy.copy(self._sparse_search)
        new._hybrid_search = copy.copy(self._hybrid_search)
        new._timeout = self._timeout
        new._max_memory = self._max_memory
        new._param_counter = self._param_counter
        return new

    def _next_param(self) -> str:
        """Generate a unique parameter name."""
        self._param_counter += 1
        return f"p{self._param_counter}"

    # ---- Immutable builder methods (shared) ----

    def filter(self: SelfT, expr: FilterExpr) -> SelfT:
        """Add a filter condition. Returns a new builder."""
        new = self._clone()
        new._filters.append(expr)
        return new

    def filter_by(self: SelfT, **kwargs: Any) -> SelfT:
        """Add equality filters by keyword arguments. Returns a new builder."""
        new = self._clone()
        for prop, value in kwargs.items():
            _validate_property(prop, self._model)
            new._filters.append(FilterExpr(prop, FilterOp.EQ, value))
        return new

    def order_by(
        self: SelfT,
        prop: PropertyProxy[Any] | str,
        descending: bool = False,
    ) -> SelfT:
        """Add an ORDER BY clause. Returns a new builder."""
        new = self._clone()
        name = prop._property_name if isinstance(prop, PropertyProxy) else prop
        new._order_by.append(OrderByClause(name, descending))
        return new

    def limit(self: SelfT, n: int) -> SelfT:
        """Limit the number of results. Returns a new builder."""
        new = self._clone()
        new._limit = n
        return new

    def skip(self: SelfT, n: int) -> SelfT:
        """Skip the first n results. Returns a new builder."""
        new = self._clone()
        new._skip = n
        return new

    def distinct(self: SelfT) -> SelfT:
        """Return only distinct results. Returns a new builder."""
        new = self._clone()
        new._distinct = True
        return new

    def traverse(
        self: SelfT,
        relationship: RelationshipDescriptor[Any] | str,
        target_model: type[UniNode] | None = None,
    ) -> SelfT:
        """Traverse a relationship to related nodes. Returns a new builder."""
        new = self._clone()
        if isinstance(relationship, RelationshipDescriptor):
            edge_type = relationship.config.edge_type
            direction = relationship.config.direction
        else:
            edge_type = relationship
            direction = "outgoing"
        target_label = target_model.__label__ if target_model else None
        new._traversals.append(TraversalStep(edge_type, direction, target_label))
        return new

    def eager_load(
        self: SelfT, *relationships: RelationshipDescriptor[Any] | str
    ) -> SelfT:
        """Eager load relationships to avoid N+1 queries. Returns a new builder."""
        new = self._clone()
        for rel in relationships:
            if isinstance(rel, RelationshipDescriptor):
                new._eager_load.append(rel.field_name)
            else:
                new._eager_load.append(rel)
        return new

    def vector_search(
        self: SelfT,
        prop: PropertyProxy[Any] | str,
        query_vector: list[float],
        k: int = 10,
        threshold: float | None = None,
        pre_filter: str | None = None,
    ) -> SelfT:
        """Perform vector similarity search. Returns a new builder."""
        new = self._clone()
        name = prop._property_name if isinstance(prop, PropertyProxy) else prop
        _validate_property(name)
        new._vector_search = VectorSearchConfig(
            property_name=name,
            query_vector=query_vector,
            k=k,
            threshold=threshold,
            pre_filter=pre_filter,
        )
        return new

    def sparse_search(
        self: SelfT,
        prop: PropertyProxy[Any] | str,
        query: Any,
        k: int = 10,
        threshold: float | None = None,
        pre_filter: str | None = None,
    ) -> SelfT:
        """Perform learned-sparse (SPLADE) similarity search.

        ``query`` may be a ``SparseVector``, a ``dict[int, float]`` of
        term id -> weight, or an ``(indices, values)`` pair. Returns a new
        builder. ``threshold`` filters on the dot-product score (``score >= t``).
        """
        new = self._clone()
        name = prop._property_name if isinstance(prop, PropertyProxy) else prop
        _validate_property(name)
        indices, values = _coerce_sparse_query(query)
        new._sparse_search = SparseSearchConfig(
            property_name=name,
            query_indices=indices,
            query_values=values,
            k=k,
            threshold=threshold,
            pre_filter=pre_filter,
        )
        return new

    def hybrid_search(
        self: SelfT,
        *,
        vector: tuple[PropertyProxy[Any] | str, list[float] | None]
        | PropertyProxy[Any]
        | str
        | None = None,
        fts: tuple[PropertyProxy[Any] | str, str]
        | PropertyProxy[Any]
        | str
        | None = None,
        sparse: tuple[PropertyProxy[Any] | str, Any] | None = None,
        query_text: str | None = None,
        k: int = 10,
        method: Literal["rrf", "weighted"] = "rrf",
        weights: list[float] | None = None,
        alpha: float | None = None,
        rrf_k: int | None = None,
        over_fetch: float | None = None,
        filter: str | None = None,
    ) -> SelfT:
        """Perform three-way fused hybrid search (dense + FTS + sparse).

        Wraps the engine's ``uni.search`` procedure. Each source is optional;
        supply at least one of ``vector`` / ``fts`` / ``sparse``.

        ``vector`` / ``fts`` accept either a ``(property, query)`` tuple or a
        bare property (``PropertyProxy`` or ``str``). A bare ``vector`` (no
        precomputed vec) auto-embeds from the shared query text; a ``(property,
        vec)`` tuple uses the precomputed dense vector. ``sparse`` is always a
        ``(property, query)`` tuple where ``query`` is a ``SparseVector``,
        ``dict[int, float]``, or ``(indices, values)`` pair.

        The engine shares a single ``query_text`` positional across FTS and dense
        auto-embed: it comes from ``fts``'s query string, or the ``query_text=``
        override kwarg (which also supplies text when ``fts`` is omitted but dense
        auto-embed is wanted).

        Results carry relevance scores via :attr:`UniNode.search_scores`.
        Returns a new builder.
        """
        vector_property: str | None = None
        query_vector: list[float] | None = None
        if vector is not None:
            if isinstance(vector, tuple):
                vprop, query_vector = vector
            else:
                vprop = vector
            vector_property = (
                vprop._property_name if isinstance(vprop, PropertyProxy) else vprop
            )

        fts_property: str | None = None
        fts_text: str | None = None
        if fts is not None:
            if isinstance(fts, tuple):
                fprop, fts_text = fts
            else:
                fprop = fts
            fts_property = (
                fprop._property_name if isinstance(fprop, PropertyProxy) else fprop
            )

        sparse_property: str | None = None
        sparse_query: tuple[list[int], list[float]] | None = None
        if sparse is not None:
            if not (isinstance(sparse, (tuple, list)) and len(sparse) == 2):
                raise QueryError("sparse= must be a (property, query) tuple")
            sprop, squery = sparse
            sparse_property = (
                sprop._property_name if isinstance(sprop, PropertyProxy) else sprop
            )
            sparse_query = _coerce_sparse_query(squery)

        if vector_property is None and fts_property is None and sparse_property is None:
            raise QueryError(
                "hybrid_search requires at least one of vector=, fts=, sparse="
            )
        if weights is not None and len(weights) != 3:
            raise QueryError("weights must be length-3 [vector, fts, sparse]")

        # Format-only validation of the interpolated property names (they are
        # f-string'd into the properties map); rejects injection attempts.
        for _name in (vector_property, fts_property, sparse_property):
            if _name is not None:
                _validate_property(_name)

        resolved_text = (
            query_text
            if query_text is not None
            else (fts_text if fts_text is not None else "")
        )

        new = self._clone()
        new._hybrid_search = HybridSearchConfig(
            query_text=resolved_text,
            k=k,
            vector_property=vector_property,
            fts_property=fts_property,
            sparse_property=sparse_property,
            query_vector=query_vector,
            sparse_query=sparse_query,
            method=method,
            alpha=alpha,
            weights=weights,
            rrf_k=rrf_k,
            over_fetch=over_fetch,
            filter=filter,
        )
        return new

    def timeout(self: SelfT, seconds: float) -> SelfT:
        """Set a query timeout. Returns a new builder."""
        new = self._clone()
        new._timeout = seconds
        return new

    def max_memory(self: SelfT, bytes_: int) -> SelfT:
        """Set a max memory limit for the query. Returns a new builder."""
        new = self._clone()
        new._max_memory = bytes_
        return new

    # ---- Cypher building (shared) ----

    def _build_match_where(self) -> tuple[str, dict[str, Any]]:
        """Build the MATCH ... WHERE portion of a Cypher query."""
        label = self._model.__label__
        params: dict[str, Any] = {}

        if self._traversals:
            match_pattern = self._build_traversal_pattern()
        else:
            match_pattern = f"(n:{label})"

        cypher = f"MATCH {match_pattern}"

        if self._filters:
            where_parts = []
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("n", param_name)
                where_parts.append(clause)
                params.update(clause_params)
            cypher += " WHERE " + " AND ".join(where_parts)

        return cypher, params

    def _build_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build the Cypher query string and parameters."""
        if self._vector_search:
            return self._build_vector_search_cypher()
        if self._sparse_search:
            return self._build_sparse_search_cypher()
        if self._hybrid_search:
            return self._build_hybrid_search_cypher()

        cypher, params = self._build_match_where()

        return_var = "n" if not self._traversals else self._get_final_var()
        cypher += " " + _node_return_clause(return_var, distinct=self._distinct)

        if self._order_by:
            order_parts = []
            for o in self._order_by:
                order_str = f"{return_var}.{o.property_name}"
                if o.descending:
                    order_str += " DESC"
                order_parts.append(order_str)
            cypher += " ORDER BY " + ", ".join(order_parts)

        if self._skip is not None:
            cypher += f" SKIP {self._skip}"
        if self._limit is not None:
            cypher += f" LIMIT {self._limit}"

        return cypher, params

    def _build_traversal_pattern(self) -> str:
        """Build MATCH pattern for relationship traversals."""
        label = self._model.__label__
        parts = [f"(n:{label})"]

        for i, step in enumerate(self._traversals):
            var = f"n{i + 1}"
            edge_var = f"r{i}"

            if step.direction == "outgoing":
                edge_pat = f"-[{edge_var}:{step.edge_type}]->"
            elif step.direction == "incoming":
                edge_pat = f"<-[{edge_var}:{step.edge_type}]-"
            else:  # both
                edge_pat = f"-[{edge_var}:{step.edge_type}]-"

            node_pat = (
                f"({var}:{step.target_label})" if step.target_label else f"({var})"
            )
            parts.append(edge_pat + node_pat)

        return "".join(parts)

    def _get_final_var(self) -> str:
        """Get the variable name for the final node in traversals."""
        if self._traversals:
            return f"n{len(self._traversals)}"
        return "n"

    def _build_vector_search_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build Cypher for vector search using uni.vector.query."""
        label = self._model.__label__
        vs = self._vector_search
        assert vs is not None

        params = {"query_vec": vs.query_vector}

        cypher = (
            f"CALL uni.vector.query('{label}', '{vs.property_name}', "
            f"$query_vec, {vs.k})"
        )
        cypher += " YIELD node, distance, score"

        where_parts: list[str] = []
        if vs.threshold is not None:
            where_parts.append(f"distance <= {vs.threshold}")

        if self._filters:
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("node", param_name)
                where_parts.append(clause)
                params.update(clause_params)

        if where_parts:
            cypher += " WHERE " + " AND ".join(where_parts)

        # Return the properties()/id()/labels() triple (not ``node AS n``) so the
        # rows hydrate through ``_row_to_node_dict``; the score columns ride
        # alongside for the ``.search_scores`` sidecar.
        cypher += (
            " RETURN properties(node) AS _props, id(node) AS _vid, "
            "labels(node) AS _labels, distance, score ORDER BY distance"
        )

        if self._limit:
            cypher += f" LIMIT {self._limit}"

        return cypher, params

    def _build_sparse_search_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build Cypher for sparse search using uni.sparse.query."""
        label = self._model.__label__
        ss = self._sparse_search
        assert ss is not None

        # The query is passed as an `{indices, values}` map; the procedure
        # accepts that shape directly (no SparseVector binding needed here).
        params: dict[str, Any] = {
            "sparse_q": {"indices": ss.query_indices, "values": ss.query_values}
        }

        cypher = (
            f"CALL uni.sparse.query('{label}', '{ss.property_name}', $sparse_q, {ss.k})"
        )
        cypher += " YIELD node, score"

        where_parts: list[str] = []
        if ss.threshold is not None:
            # Sparse score is a dot product — higher is better, so the
            # threshold is a lower bound (unlike vector distance).
            where_parts.append(f"score >= {ss.threshold}")

        if self._filters:
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("node", param_name)
                where_parts.append(clause)
                params.update(clause_params)

        if where_parts:
            cypher += " WHERE " + " AND ".join(where_parts)

        # See ``_build_vector_search_cypher`` for why we return the triple.
        cypher += (
            " RETURN properties(node) AS _props, id(node) AS _vid, "
            "labels(node) AS _labels, score ORDER BY score DESC"
        )

        if self._limit:
            cypher += f" LIMIT {self._limit}"

        return cypher, params

    def _build_hybrid_search_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build Cypher for three-way fused hybrid search using ``uni.search``.

        Emits the 7-positional ``uni.search(label, properties, query_text,
        query_vector, k, filter, options)`` call. All user data is bound as
        ``$params``; only schema identifiers (label, property names) and numeric
        knobs are interpolated — mirroring the vector/sparse helpers' injection
        boundary.
        """
        label = self._model.__label__
        hs = self._hybrid_search
        assert hs is not None

        params: dict[str, Any] = {}

        # properties map (arg 1) — only present arms.
        prop_entries: list[str] = []
        if hs.vector_property is not None:
            prop_entries.append(f"vector: '{hs.vector_property}'")
        if hs.fts_property is not None:
            prop_entries.append(f"fts: '{hs.fts_property}'")
        if hs.sparse_property is not None:
            prop_entries.append(f"sparse: '{hs.sparse_property}'")
        properties_map = "{" + ", ".join(prop_entries) + "}"

        # query_text (arg 2) — always bound.
        params["qtext"] = hs.query_text

        # query_vector (arg 3) — $qvec when precomputed, else null (auto-embed).
        if hs.query_vector is not None:
            params["qvec"] = hs.query_vector
            qvec_arg = "$qvec"
        else:
            qvec_arg = "null"

        # filter (arg 5) — $filter when set, else null.
        if hs.filter is not None:
            params["filter"] = hs.filter
            filter_arg = "$filter"
        else:
            filter_arg = "null"

        # options map (arg 6).
        opt_entries: list[str] = [f"method: '{hs.method}'"]
        if hs.alpha is not None:
            opt_entries.append(f"alpha: {hs.alpha}")
        if hs.weights is not None:
            weights_lit = ", ".join(str(w) for w in hs.weights)
            opt_entries.append(f"weights: [{weights_lit}]")
        if hs.rrf_k is not None:
            opt_entries.append(f"rrf_k: {hs.rrf_k}")
        if hs.over_fetch is not None:
            opt_entries.append(f"over_fetch: {hs.over_fetch}")
        if hs.sparse_query is not None:
            indices, values = hs.sparse_query
            params["sparse_q"] = {"indices": indices, "values": values}
            opt_entries.append("sparse_query: $sparse_q")
        options_map = "{" + ", ".join(opt_entries) + "}"

        cypher = (
            f"CALL uni.search('{label}', {properties_map}, "
            f"$qtext, {qvec_arg}, {hs.k}, {filter_arg}, {options_map})"
        )
        cypher += " YIELD node, score, vector_score, fts_score, sparse_score"

        # Model-level filters as a trailing WHERE over the returned node.
        if self._filters:
            where_parts: list[str] = []
            for f in self._filters:
                param_name = self._next_param()
                clause, clause_params = f.to_cypher("node", param_name)
                where_parts.append(clause)
                params.update(clause_params)
            cypher += " WHERE " + " AND ".join(where_parts)

        cypher += (
            " RETURN properties(node) AS _props, id(node) AS _vid, "
            "labels(node) AS _labels, score, vector_score, fts_score, sparse_score"
        )
        cypher += " ORDER BY score DESC"

        if self._limit:
            cypher += f" LIMIT {self._limit}"

        return cypher, params

    def _build_count_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build a COUNT query."""
        cypher, params = self._build_match_where()
        return_var = self._get_final_var()
        cypher += f" RETURN count({return_var}) as count"
        return cypher, params

    def _build_exists_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build an EXISTS query."""
        cypher, params = self._build_match_where()
        cypher += " RETURN true LIMIT 1"
        return cypher, params

    def _build_delete_cypher(self) -> tuple[str, dict[str, Any]]:
        """Build a DETACH DELETE query."""
        cypher, params = self._build_match_where()
        cypher += " DETACH DELETE n RETURN count(n) as count"
        return cypher, params

    def _build_update_cypher(self, **kwargs: Any) -> tuple[str, dict[str, Any]]:
        """Build an UPDATE (SET) query."""
        cypher, params = self._build_match_where()
        set_parts = []
        for prop, value in kwargs.items():
            _validate_property(prop, self._model)
            param_name = self._next_param()
            set_parts.append(f"n.{prop} = ${param_name}")
            params[param_name] = value
        cypher += " SET " + ", ".join(set_parts)
        cypher += " RETURN count(n) as count"
        return cypher, params

    def _is_search(self) -> bool:
        """Whether this builder is a vector/sparse/hybrid search query."""
        return bool(self._vector_search or self._sparse_search or self._hybrid_search)

    def _rows_to_instances(self, results: list[dict[str, Any]]) -> list[NodeT]:
        """Convert result rows to model instances."""
        instances = []
        for row in results:
            node_data = _row_to_node_dict(row)
            if node_data is None:
                continue
            instance = self._session._result_to_model(node_data, self._model)
            if instance is not None:
                instances.append(instance)
        return instances

    def _rows_to_scored_instances(self, results: list[dict[str, Any]]) -> list[NodeT]:
        """Convert search result rows to instances carrying ``.search_scores``.

        Hydrates the node exactly as ``_rows_to_instances`` does, then reads the
        score columns off the same row into a :class:`SearchScores` sidecar. The
        primary ``score`` is the fused score (hybrid) or the per-source score
        (single-source); the vector arm's raw ``distance`` is preserved too.
        """
        instances = []
        for row in results:
            node_data = _row_to_node_dict(row)
            if node_data is None:
                continue
            instance = self._session._result_to_model(node_data, self._model)
            if instance is None:
                continue
            primary = row.get("score")
            if primary is None:
                primary = row.get("distance")
            instance._scores = SearchScores(
                score=float(primary) if primary is not None else 0.0,
                vector=_opt_float(row.get("vector_score")),
                fts=_opt_float(row.get("fts_score")),
                sparse=_opt_float(row.get("sparse_score")),
                rerank=_opt_float(row.get("rerank_score")),
                distance=_opt_float(row.get("distance")),
            )
            instances.append(instance)
        return instances


class QueryBuilder(_QueryBuilderBase[NodeT]):
    """
    Immutable, type-safe query builder for graph queries.

    Each method returns a **new** QueryBuilder instance. The original is
    never mutated. Provides a fluent API for building Cypher queries
    with type checking and IDE autocomplete support.

    Example:
        >>> adults = (
        ...     session.query(Person)
        ...     .filter(Person.age >= 18)
        ...     .order_by(Person.name)
        ...     .limit(10)
        ...     .all()
        ... )
    """

    def __init__(self, session: UniSession, model: type[NodeT]) -> None:
        self._init_state(session, model)

    def _execute_query(
        self, cypher: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Execute a query, using query_with if timeout/max_memory is set."""
        if self._timeout is not None or self._max_memory is not None:
            builder = self._session._db_session.query_with(cypher)
            if params:
                builder = builder.params(params)
            if self._timeout is not None:
                builder = builder.timeout(self._timeout)
            if self._max_memory is not None:
                builder = builder.max_memory(self._max_memory)
            result = builder.fetch_all()
        else:
            result = self._session._db_session.query(cypher, params)
        return [row.to_dict() for row in result]

    def all(self) -> list[NodeT]:
        """Execute the query and return all results."""
        cypher, params = self._build_cypher()
        results = self._execute_query(cypher, params)
        if self._is_search():
            instances = self._rows_to_scored_instances(results)
        else:
            instances = self._rows_to_instances(results)
        if self._eager_load and instances:
            self._session._eager_load_relationships(instances, self._eager_load)
        return instances

    def first(self) -> NodeT | None:
        """Execute the query and return the first result."""
        clone = self._clone()
        clone._limit = 1
        results = clone.all()
        return results[0] if results else None

    def one(self) -> NodeT:
        """Execute the query and return exactly one result.

        Raises QueryError if no results or more than one result.
        """
        clone = self._clone()
        clone._limit = 2
        results = clone.all()
        if not results:
            raise QueryError("Query returned no results")
        if len(results) > 1:
            raise QueryError("Query returned more than one result")
        return results[0]

    def count(self) -> int:
        """Execute the query and return the count of results."""
        cypher, params = self._build_count_cypher()
        results = self._execute_query(cypher, params)
        return cast(int, results[0]["count"]) if results else 0

    def exists(self) -> bool:
        """Check if any matching records exist."""
        cypher, params = self._build_exists_cypher()
        results = self._execute_query(cypher, params)
        return len(results) > 0

    def delete(self) -> int:
        """Delete all matching records (DETACH DELETE)."""
        cypher, params = self._build_delete_cypher()
        with self._session._db_session.tx() as tx:
            results = tx.query(cypher, params)
            tx.commit()
        return results[0].to_dict()["count"] if results else 0

    def update(self, **kwargs: Any) -> int:
        """Update all matching records."""
        cypher, params = self._build_update_cypher(**kwargs)
        with self._session._db_session.tx() as tx:
            results = tx.query(cypher, params)
            tx.commit()
        return results[0].to_dict()["count"] if results else 0
