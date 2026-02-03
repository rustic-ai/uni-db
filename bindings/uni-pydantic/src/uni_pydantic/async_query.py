# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async query builder for uni-pydantic -- mirrors QueryBuilder with async execution."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
)

from .base import UniNode
from .exceptions import QueryError
from .query import _QueryBuilderBase

if TYPE_CHECKING:
    from .async_session import AsyncUniSession

NodeT = TypeVar("NodeT", bound=UniNode)


class AsyncQueryBuilder(_QueryBuilderBase[NodeT]):
    """
    Immutable, async query builder for graph queries.

    Inherits all Cypher-building and immutable builder methods from
    ``_QueryBuilderBase``. Only the execution methods are async.
    """

    def __init__(self, session: AsyncUniSession, model: type[NodeT]) -> None:
        self._init_state(session, model)

    async def _execute_query(
        self, cypher: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Execute a query, using query_with if timeout/max_memory is set."""
        if self._timeout is not None or self._max_memory is not None:
            builder = self._session._db.query_with(cypher)
            if params:
                builder = builder.params(params)
            if self._timeout is not None:
                builder = builder.timeout(self._timeout)
            if self._max_memory is not None:
                builder = builder.max_memory(self._max_memory)
            return await builder.run()
        return await self._session._db.query(cypher, params)

    async def all(self) -> list[NodeT]:
        """Execute the query and return all results."""
        cypher, params = self._build_cypher()
        results = await self._execute_query(cypher, params)
        instances = self._rows_to_instances(results)
        if self._eager_load and instances:
            await self._session._async_eager_load_relationships(
                instances, self._eager_load
            )
        return instances

    async def first(self) -> NodeT | None:
        """Execute the query and return the first result."""
        clone = self._clone()
        clone._limit = 1
        results = await clone.all()
        return results[0] if results else None

    async def one(self) -> NodeT:
        """Execute the query and return exactly one result.

        Raises QueryError if no results or more than one result.
        """
        clone = self._clone()
        clone._limit = 2
        results = await clone.all()
        if not results:
            raise QueryError("Query returned no results")
        if len(results) > 1:
            raise QueryError("Query returned more than one result")
        return results[0]

    async def count(self) -> int:
        """Execute the query and return the count of results."""
        cypher, params = self._build_count_cypher()
        results = await self._execute_query(cypher, params)
        return cast(int, results[0]["count"]) if results else 0

    async def exists(self) -> bool:
        """Check if any matching records exist."""
        cypher, params = self._build_exists_cypher()
        results = await self._execute_query(cypher, params)
        return len(results) > 0

    async def delete(self) -> int:
        """Delete all matching records (DETACH DELETE)."""
        cypher, params = self._build_delete_cypher()
        results = await self._session._db.query(cypher, params)
        return cast(int, results[0]["count"]) if results else 0

    async def update(self, **kwargs: Any) -> int:
        """Update all matching records."""
        cypher, params = self._build_update_cypher(**kwargs)
        results = await self._session._db.query(cypher, params)
        return cast(int, results[0]["count"]) if results else 0
