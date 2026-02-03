# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async query builder tests for uni-pydantic."""

import pytest

from uni_pydantic import (
    AsyncQueryBuilder,
    Field,
    Relationship,
    UniNode,
)

# Skip all tests if uni_db is not available
pytestmark = [
    pytest.mark.skipif(
        not pytest.importorskip("uni_db", reason="uni_db not available"),
        reason="uni_db not available",
    ),
    pytest.mark.asyncio,
]


class Person(UniNode):
    """Test person model."""

    __label__ = "Person"

    name: str
    age: int | None = None
    email: str = Field(unique=True, index="btree")

    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")


class TestAsyncQueryBuilder:
    """Tests for async query builder with database."""

    async def test_query_all(self, async_session):
        """Test querying all entities."""
        async_session.register(Person)
        await async_session.sync_schema()

        people = [
            Person(name="Alice", email="alice@test.com"),
            Person(name="Bob", email="bob@test.com"),
        ]
        async_session.add_all(people)
        await async_session.commit()

        results = await async_session.query(Person).all()
        assert len(results) >= 2

    async def test_query_filter(self, async_session):
        """Test querying with filter."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        bob = Person(name="Bob", age=25, email="bob@test.com")
        async_session.add_all([alice, bob])
        await async_session.commit()

        results = await async_session.query(Person).filter_by(name="Alice").all()
        assert len(results) == 1
        assert results[0].name == "Alice"

    async def test_query_first(self, async_session):
        """Test query first."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        result = await async_session.query(Person).first()
        assert result is not None
        assert result.name == "Alice"

    async def test_query_count(self, async_session):
        """Test query count."""
        async_session.register(Person)
        await async_session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(5)
        ]
        async_session.add_all(people)
        await async_session.commit()

        count = await async_session.query(Person).count()
        assert count >= 5

    async def test_query_exists(self, async_session):
        """Test query exists."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        assert await async_session.query(Person).filter_by(name="Alice").exists()
        assert (
            not await async_session.query(Person).filter_by(name="NonExistent").exists()
        )

    async def test_query_limit(self, async_session):
        """Test query with limit."""
        async_session.register(Person)
        await async_session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(10)
        ]
        async_session.add_all(people)
        await async_session.commit()

        results = await async_session.query(Person).limit(3).all()
        assert len(results) == 3

    async def test_query_delete(self, async_session):
        """Test query delete."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        count = await async_session.query(Person).filter_by(name="Alice").delete()
        assert count >= 1

    async def test_query_update(self, async_session):
        """Test query update."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        count = await async_session.query(Person).filter_by(name="Alice").update(age=31)
        assert count >= 1


class TestAsyncQueryBuilderImmutability:
    """Tests for async query builder immutability."""

    async def test_filter_returns_new_builder(self, async_session):
        """Test that filter returns a new builder."""
        async_session.register(Person)
        q1 = async_session.query(Person)
        q2 = q1.filter_by(name="Alice")
        assert q1 is not q2
        assert len(q1._filters) == 0
        assert len(q2._filters) == 1

    async def test_limit_returns_new_builder(self, async_session):
        """Test that limit returns a new builder."""
        async_session.register(Person)
        q1 = async_session.query(Person)
        q2 = q1.limit(10)
        assert q1 is not q2
        assert q1._limit is None
        assert q2._limit == 10

    async def test_chaining(self, async_session):
        """Test chaining multiple builder methods."""
        async_session.register(Person)
        q = (
            async_session.query(Person)
            .filter_by(name="Alice")
            .order_by("name")
            .limit(10)
            .skip(5)
            .distinct()
        )
        assert isinstance(q, AsyncQueryBuilder)
        assert len(q._filters) == 1
        assert len(q._order_by) == 1
        assert q._limit == 10
        assert q._skip == 5
        assert q._distinct is True
