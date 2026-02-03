# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async session integration tests for uni-pydantic."""

from datetime import date, datetime

import pytest

from uni_pydantic import (
    AsyncUniSession,
    Field,
    Relationship,
    UniEdge,
    UniNode,
    before_create,
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
    created_at: datetime | None = None

    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

    @before_create
    def set_created_at(self):
        self.created_at = datetime.now()


class FriendshipEdge(UniEdge):
    """Test friendship edge."""

    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person

    since: date


class TestAsyncSessionCRUD:
    """Tests for async session CRUD operations."""

    async def test_add_and_commit(self, async_session):
        """Test adding and committing a node."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        assert alice.is_persisted
        assert alice.vid is not None

    async def test_add_multiple(self, async_session):
        """Test adding multiple nodes."""
        async_session.register(Person)
        await async_session.sync_schema()

        people = [
            Person(name="Alice", email="alice@test.com"),
            Person(name="Bob", email="bob@test.com"),
        ]
        async_session.add_all(people)
        await async_session.commit()

        for person in people:
            assert person.is_persisted

    async def test_get_by_vid(self, async_session):
        """Test getting entity by vid."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        found = await async_session.get(Person, vid=alice.vid)
        assert found is not None
        assert found.name == "Alice"

    async def test_get_by_property(self, async_session):
        """Test getting entity by property."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        found = await async_session.get(Person, email="alice@test.com")
        assert found is not None
        assert found.name == "Alice"

    async def test_update_entity(self, async_session):
        """Test updating an entity."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        alice.age = 31
        await async_session.commit()

        await async_session.refresh(alice)
        assert alice.age == 31

    async def test_delete_entity(self, async_session):
        """Test deleting an entity."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        vid = alice.vid
        async_session.delete(alice)
        await async_session.commit()

        found = await async_session.get(Person, vid=vid)
        assert found is None


class TestAsyncContextManager:
    """Tests for async session context manager."""

    async def test_async_context_manager(self, async_db):
        """Test async session works as async context manager."""
        async with AsyncUniSession(async_db) as session:
            session.register(Person)
            await session.sync_schema()

            alice = Person(name="Alice", email="alice@test.com")
            session.add(alice)
            await session.commit()

            assert alice.is_persisted


class TestAsyncEdgeCRUD:
    """Tests for async edge CRUD operations."""

    async def test_create_edge(self, async_session):
        """Test creating an edge."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        async_session.add_all([alice, bob])
        await async_session.commit()

        await async_session.create_edge(
            alice, "FRIEND_OF", bob, {"since": date.today()}
        )
        await async_session._db.flush()

        results = await async_session.cypher(
            "MATCH (a:Person)-[:FRIEND_OF]->(b:Person) "
            "WHERE a.name = 'Alice' RETURN b.name as name"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Bob"

    async def test_delete_edge(self, async_session):
        """Test deleting an edge."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        async_session.add_all([alice, bob])
        await async_session.commit()

        await async_session.create_edge(alice, "FRIEND_OF", bob)
        await async_session._db.flush()

        count = await async_session.delete_edge(alice, "FRIEND_OF", bob)
        assert count >= 1


class TestAsyncBulkAdd:
    """Tests for async bulk add operations."""

    async def test_bulk_add(self, async_session):
        """Test bulk adding entities."""
        async_session.register(Person)
        await async_session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(10)
        ]

        vids = await async_session.bulk_add(people)
        assert len(vids) == 10

        for person in people:
            assert person.is_persisted


class TestAsyncTransaction:
    """Tests for async transaction handling."""

    async def test_transaction_commit(self, async_session):
        """Test async transaction commit."""
        async_session.register(Person)
        await async_session.sync_schema()

        tx = await async_session.transaction()
        async with tx:
            alice = Person(name="Alice", email="alice@test.com")
            tx.add(alice)

        assert alice.is_persisted


class TestAsyncLifecycleHooks:
    """Tests for lifecycle hooks in async session."""

    async def test_before_create_hook(self, async_session):
        """Test before_create hook is called."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        assert alice.created_at is None

        async_session.add(alice)
        await async_session.commit()

        assert alice.created_at is not None
