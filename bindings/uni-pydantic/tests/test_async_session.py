# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async session integration tests for uni-pydantic."""

from datetime import date, datetime

import pytest
from pydantic import field_validator

from uni_pydantic import (
    AsyncUniSession,
    Btic,
    Field,
    Relationship,
    UniEdge,
    UniNode,
    Vector,
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

    async def test_update_converts_vector_and_btic_values(self, async_session):
        """Regression: see the sync twin in test_integration.py -- a bare
        `get_type_hints` always raised here, so updated `Vector`/`Btic` values
        reached the database unconverted and the commit failed."""

        class VecBticDoc(UniNode):
            title: str
            emb: Vector[2]
            when: Btic | None = None

        async_session.register(VecBticDoc)
        await async_session.sync_schema()
        doc = VecBticDoc(title="a", emb=Vector[2]([1.0, 0.0]), when=Btic("2020"))
        async_session.add(doc)
        await async_session.commit()

        doc.emb = Vector[2]([0.0, 1.0])
        doc.when = Btic("2021")
        await async_session.commit()

        await async_session.refresh(doc)
        assert doc.emb == Vector[2]([0.0, 1.0])
        assert doc.when == Btic("2021")

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

    @pytest.mark.parametrize(
        "exc_name",
        ["UniBulkCommitIncompleteError", "UniBulkCommitOutcomeUnknownError"],
    )
    async def test_bulk_add_propagates_committed_outcomes(
        self, async_session, exc_name
    ):
        """A committed (or possibly committed) load is never a BulkLoadError."""
        import uni_db

        exc = getattr(uni_db, exc_name)
        async_session.register(Person)
        await async_session.sync_schema()
        async_session._db_session = _AsyncFailingDbSession(exc("committed"))
        people = [Person(name="A", email="a@test.com")]
        with pytest.raises(exc):
            await async_session.bulk_add(people)
        assert not people[0].is_persisted


class _AsyncFailingDbSession:
    """An async session whose bulk writer's commit raises `error`."""

    def __init__(self, error):
        self._error = error

    async def tx(self):
        error = self._error

        class _Writer:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def insert_vertices(self, label, rows):
                return list(range(len(rows)))

            async def commit(self):
                raise error

        class _Builder:
            async def build(self):
                return _Writer()

        class _Tx:
            def bulk_writer(self):
                return _Builder()

            async def commit(self):
                return None

        return _Tx()


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


# ---------------------------------------------------------------------------
# Async hydration must not silently drop rows either
# ---------------------------------------------------------------------------
#
# `AsyncUniSession._result_to_model` is a second copy of the sync method, with
# its own `except Exception: return None`. The row-mapping helpers in
# `query.py` are shared, so the truthiness guards were common to both; the
# swallow was not.


class AsyncFalsyThing(UniNode):
    """Legitimately falsy when empty — must survive hydration."""

    __label__ = "AsyncFalsyThing"

    name: str
    count: int = 0

    def __bool__(self) -> bool:
        return self.count > 0


class AsyncBuggyThing(UniNode):
    """Raises a non-validation error during hydration."""

    __label__ = "AsyncBuggyThing"

    name: str

    @field_validator("name")
    @classmethod
    def _boom(cls, v: str) -> str:
        if v == "boom":
            raise RuntimeError("async hydration bug, not a validation failure")
        return v


async def _async_raw_create(async_session, cypher: str) -> None:
    tx = await async_session._db_session.tx()
    await tx.execute(cypher)
    await tx.commit()


class TestAsyncHydrationDoesNotDropRows:
    async def test_falsy_instance_is_not_dropped(self, async_session):
        async_session.register(AsyncFalsyThing)
        await async_session.sync_schema()

        await _async_raw_create(
            async_session,
            "CREATE (:AsyncFalsyThing {name: 'empty', count: 0}) "
            "CREATE (:AsyncFalsyThing {name: 'full', count: 3})",
        )

        found = await async_session.query(AsyncFalsyThing).all()
        names = sorted(f.name for f in found)
        assert names == ["empty", "full"], (
            f"a validly hydrated but falsy model was dropped; got {names}"
        )

    async def test_hydration_bug_is_not_swallowed(self, async_session):
        async_session.register(AsyncBuggyThing)
        await async_session.sync_schema()

        await _async_raw_create(
            async_session,
            "CREATE (:AsyncBuggyThing {name: 'ok'}) "
            "CREATE (:AsyncBuggyThing {name: 'boom'})",
        )

        with pytest.raises(RuntimeError, match="async hydration bug"):
            await async_session.query(AsyncBuggyThing).all()


# ---------------------------------------------------------------------------
# eager_load() on the async session
# ---------------------------------------------------------------------------
#
# `AsyncUniSession._load_relationship` raises and tells the caller to use
# `eager_load()`, so the eager path is the only relationship path async has --
# and it was the one caching raw dicts. An entity with nothing attached was
# left with no cache entry at all, which sent the descriptor down that raising
# lazy path on first access.


class AsyncTag(UniNode):
    __label__ = "AsyncTag"

    name: str


class AsyncDoc(UniNode):
    __label__ = "AsyncDoc"

    title: str

    tags: list[AsyncTag] = Relationship("TAGGED")


class TestAsyncEagerLoad:
    async def _seed(self, async_session):
        async_session.register(AsyncDoc, AsyncTag)
        await async_session.sync_schema()

        doc = AsyncDoc(title="Guide")
        tag = AsyncTag(name="howto")
        bare = AsyncDoc(title="Untagged")
        async_session.add_all([doc, tag, bare])
        await async_session.commit()

        await async_session.create_edge(doc, "TAGGED", tag, {})
        await async_session._db.flush()

    async def test_eager_load_yields_models(self, async_session):
        await self._seed(async_session)

        docs = await async_session.query(AsyncDoc).eager_load("tags").all()
        guide = next(d for d in docs if d.title == "Guide")

        assert len(guide.tags) == 1
        assert isinstance(guide.tags[0], AsyncTag), (
            f"eager_load cached a raw {type(guide.tags[0]).__name__}"
        )
        assert guide.tags[0].name == "howto"

    async def test_entity_without_relations_does_not_lazy_load(self, async_session):
        await self._seed(async_session)

        docs = await async_session.query(AsyncDoc).eager_load("tags").all()
        untagged = next(d for d in docs if d.title == "Untagged")

        # Must not raise: with no cache entry the descriptor would fall through
        # to the async lazy path, which raises on principle.
        assert untagged.tags == []


class TestAsyncExplainAndProfileReachTheBuilder:
    """Async twin of ``TestExplainAndProfileReachTheBuilder``; see session.py."""

    async def test_explain_returns_a_plan(self, async_session):
        out = await async_session.explain("MATCH (n) RETURN n")
        assert out is not None

    async def test_profile_returns_results_and_stats(self, async_session):
        result, stats = await async_session.profile("MATCH (n) RETURN n")
        assert result is not None
        assert stats is not None


class AsyncCypherCo(UniNode):
    __label__ = "AsyncCypherCo"
    name: str


class TestAsyncCypherResultType:
    """Async mirror of `TestCypherResultType` in test_integration.py."""

    async def _setup(self, async_session):
        async_session.register(Person, AsyncCypherCo)
        await async_session.sync_schema()
        async_session.add(AsyncCypherCo(name="Acme"))
        await async_session.commit()

    async def test_return_node_hydrates_result_type(self, async_session):
        await self._setup(async_session)
        found = await async_session.cypher(
            "MATCH (c:AsyncCypherCo) RETURN c", result_type=AsyncCypherCo
        )
        assert [type(c) for c in found] == [AsyncCypherCo]
        assert found[0].name == "Acme"
        assert found[0].is_persisted
        assert found[0]._session is async_session

    async def test_node_of_other_label_is_not_coerced(self, async_session):
        await self._setup(async_session)
        found = await async_session.cypher(
            "MATCH (c:AsyncCypherCo) RETURN c", result_type=Person
        )
        assert found == []

    async def test_labelled_map_of_other_label_is_not_returned(self, async_session):
        await self._setup(async_session)
        found = await async_session.cypher(
            "RETURN {_label: 'AsyncCypherCo', name: 'Acme'} AS c", result_type=Person
        )
        assert found == []
