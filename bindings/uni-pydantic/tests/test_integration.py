# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Integration tests for uni-pydantic with actual database."""

from datetime import date, datetime

import pytest

from uni_pydantic import (
    Field,
    Relationship,
    UniEdge,
    UniNode,
    UniSession,
    before_create,
)

# Skip all tests if uni_db is not available
pytestmark = pytest.mark.skipif(
    not pytest.importorskip("uni_db", reason="uni_db not available"),
    reason="uni_db not available",
)


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


class Company(UniNode):
    """Test company model."""

    __label__ = "Company"

    name: str = Field(index="btree", unique=True)
    founded: date | None = None


class FriendshipEdge(UniEdge):
    """Test friendship edge."""

    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person

    since: date


class TestSessionCRUD:
    """Tests for session CRUD operations."""

    def test_add_and_commit(self, session):
        """Test adding and committing a node."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        assert alice.is_persisted
        assert alice.vid is not None

    def test_add_multiple(self, session):
        """Test adding multiple nodes."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name="Alice", email="alice@test.com"),
            Person(name="Bob", email="bob@test.com"),
            Person(name="Charlie", email="charlie@test.com"),
        ]
        session.add_all(people)
        session.commit()

        for person in people:
            assert person.is_persisted

    def test_get_by_vid(self, session):
        """Test getting entity by vid."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        found = session.get(Person, vid=alice.vid)
        assert found is not None
        assert found.name == "Alice"

    def test_get_by_property(self, session):
        """Test getting entity by property."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        found = session.get(Person, email="alice@test.com")
        assert found is not None
        assert found.name == "Alice"

    def test_update_entity(self, session):
        """Test updating an entity."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        session.add(alice)
        session.commit()

        alice.age = 31
        session.commit()

        # Refresh and verify
        session.refresh(alice)
        assert alice.age == 31

    def test_delete_entity(self, session):
        """Test deleting an entity."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        vid = alice.vid
        session.delete(alice)
        session.commit()

        found = session.get(Person, vid=vid)
        assert found is None


class TestContextManager:
    """Tests for session context manager."""

    def test_session_context_manager(self, db):
        """Test session works as context manager."""
        with UniSession(db) as session:
            session.register(Person)
            session.sync_schema()

            alice = Person(name="Alice", email="alice@test.com")
            session.add(alice)
            session.commit()

            assert alice.is_persisted

    def test_session_close(self, db):
        """Test session close clears pending state."""
        session = UniSession(db)
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        assert len(session._pending_new) == 1

        session.close()
        assert len(session._pending_new) == 0


class TestEdgeCRUD:
    """Tests for edge CRUD operations."""

    def test_create_and_get_edge(self, session):
        """Test creating and retrieving an edge."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        session.create_edge(alice, "FRIEND_OF", bob, {"since": date.today()})
        session._db.flush()

        edges = session.get_edge(alice, "FRIEND_OF", bob)
        assert len(edges) >= 1

    def test_delete_edge(self, session):
        """Test deleting an edge."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        session.create_edge(alice, "FRIEND_OF", bob)
        session._db.flush()

        count = session.delete_edge(alice, "FRIEND_OF", bob)
        assert count >= 1

    def test_update_edge(self, session):
        """Test updating edge properties."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        session.create_edge(alice, "FRIEND_OF", bob, {"strength": 0.5})
        session._db.flush()

        count = session.update_edge(alice, "FRIEND_OF", bob, {"strength": 0.9})
        assert count >= 1


class TestBulkAdd:
    """Tests for bulk add operations."""

    def test_bulk_add(self, session):
        """Test bulk adding entities."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(10)
        ]

        vids = session.bulk_add(people)
        assert len(vids) == 10

        for person in people:
            assert person.is_persisted
            assert person.vid is not None

    def test_bulk_add_empty(self, session):
        """Test bulk add with empty list."""
        vids = session.bulk_add([])
        assert vids == []


class TestQueryBuilder:
    """Tests for query builder with database."""

    def test_query_all(self, session):
        """Test querying all entities."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name="Alice", email="alice@test.com"),
            Person(name="Bob", email="bob@test.com"),
        ]
        session.add_all(people)
        session.commit()

        results = session.query(Person).all()
        assert len(results) >= 2

    def test_query_filter(self, session):
        """Test querying with filter."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        bob = Person(name="Bob", age=25, email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        results = session.query(Person).filter_by(name="Alice").all()
        assert len(results) == 1
        assert results[0].name == "Alice"

    def test_query_first(self, session):
        """Test query first."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        result = session.query(Person).first()
        assert result is not None
        assert result.name == "Alice"

    def test_query_count(self, session):
        """Test query count."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(5)
        ]
        session.add_all(people)
        session.commit()

        count = session.query(Person).count()
        assert count >= 5

    def test_query_exists(self, session):
        """Test query exists."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        assert session.query(Person).filter_by(name="Alice").exists()
        assert not session.query(Person).filter_by(name="NonExistent").exists()

    def test_query_limit(self, session):
        """Test query with limit."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(10)
        ]
        session.add_all(people)
        session.commit()

        results = session.query(Person).limit(3).all()
        assert len(results) == 3


class TestSchemaSync:
    """Tests for schema synchronization."""

    def test_sync_creates_label(self, session):
        """Test sync creates label."""
        session.register(Person)
        session.sync_schema()

        assert session._db.label_exists("Person")

    def test_sync_creates_edge_type(self, session):
        """Test sync creates edge type."""
        session.register(Person, FriendshipEdge)
        session.sync_schema()

        assert session._db.edge_type_exists("FRIEND_OF")

    def test_sync_is_idempotent(self, session):
        """Test sync can be called multiple times."""
        session.register(Person)
        session.sync_schema()
        session.sync_schema()  # Should not error

        assert session._db.label_exists("Person")


class TestRelationships:
    """Tests for relationship operations."""

    def test_create_edge(self, session):
        """Test creating an edge between nodes."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        session.create_edge(alice, "FRIEND_OF", bob, {"since": date.today()})
        session._db.flush()

        # Verify via Cypher
        results = session.cypher(
            "MATCH (a:Person)-[:FRIEND_OF]->(b:Person) "
            "WHERE a.name = 'Alice' RETURN b.name as name"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Bob"


class TestTransaction:
    """Tests for transaction handling."""

    def test_transaction_commit(self, session):
        """Test transaction commit."""
        session.register(Person)
        session.sync_schema()

        with session.transaction() as tx:
            alice = Person(name="Alice", email="alice@test.com")
            tx.add(alice)

        assert alice.is_persisted

    def test_transaction_rollback_on_error(self, session):
        """Test transaction rollback on error."""
        session.register(Person)
        session.sync_schema()

        try:
            with session.transaction() as tx:
                alice = Person(name="Alice", email="alice@test.com")
                tx.add(alice)
                raise ValueError("Test error")
        except ValueError:
            pass

        # Alice should not be persisted due to rollback
        found = session.get(Person, email="alice@test.com")
        assert found is None


class TestLifecycleHooks:
    """Tests for lifecycle hooks."""

    def test_before_create_hook(self, session):
        """Test before_create hook is called."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        assert alice.created_at is None

        session.add(alice)
        session.commit()

        # before_create should have set created_at
        assert alice.created_at is not None
