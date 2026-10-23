# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Integration tests for uni-pydantic with actual database."""

from datetime import date, datetime

import pytest
from pydantic import field_validator

from uni_pydantic import (
    Btic,
    BulkLoadError,
    Field,
    Relationship,
    UniEdge,
    UniNode,
    UniSession,
    Vector,
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

    def test_update_converts_vector_and_btic_values(self, session):
        """Regression: the dirty-update path resolved type hints with a bare
        `typing.get_type_hints`, which always raised NameError on UniNode's
        TYPE_CHECKING-only private-attr annotations; the swallowed error left
        `hints == {}`, so no value was converted and updating a `Vector` or
        `Btic` field failed with "cannot convert Python object of type ...".
        """

        class VecBticDoc(UniNode):
            title: str
            emb: Vector[2]
            when: Btic | None = None

        session.register(VecBticDoc)
        session.sync_schema()
        doc = VecBticDoc(title="a", emb=Vector[2]([1.0, 0.0]), when=Btic("2020"))
        session.add(doc)
        session.commit()

        doc.emb = Vector[2]([0.0, 1.0])
        doc.when = Btic("2021")
        session.commit()

        session.refresh(doc)
        assert doc.emb == Vector[2]([0.0, 1.0])
        assert doc.when == Btic("2021")

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

    def test_bulk_add_is_one_load(self, session):
        """Every label goes into one load and one commit, so a failure can
        never leave an earlier label applied behind a retry-inviting error."""
        session.register(Person, Company)
        session.sync_schema()
        recorder = _RecordingDbSession(session._db_session)
        session._db_session = recorder
        entities = [Person(name=f"P{i}", email=f"p{i}@test.com") for i in range(3)]
        entities += [Company(name=f"C{i}") for i in range(2)]
        vids = session.bulk_add(entities)
        assert len(vids) == 5
        assert recorder.commits == 1
        assert all(e.is_persisted for e in entities)

    @pytest.mark.parametrize(
        "exc_name",
        ["UniBulkCommitIncompleteError", "UniBulkCommitOutcomeUnknownError"],
    )
    def test_bulk_add_propagates_committed_outcomes(self, session, exc_name):
        """A load that reached (or may have reached) its commit point must not
        surface as BulkLoadError, which tells the caller to retry."""
        import uni_db

        exc = getattr(uni_db, exc_name)
        session.register(Person)
        session.sync_schema()
        session._db_session = _FailingDbSession(exc("committed; do not retry"))
        people = [Person(name="A", email="a@test.com")]
        with pytest.raises(exc):
            session.bulk_add(people)
        assert not people[0].is_persisted

    def test_bulk_add_failure_before_commit_point_is_bulk_load_error(self, session):
        """Any other failure means "not applied": BulkLoadError."""
        session.register(Person)
        session.sync_schema()
        session._db_session = _FailingDbSession(RuntimeError("disk full"))
        with pytest.raises(BulkLoadError):
            session.bulk_add([Person(name="A", email="a@test.com")])


class _RecordingWriter:
    def __init__(self, writer, owner):
        self._writer = writer
        self._owner = owner

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return self._writer.__exit__(*exc)

    def insert_vertices(self, label, rows):
        return self._writer.insert_vertices(label, rows)

    def commit(self):
        self._owner.commits += 1
        return self._writer.commit()


class _RecordingDbSession:
    """Wraps a real session, counting bulk-writer commits."""

    def __init__(self, inner):
        self._inner = inner
        self.commits = 0

    def tx(self):
        tx = self._inner.tx()
        owner = self

        class _Builder:
            def __init__(self, builder):
                self._builder = builder

            def build(self):
                return _RecordingWriter(self._builder.build(), owner)

        class _Tx:
            def bulk_writer(self):
                return _Builder(tx.bulk_writer())

            def commit(self):
                return tx.commit()

        return _Tx()


class _FailingDbSession:
    """A session whose bulk writer's commit raises `error`."""

    def __init__(self, error):
        self._error = error

    def tx(self):
        error = self._error

        class _Writer:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def insert_vertices(self, label, rows):
                return list(range(len(rows)))

            def commit(self):
                raise error

        class _Tx:
            def bulk_writer(self):
                class _B:
                    def build(self):
                        return _Writer()

                return _B()

            def commit(self):
                return None

        return _Tx()


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


class TestMultiVectorField:
    """Tests for list[Vector[N]] (multi-vector / ColBERT) OGM fields."""

    def test_list_vector_roundtrip(self, session):
        """A list[Vector[N]] field persists and retrieves as list[Vector[N]]."""

        class Article(UniNode):
            __label__ = "Article"
            title: str
            tokens: list[Vector[2]]

        session.register(Article)
        session.sync_schema()

        article = Article(
            title="A",
            tokens=[Vector[2]([1.0, 0.0]), Vector[2]([0.0, 1.0])],
        )
        session.add(article)
        session.commit()

        found = session.get(Article, vid=article.vid)
        assert found is not None
        assert len(found.tokens) == 2
        assert all(isinstance(v, Vector) for v in found.tokens)
        assert found.tokens[0].values == [1.0, 0.0]
        assert found.tokens[1].values == [0.0, 1.0]


class MapNode(UniNode):
    """Model exercising typed MAP<STRING, V> properties (issue #105)."""

    __label__ = "MapNode"

    name: str = Field(unique=True, index="btree")
    scores: dict[str, float] = {}
    nested: dict[str, list[int]] = {}


class TestTypedMapProperty:
    """OGM round-trip for `dict[str, V]` -> typed MAP<STRING, V>."""

    def test_typed_map_roundtrip(self, session):
        session.register(MapNode)
        session.sync_schema()

        n = MapNode(
            name="a",
            scores={"x": 1.5, "y": 2.25},
            nested={"k": [1, 2, 3], "m": [4]},
        )
        session.add(n)
        session.commit()

        found = session.get(MapNode, vid=n.vid)
        assert found is not None
        assert found.scores == {"x": 1.5, "y": 2.25}
        assert found.nested == {"k": [1, 2, 3], "m": [4]}


# ---------------------------------------------------------------------------
# Hydration must not silently drop rows
# ---------------------------------------------------------------------------


class FalsyThing(UniNode):
    """A model that is legitimately falsy when empty.

    Defining ``__bool__`` is ordinary Python, and nothing documents that a
    model may not. Every hydration path gated on ``if instance:`` drops such a
    row on the floor, so ``.all()`` silently returns fewer rows than matched.
    """

    __label__ = "FalsyThing"

    name: str
    count: int = 0

    def __bool__(self) -> bool:
        return self.count > 0


class PickyThing(UniNode):
    """A model whose validator legitimately rejects some stored data."""

    __label__ = "PickyThing"

    name: str

    @field_validator("name")
    @classmethod
    def _reject_bad(cls, v: str) -> str:
        if v == "bad":
            raise ValueError("name must not be 'bad'")
        return v


class BuggyThing(UniNode):
    """A model whose validator raises a *non*-validation error.

    Stands in for any defect inside hydration -- a broken type coercion, a
    typo'd attribute in ``_convert_db_values``, a raising ``@field_validator``.
    ``except Exception: return None`` cannot tell this apart from data that
    genuinely failed validation, so the bug is invisible and the row vanishes.
    """

    __label__ = "BuggyThing"

    name: str

    @field_validator("name")
    @classmethod
    def _boom(cls, v: str) -> str:
        if v == "boom":
            raise RuntimeError("hydration bug, not a validation failure")
        return v


def _raw_create(session, cypher: str) -> None:
    """Insert nodes bypassing the ORM, so model validation runs only on load."""
    tx = session._db_session.tx()
    tx.execute(cypher)
    tx.commit()


class TestHydrationDoesNotDropRows:
    def test_falsy_instance_is_not_dropped(self, session):
        session.register(FalsyThing)
        session.sync_schema()

        _raw_create(
            session,
            "CREATE (:FalsyThing {name: 'empty', count: 0}) "
            "CREATE (:FalsyThing {name: 'full', count: 3})",
        )

        found = session.query(FalsyThing).all()
        names = sorted(f.name for f in found)
        assert names == ["empty", "full"], (
            f"a validly hydrated but falsy model was dropped; got {names}"
        )

    def test_hydration_bug_is_not_swallowed(self, session):
        session.register(BuggyThing)
        session.sync_schema()

        _raw_create(
            session,
            "CREATE (:BuggyThing {name: 'ok'}) CREATE (:BuggyThing {name: 'boom'})",
        )

        # A RuntimeError from inside hydration is a defect, not a data problem.
        # Silently returning one row instead of two hides it completely.
        with pytest.raises(RuntimeError, match="hydration bug"):
            session.query(BuggyThing).all()

    def test_validation_failure_warns_instead_of_vanishing(self, session):
        session.register(PickyThing)
        session.sync_schema()

        _raw_create(
            session,
            "CREATE (:PickyThing {name: 'good'}) CREATE (:PickyThing {name: 'bad'})",
        )

        # Genuinely invalid stored data is still skipped -- but it must say so,
        # naming the label and vid, rather than shrinking the result set in
        # silence.
        with pytest.warns(UserWarning, match="PickyThing"):
            found = session.query(PickyThing).all()

        assert [p.name for p in found] == ["good"]

    def test_valid_rows_are_unaffected(self, session):
        """Inverse guard: narrowing the except must not break ordinary loads."""
        session.register(Person)
        session.sync_schema()

        session.add_all(
            [
                Person(name="Alice", email="alice@drop.test"),
                Person(name="Bob", email="bob@drop.test"),
            ]
        )
        session.commit()

        found = session.query(Person).all()
        assert sorted(p.name for p in found) == ["Alice", "Bob"]


# ---------------------------------------------------------------------------
# eager_load() must hydrate models, exactly as lazy loading does
# ---------------------------------------------------------------------------


class Author(UniNode):
    """Owner of both a to-many and a to-one relationship."""

    __label__ = "Author"

    name: str

    books: list["Book"] = Relationship("WROTE")
    bio: "Bio | None" = Relationship("HAS_BIO")


class Book(UniNode):
    __label__ = "Book"

    title: str


class Bio(UniNode):
    __label__ = "Bio"

    summary: str


class TestEagerLoadHydratesModels:
    """`_eager_load_relationships` cached raw result dicts.

    The lazy path (`_load_relationship`) routes rows through
    `_result_to_model` and honours `descriptor.is_list`; the eager path did
    neither. `RelationshipDescriptor.__get__` returns whatever is cached
    verbatim, so eager loading handed back `list[dict]` where lazy loading
    hands back `list[Model]` — and a to-one relationship came back as a list.

    This is worse on the async session, where `_load_relationship` *raises*
    and tells the caller to use `eager_load()`. There, the broken path is the
    only relationship path there is.
    """

    def _seed(self, session):
        session.register(Author, Book, Bio)
        session.sync_schema()

        author = Author(name="Ursula")
        book = Book(title="A Wizard of Earthsea")
        bio = Bio(summary="Wrote about wizards.")
        session.add_all([author, book, bio])
        session.commit()

        session.create_edge(author, "WROTE", book, {})
        session.create_edge(author, "HAS_BIO", bio, {})
        session._db.flush()
        return author

    def test_to_many_yields_models_not_dicts(self, session):
        self._seed(session)

        found = session.query(Author).eager_load("books").all()
        assert len(found) == 1
        books = found[0].books

        assert len(books) == 1
        assert isinstance(books[0], Book), (
            f"eager_load cached a raw {type(books[0]).__name__}; "
            "attribute access on it fails where lazy loading works"
        )
        # The symptom a user actually hits.
        assert books[0].title == "A Wizard of Earthsea"

    def test_to_one_yields_a_model_not_a_list(self, session):
        self._seed(session)

        found = session.query(Author).eager_load("bio").all()
        bio = found[0].bio

        assert not isinstance(bio, list), (
            "a to-one relationship must not come back as a list; "
            "`descriptor.is_list` was ignored"
        )
        assert isinstance(bio, Bio)
        assert bio.summary == "Wrote about wizards."

    def test_eager_and_lazy_agree(self, session):
        """The two paths must produce the same thing for the same data."""
        self._seed(session)

        eager = session.query(Author).eager_load("books").all()[0].books
        lazy = session.query(Author).all()[0].books

        assert [type(b) for b in eager] == [type(b) for b in lazy]
        assert [b.title for b in eager] == [b.title for b in lazy]

    def test_no_related_rows_is_still_well_typed(self, session):
        """Inverse guard: an author with nothing attached."""
        session.register(Author, Book, Bio)
        session.sync_schema()

        session.add(Author(name="Nobody"))
        session.commit()
        session._db.flush()

        found = session.query(Author).eager_load("books", "bio").all()
        assert found[0].books == []
        assert found[0].bio is None


class TestExplainAndProfileReachTheBuilder:
    """`UniSession.explain`/`.profile` must route through ``query_with()``.

    Both used to call ``self._db_session.explain(cypher)`` directly, but
    ``explain``/``profile`` are not on ``uni_db.Session`` at all -- they live on
    the ``SessionQueryBuilder`` that ``query_with()`` returns. Every call raised
    ``AttributeError: 'builtins.Session' object has no attribute 'explain'``.
    Nothing caught it because no test exercised either method.
    """

    def test_explain_returns_a_plan(self, session):
        out = session.explain("MATCH (n) RETURN n")
        assert out is not None

    def test_profile_returns_results_and_stats(self, session):
        result, stats = session.profile("MATCH (n) RETURN n")
        assert result is not None
        assert stats is not None


class TestCypherResultType:
    """`cypher(..., result_type=M)` hydrates nodes of `M`, and only those.

    The mapper only recognised node-shaped *dicts*, but a bare ``RETURN n``
    yields a ``uni_db.Node`` object -- so the documented use returned ``[]``.
    And a labelled map of another label was hydrated as that label's registered
    model, handing a ``Company`` back from a call typed ``list[Person]``.
    """

    def _setup(self, session):
        session.register(Person, Company)
        session.sync_schema()
        session.add(Company(name="Acme"))
        session.commit()

    def test_return_node_hydrates_result_type(self, session):
        self._setup(session)
        found = session.cypher("MATCH (c:Company) RETURN c", result_type=Company)
        assert len(found) == 1
        assert isinstance(found[0], Company)
        assert found[0].name == "Acme"
        assert found[0].is_persisted
        assert found[0]._session is session

    def test_node_of_other_label_is_not_coerced(self, session):
        self._setup(session)
        assert session.cypher("MATCH (c:Company) RETURN c", result_type=Person) == []

    def test_labelled_map_of_other_label_is_not_returned(self, session):
        self._setup(session)
        query = "RETURN {_label: 'Company', name: 'Acme'} AS c"
        assert session.cypher(query, result_type=Person) == []
        found = session.cypher(query, result_type=Company)
        assert [type(c) for c in found] == [Company]
        assert found[0].name == "Acme"

    def test_unlabelled_map_is_a_projection_of_result_type(self, session):
        self._setup(session)
        found = session.cypher("RETURN {name: 'Acme'} AS c", result_type=Company)
        assert [c.name for c in found] == ["Acme"]

    def test_node_column_is_found_after_scalar_columns(self, session):
        self._setup(session)
        found = session.cypher(
            "MATCH (c:Company) RETURN c.name AS name, c", result_type=Company
        )
        assert [c.name for c in found] == ["Acme"]


def test_repr_treats_id_zero_as_saved():
    """vid/eid 0 is a real id; `__repr__` used to test truthiness and say "unsaved"."""
    node = Company.from_properties({"name": "Acme"}, vid=0)
    assert "vid=0" in repr(node)
    edge = FriendshipEdge.from_properties(
        {"since": date(2020, 1, 1)}, eid=0, src_vid=0, dst_vid=1
    )
    assert "eid=0" in repr(edge)
