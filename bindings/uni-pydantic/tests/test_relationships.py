# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for relationship handling."""

import inspect
import sys
import types

import pytest

from uni_pydantic import (
    Relationship,
    RelationshipConfig,
    RelationshipDescriptor,
    SchemaGenerator,
    UniEdge,
    UniNode,
)
from uni_pydantic.fields import _RelationshipMarker


class TestRelationshipConfig:
    """Tests for RelationshipConfig."""

    def test_default_direction(self):
        """Test default direction is outgoing."""
        config = RelationshipConfig(edge_type="TEST")
        assert config.direction == "outgoing"

    def test_custom_direction(self):
        """Test custom direction."""
        config = RelationshipConfig(edge_type="TEST", direction="incoming")
        assert config.direction == "incoming"

    def test_edge_model(self):
        """Test edge model attachment."""

        class TestEdge(UniEdge):
            strength: float

        config = RelationshipConfig(edge_type="TEST", edge_model=TestEdge)
        assert config.edge_model is TestEdge

    def test_eager_loading(self):
        """Test eager loading flag."""
        config = RelationshipConfig(edge_type="TEST", eager=True)
        assert config.eager is True

    def test_cascade_delete(self):
        """Test cascade delete flag."""
        config = RelationshipConfig(edge_type="TEST", cascade_delete=True)
        assert config.cascade_delete is True


class TestRelationshipMarker:
    """Tests for _RelationshipMarker."""

    def test_marker_creation(self):
        """Test creating relationship marker."""
        config = RelationshipConfig(edge_type="TEST")
        marker = _RelationshipMarker(config)
        assert marker.config.edge_type == "TEST"

    def test_relationship_function_returns_marker(self):
        """Test Relationship() returns marker."""
        result = Relationship("TEST_EDGE")
        assert isinstance(result, _RelationshipMarker)
        assert result.config.edge_type == "TEST_EDGE"


class TestRelationshipDescriptor:
    """Tests for RelationshipDescriptor."""

    def test_descriptor_creation(self):
        """Test creating descriptor."""
        config = RelationshipConfig(edge_type="TEST")
        desc = RelationshipDescriptor(config, "friends")
        assert desc.field_name == "friends"
        assert desc.is_list is True

    def test_descriptor_single_relationship(self):
        """Test descriptor for single relationship."""
        config = RelationshipConfig(edge_type="TEST")
        desc = RelationshipDescriptor(config, "manager", is_list=False)
        assert desc.is_list is False

    def test_class_level_access_returns_descriptor(self):
        """Test accessing on class returns descriptor."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")

        # Class-level access should return the descriptor
        descriptor = Person.friends
        assert isinstance(descriptor, RelationshipDescriptor)


class TestRelationshipInModels:
    """Tests for relationships defined in models."""

    def test_outgoing_relationship(self):
        """Test outgoing relationship definition."""

        class Person(UniNode):
            name: str
            follows: list["Person"] = Relationship("FOLLOWS")

        rels = Person.get_relationship_fields()
        assert "follows" in rels
        assert rels["follows"].edge_type == "FOLLOWS"
        assert rels["follows"].direction == "outgoing"

    def test_incoming_relationship(self):
        """Test incoming relationship definition."""

        class Person(UniNode):
            name: str
            followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

        rels = Person.get_relationship_fields()
        assert rels["followers"].direction == "incoming"

    def test_bidirectional_relationship(self):
        """Test bidirectional relationship definition."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

        rels = Person.get_relationship_fields()
        assert rels["friends"].direction == "both"

    def test_multiple_relationships(self):
        """Test multiple relationships on same model."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")
            follows: list["Person"] = Relationship("FOLLOWS")
            followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

        rels = Person.get_relationship_fields()
        assert len(rels) == 3
        assert all(name in rels for name in ["friends", "follows", "followers"])

    def test_relationship_to_different_type(self):
        """Test relationship to different node type."""

        class Person(UniNode):
            name: str
            works_at: "Company | None" = Relationship("WORKS_AT")

        class Company(UniNode):
            name: str
            employees: list[Person] = Relationship("WORKS_AT", direction="incoming")

        person_rels = Person.get_relationship_fields()
        company_rels = Company.get_relationship_fields()

        assert "works_at" in person_rels
        assert "employees" in company_rels

    def test_relationship_with_edge_model(self):
        """Test relationship with edge model."""

        class Person(UniNode):
            name: str

        class FriendshipEdge(UniEdge):
            since: str
            strength: float

        class PersonWithEdge(UniNode):
            name: str
            friendships: list[tuple["PersonWithEdge", FriendshipEdge]] = Relationship(
                "FRIEND_OF",
                edge_model=FriendshipEdge,
            )

        rels = PersonWithEdge.get_relationship_fields()
        assert rels["friendships"].edge_model is FriendshipEdge


# Model source compiled under both annotation regimes. Unquoted references to
# models defined *later* (`Book`, `Bio`) are legal only where annotations are not
# evaluated eagerly: always under `from __future__ import annotations`, and on
# Python 3.14+ (PEP 649/749 lazy annotations) without it.
_PEP649_MODELS = """
from uni_pydantic import Relationship, UniNode


class Author(UniNode):
    name: str
    books: list[Book] = Relationship("WROTE")
    bio: Bio | None = Relationship("HAS_BIO")


class Book(UniNode):
    title: str


class Bio(UniNode):
    summary: str


class Unrelated(UniNode):
    note: str
"""


def _load_models(future_import: bool) -> types.ModuleType:
    """Execute `_PEP649_MODELS` as a real module (so annotation evaluation and
    `typing.get_type_hints` find its globals), with or without the future import."""
    name = f"_pep649_models_{'future' if future_import else 'native'}"
    module = types.ModuleType(name)
    sys.modules[name] = module
    header = "from __future__ import annotations\n" if future_import else ""
    exec(compile(header + _PEP649_MODELS, f"<{name}>", "exec"), module.__dict__)
    return module


@pytest.mark.parametrize(
    "future_import",
    [
        pytest.param(True, id="future-annotations"),
        pytest.param(
            False,
            id="native-annotations",
            marks=pytest.mark.skipif(
                sys.version_info < (3, 14),
                reason="unquoted forward references need PEP 649 lazy annotations",
            ),
        ),
    ],
)
class TestRelationshipsUnderLazyAnnotations:
    """Regression: on Python 3.14 (PEP 649/749) a class body without
    `from __future__ import annotations` puts no `__annotations__` dict in the
    namespace the metaclass receives -- only a lazy annotate function. The
    metaclass read `namespace.get("__annotations__", {})`, saw no annotations,
    and so missed every `Relationship(...)` marker: the marker stayed a pydantic
    field default, schema sync declared the relationship as a `json` property
    with no edge type, and writes failed with "cannot convert Python object of
    type '_RelationshipMarker' to a Uni value"."""

    def test_relationships_are_discovered(self, future_import):
        m = _load_models(future_import)
        rels = m.Author.get_relationship_fields()
        assert set(rels) == {"books", "bio"}
        assert rels["books"].edge_type == "WROTE"
        assert rels["bio"].edge_type == "HAS_BIO"

    def test_relationships_are_not_pydantic_fields(self, future_import):
        m = _load_models(future_import)
        assert set(m.Author.model_fields) == {"name"}
        # Every annotation reader -- not just pydantic -- must see the filtered set.
        assert set(inspect.get_annotations(m.Author)) == {"name"}
        author = m.Author(name="Ann")
        assert "books" not in author.model_dump()
        assert "bio" not in author.model_dump()
        assert isinstance(m.Author.__dict__["books"], RelationshipDescriptor)

    def test_relationship_shape_is_captured(self, future_import):
        m = _load_models(future_import)
        books = m.Author.__dict__["books"]
        bio = m.Author.__dict__["bio"]
        assert books.is_list is True
        assert bio.is_list is False
        # Lazy annotations give an undefined name as a ForwardRef, the future
        # import gives the whole annotation as a string (`"list[Book]"`). Either
        # way the list/optional shape must be recognized -- under the future
        # import `is_list` used to be False, so a to-many relationship loaded a
        # single node -- and the target carried as the bare model name.
        assert books.target_type == "Book"
        assert bio.target_type == "Bio"

    def test_schema_declares_edges_not_json_properties(self, future_import):
        m = _load_models(future_import)
        gen = SchemaGenerator()
        for model in (m.Author, m.Book, m.Bio, m.Unrelated):
            gen.register_node(model)
        schema = gen.generate()
        assert set(schema.labels["Author"].properties) == {"name"}
        wrote = schema.edge_types["WROTE"]
        assert (wrote.from_labels, wrote.to_labels) == (["Author"], ["Book"])
        has_bio = schema.edge_types["HAS_BIO"]
        assert (has_bio.from_labels, has_bio.to_labels) == (["Author"], ["Bio"])

    def test_to_many_loads_every_target(self, future_import, session):
        m = _load_models(future_import)
        session.register(m.Author, m.Book, m.Bio)
        session.sync_schema()
        author = m.Author(name="Ursula")
        books = [m.Book(title="Earthsea"), m.Book(title="Lathe")]
        session.add_all([author, *books])
        session.commit()
        for book in books:
            session.create_edge(author, "WROTE", book, {})
        session._db.flush()

        loaded = session.query(m.Author).eager_load("books").all()[0].books
        assert isinstance(loaded, list)
        assert sorted(b.title for b in loaded) == ["Earthsea", "Lathe"]


class TestStringAnnotationShape:
    """`_string_annotation_shape` reads list/optional shape from source text."""

    @pytest.mark.parametrize(
        ("source", "expected"),
        [
            ("Book", (False, "Book")),
            ("Bio | None", (False, "Bio")),
            ("None | Bio", (False, "Bio")),
            ("Optional[Bio]", (False, "Bio")),
            ("typing.Optional[Bio]", (False, "Bio")),
            ("Union[Bio, None]", (False, "Bio")),
            ("list[Book]", (True, "Book")),
            ("List[Book]", (True, "Book")),
            ("typing.List[Book]", (True, "Book")),
            ("list['Book']", (True, "Book")),
            ("list[Book] | None", (True, "Book")),
            ("Optional[list[Book]]", (True, "Book")),
            ("list[tuple[Person, Edge]]", (True, "tuple[Person, Edge]")),
            ("Union[Book, Bio]", (False, "Union[Book, Bio]")),
            ("not valid (", (False, "not valid (")),
        ],
    )
    def test_shape(self, source, expected):
        from uni_pydantic.base import _string_annotation_shape

        assert _string_annotation_shape(source) == expected
