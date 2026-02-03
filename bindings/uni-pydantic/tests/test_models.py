# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for UniNode and UniEdge models."""

from datetime import date, datetime

import pytest
from pydantic import ValidationError as PydanticValidationError

from uni_pydantic import (
    Field,
    FilterExpr,
    PropertyProxy,
    Relationship,
    UniEdge,
    UniNode,
    Vector,
    after_create,
    before_create,
)


class TestUniNode:
    """Tests for UniNode base class."""

    def test_basic_model(self):
        """Test basic model definition."""

        class Person(UniNode):
            name: str
            age: int

        person = Person(name="Alice", age=30)
        assert person.name == "Alice"
        assert person.age == 30
        assert person.__label__ == "Person"

    def test_custom_label(self):
        """Test custom label override."""

        class User(UniNode):
            __label__ = "AppUser"
            name: str

        assert User.__label__ == "AppUser"

    def test_optional_fields(self):
        """Test optional (nullable) fields."""

        class Person(UniNode):
            name: str
            nickname: str | None = None

        person = Person(name="Alice")
        assert person.name == "Alice"
        assert person.nickname is None

    def test_default_values(self):
        """Test fields with default values."""

        class Person(UniNode):
            name: str
            active: bool = True
            views: int = 0

        person = Person(name="Alice")
        assert person.active is True
        assert person.views == 0

    def test_default_factory(self):
        """Test fields with default_factory."""

        class Person(UniNode):
            name: str
            tags: list[str] = Field(default_factory=list)

        person = Person(name="Alice")
        assert person.tags == []
        person.tags.append("test")
        assert person.tags == ["test"]

    def test_vector_field(self):
        """Test Vector field returns Vector instances."""

        class Document(UniNode):
            title: str
            embedding: Vector[128]

        vec = [0.1] * 128
        doc = Document(title="Test", embedding=vec)
        assert isinstance(doc.embedding, Vector)
        assert doc.embedding.values == vec

    def test_property_fields(self):
        """Test get_property_fields excludes relationships."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")

        fields = Person.get_property_fields()
        assert "name" in fields
        assert "friends" not in fields

    def test_relationship_fields(self):
        """Test get_relationship_fields."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")

        rels = Person.get_relationship_fields()
        assert "friends" in rels
        assert rels["friends"].edge_type == "FRIEND_OF"

    def test_to_properties(self):
        """Test converting model to property dict."""

        class Person(UniNode):
            name: str
            age: int | None = None

        person = Person(name="Alice", age=30)
        props = person.to_properties()
        assert props["name"] == "Alice"
        assert props["age"] == 30

    def test_to_properties_includes_none(self):
        """Test that to_properties includes None values for null-outs."""

        class Person(UniNode):
            name: str
            age: int | None = None

        person = Person(name="Alice")
        props = person.to_properties()
        assert "name" in props
        assert "age" in props
        assert props["age"] is None

    def test_from_properties(self):
        """Test creating model from property dict."""

        class Person(UniNode):
            name: str
            age: int

        person = Person.from_properties({"name": "Alice", "age": 30})
        assert person.name == "Alice"
        assert person.age == 30

    def test_from_properties_with_id_and_label(self):
        """Test from_properties handles _id and _label from uni-db."""

        class Person(UniNode):
            name: str

        person = Person.from_properties(
            {"_id": 42, "_label": "Person", "name": "Alice"}
        )
        assert person.name == "Alice"
        assert person.vid == 42

    def test_from_properties_does_not_mutate_input(self):
        """Test from_properties doesn't mutate the input dict."""

        class Person(UniNode):
            name: str

        data = {"_id": 42, "_label": "Person", "name": "Alice"}
        original = dict(data)
        Person.from_properties(data)
        assert data == original

    def test_is_persisted(self):
        """Test is_persisted property."""

        class Person(UniNode):
            name: str

        person = Person(name="Alice")
        assert person.is_persisted is False
        assert person.vid is None

    def test_dirty_tracking_clean_after_init(self):
        """Test that fields are NOT dirty after construction."""

        class Person(UniNode):
            name: str
            age: int

        person = Person(name="Alice", age=30)
        assert person.is_dirty is False
        assert len(person._dirty) == 0

    def test_dirty_tracking_after_set(self):
        """Test dirty field tracking after assignment."""

        class Person(UniNode):
            name: str
            age: int

        person = Person(name="Alice", age=30)
        assert person.is_dirty is False

        person.age = 31
        assert person.is_dirty is True
        assert "age" in person._dirty

    def test_dirty_tracking_mark_clean(self):
        """Test _mark_clean clears dirty set."""

        class Person(UniNode):
            name: str
            age: int

        person = Person(name="Alice", age=30)
        person.age = 31
        person._mark_clean()
        assert person.is_dirty is False

    def test_validation(self):
        """Test Pydantic validation."""

        class Person(UniNode):
            name: str
            age: int

        with pytest.raises(PydanticValidationError):
            Person(name="Alice", age="not_an_int")  # type: ignore


class TestFilterDSL:
    """Tests for the __class_getattr__ filter DSL."""

    def test_model_getattr_returns_property_proxy(self):
        """Test that Person.age returns a PropertyProxy."""

        class Person(UniNode):
            name: str
            age: int

        proxy = Person.age  # type: ignore[attr-defined]
        assert isinstance(proxy, PropertyProxy)

    def test_model_getattr_filter_expr(self):
        """Test that Person.age >= 18 returns a FilterExpr."""

        class Person(UniNode):
            name: str
            age: int

        expr = Person.age >= 18  # type: ignore[attr-defined]
        assert isinstance(expr, FilterExpr)
        assert expr.property_name == "age"

    def test_model_getattr_equality(self):
        """Test that Person.name == 'Alice' returns a FilterExpr."""

        class Person(UniNode):
            name: str
            age: int

        expr = Person.name == "Alice"  # type: ignore[attr-defined]
        assert isinstance(expr, FilterExpr)
        assert expr.property_name == "name"
        assert expr.value == "Alice"

    def test_model_getattr_invalid_raises(self):
        """Test that accessing nonexistent field raises AttributeError."""

        class Person(UniNode):
            name: str

        with pytest.raises(AttributeError):
            Person.nonexistent_field  # type: ignore[attr-defined]


class TestUniEdge:
    """Tests for UniEdge base class."""

    def test_basic_edge(self):
        """Test basic edge definition."""

        class Person(UniNode):
            name: str

        class FriendshipEdge(UniEdge):
            __edge_type__ = "FRIEND_OF"
            __from__ = Person
            __to__ = Person

            since: date
            strength: float = 1.0

        edge = FriendshipEdge(since=date(2020, 1, 1))
        assert edge.since == date(2020, 1, 1)
        assert edge.strength == 1.0
        assert edge.__edge_type__ == "FRIEND_OF"

    def test_edge_from_to(self):
        """Test edge from/to labels."""

        class Person(UniNode):
            __label__ = "Person"
            name: str

        class Company(UniNode):
            __label__ = "Company"
            name: str

        class WorksAtEdge(UniEdge):
            __edge_type__ = "WORKS_AT"
            __from__ = Person
            __to__ = Company

            role: str

        assert WorksAtEdge.get_from_labels() == ["Person"]
        assert WorksAtEdge.get_to_labels() == ["Company"]

    def test_edge_to_properties(self):
        """Test edge to_properties."""

        class FriendshipEdge(UniEdge):
            since: date
            strength: float = 1.0

        edge = FriendshipEdge(since=date(2020, 1, 1), strength=0.8)
        props = edge.to_properties()
        # since is a date, so python_to_db_value converts to days
        assert "since" in props
        assert "strength" in props
        assert props["strength"] == 0.8

    def test_edge_from_edge_result(self):
        """Test from_edge_result class method."""

        class FriendshipEdge(UniEdge):
            strength: float = 1.0

        edge = FriendshipEdge.from_edge_result(
            {"_id": 10, "_type": "FRIEND_OF", "_src": 1, "_dst": 2, "strength": 0.9}
        )
        assert edge.eid == 10
        assert edge.src_vid == 1
        assert edge.dst_vid == 2
        assert edge.strength == 0.9


class TestLifecycleHooks:
    """Tests for lifecycle hooks."""

    def test_before_create_hook(self):
        """Test before_create hook."""
        called = []

        class Person(UniNode):
            name: str
            created_at: datetime | None = None

            @before_create
            def set_created(self):
                called.append("before_create")
                self.created_at = datetime(2020, 1, 1)

        _person = Person(name="Alice")
        # Hook marker should be set
        assert hasattr(Person.set_created, "_uni_before_create")

    def test_after_create_hook(self):
        """Test after_create hook."""
        called = []

        class Person(UniNode):
            name: str

            @after_create
            def log_created(self):
                called.append(f"created:{self.name}")

        _person = Person(name="Alice")
        assert hasattr(Person.log_created, "_uni_after_create")


class TestFieldConfiguration:
    """Tests for Field configuration."""

    def test_index_field(self):
        """Test indexed field."""

        class Person(UniNode):
            email: str = Field(index="btree")

        info = Person.model_fields["email"]
        assert info.json_schema_extra is not None
        config = info.json_schema_extra.get("uni_config")
        assert config.index == "btree"

    def test_unique_field(self):
        """Test unique field."""

        class Person(UniNode):
            email: str = Field(unique=True)

        info = Person.model_fields["email"]
        config = info.json_schema_extra.get("uni_config")
        assert config.unique is True

    def test_fulltext_field(self):
        """Test fulltext indexed field."""

        class Article(UniNode):
            content: str = Field(index="fulltext", tokenizer="standard")

        info = Article.model_fields["content"]
        config = info.json_schema_extra.get("uni_config")
        assert config.index == "fulltext"
        assert config.tokenizer == "standard"

    def test_fulltext_field_default_tokenizer(self):
        """Test fulltext indexed field defaults tokenizer to 'standard'."""

        class Article(UniNode):
            content: str = Field(index="fulltext")

        info = Article.model_fields["content"]
        config = info.json_schema_extra.get("uni_config")
        assert config.index == "fulltext"
        assert config.tokenizer == "standard"

    def test_vector_metric(self):
        """Test vector field with metric."""

        class Document(UniNode):
            embedding: Vector[128] = Field(metric="cosine")

        info = Document.model_fields["embedding"]
        config = info.json_schema_extra.get("uni_config")
        assert config.metric == "cosine"


class TestRelationshipDeclaration:
    """Tests for Relationship declaration."""

    def test_basic_relationship(self):
        """Test basic relationship."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")

        rels = Person.get_relationship_fields()
        assert "friends" in rels
        assert rels["friends"].edge_type == "FRIEND_OF"
        assert rels["friends"].direction == "outgoing"

    def test_incoming_relationship(self):
        """Test incoming relationship."""

        class Person(UniNode):
            name: str
            followers: list["Person"] = Relationship("FOLLOWS", direction="incoming")

        rels = Person.get_relationship_fields()
        assert rels["followers"].direction == "incoming"

    def test_bidirectional_relationship(self):
        """Test bidirectional relationship."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

        rels = Person.get_relationship_fields()
        assert rels["friends"].direction == "both"

    def test_optional_single_relationship(self):
        """Test optional single relationship."""

        class Person(UniNode):
            name: str
            manager: "Person | None" = Relationship("REPORTS_TO")

        rels = Person.get_relationship_fields()
        assert "manager" in rels
