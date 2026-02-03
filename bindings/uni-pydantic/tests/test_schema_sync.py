# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for schema generation and synchronization."""

from datetime import date

from uni_pydantic import (
    EdgeTypeSchema,
    Field,
    LabelSchema,
    PropertySchema,
    Relationship,
    SchemaGenerator,
    UniEdge,
    UniNode,
    Vector,
    generate_schema,
)


class TestSchemaGenerator:
    """Tests for SchemaGenerator."""

    def test_register_node_model(self):
        """Test registering a node model."""

        class Person(UniNode):
            name: str

        gen = SchemaGenerator()
        gen.register_node(Person)
        assert "Person" in gen._node_models

    def test_register_edge_model(self):
        """Test registering an edge model."""

        class Person(UniNode):
            name: str

        class FriendshipEdge(UniEdge):
            __edge_type__ = "FRIEND_OF"
            __from__ = Person
            __to__ = Person
            since: date

        gen = SchemaGenerator()
        gen.register_edge(FriendshipEdge)
        assert "FRIEND_OF" in gen._edge_models

    def test_register_multiple(self):
        """Test registering multiple models at once."""

        class Person(UniNode):
            name: str

        class Company(UniNode):
            name: str

        gen = SchemaGenerator()
        gen.register(Person, Company)
        assert "Person" in gen._node_models
        assert "Company" in gen._node_models

    def test_generate_basic_schema(self):
        """Test generating schema for basic model."""

        class Person(UniNode):
            name: str
            age: int

        gen = SchemaGenerator()
        gen.register(Person)
        schema = gen.generate()

        assert "Person" in schema.labels
        label_schema = schema.labels["Person"]
        assert "name" in label_schema.properties
        assert "age" in label_schema.properties
        assert label_schema.properties["name"].data_type == "string"
        assert label_schema.properties["age"].data_type == "int64"

    def test_generate_nullable_field(self):
        """Test generating schema for nullable field."""

        class Person(UniNode):
            name: str
            nickname: str | None = None

        gen = SchemaGenerator()
        gen.register(Person)
        schema = gen.generate()

        assert schema.labels["Person"].properties["name"].nullable is False
        assert schema.labels["Person"].properties["nickname"].nullable is True

    def test_generate_indexed_field(self):
        """Test generating schema for indexed field."""

        class Person(UniNode):
            email: str = Field(index="btree")

        gen = SchemaGenerator()
        gen.register(Person)
        schema = gen.generate()

        prop = schema.labels["Person"].properties["email"]
        assert prop.index_type == "btree"

    def test_generate_unique_field(self):
        """Test generating schema for unique field."""

        class Person(UniNode):
            email: str = Field(unique=True)

        gen = SchemaGenerator()
        gen.register(Person)
        schema = gen.generate()

        prop = schema.labels["Person"].properties["email"]
        assert prop.unique is True

    def test_generate_vector_field(self):
        """Test generating schema for vector field."""

        class Document(UniNode):
            title: str
            embedding: Vector[128]

        gen = SchemaGenerator()
        gen.register(Document)
        schema = gen.generate()

        prop = schema.labels["Document"].properties["embedding"]
        assert prop.data_type == "vector:128"
        assert prop.index_type == "vector"  # Auto-indexed

    def test_generate_vector_with_metric(self):
        """Test generating schema for vector with metric."""

        class Document(UniNode):
            embedding: Vector[128] = Field(metric="cosine")

        gen = SchemaGenerator()
        gen.register(Document)
        schema = gen.generate()

        prop = schema.labels["Document"].properties["embedding"]
        assert prop.metric == "cosine"

    def test_generate_fulltext_field(self):
        """Test generating schema for fulltext field."""

        class Article(UniNode):
            content: str = Field(index="fulltext", tokenizer="standard")

        gen = SchemaGenerator()
        gen.register(Article)
        schema = gen.generate()

        prop = schema.labels["Article"].properties["content"]
        assert prop.index_type == "fulltext"
        assert prop.tokenizer == "standard"

    def test_generate_edge_schema(self):
        """Test generating schema for edge model."""

        class Person(UniNode):
            name: str

        class FriendshipEdge(UniEdge):
            __edge_type__ = "FRIEND_OF"
            __from__ = Person
            __to__ = Person

            since: date
            strength: float = 1.0

        gen = SchemaGenerator()
        gen.register(Person, FriendshipEdge)
        schema = gen.generate()

        assert "FRIEND_OF" in schema.edge_types
        edge_schema = schema.edge_types["FRIEND_OF"]
        assert edge_schema.from_labels == ["Person"]
        assert edge_schema.to_labels == ["Person"]
        assert "since" in edge_schema.properties
        assert "strength" in edge_schema.properties

    def test_generate_relationship_creates_edge_type(self):
        """Test that relationships create edge types in schema."""

        class Person(UniNode):
            name: str
            friends: list["Person"] = Relationship("FRIEND_OF")

        gen = SchemaGenerator()
        gen.register(Person)
        schema = gen.generate()

        assert "FRIEND_OF" in schema.edge_types


class TestGenerateSchema:
    """Tests for generate_schema convenience function."""

    def test_generate_schema_function(self):
        """Test generate_schema function."""

        class Person(UniNode):
            name: str

        class Company(UniNode):
            name: str

        schema = generate_schema(Person, Company)
        assert "Person" in schema.labels
        assert "Company" in schema.labels


class TestPropertySchema:
    """Tests for PropertySchema dataclass."""

    def test_property_schema_defaults(self):
        """Test PropertySchema default values."""
        prop = PropertySchema(name="test", data_type="string")
        assert prop.nullable is False
        assert prop.index_type is None
        assert prop.unique is False


class TestLabelSchema:
    """Tests for LabelSchema dataclass."""

    def test_label_schema_defaults(self):
        """Test LabelSchema default values."""
        label = LabelSchema(name="Test")
        assert label.properties == {}


class TestEdgeTypeSchema:
    """Tests for EdgeTypeSchema dataclass."""

    def test_edge_type_schema_defaults(self):
        """Test EdgeTypeSchema default values."""
        edge = EdgeTypeSchema(name="TEST_EDGE")
        assert edge.from_labels == []
        assert edge.to_labels == []
        assert edge.properties == {}
