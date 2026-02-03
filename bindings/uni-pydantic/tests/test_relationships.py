# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for relationship handling."""

from uni_pydantic import (
    Relationship,
    RelationshipConfig,
    RelationshipDescriptor,
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
