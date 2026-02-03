# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for async schema operations.

Includes tests for both direct methods (create_label, add_property, etc.)
and the AsyncSchemaBuilder pattern (db.schema().label(...).apply()).
"""

import pytest

import uni_db


class TestAsyncSchemaCreation:
    """Tests for async schema creation using direct methods."""

    @pytest.fixture
    async def db(self):
        """Create a temporary async database."""
        return await uni_db.AsyncDatabase.temporary()

    @pytest.mark.asyncio
    async def test_create_label_and_properties(self, db):
        """Test creating a label with properties."""
        await db.create_label("Person")
        await db.add_property("Person", "name", "string", False)
        await db.add_property("Person", "age", "int", False)

        assert await db.label_exists("Person")
        await db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        await db.flush()
        results = await db.query("MATCH (n:Person) RETURN n.name, n.age")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_create_label_with_nullable(self, db):
        """Test creating a label with nullable property."""
        await db.create_label("Person")
        await db.add_property("Person", "name", "string", False)
        await db.add_property("Person", "nickname", "string", True)

        await db.execute("CREATE (n:Person {name: 'Bob'})")
        await db.flush()
        results = await db.query("MATCH (n:Person) RETURN n.name, n.nickname")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_create_edge_type(self, db):
        """Test creating an edge type."""
        await db.create_label("Person")
        await db.add_property("Person", "name", "string", False)
        await db.create_edge_type("KNOWS", ["Person"], ["Person"])

        assert await db.edge_type_exists("KNOWS")

    @pytest.mark.asyncio
    async def test_create_multiple_labels_and_edges(self, db):
        """Test creating multiple labels and edge types."""
        await db.create_label("Person")
        await db.add_property("Person", "name", "string", False)
        await db.create_label("Company")
        await db.add_property("Company", "name", "string", False)
        await db.create_edge_type("WORKS_AT", ["Person"], ["Company"])

        assert await db.label_exists("Person")
        assert await db.label_exists("Company")
        assert await db.edge_type_exists("WORKS_AT")


class TestAsyncSchemaQueries:
    """Tests for async schema query methods."""

    @pytest.fixture
    async def db_with_schema(self):
        """Create a database with a predefined schema."""
        db = await uni_db.AsyncDatabase.temporary()
        await db.create_label("Person")
        await db.add_property("Person", "name", "string", False)
        await db.add_property("Person", "age", "int", False)
        await db.create_label("Company")
        await db.add_property("Company", "name", "string", False)
        await db.create_edge_type("WORKS_AT", ["Person"], ["Company"])
        return db

    @pytest.mark.asyncio
    async def test_label_exists(self, db_with_schema):
        """Test checking if a label exists."""
        assert await db_with_schema.label_exists("Person")
        assert await db_with_schema.label_exists("Company")
        assert not await db_with_schema.label_exists("NonExistent")

    @pytest.mark.asyncio
    async def test_edge_type_exists(self, db_with_schema):
        """Test checking if an edge type exists."""
        assert await db_with_schema.edge_type_exists("WORKS_AT")
        assert not await db_with_schema.edge_type_exists("KNOWS")

    @pytest.mark.asyncio
    async def test_list_labels(self, db_with_schema):
        """Test listing all labels."""
        labels = await db_with_schema.list_labels()
        assert "Person" in labels
        assert "Company" in labels

    @pytest.mark.asyncio
    async def test_list_edge_types(self, db_with_schema):
        """Test listing all edge types."""
        edge_types = await db_with_schema.list_edge_types()
        assert "WORKS_AT" in edge_types

    @pytest.mark.asyncio
    async def test_get_label_info(self, db_with_schema):
        """Test getting label information."""
        info = await db_with_schema.get_label_info("Person")
        assert info is not None
        assert info.name == "Person"
        assert len(info.properties) >= 2


class TestAsyncDataTypes:
    """Tests for different data types in async schema."""

    @pytest.fixture
    async def db(self):
        """Create a temporary async database."""
        return await uni_db.AsyncDatabase.temporary()

    @pytest.mark.asyncio
    async def test_string_type(self, db):
        """Test string data type."""
        await db.create_label("Test")
        await db.add_property("Test", "text", "string", False)
        await db.execute("CREATE (n:Test {text: 'hello world'})")
        await db.flush()
        results = await db.query("MATCH (n:Test) RETURN n.text")
        assert results[0]["n.text"] == "hello world"

    @pytest.mark.asyncio
    async def test_int_type(self, db):
        """Test integer data type."""
        await db.create_label("Test")
        await db.add_property("Test", "num", "int", False)
        await db.execute("CREATE (n:Test {num: 42})")
        await db.flush()
        results = await db.query("MATCH (n:Test) RETURN n.num")
        assert results[0]["n.num"] == 42

    @pytest.mark.asyncio
    async def test_float_type(self, db):
        """Test float data type."""
        await db.create_label("Test")
        await db.add_property("Test", "value", "float", False)
        await db.execute("CREATE (n:Test {value: 3.14})")
        await db.flush()
        results = await db.query("MATCH (n:Test) RETURN n.value")
        assert abs(results[0]["n.value"] - 3.14) < 0.001

    @pytest.mark.asyncio
    async def test_bool_type(self, db):
        """Test boolean data type."""
        await db.create_label("Test")
        await db.add_property("Test", "active", "bool", False)
        await db.execute("CREATE (n:Test {active: true})")
        await db.flush()
        results = await db.query("MATCH (n:Test) RETURN n.active")
        assert results[0]["n.active"] is True


class TestAsyncSchemaBuilder:
    """Tests for the AsyncSchemaBuilder pattern (db.schema())."""

    @pytest.fixture
    async def db(self):
        """Create a temporary async database."""
        return await uni_db.AsyncDatabase.temporary()

    @pytest.mark.asyncio
    async def test_schema_builder_label(self, db):
        """Test creating a label via schema builder."""
        await (
            db.schema()
            .label("Person")
            .property("name", "string")
            .property("age", "int")
            .done()
            .apply()
        )

        assert await db.label_exists("Person")
        await db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        await db.flush()
        results = await db.query("MATCH (n:Person) RETURN n.name, n.age")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_schema_builder_edge_type(self, db):
        """Test creating an edge type via schema builder."""
        await (
            db.schema()
            .label("Person")
            .property("name", "string")
            .done()
            .label("Company")
            .property("name", "string")
            .done()
            .edge_type("WORKS_AT", ["Person"], ["Company"])
            .done()
            .apply()
        )

        assert await db.label_exists("Person")
        assert await db.label_exists("Company")
        assert await db.edge_type_exists("WORKS_AT")

    @pytest.mark.asyncio
    async def test_schema_builder_label_apply_shortcut(self, db):
        """Test applying schema directly from label builder."""
        await db.schema().label("Item").property("name", "string").apply()
        assert await db.label_exists("Item")

    @pytest.mark.asyncio
    async def test_schema_builder_nullable_property(self, db):
        """Test creating a label with nullable property."""
        await (
            db.schema()
            .label("Contact")
            .property("name", "string")
            .property_nullable("email", "string")
            .done()
            .apply()
        )

        assert await db.label_exists("Contact")
        await db.execute("CREATE (n:Contact {name: 'Bob'})")
        await db.flush()
        results = await db.query("MATCH (n:Contact) RETURN n.name")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_schema_builder_edge_type_with_properties(self, db):
        """Test creating an edge type with properties."""
        await (
            db.schema()
            .label("Person")
            .property("name", "string")
            .done()
            .edge_type("KNOWS", ["Person"], ["Person"])
            .property("since", "int")
            .done()
            .apply()
        )

        assert await db.edge_type_exists("KNOWS")
