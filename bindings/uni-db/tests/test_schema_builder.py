# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for SchemaBuilder API."""

import pytest

import uni_db


class TestSchemaBuilder:
    """Tests for SchemaBuilder functionality."""

    @pytest.fixture
    def db(self):
        """Create a temporary database.

        Uses ``UniBuilder.temporary()`` (Rust-owned temp dir) rather than an
        external ``tempfile.TemporaryDirectory``: the latter races the database's
        background flush/index-build threads, which keep writing after the test
        body returns, so ``rmtree`` can hit "Directory not empty". With
        ``temporary()`` the Rust side stops those threads on drop and removes its
        own temp dir, so teardown is race-free.
        """
        return uni_db.UniBuilder.temporary().build()

    def test_label_builder_basic(self, db):
        """Test creating a label with basic properties."""
        db.schema().label("Person").property("name", "string").property(
            "age", "int"
        ).apply()

        assert db.label_exists("Person")
        # Verify we can insert data
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        tx.commit()
        db.flush()
        results = session.query("MATCH (n:Person) RETURN n.name, n.age")
        assert len(results) == 1

    def test_label_builder_nullable(self, db):
        """Test creating a label with nullable property."""
        db.schema().label("Person").property("name", "string").property_nullable(
            "nickname", "string"
        ).apply()

        # Insert without nickname
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (n:Person {name: 'Bob'})")
        tx.commit()
        db.flush()
        results = session.query("MATCH (n:Person) RETURN n.name, n.nickname")
        assert len(results) == 1

    def test_label_builder_with_index(self, db):
        """Test creating a label with an index."""
        db.schema().label("Person").property("name", "string").index(
            "name", "btree"
        ).apply()

        assert db.label_exists("Person")

    def test_edge_type_builder(self, db):
        """Test creating an edge type."""
        db.schema().label("Person").property("name", "string").apply()

        db.schema().edge_type("KNOWS", ["Person"], ["Person"]).property(
            "since", "int"
        ).apply()

        assert db.edge_type_exists("KNOWS")

    def test_chained_schema_building(self, db):
        """Test building multiple labels and edge types."""
        schema = db.schema()
        schema = schema.label("Person").property("name", "string").done()
        schema = schema.label("Company").property("name", "string").done()
        schema = (
            schema.edge_type("WORKS_AT", ["Person"], ["Company"])
            .property("role", "string")
            .done()
        )
        schema.apply()

        assert db.label_exists("Person")
        assert db.label_exists("Company")
        assert db.edge_type_exists("WORKS_AT")


class TestSchemaQueries:
    """Tests for schema query methods."""

    @pytest.fixture
    def db_with_schema(self):
        """Create a database with a predefined schema."""
        db = uni_db.UniBuilder.temporary().build()
        (
            db.schema()
            .label("Person")
            .property("name", "string")
            .property("age", "int")
            .done()
            .label("Company")
            .property("name", "string")
            .done()
            .edge_type("WORKS_AT", ["Person"], ["Company"])
            .done()
            .apply()
        )
        return db

    def test_label_exists(self, db_with_schema):
        """Test checking if a label exists."""
        assert db_with_schema.label_exists("Person")
        assert db_with_schema.label_exists("Company")
        assert not db_with_schema.label_exists("NonExistent")

    def test_edge_type_exists(self, db_with_schema):
        """Test checking if an edge type exists."""
        assert db_with_schema.edge_type_exists("WORKS_AT")
        assert not db_with_schema.edge_type_exists("KNOWS")

    def test_list_labels(self, db_with_schema):
        """Test listing all labels."""
        labels = db_with_schema.list_labels()
        assert "Person" in labels
        assert "Company" in labels

    def test_list_edge_types(self, db_with_schema):
        """Test listing all edge types."""
        edge_types = db_with_schema.list_edge_types()
        assert "WORKS_AT" in edge_types

    def test_get_label_info(self, db_with_schema):
        """Test getting label information."""
        info = db_with_schema.get_label_info("Person")
        assert info is not None
        assert info.name == "Person"
        assert len(info.properties) >= 2  # name and age


class TestDataTypes:
    """Tests for different data types in schema."""

    @pytest.fixture
    def db(self):
        """Create a temporary database.

        Uses ``UniBuilder.temporary()`` (Rust-owned temp dir) rather than an
        external ``tempfile.TemporaryDirectory``: the latter races the database's
        background flush/index-build threads, which keep writing after the test
        body returns, so ``rmtree`` can hit "Directory not empty". With
        ``temporary()`` the Rust side stops those threads on drop and removes its
        own temp dir, so teardown is race-free.
        """
        return uni_db.UniBuilder.temporary().build()

    def test_string_type(self, db):
        """Test string data type."""
        db.schema().label("Test").property("text", "string").apply()
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (n:Test {text: 'hello world'})")
        tx.commit()
        db.flush()
        results = session.query("MATCH (n:Test) RETURN n.text")
        assert results[0]["n.text"] == "hello world"

    def test_int_type(self, db):
        """Test integer data type."""
        db.schema().label("Test").property("num", "int").apply()
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (n:Test {num: 42})")
        tx.commit()
        db.flush()
        results = session.query("MATCH (n:Test) RETURN n.num")
        assert results[0]["n.num"] == 42

    def test_float_type(self, db):
        """Test float data type."""
        db.schema().label("Test").property("value", "float").apply()
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (n:Test {value: 3.14})")
        tx.commit()
        db.flush()
        results = session.query("MATCH (n:Test) RETURN n.value")
        assert abs(results[0]["n.value"] - 3.14) < 0.001

    def test_bool_type(self, db):
        """Test boolean data type."""
        db.schema().label("Test").property("active", "bool").apply()
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (n:Test {active: true})")
        tx.commit()
        db.flush()
        results = session.query("MATCH (n:Test) RETURN n.active")
        assert results[0]["n.active"] is True


def test_scalar_index_on_a_nested_type_is_refused():
    """A BTree on a DATETIME (a nested column) can never build, so it is refused."""
    db = uni_db.Uni.temporary()
    with pytest.raises(Exception, match="ts"):
        (
            db.schema()
            .label("Message")
            .property("ts", "datetime")
            .index("ts", "btree")
            .apply()
        )
    db.schema().label("Event").property("n", "int").index("n", "btree").apply()
