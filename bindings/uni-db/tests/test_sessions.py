# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for Session API."""

import tempfile

import pytest

import uni_db


class TestSessionBuilder:
    """Tests for SessionBuilder functionality."""

    @pytest.fixture
    def db(self):
        """Create a database with test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Person")
            db.add_property("Person", "name", "string", False)
            db.add_property("Person", "age", "int", False)
            db.query("CREATE (n:Person {name: 'Alice', age: 30})")
            db.query("CREATE (n:Person {name: 'Bob', age: 25})")
            db.flush()
            yield db

    def test_session_with_variable(self, db):
        """Test creating a session with a variable."""
        session_builder = db.session()
        session_builder.set("user_name", "Alice")
        session = session_builder.build()

        # The session variable should be accessible via session.get()
        name = session.get("user_name")
        assert name == "Alice"

    def test_session_query(self, db):
        """Test executing a query through a session."""
        session = db.session().build()
        results = session.query("MATCH (n:Person) RETURN n.name")
        assert len(results) == 2

    def test_session_execute(self, db):
        """Test executing a mutation through a session."""
        session = db.session().build()
        affected = session.execute("CREATE (n:Person {name: 'Charlie', age: 35})")
        assert affected >= 0

        # Verify the node was created
        results = session.query(
            "MATCH (n:Person {name: 'Charlie'}) RETURN n.age AS age"
        )
        assert len(results) == 1
        assert results[0]["age"] == 35

    def test_multiple_session_variables(self, db):
        """Test session with multiple variables."""
        builder = db.session()
        builder.set("var1", "value1")
        builder.set("var2", 42)
        builder.set("var3", True)
        session = builder.build()

        assert session.get("var1") == "value1"
        assert session.get("var2") == 42
        assert session.get("var3") is True

    def test_session_get_nonexistent(self, db):
        """Test getting a nonexistent session variable."""
        session = db.session().build()
        result = session.get("nonexistent")
        assert result is None
