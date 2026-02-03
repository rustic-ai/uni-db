# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for DatabaseBuilder API."""

import os
import tempfile

import pytest

import uni_db


class TestDatabaseBuilderOpenModes:
    """Tests for different database open modes."""

    def test_create_new_database(self):
        """Test creating a new database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = uni_db.DatabaseBuilder.create(path).build()
            assert db is not None
            # Database should be empty
            db.create_label("Test")
            results = db.query("MATCH (n:Test) RETURN n")
            assert len(results) == 0

    def test_create_fails_if_exists(self):
        """Test that create() fails if database exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            # Create first
            db1 = uni_db.DatabaseBuilder.create(path).build()
            db1.create_label("Test")
            db1.flush()
            del db1

            # Create again should fail
            with pytest.raises(OSError):
                uni_db.DatabaseBuilder.create(path).build()

    def test_open_existing_database(self):
        """Test opening an existing database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            # Create and add data
            db1 = uni_db.DatabaseBuilder.create(path).build()
            db1.create_label("Person")
            db1.add_property("Person", "name", "string", False)
            db1.query("CREATE (n:Person {name: 'Alice'})")
            db1.flush()
            del db1

            # Open existing
            db2 = uni_db.DatabaseBuilder.open_existing(path).build()
            results = db2.query("MATCH (n:Person) RETURN n.name AS name")
            assert len(results) == 1
            assert results[0]["name"] == "Alice"

    def test_open_existing_fails_if_not_exists(self):
        """Test that open_existing() fails if database doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "nonexistent")
            with pytest.raises(OSError):
                uni_db.DatabaseBuilder.open_existing(path).build()

    def test_open_creates_if_needed(self):
        """Test that open() creates database if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = uni_db.DatabaseBuilder.open(path).build()
            assert db is not None
            db.create_label("Test")
            db.execute("CREATE (n:Test)")
            db.flush()

    def test_open_reuses_existing(self):
        """Test that open() reuses existing database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            # Create and add data
            db1 = uni_db.DatabaseBuilder.create(path).build()
            db1.create_label("Person")
            db1.add_property("Person", "name", "string", False)
            db1.query("CREATE (n:Person {name: 'Bob'})")
            db1.flush()
            del db1

            # Open should see existing data
            db2 = uni_db.DatabaseBuilder.open(path).build()
            results = db2.query("MATCH (n:Person) RETURN n.name AS name")
            assert len(results) == 1
            assert results[0]["name"] == "Bob"

    def test_temporary_database(self):
        """Test creating a temporary database."""
        db = uni_db.DatabaseBuilder.temporary().build()
        assert db is not None
        db.create_label("Temp")
        db.add_property("Temp", "value", "int", False)
        db.query("CREATE (n:Temp {value: 42})")
        db.flush()
        results = db.query("MATCH (n:Temp) RETURN n.value AS value")
        assert len(results) == 1
        assert results[0]["value"] == 42


class TestDatabaseBuilderConfiguration:
    """Tests for database builder configuration options."""

    def test_cache_size(self):
        """Test setting cache size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = (
                uni_db.DatabaseBuilder.create(path)
                .cache_size(1024 * 1024 * 100)  # 100 MB
                .build()
            )
            assert db is not None

    def test_parallelism(self):
        """Test setting parallelism level."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = uni_db.DatabaseBuilder.create(path).parallelism(4).build()
            assert db is not None

    def test_chained_configuration(self):
        """Test chaining multiple configuration options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = (
                uni_db.DatabaseBuilder.create(path)
                .cache_size(1024 * 1024 * 50)
                .parallelism(2)
                .build()
            )
            assert db is not None
            # Verify database works
            db.create_label("Test")
            db.execute("CREATE (n:Test)")
            db.flush()
            results = db.query("MATCH (n:Test) RETURN n")
            assert len(results) == 1


class TestBackwardCompatibility:
    """Tests for backward compatibility with Database() constructor."""

    def test_database_constructor(self):
        """Test that Database(path) constructor still works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.Database(tmpdir)
            assert db is not None
            db.create_label("Legacy")
            db.execute("CREATE (n:Legacy)")
            db.flush()
            results = db.query("MATCH (n:Legacy) RETURN n")
            assert len(results) == 1
