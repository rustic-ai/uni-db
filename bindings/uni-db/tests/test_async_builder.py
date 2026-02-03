# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for AsyncDatabaseBuilder API."""

import os
import tempfile

import pytest

import uni_db


class TestAsyncDatabaseBuilderOpenModes:
    """Tests for different async database open modes."""

    @pytest.mark.asyncio
    async def test_create_new_database(self):
        """Test creating a new database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await uni_db.AsyncDatabaseBuilder.create(path).build()
            assert db is not None
            await db.create_label("Test")
            results = await db.query("MATCH (n:Test) RETURN n")
            assert len(results) == 0

    @pytest.mark.asyncio
    async def test_create_fails_if_exists(self):
        """Test that create() fails if database exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db1 = await uni_db.AsyncDatabaseBuilder.create(path).build()
            await db1.create_label("Test")
            await db1.flush()
            del db1

            with pytest.raises(Exception):
                await uni_db.AsyncDatabaseBuilder.create(path).build()

    @pytest.mark.asyncio
    async def test_open_existing_database(self):
        """Test opening an existing database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db1 = await uni_db.AsyncDatabaseBuilder.create(path).build()
            await db1.create_label("Person")
            await db1.add_property("Person", "name", "string", False)
            await db1.query("CREATE (n:Person {name: 'Alice'})")
            await db1.flush()
            del db1

            db2 = await uni_db.AsyncDatabaseBuilder.open_existing(path).build()
            results = await db2.query("MATCH (n:Person) RETURN n.name AS name")
            assert len(results) == 1
            assert results[0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_open_existing_fails_if_not_exists(self):
        """Test that open_existing() fails if database doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "nonexistent")
            with pytest.raises(Exception):
                await uni_db.AsyncDatabaseBuilder.open_existing(path).build()

    @pytest.mark.asyncio
    async def test_open_creates_if_needed(self):
        """Test that open() creates database if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await uni_db.AsyncDatabaseBuilder.open(path).build()
            assert db is not None
            await db.create_label("Test")
            await db.execute("CREATE (n:Test)")
            await db.flush()

    @pytest.mark.asyncio
    async def test_open_reuses_existing(self):
        """Test that open() reuses existing database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db1 = await uni_db.AsyncDatabaseBuilder.create(path).build()
            await db1.create_label("Person")
            await db1.add_property("Person", "name", "string", False)
            await db1.query("CREATE (n:Person {name: 'Bob'})")
            await db1.flush()
            del db1

            db2 = await uni_db.AsyncDatabaseBuilder.open(path).build()
            results = await db2.query("MATCH (n:Person) RETURN n.name AS name")
            assert len(results) == 1
            assert results[0]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_temporary_database(self):
        """Test creating a temporary database."""
        db = await uni_db.AsyncDatabaseBuilder.temporary().build()
        assert db is not None
        await db.create_label("Temp")
        await db.add_property("Temp", "value", "int", False)
        await db.query("CREATE (n:Temp {value: 42})")
        await db.flush()
        results = await db.query("MATCH (n:Temp) RETURN n.value AS value")
        assert len(results) == 1
        assert results[0]["value"] == 42


class TestAsyncDatabaseBuilderConfiguration:
    """Tests for async database builder configuration options."""

    @pytest.mark.asyncio
    async def test_cache_size(self):
        """Test setting cache size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await (
                uni_db.AsyncDatabaseBuilder.create(path)
                .cache_size(1024 * 1024 * 100)
                .build()
            )
            assert db is not None

    @pytest.mark.asyncio
    async def test_parallelism(self):
        """Test setting parallelism level."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await uni_db.AsyncDatabaseBuilder.create(path).parallelism(4).build()
            assert db is not None

    @pytest.mark.asyncio
    async def test_chained_configuration(self):
        """Test chaining multiple configuration options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await (
                uni_db.AsyncDatabaseBuilder.create(path)
                .cache_size(1024 * 1024 * 50)
                .parallelism(2)
                .build()
            )
            assert db is not None
            await db.create_label("Test")
            await db.execute("CREATE (n:Test)")
            await db.flush()
            results = await db.query("MATCH (n:Test) RETURN n")
            assert len(results) == 1


class TestAsyncInMemory:
    """Tests for in_memory builder method."""

    @pytest.mark.asyncio
    async def test_in_memory_database(self):
        """Test creating an in-memory database via builder."""
        db = await uni_db.AsyncDatabaseBuilder.in_memory().build()
        assert db is not None
        await db.create_label("Mem")
        await db.add_property("Mem", "x", "int", False)
        await db.execute("CREATE (n:Mem {x: 42})")
        await db.flush()
        results = await db.query("MATCH (n:Mem) RETURN n.x AS x")
        assert len(results) == 1
        assert results[0]["x"] == 42


class TestAsyncBackwardCompatibility:
    """Tests for backward compatibility with AsyncDatabase constructors."""

    @pytest.mark.asyncio
    async def test_async_database_open(self):
        """Test that AsyncDatabase.open() still works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = await uni_db.AsyncDatabase.open(tmpdir)
            assert db is not None
            await db.create_label("Legacy")
            await db.execute("CREATE (n:Legacy)")
            await db.flush()
            results = await db.query("MATCH (n:Legacy) RETURN n")
            assert len(results) == 1
