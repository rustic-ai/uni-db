# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for async Session API."""

import pytest

import uni_db


@pytest.fixture
async def db():
    """Create an async database with test data."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)
    await db.add_property("Person", "age", "int", False)
    await db.query("CREATE (n:Person {name: 'Alice', age: 30})")
    await db.query("CREATE (n:Person {name: 'Bob', age: 25})")
    await db.flush()
    return db


@pytest.mark.asyncio
async def test_async_session_with_variable(db):
    """Test creating an async session with a variable."""
    session_builder = db.session()
    session_builder.set("user_name", "Alice")
    session = session_builder.build()

    name = session.get("user_name")
    assert name == "Alice"


@pytest.mark.asyncio
async def test_async_session_query(db):
    """Test executing a query through an async session."""
    session = db.session().build()
    results = await session.query("MATCH (n:Person) RETURN n.name")
    assert len(results) == 2


@pytest.mark.asyncio
async def test_async_session_execute(db):
    """Test executing a mutation through an async session."""
    session = db.session().build()
    affected = await session.execute("CREATE (n:Person {name: 'Charlie', age: 35})")
    assert affected >= 0

    results = await session.query(
        "MATCH (n:Person {name: 'Charlie'}) RETURN n.age AS age"
    )
    assert len(results) == 1
    assert results[0]["age"] == 35


@pytest.mark.asyncio
async def test_async_multiple_session_variables(db):
    """Test async session with multiple variables."""
    builder = db.session()
    builder.set("var1", "value1")
    builder.set("var2", 42)
    builder.set("var3", True)
    session = builder.build()

    assert session.get("var1") == "value1"
    assert session.get("var2") == 42
    assert session.get("var3") is True


@pytest.mark.asyncio
async def test_async_session_get_nonexistent(db):
    """Test getting a nonexistent async session variable."""
    session = db.session().build()
    result = session.get("nonexistent")
    assert result is None
