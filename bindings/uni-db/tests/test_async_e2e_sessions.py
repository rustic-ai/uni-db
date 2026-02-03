# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for session functionality."""

import pytest


@pytest.mark.asyncio
async def test_session_with_single_variable(async_social_db):
    """Test creating a session with a single variable."""
    await async_social_db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    await async_social_db.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    await async_social_db.flush()

    # Create session with single variable
    builder = async_social_db.session()
    builder.set("min_age", 25)
    session = builder.build()

    result = await session.query(
        "MATCH (p:Person) WHERE p.age >= $session.min_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Bob"


@pytest.mark.asyncio
async def test_session_with_multiple_variables(async_social_db):
    """Test creating a session with multiple variables."""
    await async_social_db.execute(
        "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@nyc.com'})"
    )
    await async_social_db.execute(
        "CREATE (p:Person {name: 'Bob', age: 25, email: 'bob@la.com'})"
    )
    await async_social_db.execute(
        "CREATE (p:Person {name: 'Charlie', age: 35, email: 'charlie@nyc.com'})"
    )
    await async_social_db.flush()

    builder = async_social_db.session()
    builder.set("min_age", 28)
    builder.set("max_age", 40)
    session = builder.build()

    result = await session.query(
        "MATCH (p:Person) WHERE p.age >= $session.min_age AND p.age <= $session.max_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"


@pytest.mark.asyncio
async def test_session_get_nonexistent_returns_none(async_social_db):
    """Test that getting a nonexistent variable returns None."""
    builder = async_social_db.session()
    builder.set("existing_key", "value")
    session = builder.build()

    value = session.get("existing_key")
    assert value == "value"

    value = session.get("nonexistent_key")
    assert value is None


@pytest.mark.asyncio
async def test_session_query(async_social_db):
    """Test querying through a session."""
    await async_social_db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    await async_social_db.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    await async_social_db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)"
    )
    await async_social_db.flush()

    builder = async_social_db.session()
    builder.set("person_name", "Alice")
    session = builder.build()

    result = await session.query(
        "MATCH (p:Person {name: $session.person_name})-[:KNOWS]->(friend) RETURN friend.name as friend_name"
    )
    assert len(result) == 1
    assert result[0]["friend_name"] == "Bob"

    # Query with additional params passed to query()
    result = await session.query(
        "MATCH (p:Person {name: $session.person_name})-[:KNOWS]->(friend) WHERE friend.age >= $min_age RETURN friend.name as friend_name",
        params={"min_age": 20},
    )
    assert len(result) == 1
    assert result[0]["friend_name"] == "Bob"


@pytest.mark.asyncio
async def test_session_execute(async_social_db):
    """Test executing mutations through a session."""
    builder = async_social_db.session()
    builder.set("person_name", "SessionPerson")
    builder.set("person_age", 40)
    session = builder.build()

    count = await session.execute(
        "CREATE (p:Person {name: $session.person_name, age: $session.person_age})"
    )
    assert count == 1

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'SessionPerson'}) RETURN p.age as age"
    )
    assert len(result) == 1
    assert result[0]["age"] == 40

    builder2 = async_social_db.session()
    builder2.set("name_to_update", "SessionPerson")
    builder2.set("new_age", 41)
    session2 = builder2.build()

    count = await session2.execute(
        "MATCH (p:Person {name: $session.name_to_update}) SET p.age = $session.new_age"
    )
    assert count == 1

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'SessionPerson'}) RETURN p.age as age"
    )
    assert len(result) == 1
    assert result[0]["age"] == 41


@pytest.mark.asyncio
async def test_session_variables_persist_across_queries(async_social_db):
    """Test that session variables persist across multiple queries."""
    await async_social_db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    await async_social_db.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    await async_social_db.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
    await async_social_db.flush()

    builder = async_social_db.session()
    builder.set("min_age", 30)
    builder.set("max_age", 40)
    session = builder.build()

    # First query using min_age
    result = await session.query(
        "MATCH (p:Person) WHERE p.age >= $session.min_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"

    # Second query using max_age
    result = await session.query(
        "MATCH (p:Person) WHERE p.age <= $session.max_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 3

    # Third query using both variables
    result = await session.query(
        "MATCH (p:Person) WHERE p.age >= $session.min_age AND p.age <= $session.max_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"

    assert session.get("min_age") == 30
    assert session.get("max_age") == 40

    # Execute mutation using session variables
    count = await session.execute(
        "MATCH (p:Person) WHERE p.age >= $session.min_age SET p.email = 'senior@example.com'"
    )
    assert count == 2

    result = await async_social_db.query(
        "MATCH (p:Person {email: 'senior@example.com'}) RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"
