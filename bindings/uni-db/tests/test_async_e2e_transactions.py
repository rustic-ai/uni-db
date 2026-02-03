# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for transaction begin/commit/rollback semantics and context managers."""

import pytest


@pytest.mark.asyncio
async def test_begin_commit_data_persists(async_social_db):
    """Test that committed transaction data persists."""

    tx = await async_social_db.begin()
    await tx.query("CREATE (p:Person {name: 'Alice', age: 30})")
    await tx.commit()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.age"
    )
    assert len(result) == 1
    assert result[0]["p.name"] == "Alice"
    assert result[0]["p.age"] == 30


@pytest.mark.asyncio
async def test_begin_rollback_data_reverted(async_social_db):
    """Test that rolled back transaction data is reverted."""

    tx = await async_social_db.begin()
    await tx.query("CREATE (p:Person {name: 'Bob', age: 25})")
    await tx.rollback()

    result = await async_social_db.query("MATCH (p:Person {name: 'Bob'}) RETURN p.name")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_transaction_query_with_params(async_social_db):
    """Test transaction query with parameters."""

    tx = await async_social_db.begin()
    await tx.query(
        "CREATE (p:Person {name: $name, age: $age})",
        params={"name": "Charlie", "age": 35},
    )
    await tx.commit()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Charlie'}) RETURN p.age"
    )
    assert len(result) == 1
    assert result[0]["p.age"] == 35


@pytest.mark.asyncio
async def test_multiple_operations_in_one_transaction(async_social_db):
    """Test multiple operations within a single transaction."""

    tx = await async_social_db.begin()

    await tx.query("CREATE (p:Person {name: 'David', age: 28})")
    await tx.query("CREATE (p:Person {name: 'Eve', age: 32})")

    await tx.query("""
        MATCH (p1:Person {name: 'David'}), (p2:Person {name: 'Eve'})
        CREATE (p1)-[:KNOWS]->(p2)
    """)

    await tx.commit()

    people = await async_social_db.query(
        "MATCH (p:Person) WHERE p.name IN ['David', 'Eve'] RETURN p.name ORDER BY p.name"
    )
    assert len(people) == 2
    assert people[0]["p.name"] == "David"
    assert people[1]["p.name"] == "Eve"

    knows = await async_social_db.query("""
        MATCH (p1:Person {name: 'David'})-[:KNOWS]->(p2:Person {name: 'Eve'})
        RETURN p1.name, p2.name
    """)
    assert len(knows) == 1


@pytest.mark.asyncio
async def test_context_manager_auto_commit(async_social_db):
    """Test that context manager auto-commits on success."""

    tx = await async_social_db.begin()
    async with tx:
        await tx.query("CREATE (p:Person {name: 'Frank', age: 40})")

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Frank'}) RETURN p.age"
    )
    assert len(result) == 1
    assert result[0]["p.age"] == 40


@pytest.mark.asyncio
async def test_context_manager_auto_rollback_on_exception(async_social_db):
    """Test that context manager auto-rolls back on exception."""

    tx = await async_social_db.begin()
    try:
        async with tx:
            await tx.query("CREATE (p:Person {name: 'Grace', age: 45})")
            raise ValueError("Simulated error")
    except ValueError:
        pass

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Grace'}) RETURN p.name"
    )
    assert len(result) == 0


@pytest.mark.asyncio
async def test_double_commit_raises_error(async_social_db):
    """Test that committing twice raises RuntimeError."""

    tx = await async_social_db.begin()
    await tx.query("CREATE (p:Person {name: 'Henry', age: 50})")
    await tx.commit()

    with pytest.raises(RuntimeError, match=".*completed.*|.*committed.*"):
        await tx.commit()


@pytest.mark.asyncio
async def test_double_rollback_raises_error(async_social_db):
    """Test that rolling back twice raises RuntimeError."""

    tx = await async_social_db.begin()
    await tx.query("CREATE (p:Person {name: 'Iris', age: 55})")
    await tx.rollback()

    with pytest.raises(RuntimeError, match=".*completed.*|.*rolled.*back.*"):
        await tx.rollback()


@pytest.mark.asyncio
async def test_operations_after_commit_raise_error(async_social_db):
    """Test that operations after commit raise RuntimeError."""

    tx = await async_social_db.begin()
    await tx.query("CREATE (p:Person {name: 'Jack', age: 60})")
    await tx.commit()

    with pytest.raises(RuntimeError, match=".*completed.*|.*committed.*"):
        await tx.query("CREATE (p:Person {name: 'Kate', age: 65})")


@pytest.mark.asyncio
async def test_operations_after_rollback_raise_error(async_social_db):
    """Test that operations after rollback raise RuntimeError."""

    tx = await async_social_db.begin()
    await tx.query("CREATE (p:Person {name: 'Liam', age: 70})")
    await tx.rollback()

    with pytest.raises(RuntimeError, match=".*completed.*|.*rolled.*back.*"):
        await tx.query("CREATE (p:Person {name: 'Mia', age: 75})")
