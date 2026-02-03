# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for async transactions: begin, commit, rollback, context manager."""

import pytest

import uni_db


@pytest.mark.asyncio
async def test_async_transaction_commit():
    """Test begin/commit transaction."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)

    tx = await db.begin()
    await tx.query("CREATE (n:Person {name: 'Alice'})")
    await tx.commit()

    results = await db.query("MATCH (n:Person) RETURN n.name AS name")
    assert len(results) == 1
    assert results[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_async_transaction_rollback():
    """Test begin/rollback transaction."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)

    # Insert a baseline
    await db.execute("CREATE (n:Person {name: 'Existing'})")

    tx = await db.begin()
    await tx.query("CREATE (n:Person {name: 'WillBeRolledBack'})")
    await tx.rollback()

    results = await db.query("MATCH (n:Person) RETURN n.name AS name")
    # Only the baseline should exist
    assert len(results) == 1
    assert results[0]["name"] == "Existing"


@pytest.mark.asyncio
async def test_async_transaction_context_manager_commit():
    """Test async context manager auto-commit on success."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Item")
    await db.add_property("Item", "value", "int", False)

    tx = await db.begin()
    async with tx:
        await tx.query("CREATE (n:Item {value: 42})")

    results = await db.query("MATCH (n:Item) RETURN n.value AS value")
    assert len(results) == 1
    assert results[0]["value"] == 42


@pytest.mark.asyncio
async def test_async_transaction_context_manager_rollback_on_error():
    """Test async context manager auto-rollback on exception."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Item")
    await db.add_property("Item", "value", "int", False)
    await db.execute("CREATE (n:Item {value: 0})")

    tx = await db.begin()
    with pytest.raises(ValueError):
        async with tx:
            await tx.query("CREATE (n:Item {value: 99})")
            raise ValueError("Something went wrong")

    results = await db.query("MATCH (n:Item) RETURN n.value AS value")
    assert len(results) == 1
    assert results[0]["value"] == 0


@pytest.mark.asyncio
async def test_async_transaction_query_with_params():
    """Test parameterized queries within a transaction."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)

    tx = await db.begin()
    await tx.query("CREATE (n:Person {name: $name})", {"name": "Bob"})
    await tx.commit()

    results = await db.query(
        "MATCH (n:Person {name: 'Bob'}) RETURN n.name AS name",
    )
    assert len(results) == 1
    assert results[0]["name"] == "Bob"


@pytest.mark.asyncio
async def test_async_transaction_double_commit_raises():
    """Test that committing twice raises an error."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("X")

    tx = await db.begin()
    await tx.query("CREATE (n:X {v: 1})")
    await tx.commit()

    with pytest.raises(RuntimeError):
        await tx.commit()


@pytest.mark.asyncio
async def test_async_transaction_double_rollback_raises():
    """Test that rolling back twice raises an error."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("X")

    tx = await db.begin()
    await tx.rollback()

    with pytest.raises(RuntimeError):
        await tx.rollback()
