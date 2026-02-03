# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for the async API: open, query, execute, flush."""

import pytest

import uni_db


@pytest.fixture
def test_dir(tmp_path):
    """Provide a temporary directory for database tests."""
    return str(tmp_path / "test_async_db")


@pytest.mark.asyncio
async def test_async_open_and_query(test_dir):
    """Test basic open, create label, insert, and query."""
    db = await uni_db.AsyncDatabase.open(test_dir)
    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)
    await db.add_property("Person", "age", "int", False)
    await db.execute("CREATE (n:Person {name: 'Alice', age: 30})")

    results = await db.query("MATCH (n:Person) RETURN n.name AS name, n.age AS age")
    assert len(results) == 1
    assert results[0]["name"] == "Alice"
    assert results[0]["age"] == 30


@pytest.mark.asyncio
async def test_async_temporary():
    """Test temporary in-memory database."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Thing")
    await db.add_property("Thing", "value", "int", False)
    await db.execute("CREATE (n:Thing {value: 42})")

    results = await db.query("MATCH (n:Thing) RETURN n.value AS value")
    assert len(results) == 1
    assert results[0]["value"] == 42


@pytest.mark.asyncio
async def test_async_query_with_params(test_dir):
    """Test parameterized queries."""
    db = await uni_db.AsyncDatabase.open(test_dir)
    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)
    await db.add_property("Person", "age", "int", False)
    await db.execute("CREATE (n:Person {name: 'Bob', age: 25})")

    results = await db.query(
        "MATCH (n:Person {name: 'Bob'}) RETURN n.age AS age",
    )
    assert len(results) == 1
    assert results[0]["age"] == 25


@pytest.mark.asyncio
async def test_async_execute_returns_count(test_dir):
    """Test that execute returns affected row count."""
    db = await uni_db.AsyncDatabase.open(test_dir)
    await db.create_label("Counter")
    count = await db.execute("CREATE (n:Counter {value: 1})")
    assert isinstance(count, int)


@pytest.mark.asyncio
async def test_async_flush(test_dir):
    """Test flushing writes to storage."""
    db = await uni_db.AsyncDatabase.open(test_dir)
    await db.create_label("Flushed")
    await db.execute("CREATE (n:Flushed {val: 1})")
    await db.flush()

    # After flush, data should be persisted
    results = await db.query("MATCH (n:Flushed) RETURN n.val AS val")
    assert len(results) == 1


@pytest.mark.asyncio
async def test_async_explain(test_dir):
    """Test query plan explanation."""
    db = await uni_db.AsyncDatabase.open(test_dir)
    await db.create_label("Person")

    plan = await db.explain("MATCH (n:Person) RETURN n")
    assert "plan_text" in plan
    assert "cost_estimates" in plan


@pytest.mark.asyncio
async def test_async_builder():
    """Test AsyncDatabaseBuilder."""
    builder = uni_db.AsyncDatabaseBuilder.temporary()
    db = await builder.build()
    await db.create_label("Built")
    await db.add_property("Built", "x", "int", False)
    await db.execute("CREATE (n:Built {x: 1})")
    results = await db.query("MATCH (n:Built) RETURN n.x AS x")
    assert len(results) == 1
    assert results[0]["x"] == 1


@pytest.mark.asyncio
async def test_async_multiple_queries():
    """Test running multiple queries sequentially."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Node")
    await db.add_property("Node", "idx", "int", False)

    for i in range(10):
        await db.execute(f"CREATE (n:Node {{idx: {i}}})")

    results = await db.query("MATCH (n:Node) RETURN n.idx AS idx ORDER BY n.idx")
    assert len(results) == 10
    assert results[0]["idx"] == 0
    assert results[9]["idx"] == 9


@pytest.mark.asyncio
async def test_async_query_with_builder():
    """Test AsyncQueryBuilder via query_with()."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Item")
    await db.add_property("Item", "name", "string", False)
    await db.add_property("Item", "value", "int", False)
    await db.execute("CREATE (n:Item {name: 'Widget', value: 42})")
    await db.flush()

    results = await (
        db.query_with("MATCH (n:Item) WHERE n.value = $val RETURN n.name AS name")
        .param("val", 42)
        .run()
    )
    assert len(results) == 1
    assert results[0]["name"] == "Widget"


@pytest.mark.asyncio
async def test_async_query_with_timeout():
    """Test AsyncQueryBuilder with timeout."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Node")
    await db.add_property("Node", "x", "int", False)
    await db.execute("CREATE (n:Node {x: 1})")

    results = await db.query_with("MATCH (n:Node) RETURN n.x AS x").timeout(30.0).run()
    assert len(results) == 1
