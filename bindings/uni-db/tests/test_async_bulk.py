# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for async BulkWriter API."""

import pytest

import uni_db


@pytest.fixture
async def bulk_db():
    """Create an async database with schema for bulk loading."""
    db = await uni_db.AsyncDatabase.temporary()
    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)
    await db.add_property("Person", "age", "int", False)
    await db.create_label("Company")
    await db.add_property("Company", "name", "string", False)
    await db.create_edge_type("WORKS_AT", ["Person"], ["Company"])
    return db


@pytest.mark.asyncio
async def test_async_bulk_writer_builder(bulk_db):
    """Test creating an async bulk writer with builder."""
    writer = bulk_db.bulk_writer().build()
    assert writer is not None


@pytest.mark.asyncio
async def test_async_bulk_insert_vertices(bulk_db):
    """Test async bulk inserting vertices."""
    writer = bulk_db.bulk_writer().build()

    vids = await writer.insert_vertices(
        "Person",
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ],
    )

    assert len(vids) == 3
    await writer.commit()

    results = await bulk_db.query(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name"
    )
    assert len(results) == 3
    assert results[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_async_bulk_insert_edges(bulk_db):
    """Test async bulk inserting edges."""
    writer = bulk_db.bulk_writer().build()

    person_vids = await writer.insert_vertices(
        "Person",
        [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
    )
    company_vids = await writer.insert_vertices("Company", [{"name": "TechCorp"}])

    await writer.insert_edges(
        "WORKS_AT",
        [
            (person_vids[0], company_vids[0], {}),
            (person_vids[1], company_vids[0], {}),
        ],
    )

    await writer.commit()

    results = await bulk_db.query(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN p.name AS p_name, c.name AS c_name"
    )
    assert len(results) == 2


@pytest.mark.asyncio
async def test_async_bulk_writer_abort(bulk_db):
    """Test aborting an async bulk write operation."""
    writer = bulk_db.bulk_writer().build()

    await writer.insert_vertices(
        "Person",
        [{"name": "BeforeAbort", "age": 99}],
    )

    writer.abort()

    with pytest.raises(RuntimeError):
        await writer.insert_vertices("Person", [{"name": "AfterAbort", "age": 100}])


@pytest.mark.asyncio
async def test_async_bulk_stats_attributes(bulk_db):
    """Test BulkStats attributes from async commit."""
    writer = bulk_db.bulk_writer().build()
    await writer.insert_vertices(
        "Person",
        [{"name": f"Person{i}", "age": i} for i in range(10)],
    )
    stats = await writer.commit()

    assert hasattr(stats, "vertices_inserted")
    assert hasattr(stats, "edges_inserted")
    assert hasattr(stats, "duration_secs")
    assert stats.vertices_inserted == 10


@pytest.mark.asyncio
async def test_async_bulk_writer_builder_config(bulk_db):
    """Test async bulk writer builder with configuration options."""
    writer = (
        bulk_db.bulk_writer()
        .defer_vector_indexes(True)
        .defer_scalar_indexes(True)
        .batch_size(5000)
        .async_indexes(False)
        .build()
    )
    assert writer is not None

    vids = await writer.insert_vertices(
        "Person",
        [{"name": "ConfigTest", "age": 40}],
    )
    assert len(vids) == 1
    await writer.commit()


@pytest.mark.asyncio
async def test_async_bulk_insert_vertices_convenience(bulk_db):
    """Test bulk_insert_vertices convenience method on AsyncDatabase."""
    vids = await bulk_db.bulk_insert_vertices(
        "Person",
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ],
    )
    assert len(vids) == 2
    results = await bulk_db.query(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name"
    )
    assert len(results) == 2


@pytest.mark.asyncio
async def test_async_bulk_insert_edges_convenience(bulk_db):
    """Test bulk_insert_edges convenience method on AsyncDatabase."""
    person_vids = await bulk_db.bulk_insert_vertices(
        "Person",
        [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
    )
    company_vids = await bulk_db.bulk_insert_vertices("Company", [{"name": "TechCorp"}])

    await bulk_db.bulk_insert_edges(
        "WORKS_AT",
        [
            (person_vids[0], company_vids[0], {}),
            (person_vids[1], company_vids[0], {}),
        ],
    )

    results = await bulk_db.query(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN p.name AS p_name, c.name AS c_name"
    )
    assert len(results) == 2
