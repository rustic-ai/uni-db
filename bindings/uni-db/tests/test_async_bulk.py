# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for async BulkWriter API."""

import pytest

import uni_db


@pytest.fixture
async def bulk_db():
    """Create an async database with schema for bulk loading."""
    db = await uni_db.AsyncUni.temporary()
    await (
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


@pytest.mark.asyncio
async def test_async_bulk_writer_builder(bulk_db):
    """Test creating an async bulk writer with builder."""
    session = bulk_db.session()
    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    assert writer is not None


@pytest.mark.asyncio
async def test_async_bulk_insert_vertices(bulk_db):
    """Test async bulk inserting vertices."""
    session = bulk_db.session()
    tx = await session.tx()
    writer = await tx.bulk_writer().build()

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
    await tx.commit()

    results = await session.query(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name"
    )
    assert len(results) == 3
    assert results[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_async_bulk_insert_edges(bulk_db):
    """Test async bulk inserting edges."""
    session = bulk_db.session()
    tx = await session.tx()
    writer = await tx.bulk_writer().build()

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
    await tx.commit()

    results = await session.query(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN p.name AS p_name, c.name AS c_name"
    )
    assert len(results) == 2


@pytest.mark.asyncio
async def test_async_bulk_writer_abort(bulk_db):
    """Test aborting an async bulk write operation."""
    session = bulk_db.session()
    tx = await session.tx()
    writer = await tx.bulk_writer().build()

    await writer.insert_vertices(
        "Person",
        [{"name": "BeforeAbort", "age": 99}],
    )

    await writer.abort()
    await tx.rollback()

    with pytest.raises(RuntimeError):
        await writer.insert_vertices("Person", [{"name": "AfterAbort", "age": 100}])


@pytest.mark.asyncio
async def test_async_bulk_stats_attributes(bulk_db):
    """Test BulkStats attributes from async commit."""
    session = bulk_db.session()
    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    await writer.insert_vertices(
        "Person",
        [{"name": f"Person{i}", "age": i} for i in range(10)],
    )
    stats = await writer.commit()
    await tx.commit()

    assert hasattr(stats, "vertices_inserted")
    assert hasattr(stats, "edges_inserted")
    assert hasattr(stats, "duration_secs")
    assert stats.vertices_inserted == 10


@pytest.mark.asyncio
async def test_async_bulk_writer_builder_config(bulk_db):
    """Test async bulk writer builder with configuration options."""
    session = bulk_db.session()
    tx = await session.tx()
    writer = await (
        tx.bulk_writer()
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
    await tx.commit()


@pytest.mark.asyncio
async def test_async_bulk_insert_vertices_convenience(bulk_db):
    """Test bulk_insert_vertices convenience method via session."""
    session = bulk_db.session()
    tx = await session.tx()
    bw = await tx.bulk_writer().build()
    vids = await bw.insert_vertices(
        "Person",
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ],
    )
    await bw.commit()
    await tx.commit()
    assert len(vids) == 2
    results = await session.query(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name"
    )
    assert len(results) == 2


@pytest.mark.asyncio
async def test_async_bulk_insert_edges_convenience(bulk_db):
    """Test bulk_insert_edges convenience method via session."""
    session = bulk_db.session()
    tx = await session.tx()
    bw = await tx.bulk_writer().build()
    person_vids = await bw.insert_vertices(
        "Person",
        [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
    )
    company_vids = await bw.insert_vertices("Company", [{"name": "TechCorp"}])

    await bw.insert_edges(
        "WORKS_AT",
        [
            (person_vids[0], company_vids[0], {}),
            (person_vids[1], company_vids[0], {}),
        ],
    )
    await bw.commit()
    await tx.commit()

    results = await session.query(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN p.name AS p_name, c.name AS c_name"
    )
    assert len(results) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("skip", [True, False])
async def test_async_bulk_writer_skip_before_triggers(bulk_db, skip):
    """`skip_before_triggers` chains and, with no before-trigger, loads."""
    tx = await bulk_db.session().tx()
    writer = await tx.bulk_writer().skip_before_triggers(skip).build()
    await writer.insert_vertices("Person", [{"name": "Ada", "age": 36}])
    stats = await writer.commit()
    await tx.commit()
    assert stats.vertices_inserted == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("skip", [True, False])
async def test_async_appender_builder_skip_before_triggers(bulk_db, skip):
    """The async appender builder accepts `skip_before_triggers` too."""
    tx = await bulk_db.session().tx()
    builder = tx.appender_builder("Person").skip_before_triggers(skip)
    assert type(builder).__name__ == "AsyncTxAppenderBuilder"
    app = await builder.build()
    await app.append({"name": "Ada", "age": 36})
    stats = await app.finish()
    await tx.commit()
    assert stats.vertices_inserted == 1
