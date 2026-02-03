# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for index functionality."""

import pytest


@pytest.mark.asyncio
async def test_verify_preexisting_indexes(async_indexed_db):
    """Test that pre-existing indexes are present."""
    label_info = await async_indexed_db.get_label_info("Item")
    assert label_info is not None

    await async_indexed_db.execute(
        "CREATE (i:Item {sku: 'TEST-001', name: 'TestItem', price: 9.99, active: true, embedding: [0.1, 0.2, 0.3, 0.4]})"
    )
    await async_indexed_db.flush()

    result = await async_indexed_db.query(
        "MATCH (i:Item {sku: 'TEST-001'}) RETURN i.name as name"
    )
    assert len(result) == 1
    assert result[0]["name"] == "TestItem"

    result = await async_indexed_db.query(
        "MATCH (i:Item {name: 'TestItem'}) RETURN i.sku as sku"
    )
    assert len(result) == 1
    assert result[0]["sku"] == "TEST-001"


@pytest.mark.asyncio
async def test_create_additional_scalar_index(async_indexed_db):
    """Test creating an additional scalar index."""
    await async_indexed_db.create_label("Product")
    await async_indexed_db.add_property("Product", "name", "string", False)
    await async_indexed_db.add_property("Product", "sku", "string", False)
    await async_indexed_db.add_property("Product", "price", "float", False)

    await async_indexed_db.execute(
        "CREATE (p:Product {name: 'Widget', sku: 'WDG-001', price: 9.99})"
    )
    await async_indexed_db.execute(
        "CREATE (p:Product {name: 'Gadget', sku: 'GDG-001', price: 19.99})"
    )
    await async_indexed_db.flush()

    await async_indexed_db.create_scalar_index("Product", "sku", "btree")

    result = await async_indexed_db.query(
        "MATCH (p:Product {sku: 'WDG-001'}) RETURN p.name as name"
    )
    assert len(result) == 1
    assert result[0]["name"] == "Widget"

    await async_indexed_db.create_scalar_index("Product", "name", "hash")

    result = await async_indexed_db.query(
        "MATCH (p:Product {name: 'Gadget'}) RETURN p.sku as sku"
    )
    assert len(result) == 1
    assert result[0]["sku"] == "GDG-001"


@pytest.mark.asyncio
async def test_create_vector_index(async_indexed_db):
    """Test creating a vector index."""
    await async_indexed_db.create_label("Document")
    await async_indexed_db.add_property("Document", "title", "string", False)
    await async_indexed_db.add_property("Document", "embedding", "vector:3", False)

    await async_indexed_db.execute(
        "CREATE (d:Document {title: 'Doc1', embedding: [0.1, 0.2, 0.3]})"
    )
    await async_indexed_db.execute(
        "CREATE (d:Document {title: 'Doc2', embedding: [0.4, 0.5, 0.6]})"
    )
    await async_indexed_db.flush()

    await async_indexed_db.create_vector_index("Document", "embedding", "l2")

    await async_indexed_db.create_label("Image")
    await async_indexed_db.add_property("Image", "name", "string", False)
    await async_indexed_db.add_property("Image", "features", "vector:3", False)

    await async_indexed_db.execute(
        "CREATE (i:Image {name: 'img1', features: [0.7, 0.8, 0.9]})"
    )
    await async_indexed_db.flush()

    await async_indexed_db.create_vector_index("Image", "features", "cosine")

    result = await async_indexed_db.query("MATCH (d:Document) RETURN count(d) as count")
    assert result[0]["count"] == 2


@pytest.mark.asyncio
async def test_indexed_queries_return_correct_results(async_indexed_db):
    """Test that queries on indexed properties return correct results."""
    # Add test data with known values using Item label (which has indexes)
    await async_indexed_db.execute(
        "CREATE (i:Item {sku: 'IDX-001', name: 'IndexedAlice', price: 100.0, active: true, embedding: [0.1, 0.2, 0.3, 0.4]})"
    )
    await async_indexed_db.execute(
        "CREATE (i:Item {sku: 'IDX-002', name: 'IndexedBob', price: 101.0, active: true, embedding: [0.5, 0.6, 0.7, 0.8]})"
    )
    await async_indexed_db.execute(
        "CREATE (i:Item {sku: 'IDX-003', name: 'IndexedCharlie', price: 102.0, active: false, embedding: [0.9, 1.0, 1.1, 1.2]})"
    )
    await async_indexed_db.flush()

    # Query by name (should use hash index)
    result = await async_indexed_db.query(
        "MATCH (i:Item {name: 'IndexedAlice'}) RETURN i.price as price, i.sku as sku"
    )
    assert len(result) == 1
    assert result[0]["price"] == 100.0
    assert result[0]["sku"] == "IDX-001"

    # Query by sku (should use btree index)
    result = await async_indexed_db.query(
        "MATCH (i:Item {sku: 'IDX-002'}) RETURN i.name as name"
    )
    assert len(result) == 1
    assert result[0]["name"] == "IndexedBob"

    # Range query on price
    result = await async_indexed_db.query(
        "MATCH (i:Item) WHERE i.price >= 101.0 AND i.sku >= 'IDX-' RETURN i.name as name ORDER BY name"
    )
    assert len(result) >= 2
    names = [r["name"] for r in result]
    assert "IndexedBob" in names
    assert "IndexedCharlie" in names
