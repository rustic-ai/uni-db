# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for vector search functionality."""

import pytest


@pytest.mark.asyncio
async def test_basic_vector_search_knn(async_ecommerce_db_populated):
    """Test basic K-NN vector search returns top k results."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 3


@pytest.mark.asyncio
async def test_vector_search_ordered_by_distance(async_ecommerce_db_populated):
    """Test vector search results are ordered by increasing distance."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 4)
        YIELD vid, distance
        RETURN vid, distance
    """)

    distances = [r["distance"] for r in results]
    assert distances == sorted(distances), "Results should be ordered by distance"
    assert results[0]["distance"] < 0.01, (
        "Closest match should be the query vector itself"
    )


@pytest.mark.asyncio
async def test_vector_search_vid_and_distance(async_ecommerce_db_populated):
    """Test vector search returns vid and distance with correct types."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 1)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 1
    row = results[0]
    assert isinstance(row["vid"], int), "vid should be an integer"
    assert isinstance(row["distance"], float), "distance should be a float"
    assert row["vid"] >= 0, "vid should be non-negative"
    assert row["distance"] >= 0.0, "distance should be non-negative"


@pytest.mark.asyncio
async def test_vector_search_with_k(async_ecommerce_db_populated):
    """Test vector search with different k values."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 2


@pytest.mark.asyncio
async def test_vector_search_with_threshold(async_ecommerce_db_populated):
    """Test vector search with distance threshold filtering."""

    # Small threshold: only very close matches
    results_tight = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, NULL, 0.5)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert all(r["distance"] <= 0.5 for r in results_tight), (
        "All matches should be within threshold"
    )

    # Larger threshold: more results
    results_wide = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, NULL, 2.0)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results_wide) >= len(results_tight), (
        "Larger threshold should return more results"
    )


@pytest.mark.asyncio
async def test_vector_search_fetch_nodes(async_ecommerce_db_populated):
    """Test vector search with YIELD node to get full node properties."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    assert len(results) == 3

    for row in results:
        assert "name" in row, "Should have 'name' property"
        assert "price" in row, "Should have 'price' property"
        assert isinstance(row["distance"], float)
        assert row["distance"] >= 0.0


@pytest.mark.asyncio
async def test_fetch_nodes_returns_properties_and_distance(
    async_ecommerce_db_populated,
):
    """Test fetch_nodes returns node properties and ordered distances."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 2)
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    distances = [r["distance"] for r in results]
    assert distances == sorted(distances), "Results should be ordered by distance"


@pytest.mark.asyncio
async def test_cosine_metric_index(async_empty_db):
    """Test creating and using a cosine metric vector index."""

    await (
        async_empty_db.schema()
        .label("CosineDoc")
        .property("title", "string")
        .vector("vec", 3)
        .done()
        .apply()
    )

    await async_empty_db.execute(
        "CREATE (d:CosineDoc {title: 'Doc1', vec: [1.0, 0.0, 0.0]})"
    )
    await async_empty_db.execute(
        "CREATE (d:CosineDoc {title: 'Doc2', vec: [0.0, 1.0, 0.0]})"
    )
    await async_empty_db.execute(
        "CREATE (d:CosineDoc {title: 'Doc3', vec: [0.707, 0.707, 0.0]})"
    )
    await async_empty_db.flush()

    await async_empty_db.create_vector_index("CosineDoc", "vec", "cosine")

    results = await async_empty_db.query("""
        CALL uni.vector.query('CosineDoc', 'vec', [1.0, 0.0, 0.0], 3)
        YIELD node, distance
        RETURN node.title AS title, distance
    """)

    assert len(results) == 3

    # For cosine metric: Doc1 [1,0,0] most similar, Doc2 [0,1,0] least similar
    assert results[0]["distance"] < 0.01, "Most similar should have distance ~0"
    assert results[2]["distance"] > results[1]["distance"], "Distances should increase"


@pytest.mark.asyncio
async def test_vector_search_with_graph_traversal(async_ecommerce_db_populated):
    """Test combining vector search with graph traversal."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
        YIELD node, distance
        MATCH (node)-[:IN_CATEGORY]->(c:Category)
        RETURN node.name AS product, c.name AS category, distance
    """)

    assert len(results) > 0, "Should find categories for similar products"


@pytest.mark.asyncio
async def test_vector_search_with_filter_expression(async_ecommerce_db_populated):
    """Test vector search with pre-filter expression."""

    # Filter for expensive products (price > 500)
    results_expensive = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, 'price > 500')
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    for row in results_expensive:
        assert row["price"] > 500, f"Product {row['name']} should have price > 500"

    # Filter for cheap products (price < 100)
    results_cheap = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, 'price < 100')
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    for row in results_cheap:
        assert row["price"] < 100, f"Product {row['name']} should have price < 100"

    names_expensive = {r["name"] for r in results_expensive}
    names_cheap = {r["name"] for r in results_cheap}
    assert names_expensive != names_cheap, (
        "Different filters should return different products"
    )


@pytest.mark.asyncio
async def test_vector_search_empty_results(async_empty_db):
    """Test vector search on label with no data returns empty results."""

    await (
        async_empty_db.schema()
        .label("EmptyLabel")
        .property("name", "string")
        .vector("vec", 4)
        .done()
        .apply()
    )

    await async_empty_db.create_vector_index("EmptyLabel", "vec", "l2")

    results = await async_empty_db.query("""
        CALL uni.vector.query('EmptyLabel', 'vec', [1.0, 0.0, 0.0, 0.0], 5)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert results == [], "Should return empty list for label with no data"


@pytest.mark.asyncio
async def test_vector_search_k_larger_than_dataset(async_ecommerce_db_populated):
    """Test vector search with k larger than available nodes."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 100)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) <= 100, "Should return at most k results"
    assert len(results) >= 4, "Should return all available products"


@pytest.mark.asyncio
async def test_vector_search_threshold_excludes_distant_results(
    async_ecommerce_db_populated,
):
    """Test threshold properly excludes results beyond distance limit."""

    # Search for Book [0,0,1,0] with very tight threshold
    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [0.0, 0.0, 1.0, 0.0], 10, NULL, 0.1)
        YIELD node, distance
        RETURN node.name AS name, distance
    """)

    assert len(results) >= 1, "Should find at least the exact match"
    for row in results:
        assert row["distance"] <= 0.1, "All results should be within threshold"


@pytest.mark.asyncio
async def test_vector_search_chained_constraints(async_ecommerce_db_populated):
    """Test vector search with both filter and threshold."""

    results = await async_ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 5, 'price > 0', 1.5)
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    assert isinstance(results, list)
    for row in results:
        assert row["distance"] <= 1.5, "Distance should be within threshold"
        assert row["price"] > 0, "Price should satisfy filter"


@pytest.mark.asyncio
async def test_vector_search_different_dimensions(async_empty_db):
    """Test vector search with different dimensionality vectors."""

    await (
        async_empty_db.schema()
        .label("Doc5D")
        .property("name", "string")
        .vector("vec5", 5)
        .done()
        .apply()
    )

    await async_empty_db.execute(
        "CREATE (d:Doc5D {name: 'A', vec5: [1.0, 0.0, 0.0, 0.0, 0.0]})"
    )
    await async_empty_db.execute(
        "CREATE (d:Doc5D {name: 'B', vec5: [0.0, 1.0, 0.0, 0.0, 0.0]})"
    )
    await async_empty_db.execute(
        "CREATE (d:Doc5D {name: 'C', vec5: [0.0, 0.0, 1.0, 0.0, 0.0]})"
    )
    await async_empty_db.flush()

    await async_empty_db.create_vector_index("Doc5D", "vec5", "l2")

    results = await async_empty_db.query("""
        CALL uni.vector.query('Doc5D', 'vec5', [1.0, 0.0, 0.0, 0.0, 0.0], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 2
    assert results[0]["distance"] < 0.01


@pytest.mark.asyncio
async def test_create_vector_index_l2(async_empty_db):
    """Test creating L2 vector index and searching."""

    await (
        async_empty_db.schema()
        .label("Item")
        .property("name", "string")
        .vector("vec", 3)
        .done()
        .apply()
    )

    await async_empty_db.execute("CREATE (:Item {name: 'Item1', vec: [1.0, 2.0, 3.0]})")
    await async_empty_db.execute("CREATE (:Item {name: 'Item2', vec: [2.0, 3.0, 4.0]})")
    await async_empty_db.flush()

    await async_empty_db.create_vector_index("Item", "vec", "l2")

    results = await async_empty_db.query("""
        CALL uni.vector.query('Item', 'vec', [1.5, 2.5, 3.5], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) >= 1
