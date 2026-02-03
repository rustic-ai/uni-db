# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for synchronous vector search functionality."""


def test_basic_vector_search_knn(ecommerce_db_populated):
    """Test basic K-NN vector search returns top k results."""

    results = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 3


def test_vector_search_ordered_by_distance(ecommerce_db_populated):
    """Test vector search results are ordered by increasing distance."""

    results = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 4)
        YIELD vid, distance
        RETURN vid, distance
    """)

    distances = [r["distance"] for r in results]
    assert distances == sorted(distances), "Results should be ordered by distance"
    assert results[0]["distance"] < 0.01, (
        "Closest match should be the query vector itself"
    )


def test_vector_search_vid_and_distance(ecommerce_db_populated):
    """Test vector search returns vid and distance with correct types."""

    results = ecommerce_db_populated.query("""
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


def test_vector_search_with_k(ecommerce_db_populated):
    """Test vector search with different k values."""

    results = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 2


def test_vector_search_with_threshold(ecommerce_db_populated):
    """Test vector search with distance threshold filtering."""

    results_tight = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, NULL, 0.5)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert all(r["distance"] <= 0.5 for r in results_tight), (
        "All matches should be within threshold"
    )

    results_wide = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, NULL, 2.0)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results_wide) >= len(results_tight), (
        "Larger threshold should return more results"
    )


def test_vector_search_fetch_nodes(ecommerce_db_populated):
    """Test vector search with YIELD node to get full node properties."""

    results = ecommerce_db_populated.query("""
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


def test_fetch_nodes_returns_properties_and_distance(ecommerce_db_populated):
    """Test fetch_nodes returns node properties and ordered distances."""

    results = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 2)
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    distances = [r["distance"] for r in results]
    assert distances == sorted(distances), "Results should be ordered by distance"


def test_cosine_metric_index(empty_db):
    """Test creating and using a cosine metric vector index."""

    (
        empty_db.schema()
        .label("CosineDoc")
        .property("title", "string")
        .vector("vec", 3)
        .done()
        .apply()
    )

    empty_db.execute("CREATE (d:CosineDoc {title: 'Doc1', vec: [1.0, 0.0, 0.0]})")
    empty_db.execute("CREATE (d:CosineDoc {title: 'Doc2', vec: [0.0, 1.0, 0.0]})")
    empty_db.execute("CREATE (d:CosineDoc {title: 'Doc3', vec: [0.707, 0.707, 0.0]})")
    empty_db.flush()

    empty_db.create_vector_index("CosineDoc", "vec", "cosine")

    results = empty_db.query("""
        CALL uni.vector.query('CosineDoc', 'vec', [1.0, 0.0, 0.0], 3)
        YIELD node, distance
        RETURN node.title AS title, distance
    """)

    assert len(results) == 3

    assert results[0]["distance"] < 0.01, "Most similar should have distance ~0"
    assert results[2]["distance"] > results[1]["distance"], "Distances should increase"


def test_vector_search_with_graph_traversal(ecommerce_db_populated):
    """Test combining vector search with graph traversal."""

    results = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
        YIELD node, distance
        MATCH (node)-[:IN_CATEGORY]->(c:Category)
        RETURN node.name AS product, c.name AS category, distance
    """)

    assert len(results) > 0, "Should find categories for similar products"


def test_vector_search_with_filter_expression(ecommerce_db_populated):
    """Test vector search with pre-filter expression."""

    results_expensive = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, 'price > 500')
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    for row in results_expensive:
        assert row["price"] > 500, f"Product {row['name']} should have price > 500"

    results_cheap = ecommerce_db_populated.query("""
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


def test_vector_search_empty_results(empty_db):
    """Test vector search on label with no data returns empty results."""

    (
        empty_db.schema()
        .label("EmptyLabel")
        .property("name", "string")
        .vector("vec", 4)
        .done()
        .apply()
    )

    empty_db.create_vector_index("EmptyLabel", "vec", "l2")

    results = empty_db.query("""
        CALL uni.vector.query('EmptyLabel', 'vec', [1.0, 0.0, 0.0, 0.0], 5)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert results == [], "Should return empty list for label with no data"


def test_vector_search_k_larger_than_dataset(ecommerce_db_populated):
    """Test vector search with k larger than available nodes."""

    results = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 100)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) <= 100, "Should return at most k results"
    assert len(results) >= 4, "Should return all available products"


def test_vector_search_threshold_excludes_distant_results(ecommerce_db_populated):
    """Test threshold properly excludes results beyond distance limit."""

    results = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [0.0, 0.0, 1.0, 0.0], 10, NULL, 0.1)
        YIELD node, distance
        RETURN node.name AS name, distance
    """)

    assert len(results) >= 1, "Should find at least the exact match"
    for row in results:
        assert row["distance"] <= 0.1, "All results should be within threshold"


def test_vector_search_chained_constraints(ecommerce_db_populated):
    """Test vector search with both filter and threshold."""

    results = ecommerce_db_populated.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 5, 'price > 0', 1.5)
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    assert isinstance(results, list)
    for row in results:
        assert row["distance"] <= 1.5, "Distance should be within threshold"
        assert row["price"] > 0, "Price should satisfy filter"


def test_vector_search_different_dimensions(empty_db):
    """Test vector search with different dimensionality vectors."""

    (
        empty_db.schema()
        .label("Doc5D")
        .property("name", "string")
        .vector("vec5", 5)
        .done()
        .apply()
    )

    empty_db.execute("CREATE (d:Doc5D {name: 'A', vec5: [1.0, 0.0, 0.0, 0.0, 0.0]})")
    empty_db.execute("CREATE (d:Doc5D {name: 'B', vec5: [0.0, 1.0, 0.0, 0.0, 0.0]})")
    empty_db.execute("CREATE (d:Doc5D {name: 'C', vec5: [0.0, 0.0, 1.0, 0.0, 0.0]})")
    empty_db.flush()

    empty_db.create_vector_index("Doc5D", "vec5", "l2")

    results = empty_db.query("""
        CALL uni.vector.query('Doc5D', 'vec5', [1.0, 0.0, 0.0, 0.0, 0.0], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 2
    assert results[0]["distance"] < 0.01
