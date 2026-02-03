# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for query features."""

import pytest


@pytest.mark.asyncio
async def test_parameterized_query_string_param(async_social_db_populated):
    """Test parameterized query with string parameter."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person {name: $name}) RETURN p.name AS name, p.age AS age",
        params={"name": "Alice"},
    )

    assert len(results) == 1
    assert results[0]["name"] == "Alice"
    assert results[0]["age"] == 30


@pytest.mark.asyncio
async def test_parameterized_query_int_param(async_social_db_populated):
    """Test parameterized query with integer parameter."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS name ORDER BY p.name",
        params={"min_age": 30},
    )

    assert len(results) == 2
    names = [r["name"] for r in results]
    assert names == ["Charlie", "Eve"]


@pytest.mark.asyncio
async def test_parameterized_query_multiple_params(async_social_db_populated):
    """Test parameterized query with multiple parameters."""

    results = await async_social_db_populated.query(
        """
        MATCH (p:Person)
        WHERE p.age >= $min_age AND p.age <= $max_age
        RETURN p.name AS name
        ORDER BY p.name
        """,
        params={"min_age": 25, "max_age": 30},
    )

    assert len(results) == 3
    names = [r["name"] for r in results]
    assert names == ["Alice", "Bob", "Diana"]


@pytest.mark.asyncio
async def test_count_aggregation(async_social_db_populated):
    """Test COUNT aggregation."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) RETURN count(p) AS total"
    )

    assert len(results) == 1
    assert results[0]["total"] == 5


@pytest.mark.asyncio
async def test_sum_aggregation(async_social_db_populated):
    """Test SUM aggregation."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) RETURN sum(p.age) AS total_age"
    )

    assert len(results) == 1
    # Alice(30) + Bob(25) + Charlie(35) + Diana(28) + Eve(32) = 150
    assert results[0]["total_age"] == 150


@pytest.mark.asyncio
async def test_avg_aggregation(async_social_db_populated):
    """Test AVG aggregation."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) RETURN avg(p.age) AS avg_age"
    )

    assert len(results) == 1
    # 150 / 5 = 30
    assert results[0]["avg_age"] == 30.0


@pytest.mark.asyncio
async def test_min_max_aggregation(async_social_db_populated):
    """Test MIN and MAX aggregation."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) RETURN min(p.age) AS min_age, max(p.age) AS max_age"
    )

    assert len(results) == 1
    assert results[0]["min_age"] == 25  # Bob
    assert results[0]["max_age"] == 35  # Charlie


@pytest.mark.asyncio
async def test_group_by_with_aggregation(async_social_db_populated):
    """Test GROUP BY with aggregation."""

    results = await async_social_db_populated.query(
        """
        MATCH (p:Person)-[:WORKS_AT]->(c:Company)
        RETURN c.name AS company, count(p) AS employee_count
        ORDER BY c.name
        """
    )

    assert len(results) == 2
    assert results[0]["company"] == "StartupInc"
    assert results[0]["employee_count"] == 1  # Charlie
    assert results[1]["company"] == "TechCorp"
    assert results[1]["employee_count"] == 2  # Alice, Bob


@pytest.mark.asyncio
async def test_order_by_asc(async_social_db_populated):
    """Test ORDER BY ascending."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY p.age ASC"
    )

    assert len(results) == 5
    ages = [r["age"] for r in results]
    assert ages == [25, 28, 30, 32, 35]  # Bob, Diana, Alice, Eve, Charlie


@pytest.mark.asyncio
async def test_order_by_desc(async_social_db_populated):
    """Test ORDER BY descending."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY p.age DESC"
    )

    assert len(results) == 5
    ages = [r["age"] for r in results]
    assert ages == [35, 32, 30, 28, 25]  # Charlie, Eve, Alice, Diana, Bob


@pytest.mark.asyncio
async def test_limit(async_social_db_populated):
    """Test LIMIT clause."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name LIMIT 3"
    )

    assert len(results) == 3
    names = [r["name"] for r in results]
    assert names == ["Alice", "Bob", "Charlie"]


@pytest.mark.asyncio
async def test_skip_and_limit(async_social_db_populated):
    """Test SKIP and LIMIT combination."""

    results = await async_social_db_populated.query(
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name SKIP 2 LIMIT 2"
    )

    assert len(results) == 2
    names = [r["name"] for r in results]
    assert names == ["Charlie", "Diana"]


@pytest.mark.asyncio
async def test_optional_match(async_social_db_populated):
    """Test OPTIONAL MATCH for nodes that may not have relationships."""

    results = await async_social_db_populated.query(
        """
        MATCH (p:Person {name: 'Eve'})
        OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
        RETURN p.name AS name, c.name AS company
        """
    )

    assert len(results) == 1
    assert results[0]["name"] == "Eve"
    assert results[0]["company"] is None


@pytest.mark.asyncio
async def test_union_queries(async_social_db_populated):
    """Test UNION of multiple queries."""

    results = await async_social_db_populated.query(
        """
        MATCH (p:Person {name: 'Alice'}) RETURN p.name AS name
        UNION
        MATCH (p:Person {name: 'Bob'}) RETURN p.name AS name
        """
    )

    assert len(results) == 2
    names = sorted([r["name"] for r in results])
    assert names == ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_with_clause(async_social_db_populated):
    """Test WITH clause for query composition."""

    results = await async_social_db_populated.query(
        """
        MATCH (p:Person)
        WITH p
        WHERE p.age > 30
        RETURN p.name AS name
        ORDER BY p.name
        """
    )

    assert len(results) == 2
    names = [r["name"] for r in results]
    assert names == ["Charlie", "Eve"]


@pytest.mark.asyncio
async def test_variable_length_paths(async_social_db_populated):
    """Test variable-length path patterns."""

    results = await async_social_db_populated.query(
        """
        MATCH (alice:Person {name: 'Alice'})-[:KNOWS*1..2]->(friend:Person)
        RETURN DISTINCT friend.name AS name
        ORDER BY friend.name
        """
    )

    # Alice knows Bob directly and Charlie both directly and via Bob (1-2 hops)
    assert len(results) >= 2
    names = [r["name"] for r in results]
    assert "Bob" in names
    assert "Charlie" in names


@pytest.mark.asyncio
async def test_where_with_logical_operators(async_social_db_populated):
    """Test WHERE clause with AND, OR, NOT operators."""

    # Test AND
    results = await async_social_db_populated.query(
        """
        MATCH (p:Person)
        WHERE p.age > 25 AND p.age < 32
        RETURN p.name AS name
        ORDER BY name
        """
    )

    names = [r["name"] for r in results]
    assert "Alice" in names  # 30
    assert "Diana" in names  # 28
    assert "Bob" not in names  # 25
    assert "Eve" not in names  # 32

    # Test OR
    results = await async_social_db_populated.query(
        """
        MATCH (p:Person)
        WHERE p.name = 'Alice' OR p.name = 'Bob'
        RETURN p.name AS name
        ORDER BY name
        """
    )

    assert len(results) == 2
    names = [r["name"] for r in results]
    assert names == ["Alice", "Bob"]

    # Test NOT
    results = await async_social_db_populated.query(
        """
        MATCH (p:Person)
        WHERE NOT p.age > 30
        RETURN p.name AS name
        ORDER BY name
        """
    )

    names = [r["name"] for r in results]
    assert "Bob" in names  # 25
    assert "Alice" in names  # 30
    assert "Diana" in names  # 28
    assert "Charlie" not in names  # 35
    assert "Eve" not in names  # 32


@pytest.mark.asyncio
async def test_where_with_comparisons(async_social_db_populated):
    """Test WHERE clause with various comparison operators."""

    # Greater than
    results = await async_social_db_populated.query(
        "MATCH (p:Person) WHERE p.age > 30 RETURN count(p) AS count"
    )
    assert results[0]["count"] == 2  # Charlie(35), Eve(32)

    # Greater than or equal
    results = await async_social_db_populated.query(
        "MATCH (p:Person) WHERE p.age >= 30 RETURN count(p) AS count"
    )
    assert results[0]["count"] == 3  # Alice(30), Charlie(35), Eve(32)

    # Less than
    results = await async_social_db_populated.query(
        "MATCH (p:Person) WHERE p.age < 30 RETURN count(p) AS count"
    )
    assert results[0]["count"] == 2  # Bob(25), Diana(28)

    # Less than or equal
    results = await async_social_db_populated.query(
        "MATCH (p:Person) WHERE p.age <= 30 RETURN count(p) AS count"
    )
    assert results[0]["count"] == 3  # Bob(25), Diana(28), Alice(30)

    # Equality
    results = await async_social_db_populated.query(
        "MATCH (p:Person) WHERE p.age = 30 RETURN p.name AS name"
    )
    assert len(results) == 1
    assert results[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_distinct(async_social_db_populated):
    """Test DISTINCT keyword to remove duplicates."""

    results = await async_social_db_populated.query(
        """
        MATCH (p:Person)-[:KNOWS]->(friend:Person)
        RETURN DISTINCT friend.name AS name
        ORDER BY friend.name
        """
    )

    # Bob is known by Alice, Charlie is known by both Alice and Bob
    names = [r["name"] for r in results]
    assert "Bob" in names
    assert "Charlie" in names
    # Ensure no duplicates
    assert len(names) == len(set(names))


@pytest.mark.asyncio
async def test_return_aliases(async_social_db_populated):
    """Test RETURN clause with aliases."""

    results = await async_social_db_populated.query(
        """
        MATCH (p:Person {name: 'Alice'})
        RETURN p.name AS person_name, p.age AS person_age, p.email AS contact_email
        """
    )

    assert len(results) == 1
    result = results[0]
    assert "person_name" in result
    assert "person_age" in result
    assert "contact_email" in result
    assert result["person_name"] == "Alice"
    assert result["person_age"] == 30
    assert result["contact_email"] == "alice@example.com"


@pytest.mark.asyncio
async def test_query_with_builder_param_and_run(async_social_db_populated):
    """Test query_with() builder with param() and run()."""

    results = (
        await async_social_db_populated.query_with(
            "MATCH (p:Person {name: $name}) RETURN p.name AS name, p.age AS age"
        )
        .param("name", "Bob")
        .run()
    )

    assert len(results) == 1
    assert results[0]["name"] == "Bob"
    assert results[0]["age"] == 25


@pytest.mark.asyncio
async def test_query_with_builder_timeout(async_social_db):
    """Test query_with() builder with timeout."""
    results = (
        await async_social_db.query_with("MATCH (p:Person) RETURN count(p) AS count")
        .timeout(10.0)
        .run()
    )

    assert len(results) == 1
    assert results[0]["count"] == 0


@pytest.mark.asyncio
async def test_explain(async_social_db_populated):
    """Test explain() to get query execution plan."""

    plan = await async_social_db_populated.explain(
        "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"
    )

    assert isinstance(plan, dict)
    assert len(plan) > 0


@pytest.mark.asyncio
async def test_profile(async_social_db_populated):
    """Test profile() to get query execution statistics."""

    results, stats = await async_social_db_populated.profile(
        "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"
    )

    assert isinstance(results, list)
    assert isinstance(stats, dict)

    assert len(results) == 2
    names = sorted([r["name"] for r in results])
    assert names == ["Charlie", "Eve"]

    assert len(stats) > 0
