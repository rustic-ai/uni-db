# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Comprehensive end-to-end tests for sync query features.

Tests cover the full range of Cypher query capabilities including:
- Parameterized queries
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY
- ORDER BY (ASC/DESC)
- LIMIT and SKIP
- OPTIONAL MATCH (may be unsupported)
- UNION queries (may be unsupported)
- WITH clauses (multi-stage queries)
- Variable-length paths
- WHERE clause filters (AND, OR, NOT, comparisons)
- DISTINCT
- RETURN aliases
- Query builder API (query_with)
- Query introspection (explain, profile)
"""


class TestParameterizedQueries:
    """Tests for parameterized queries using the sync API."""

    def test_query_with_string_param(self, social_db_populated):
        """Test query with string parameter."""
        results = social_db_populated.query(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.age AS age",
            {"name": "Alice"},
        )

        assert len(results) == 1
        assert results[0]["age"] == 30

    def test_query_with_int_param(self, social_db_populated):
        """Test query with integer parameter."""
        results = social_db_populated.query(
            "MATCH (n:Person) WHERE n.age > $min_age RETURN n.name AS name ORDER BY n.name",
            {"min_age": 30},
        )

        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "Charlie" in names
        assert "Eve" in names

    def test_query_with_multiple_params(self, social_db_populated):
        """Test query with multiple parameters."""
        results = social_db_populated.query(
            "MATCH (n:Person) WHERE n.name = $name AND n.age = $age RETURN n.name AS name",
            {"name": "Bob", "age": 25},
        )

        assert len(results) == 1
        assert results[0]["name"] == "Bob"


class TestAggregations:
    """Tests for Cypher aggregation functions."""

    def test_count_aggregation(self, social_db_populated):
        """Test COUNT aggregation."""
        results = social_db_populated.query("MATCH (p:Person) RETURN count(p) AS total")
        assert len(results) == 1
        assert results[0]["total"] == 5

    def test_sum_aggregation(self, social_db_populated):
        """Test SUM aggregation."""
        results = social_db_populated.query(
            "MATCH (p:Person) RETURN sum(p.age) AS total_age"
        )
        assert len(results) == 1
        assert results[0]["total_age"] == 150  # 30 + 25 + 35 + 28 + 32

    def test_avg_aggregation(self, social_db_populated):
        """Test AVG aggregation."""
        results = social_db_populated.query(
            "MATCH (p:Person) RETURN avg(p.age) AS avg_age"
        )
        assert len(results) == 1
        assert abs(results[0]["avg_age"] - 30.0) < 0.01  # 150 / 5 = 30

    def test_min_max_aggregation(self, social_db_populated):
        """Test MIN and MAX aggregations."""
        results = social_db_populated.query(
            "MATCH (p:Person) RETURN min(p.age) AS min_age, max(p.age) AS max_age"
        )
        assert len(results) == 1
        assert results[0]["min_age"] == 25
        assert results[0]["max_age"] == 35

    def test_group_by_aggregation(self, social_db_populated):
        """Test aggregation with GROUP BY."""
        results = social_db_populated.query(
            """
            MATCH (p:Person)-[:WORKS_AT]->(c:Company)
            RETURN c.name AS company, count(p) AS employee_count
            ORDER BY company
        """
        )
        assert len(results) == 2
        companies = {r["company"]: r["employee_count"] for r in results}
        assert companies["TechCorp"] == 2
        assert companies["StartupInc"] == 1


class TestOrderingAndLimits:
    """Tests for ORDER BY, LIMIT, and SKIP clauses."""

    def test_order_by_asc(self, social_db_populated):
        """Test ORDER BY ascending."""
        results = social_db_populated.query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC"
        )
        assert len(results) == 5
        # Bob(25), Diana(28), Alice(30), Eve(32), Charlie(35)
        assert results[0]["name"] == "Bob"
        assert results[1]["name"] == "Diana"
        assert results[2]["name"] == "Alice"

    def test_order_by_desc(self, social_db_populated):
        """Test ORDER BY descending."""
        results = social_db_populated.query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age DESC"
        )
        assert len(results) == 5
        # Charlie(35), Eve(32), Alice(30), Diana(28), Bob(25)
        assert results[0]["name"] == "Charlie"
        assert results[1]["name"] == "Eve"
        assert results[2]["name"] == "Alice"

    def test_limit(self, social_db_populated):
        """Test LIMIT clause."""
        results = social_db_populated.query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age LIMIT 3"
        )
        assert len(results) == 3
        assert results[0]["name"] == "Bob"
        assert results[1]["name"] == "Diana"
        assert results[2]["name"] == "Alice"

    def test_skip_and_limit(self, social_db_populated):
        """Test SKIP and LIMIT together."""
        results = social_db_populated.query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age SKIP 2 LIMIT 2"
        )
        assert len(results) == 2
        # Skip Bob(25), Diana(28), then take Alice(30), Eve(32)
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Eve"


class TestAdvancedCypher:
    """Tests for advanced Cypher features."""

    def test_optional_match(self, social_db_populated):
        """Test OPTIONAL MATCH for nullable relationships."""
        results = social_db_populated.query(
            """
            MATCH (p:Person {name: 'Diana'})
            OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
            RETURN p.name AS name, c.name AS company
        """
        )
        assert len(results) == 1
        assert results[0]["name"] == "Diana"
        assert results[0]["company"] is None

    def test_union_queries(self, social_db_populated):
        """Test UNION to combine multiple query results."""
        results = social_db_populated.query(
            """
            MATCH (p:Person {name: 'Alice'})
            RETURN p.name AS name
            UNION
            MATCH (p:Person {name: 'Bob'})
            RETURN p.name AS name
        """
        )
        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Bob" in names

    def test_with_clause(self, social_db_populated):
        """Test WITH clause for multi-stage queries."""
        results = social_db_populated.query(
            """
            MATCH (p:Person)
            WITH p.age AS age, count(p) AS cnt
            WHERE age > 28
            RETURN age, cnt
            ORDER BY age
        """
        )
        assert len(results) == 3
        # Ages: 30 (Alice), 32 (Eve), 35 (Charlie)
        assert results[0]["age"] == 30
        assert results[0]["cnt"] == 1
        assert results[1]["age"] == 32
        assert results[2]["age"] == 35

    def test_variable_length_path_1_to_2(self, social_db_populated):
        """Test variable-length path pattern *1..2."""
        results = social_db_populated.query(
            """
            MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person)
            RETURN DISTINCT b.name AS name
            ORDER BY name
        """
        )
        # Alice knows Bob (1 hop) and Charlie (1 hop)
        # Bob knows Charlie (2 hops from Alice)
        # So we should get Bob and Charlie
        names = [r["name"] for r in results]
        assert "Bob" in names
        assert "Charlie" in names

    def test_variable_length_path_1_to_3(self, social_db_populated):
        """Test variable-length path pattern *1..3."""
        results = social_db_populated.query(
            """
            MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person)
            RETURN DISTINCT b.name AS name
            ORDER BY name
        """
        )
        # Alice -> Bob (1), Alice -> Charlie (1)
        # Alice -> Bob -> Charlie (2)
        names = [r["name"] for r in results]
        assert "Bob" in names
        assert "Charlie" in names


class TestWhereClause:
    """Tests for WHERE clause filters."""

    def test_where_and(self, social_db_populated):
        """Test WHERE with AND."""
        results = social_db_populated.query(
            """
            MATCH (p:Person)
            WHERE p.age > 25 AND p.age < 35
            RETURN p.name AS name
            ORDER BY name
        """
        )
        # Ages: 28 (Diana), 30 (Alice), 32 (Eve)
        assert len(results) == 3
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Diana" in names
        assert "Eve" in names

    def test_where_or(self, social_db_populated):
        """Test WHERE with OR."""
        results = social_db_populated.query(
            """
            MATCH (p:Person)
            WHERE p.name = 'Alice' OR p.name = 'Bob'
            RETURN p.name AS name
            ORDER BY name
        """
        )
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Bob"

    def test_where_not(self, social_db_populated):
        """Test WHERE with NOT."""
        results = social_db_populated.query(
            """
            MATCH (p:Person)
            WHERE NOT p.age >= 30
            RETURN p.name AS name
            ORDER BY name
        """
        )
        # Ages < 30: Bob (25), Diana (28)
        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "Bob" in names
        assert "Diana" in names

    def test_where_comparisons(self, social_db_populated):
        """Test WHERE with comparison operators."""
        # Test >
        results = social_db_populated.query(
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name ORDER BY name"
        )
        names = [r["name"] for r in results]
        assert "Charlie" in names
        assert "Eve" in names

        # Test <
        results = social_db_populated.query(
            "MATCH (p:Person) WHERE p.age < 30 RETURN p.name AS name ORDER BY name"
        )
        names = [r["name"] for r in results]
        assert "Bob" in names
        assert "Diana" in names

        # Test >=
        results = social_db_populated.query(
            "MATCH (p:Person) WHERE p.age >= 32 RETURN p.name AS name ORDER BY name"
        )
        names = [r["name"] for r in results]
        assert "Charlie" in names
        assert "Eve" in names

        # Test <=
        results = social_db_populated.query(
            "MATCH (p:Person) WHERE p.age <= 28 RETURN p.name AS name ORDER BY name"
        )
        names = [r["name"] for r in results]
        assert "Bob" in names
        assert "Diana" in names

        # Test <> (not equal)
        results = social_db_populated.query(
            "MATCH (p:Person) WHERE p.age <> 30 RETURN p.name AS name ORDER BY p.age"
        )
        assert len(results) == 4
        names = [r["name"] for r in results]
        assert "Alice" not in names


class TestDistinctAndAliases:
    """Tests for DISTINCT and RETURN aliases."""

    def test_distinct(self, social_db_populated):
        """Test DISTINCT to remove duplicates."""
        results = social_db_populated.query(
            """
            MATCH (p:Person)-[:WORKS_AT]->(c:Company)
            RETURN DISTINCT c.name AS company
            ORDER BY company
        """
        )
        companies = [r["company"] for r in results]
        assert len(companies) >= 2
        assert "StartupInc" in companies
        assert "TechCorp" in companies

    def test_return_aliases(self, social_db_populated):
        """Test RETURN with various aliases."""
        results = social_db_populated.query(
            """
            MATCH (p:Person)
            RETURN p.name AS person_name, p.age AS years_old, p.email AS contact_email
            ORDER BY person_name
            LIMIT 1
        """
        )
        assert len(results) == 1
        assert "person_name" in results[0]
        assert "years_old" in results[0]
        assert "contact_email" in results[0]
        assert results[0]["person_name"] == "Alice"
        assert results[0]["years_old"] == 30
        assert results[0]["contact_email"] == "alice@example.com"


class TestQueryBuilder:
    """Tests for query_with() builder API."""

    def test_query_with_param_fetch_all(self, social_db_populated):
        """Test query_with() builder with param() and fetch_all()."""
        results = (
            social_db_populated.query_with(
                "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS name ORDER BY name"
            )
            .param("min_age", 30)
            .fetch_all()
        )

        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "Charlie" in names
        assert "Eve" in names

    def test_query_with_multiple_params(self, social_db_populated):
        """Test query_with() builder with multiple params."""
        results = (
            social_db_populated.query_with(
                "MATCH (p:Person) WHERE p.age >= $min AND p.age <= $max RETURN p.name AS name ORDER BY name"
            )
            .param("min", 28)
            .param("max", 32)
            .fetch_all()
        )

        assert len(results) == 3
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Diana" in names
        assert "Eve" in names

    def test_query_with_params_dict(self, social_db_populated):
        """Test query_with() builder with multiple chained param() calls."""
        results = (
            social_db_populated.query_with(
                "MATCH (p:Person) WHERE p.name = $name AND p.age = $age RETURN p"
            )
            .param("name", "Bob")
            .param("age", 25)
            .fetch_all()
        )

        assert len(results) == 1

    def test_query_with_timeout(self, social_db_populated):
        """Test query_with() builder with timeout."""
        results = (
            social_db_populated.query_with(
                "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
            )
            .timeout(30.0)
            .fetch_all()
        )

        assert len(results) == 5

    def test_query_with_max_memory(self, social_db_populated):
        """Test query_with() builder with max_memory."""
        results = (
            social_db_populated.query_with(
                "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
            )
            .max_memory(100 * 1024 * 1024)  # 100 MB
            .fetch_all()
        )

        assert len(results) == 5


class TestExplainAndProfile:
    """Tests for query introspection via explain and profile."""

    def test_explain_returns_plan(self, social_db_populated):
        """Test that explain returns a query plan."""
        result = social_db_populated.explain("MATCH (n:Person) RETURN n.name")

        assert "plan_text" in result
        assert isinstance(result["plan_text"], str)
        assert len(result["plan_text"]) > 0

    def test_explain_includes_cost_estimates(self, social_db_populated):
        """Test that explain includes cost estimates."""
        result = social_db_populated.explain("MATCH (n:Person) RETURN n.name")

        assert "cost_estimates" in result
        assert "estimated_rows" in result["cost_estimates"]
        assert "estimated_cost" in result["cost_estimates"]

    def test_explain_includes_index_usage(self, social_db_populated):
        """Test that explain shows index usage information."""
        result = social_db_populated.explain(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n"
        )

        assert "index_usage" in result
        assert isinstance(result["index_usage"], list)

    def test_profile_returns_results_and_stats(self, social_db_populated):
        """Test that profile returns both results and execution statistics."""
        results, profile = social_db_populated.profile(
            "MATCH (n:Person) RETURN n.name AS name"
        )

        assert isinstance(results, list)
        assert len(results) == 5

        assert "total_time_ms" in profile
        assert "peak_memory_bytes" in profile
        assert "operators" in profile

    def test_profile_operator_stats(self, social_db_populated):
        """Test that profile includes detailed operator statistics."""
        _, profile = social_db_populated.profile(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name"
        )

        assert "operators" in profile
        operators = profile["operators"]
        assert isinstance(operators, list)

        for op in operators:
            assert "operator" in op
            assert "actual_rows" in op
            assert "time_ms" in op


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_empty_result_set(self, social_db_populated):
        """Test query returning empty result set."""
        results = social_db_populated.query(
            "MATCH (p:Person) WHERE p.age > 100 RETURN p.name AS name"
        )
        assert len(results) == 0

    def test_nullable_property_filter(self, social_db_populated):
        """Test filtering on nullable properties."""
        # Charlie and Eve don't have email
        results = social_db_populated.query(
            """
            MATCH (p:Person)
            WHERE p.email IS NULL
            RETURN p.name AS name
            ORDER BY name
        """
        )
        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "Charlie" in names
        assert "Eve" in names

    def test_nullable_property_not_null(self, social_db_populated):
        """Test filtering for non-null nullable properties."""
        # Alice, Bob, Diana have email
        results = social_db_populated.query(
            """
            MATCH (p:Person)
            WHERE p.email IS NOT NULL
            RETURN p.name AS name
            ORDER BY name
        """
        )
        assert len(results) == 3
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Bob" in names
        assert "Diana" in names

    def test_relationship_property_filter(self, social_db_populated):
        """Test filtering on relationship properties."""
        results = social_db_populated.query(
            """
            MATCH (a:Person)-[k:KNOWS]->(b:Person)
            WHERE k.since IS NOT NULL AND k.since >= 2018
            RETURN a.name AS src, b.name AS dst, k.since AS year
            ORDER BY k.since
        """
        )
        assert len(results) == 2
        # Bob->Charlie (2018), Alice->Charlie (2020)
        assert results[0]["year"] == 2018
        assert results[1]["year"] == 2020

    def test_complex_path_pattern(self, social_db_populated):
        """Test complex path pattern matching."""
        results = social_db_populated.query(
            """
            MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)
            RETURN a.name AS person, b.name AS friend, c.name AS company
            ORDER BY person, friend
        """
        )
        # Alice->Bob->TechCorp, Alice->Charlie->StartupInc
        # Bob->Charlie->StartupInc
        assert len(results) >= 2


class TestSchemaOnEmptyDb:
    """Tests that create schema on empty database and run queries."""

    def test_create_and_query_on_empty_db(self, social_db):
        """Test creating data and querying on initially empty database."""
        # Create a single person
        social_db.execute("CREATE (p:Person {name: 'TestUser', age: 99})")
        social_db.flush()

        # Query it back
        results = social_db.query(
            "MATCH (p:Person) WHERE p.name = 'TestUser' RETURN p.age AS age"
        )
        assert len(results) == 1
        assert results[0]["age"] == 99
