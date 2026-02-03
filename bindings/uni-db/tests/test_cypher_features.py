# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for Cypher features accessible through the Python API."""

import tempfile

import pytest

import uni_db


class TestExplainProfile:
    """Tests for EXPLAIN and PROFILE functionality."""

    @pytest.fixture
    def db_with_data(self):
        """Create a database with test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Person")
            db.add_property("Person", "name", "string", False)
            db.add_property("Person", "age", "int", False)
            db.create_edge_type("KNOWS", ["Person"], ["Person"])

            # Insert test data
            db.query("CREATE (p:Person {name: 'Alice', age: 30})")
            db.query("CREATE (p:Person {name: 'Bob', age: 25})")
            db.query("CREATE (p:Person {name: 'Charlie', age: 35})")
            db.query(
                """
                MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
                CREATE (a)-[:KNOWS]->(b)
            """
            )
            db.query(
                """
                MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'})
                CREATE (b)-[:KNOWS]->(c)
            """
            )
            db.flush()
            yield db

    def test_explain_returns_plan(self, db_with_data):
        """Test that explain returns a query plan."""
        result = db_with_data.explain("MATCH (n:Person) RETURN n.name")

        assert "plan_text" in result
        assert isinstance(result["plan_text"], str)
        assert len(result["plan_text"]) > 0

    def test_explain_includes_cost_estimates(self, db_with_data):
        """Test that explain includes cost estimates."""
        result = db_with_data.explain("MATCH (n:Person) RETURN n.name")

        assert "cost_estimates" in result
        assert "estimated_rows" in result["cost_estimates"]
        assert "estimated_cost" in result["cost_estimates"]

    def test_explain_includes_index_usage(self, db_with_data):
        """Test that explain shows index usage information."""
        result = db_with_data.explain(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n"
        )

        assert "index_usage" in result
        assert isinstance(result["index_usage"], list)

    def test_profile_returns_results_and_stats(self, db_with_data):
        """Test that profile returns both results and execution statistics."""
        results, profile = db_with_data.profile("MATCH (n:Person) RETURN n.name")

        # Check results
        assert isinstance(results, list)
        assert len(results) == 3

        # Check profile output
        assert "total_time_ms" in profile
        assert "peak_memory_bytes" in profile
        assert "operators" in profile

    def test_profile_operator_stats(self, db_with_data):
        """Test that profile includes detailed operator statistics."""
        _, profile = db_with_data.profile("MATCH (n:Person) RETURN n.name")

        assert "operators" in profile
        operators = profile["operators"]
        assert isinstance(operators, list)

        for op in operators:
            assert "operator" in op
            assert "actual_rows" in op
            assert "time_ms" in op


class TestQueryWithParameters:
    """Tests for parameterized queries."""

    @pytest.fixture
    def db(self):
        """Create a test database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Person")
            db.add_property("Person", "name", "string", False)
            db.add_property("Person", "age", "int", False)
            db.query("CREATE (p:Person {name: 'Alice', age: 30})")
            db.query("CREATE (p:Person {name: 'Bob', age: 25})")
            db.flush()
            yield db

    def test_query_with_string_param(self, db):
        """Test query with string parameter."""
        builder = db.query_with(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.age AS age"
        )
        builder.param("name", "Alice")
        results = builder.fetch_all()

        assert len(results) == 1
        assert results[0]["age"] == 30

    def test_query_with_int_param(self, db):
        """Test query with integer parameter."""
        builder = db.query_with(
            "MATCH (n:Person) WHERE n.age > $min_age RETURN n.name AS name"
        )
        builder.param("min_age", 27)
        results = builder.fetch_all()

        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_query_with_multiple_params(self, db):
        """Test query with multiple parameters."""
        builder = db.query_with(
            "MATCH (n:Person) WHERE n.name = $name AND n.age = $age RETURN n"
        )
        builder.param("name", "Alice")
        builder.param("age", 30)
        results = builder.fetch_all()

        assert len(results) == 1


class TestAggregations:
    """Tests for Cypher aggregation functions."""

    @pytest.fixture
    def db(self):
        """Create a database with test data for aggregations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Product")
            db.add_property("Product", "category", "string", False)
            db.add_property("Product", "price", "float", False)
            db.add_property("Product", "quantity", "int", False)

            db.query(
                "CREATE (p:Product {category: 'Electronics', price: 100.0, quantity: 5})"
            )
            db.query(
                "CREATE (p:Product {category: 'Electronics', price: 200.0, quantity: 3})"
            )
            db.query(
                "CREATE (p:Product {category: 'Books', price: 20.0, quantity: 10})"
            )
            db.query("CREATE (p:Product {category: 'Books', price: 30.0, quantity: 8})")
            db.flush()
            yield db

    def test_count_aggregation(self, db):
        """Test COUNT aggregation."""
        results = db.query("MATCH (p:Product) RETURN count(p) AS total")
        assert len(results) == 1
        assert results[0]["total"] == 4

    def test_sum_aggregation(self, db):
        """Test SUM aggregation."""
        results = db.query("MATCH (p:Product) RETURN sum(p.quantity) AS total_qty")
        assert len(results) == 1
        assert results[0]["total_qty"] == 26

    def test_avg_aggregation(self, db):
        """Test AVG aggregation."""
        results = db.query("MATCH (p:Product) RETURN avg(p.price) AS avg_price")
        assert len(results) == 1
        # Average of 100, 200, 20, 30 = 350 / 4 = 87.5
        assert abs(results[0]["avg_price"] - 87.5) < 0.01

    def test_min_max_aggregation(self, db):
        """Test MIN and MAX aggregations."""
        results = db.query(
            "MATCH (p:Product) RETURN min(p.price) AS min_price, max(p.price) AS max_price"
        )
        assert len(results) == 1
        assert results[0]["min_price"] == 20.0
        assert results[0]["max_price"] == 200.0

    def test_group_by_aggregation(self, db):
        """Test aggregation with GROUP BY."""
        results = db.query(
            """
            MATCH (p:Product)
            RETURN p.category AS category, sum(p.quantity) AS total_qty
            ORDER BY category
        """
        )
        assert len(results) == 2
        # Check that both categories are present with correct totals
        categories = {r["category"]: r["total_qty"] for r in results}
        assert categories["Books"] == 18
        assert categories["Electronics"] == 8


class TestOrderingAndLimits:
    """Tests for ORDER BY, LIMIT, and SKIP."""

    @pytest.fixture
    def db(self):
        """Create a database with numbered test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Item")
            db.add_property("Item", "num", "int", False)
            db.add_property("Item", "name", "string", False)

            for i in range(10):
                db.query(f"CREATE (n:Item {{num: {i}, name: 'Item{i}'}})")
            db.flush()
            yield db

    def test_order_by_asc(self, db):
        """Test ORDER BY ascending."""
        results = db.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num ASC LIMIT 3"
        )
        assert [r["num"] for r in results] == [0, 1, 2]

    def test_order_by_desc(self, db):
        """Test ORDER BY descending."""
        results = db.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num DESC LIMIT 3"
        )
        assert [r["num"] for r in results] == [9, 8, 7]

    def test_limit(self, db):
        """Test LIMIT clause."""
        results = db.query("MATCH (n:Item) RETURN n.num AS num LIMIT 5")
        assert len(results) == 5

    def test_skip(self, db):
        """Test SKIP clause."""
        results = db.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num SKIP 5 LIMIT 5"
        )
        assert [r["num"] for r in results] == [5, 6, 7, 8, 9]


class TestPatternMatching:
    """Tests for Cypher pattern matching."""

    @pytest.fixture
    def db(self):
        """Create a database with a simple social graph."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Person")
            db.add_property("Person", "name", "string", False)
            db.create_edge_type("KNOWS", ["Person"], ["Person"])
            db.create_edge_type("WORKS_WITH", ["Person"], ["Person"])

            # Create a small social network
            db.query("CREATE (p:Person {name: 'Alice'})")
            db.query("CREATE (p:Person {name: 'Bob'})")
            db.query("CREATE (p:Person {name: 'Charlie'})")
            db.query("CREATE (p:Person {name: 'David'})")

            db.query(
                """
                MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
                CREATE (a)-[:KNOWS]->(b)
            """
            )
            db.query(
                """
                MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'})
                CREATE (b)-[:KNOWS]->(c)
            """
            )
            db.query(
                """
                MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'})
                CREATE (a)-[:WORKS_WITH]->(c)
            """
            )
            db.flush()
            yield db

    def test_simple_relationship_match(self, db):
        """Test matching a simple relationship."""
        results = db.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a_name, b.name AS b_name"
        )
        assert len(results) == 2

    def test_relationship_type_filter(self, db):
        """Test filtering by relationship type."""
        results = db.query(
            "MATCH (a:Person)-[:WORKS_WITH]->(b:Person) RETURN a.name AS a_name, b.name AS b_name"
        )
        assert len(results) == 1
        assert results[0]["a_name"] == "Alice"
        assert results[0]["b_name"] == "Charlie"

    def test_variable_length_path(self, db):
        """Test variable length path pattern."""
        results = db.query(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN b.name AS name"
        )
        # Alice->Bob, Alice->Bob->Charlie
        names = [r["name"] for r in results]
        assert "Bob" in names
        assert "Charlie" in names

    def test_bidirectional_relationship(self, db):
        """Test matching relationships in any direction."""
        results = db.query(
            "MATCH (a:Person {name: 'Bob'})-[:KNOWS]-(b:Person) RETURN b.name AS name"
        )
        # Bob is connected to Alice (incoming) and Charlie (outgoing)
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Charlie" in names
