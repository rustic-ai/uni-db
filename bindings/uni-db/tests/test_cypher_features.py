# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for Cypher features accessible through the Python API."""

import pytest

import uni_db


class TestExplainProfile:
    """Tests for EXPLAIN and PROFILE functionality."""

    @pytest.fixture
    def db_with_data(self):
        """Create a database with test data."""
        db = uni_db.UniBuilder.temporary().build()
        db.schema().label("Person").property("name", "string").property(
            "age", "int"
        ).done().edge_type("KNOWS", ["Person"], ["Person"]).done().apply()

        # Insert test data via transaction
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
        tx.execute("""
            MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
            CREATE (a)-[:KNOWS]->(b)
        """)
        tx.execute("""
            MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'})
            CREATE (b)-[:KNOWS]->(c)
        """)
        tx.commit()
        db.flush()
        return db, session

    def test_explain_returns_plan(self, db_with_data):
        """Test that explain returns a query plan."""
        db, session = db_with_data
        result = session.query_with("MATCH (n:Person) RETURN n.name").explain()

        assert hasattr(result, "plan_text")
        assert isinstance(result.plan_text, str)
        assert len(result.plan_text) > 0

    def test_explain_includes_cost_estimates(self, db_with_data):
        """Test that explain includes cost estimates."""
        db, session = db_with_data
        result = session.query_with("MATCH (n:Person) RETURN n.name").explain()

        assert hasattr(result, "cost_estimates")
        # cost_estimates is a dict
        assert isinstance(result.cost_estimates, dict)

    def test_explain_includes_index_usage(self, db_with_data):
        """Test that explain shows index usage information."""
        db, session = db_with_data
        result = session.query_with(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n"
        ).explain()

        assert hasattr(result, "index_usage")
        assert isinstance(result.index_usage, list)

    def test_profile_returns_results_and_stats(self, db_with_data):
        """Test that profile returns both results and execution statistics."""
        db, session = db_with_data
        results, profile = session.query_with(
            "MATCH (n:Person) RETURN n.name"
        ).profile()

        # Check results - QueryResult supports len() and iteration
        assert len(results) == 3

        # Check profile output - ProfileOutput object
        assert hasattr(profile, "total_time_ms")
        assert hasattr(profile, "peak_memory_bytes")
        assert hasattr(profile, "operators")

    def test_profile_operator_stats(self, db_with_data):
        """Test that profile includes detailed operator statistics."""
        db, session = db_with_data
        _, profile = session.query_with("MATCH (n:Person) RETURN n.name").profile()

        assert hasattr(profile, "operators")
        operators = profile.operators
        assert isinstance(operators, list)

        for op in operators:
            # operators are dicts with operator, actual_rows, time_ms keys
            assert "operator" in op
            assert "actual_rows" in op
            assert "time_ms" in op


class TestQueryWithParameters:
    """Tests for parameterized queries."""

    @pytest.fixture
    def db(self):
        """Create a test database."""
        db = uni_db.UniBuilder.temporary().build()
        db.schema().label("Person").property("name", "string").property(
            "age", "int"
        ).apply()
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.commit()
        db.flush()
        return db, session

    def test_query_with_string_param(self, db):
        """Test query with string parameter."""
        _, session = db
        builder = session.query_with(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.age AS age"
        )
        builder.param("name", "Alice")
        results = builder.fetch_all()

        assert len(results) == 1
        assert results[0]["age"] == 30

    def test_query_with_int_param(self, db):
        """Test query with integer parameter."""
        _, session = db
        builder = session.query_with(
            "MATCH (n:Person) WHERE n.age > $min_age RETURN n.name AS name"
        )
        builder.param("min_age", 27)
        results = builder.fetch_all()

        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_query_with_multiple_params(self, db):
        """Test query with multiple parameters."""
        _, session = db
        builder = session.query_with(
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
        db = uni_db.UniBuilder.temporary().build()
        db.schema().label("Product").property("category", "string").property(
            "price", "float"
        ).property("quantity", "int").apply()

        session = db.session()
        tx = session.tx()
        tx.execute(
            "CREATE (p:Product {category: 'Electronics', price: 100.0, quantity: 5})"
        )
        tx.execute(
            "CREATE (p:Product {category: 'Electronics', price: 200.0, quantity: 3})"
        )
        tx.execute("CREATE (p:Product {category: 'Books', price: 20.0, quantity: 10})")
        tx.commit()
        tx = session.tx()
        tx.execute("CREATE (p:Product {category: 'Books', price: 30.0, quantity: 8})")
        tx.commit()
        db.flush()
        return db, session

    def test_count_aggregation(self, db):
        """Test COUNT aggregation."""
        _, session = db
        results = session.query("MATCH (p:Product) RETURN count(p) AS total")
        assert len(results) == 1
        assert results[0]["total"] == 4

    def test_sum_aggregation(self, db):
        """Test SUM aggregation."""
        _, session = db
        results = session.query("MATCH (p:Product) RETURN sum(p.quantity) AS total_qty")
        assert len(results) == 1
        assert results[0]["total_qty"] == 26

    def test_avg_aggregation(self, db):
        """Test AVG aggregation."""
        _, session = db
        results = session.query("MATCH (p:Product) RETURN avg(p.price) AS avg_price")
        assert len(results) == 1
        # Average of 100, 200, 20, 30 = 350 / 4 = 87.5
        assert abs(results[0]["avg_price"] - 87.5) < 0.01

    def test_min_max_aggregation(self, db):
        """Test MIN and MAX aggregations."""
        _, session = db
        results = session.query(
            "MATCH (p:Product) RETURN min(p.price) AS min_price, max(p.price) AS max_price"
        )
        assert len(results) == 1
        assert results[0]["min_price"] == 20.0
        assert results[0]["max_price"] == 200.0

    def test_group_by_aggregation(self, db):
        """Test aggregation with GROUP BY."""
        _, session = db
        results = session.query("""
            MATCH (p:Product)
            RETURN p.category AS category, sum(p.quantity) AS total_qty
            ORDER BY category
        """)
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
        db = uni_db.UniBuilder.temporary().build()
        db.schema().label("Item").property("num", "int").property(
            "name", "string"
        ).apply()

        session = db.session()
        tx = session.tx()
        for i in range(10):
            tx.execute(f"CREATE (n:Item {{num: {i}, name: 'Item{i}'}})")
        tx.commit()
        db.flush()
        return db, session

    def test_order_by_asc(self, db):
        """Test ORDER BY ascending."""
        _, session = db
        results = session.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num ASC LIMIT 3"
        )
        assert [r["num"] for r in results] == [0, 1, 2]

    def test_order_by_desc(self, db):
        """Test ORDER BY descending."""
        _, session = db
        results = session.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num DESC LIMIT 3"
        )
        assert [r["num"] for r in results] == [9, 8, 7]

    def test_limit(self, db):
        """Test LIMIT clause."""
        _, session = db
        results = session.query("MATCH (n:Item) RETURN n.num AS num LIMIT 5")
        assert len(results) == 5

    def test_skip(self, db):
        """Test SKIP clause."""
        _, session = db
        results = session.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num SKIP 5 LIMIT 5"
        )
        assert [r["num"] for r in results] == [5, 6, 7, 8, 9]


class TestPatternMatching:
    """Tests for Cypher pattern matching."""

    @pytest.fixture
    def db(self):
        """Create a database with a simple social graph."""
        db = uni_db.UniBuilder.temporary().build()
        db.schema().label("Person").property("name", "string").done().edge_type(
            "KNOWS", ["Person"], ["Person"]
        ).done().edge_type("WORKS_WITH", ["Person"], ["Person"]).done().apply()

        # Create a small social network via transaction
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice'})")
        tx.execute("CREATE (p:Person {name: 'Bob'})")
        tx.execute("CREATE (p:Person {name: 'Charlie'})")
        tx.execute("CREATE (p:Person {name: 'David'})")
        tx.execute("""
            MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
            CREATE (a)-[:KNOWS]->(b)
        """)
        tx.execute("""
            MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'})
            CREATE (b)-[:KNOWS]->(c)
        """)
        tx.execute("""
            MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'})
            CREATE (a)-[:WORKS_WITH]->(c)
        """)
        tx.commit()
        db.flush()
        return db, session

    def test_simple_relationship_match(self, db):
        """Test matching a simple relationship."""
        _, session = db
        results = session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a_name, b.name AS b_name"
        )
        assert len(results) == 2

    def test_relationship_type_filter(self, db):
        """Test filtering by relationship type."""
        _, session = db
        results = session.query(
            "MATCH (a:Person)-[:WORKS_WITH]->(b:Person) RETURN a.name AS a_name, b.name AS b_name"
        )
        assert len(results) == 1
        assert results[0]["a_name"] == "Alice"
        assert results[0]["b_name"] == "Charlie"

    def test_variable_length_path(self, db):
        """Test variable length path pattern."""
        _, session = db
        results = session.query(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN b.name AS name"
        )
        # Alice->Bob, Alice->Bob->Charlie
        names = [r["name"] for r in results]
        assert "Bob" in names
        assert "Charlie" in names

    def test_bidirectional_relationship(self, db):
        """Test matching relationships in any direction."""
        _, session = db
        results = session.query(
            "MATCH (a:Person {name: 'Bob'})-[:KNOWS]-(b:Person) RETURN b.name AS name"
        )
        # Bob is connected to Alice (incoming) and Charlie (outgoing)
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Charlie" in names


class TestQuotedIdentifiers:
    """A quoted identifier, with the doubled-backtick escape, is one name."""

    def test_escaped_names_round_trip(self):
        db = uni_db.UniBuilder.temporary().build()
        db.schema().label("Doc").property("my prop", "string").apply()
        session = db.session()
        tx = session.tx()
        tx.execute("CREATE (:Doc {`my prop`: 'a', `has``tick`: 't', `a.b`: 'd'})")
        tx.commit()
        db.flush()

        builder = session.query_with(
            "MATCH (`d d`:Doc) WHERE `d d`.`my prop` = $`p q` "
            "RETURN `d d`.`has``tick` AS `x``y`, `d d`.`a.b` AS dotted"
        )
        builder.param("p q", "a")
        results = builder.fetch_all()

        assert len(results) == 1
        assert results[0]["x`y"] == "t"
        assert results[0]["dotted"] == "d"

    def test_declaring_an_unstorable_property_name_is_refused(self):
        db = uni_db.UniBuilder.temporary().build()
        for name in ("a.b", "has`tick"):
            with pytest.raises(Exception, match="cannot contain"):
                db.schema().label("Doc").property(name, "string").apply()
