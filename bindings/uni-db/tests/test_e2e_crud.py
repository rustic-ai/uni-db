# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Comprehensive end-to-end CRUD tests for uni-db sync API.

Tests cover the full spectrum of CRUD operations:
- CREATE: vertices and edges with various property configurations
- READ: MATCH queries with filtering and pattern matching
- UPDATE: SET operations on vertices and edges
- DELETE: removal of vertices and edges
- MERGE: upsert operations for vertices and edges
- Parameterized queries using $param syntax
"""

import pytest


@pytest.fixture
def social_session(social_db):
    """Return a session for the social_db fixture."""
    return social_db.session()


class TestVertexCRUD:
    """Tests for vertex CRUD operations."""

    def test_create_single_vertex_and_read_back(self, social_db, social_session):
        """Test creating a single vertex and reading it back with MATCH."""
        # Create
        tx = social_session.tx()
        result = tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        assert result.nodes_created >= 1
        tx.commit()

        # Read
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.name AS name, p.age AS age"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30

    def test_create_vertex_with_all_properties(self, social_db, social_session):
        """Test creating a vertex with all defined properties including nullable ones."""
        # Create with all properties
        tx = social_session.tx()
        result = tx.execute(
            "CREATE (p:Person {name: 'Bob', age: 25, email: 'bob@example.com'})"
        )
        assert result.nodes_created >= 1
        tx.commit()

        # Read back
        results = social_session.query(
            "MATCH (p:Person {name: 'Bob'}) RETURN p.name AS name, p.age AS age, p.email AS email"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Bob"
        assert results[0]["age"] == 25
        assert results[0]["email"] == "bob@example.com"

    def test_create_vertex_with_nullable_property_omitted(
        self, social_db, social_session
    ):
        """Test creating a vertex with nullable property omitted."""
        # Create without email (nullable)
        tx = social_session.tx()
        result = tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
        assert result.nodes_created >= 1
        tx.commit()

        # Read back - email should be null/None
        results = social_session.query(
            "MATCH (p:Person {name: 'Charlie'}) RETURN p.name AS name, p.age AS age, p.email AS email"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Charlie"
        assert results[0]["age"] == 35
        assert results[0]["email"] is None

    def test_create_multiple_vertices(self, social_db, social_session):
        """Test creating multiple vertices in sequence."""
        # Create multiple vertices
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
        tx.commit()
        social_db.flush()

        # Read all back
        results = social_session.query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age"
        )
        assert len(results) == 3
        assert results[0]["name"] == "Bob"
        assert results[1]["name"] == "Alice"
        assert results[2]["name"] == "Charlie"

    def test_create_company_vertex(self, social_db, social_session):
        """Test creating a Company vertex with nullable founded property."""
        # Create with founded
        tx = social_session.tx()
        tx.execute("CREATE (c:Company {name: 'TechCorp', founded: 2010})")

        # Create without founded
        tx.execute("CREATE (c:Company {name: 'StartupInc'})")
        tx.commit()
        social_db.flush()

        # Read back
        results = social_session.query(
            "MATCH (c:Company) RETURN c.name AS name, c.founded AS founded ORDER BY c.name"
        )
        assert len(results) == 2
        assert results[0]["name"] == "StartupInc"
        assert results[0]["founded"] is None
        assert results[1]["name"] == "TechCorp"
        assert results[1]["founded"] == 2010


class TestEdgeCRUD:
    """Tests for edge CRUD operations."""

    def test_create_edge_between_vertices(self, social_db, social_session):
        """Test creating an edge between two vertices."""
        # Create vertices and edge
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")

        # Create edge
        result = tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        assert result.relationships_created >= 1
        tx.commit()
        social_db.flush()

        # Read back
        results = social_session.query(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) "
            "RETURN a.name AS a_name, b.name AS b_name"
        )
        assert len(results) == 1
        assert results[0]["a_name"] == "Alice"
        assert results[0]["b_name"] == "Bob"

    def test_create_edge_with_properties(self, social_db, social_session):
        """Test creating an edge with properties."""
        # Create vertices and edge with properties
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")

        # Create edge with properties
        result = tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2015}]->(b)"
        )
        assert result.relationships_created >= 1
        tx.commit()
        social_db.flush()

        # Read back edge property
        results = social_session.query(
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) "
            "RETURN k.since AS since"
        )
        assert len(results) == 1
        assert results[0]["since"] == 2015

    def test_create_works_at_edge_with_role(self, social_db, social_session):
        """Test creating a WORKS_AT edge with role property."""
        # Create vertices and edge with role
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (c:Company {name: 'TechCorp', founded: 2010})")

        # Create edge with role
        result = tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) "
            "CREATE (a)-[:WORKS_AT {role: 'Engineer'}]->(c)"
        )
        assert result.relationships_created >= 1
        tx.commit()
        social_db.flush()

        # Read back
        results = social_session.query(
            "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) "
            "RETURN p.name AS person, c.name AS company, w.role AS role"
        )
        assert len(results) == 1
        assert results[0]["person"] == "Alice"
        assert results[0]["company"] == "TechCorp"
        assert results[0]["role"] == "Engineer"


class TestQueryOperations:
    """Tests for various query and read operations."""

    def test_match_vertex_by_property(self, social_db, social_session):
        """Test matching vertices by specific property values."""
        # Create test data
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.execute("CREATE (p:Person {name: 'Charlie', age: 30})")
        tx.commit()
        social_db.flush()

        # Match by age
        results = social_session.query(
            "MATCH (p:Person {age: 30}) RETURN p.name AS name ORDER BY p.name"
        )
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Charlie"

    def test_match_with_where_clause(self, social_db, social_session):
        """Test MATCH with WHERE clause for filtering."""
        # Create test data
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
        tx.commit()
        social_db.flush()

        # Match with WHERE
        results = social_session.query(
            "MATCH (p:Person) WHERE p.age > 27 RETURN p.name AS name ORDER BY p.age"
        )
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Charlie"

    def test_match_return_multiple_properties(self, social_db, social_session):
        """Test returning multiple properties from a match."""
        # Create test data
        tx = social_session.tx()
        tx.execute(
            "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})"
        )
        tx.commit()
        social_db.flush()

        # Return multiple properties
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) "
            "RETURN p.name AS name, p.age AS age, p.email AS email"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30
        assert results[0]["email"] == "alice@example.com"


class TestUpdateOperations:
    """Tests for SET operations to update vertices and edges."""

    def test_set_property_on_vertex(self, social_db, social_session):
        """Test updating a property on a vertex using SET."""
        # Create vertex
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.commit()
        social_db.flush()

        # Update age
        tx = social_session.tx()
        result = tx.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 31")
        assert result.properties_set >= 1
        tx.commit()
        social_db.flush()

        # Verify update
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age"
        )
        assert len(results) == 1
        assert results[0]["age"] == 31

    def test_set_nullable_property_on_vertex(self, social_db, social_session):
        """Test setting a nullable property that was initially null."""
        # Create vertex without email
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.commit()
        social_db.flush()

        # Set email
        tx = social_session.tx()
        result = tx.execute(
            "MATCH (p:Person {name: 'Bob'}) SET p.email = 'bob@example.com'"
        )
        assert result.properties_set >= 1
        tx.commit()
        social_db.flush()

        # Verify
        results = social_session.query(
            "MATCH (p:Person {name: 'Bob'}) RETURN p.email AS email"
        )
        assert len(results) == 1
        assert results[0]["email"] == "bob@example.com"

    def test_set_property_on_edge(self, social_db, social_session):
        """Test updating a property on an edge using SET."""
        # Create vertices and edge
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2015}]->(b)"
        )
        tx.commit()
        social_db.flush()

        # Update edge property
        tx = social_session.tx()
        result = tx.execute(
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) "
            "SET k.since = 2016"
        )
        assert result.properties_set >= 1
        tx.commit()
        social_db.flush()

        # Verify
        results = social_session.query(
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) "
            "RETURN k.since AS since"
        )
        assert len(results) == 1
        assert results[0]["since"] == 2016


class TestDeleteOperations:
    """Tests for DELETE operations on vertices and edges."""

    def test_delete_edge(self, social_db, social_session):
        """Test deleting an edge."""
        # Create vertices and edge
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        tx.commit()
        social_db.flush()

        # Verify edge exists
        results = social_session.query(
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) "
            "RETURN a.name AS a_name, b.name AS b_name"
        )
        assert len(results) == 1

        # Delete edge
        tx = social_session.tx()
        result = tx.execute(
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) DELETE k"
        )
        assert result.relationships_deleted >= 1
        tx.commit()
        social_db.flush()

        # Verify edge deleted
        results = social_session.query(
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) "
            "RETURN a.name AS a_name"
        )
        assert len(results) == 0

    def test_delete_vertex(self, social_db, social_session):
        """Test deleting a vertex with no edges."""
        # Create vertex
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.commit()
        social_db.flush()

        # Verify exists
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN count(p) AS count"
        )
        assert results[0]["count"] == 1

        # Delete vertex
        tx = social_session.tx()
        result = tx.execute("MATCH (p:Person {name: 'Alice'}) DELETE p")
        assert result.nodes_deleted >= 1
        tx.commit()
        social_db.flush()

        # Verify deleted
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN count(p) AS count"
        )
        assert results[0]["count"] == 0

    def test_delete_vertex_with_cascading_edge_removal(self, social_db, social_session):
        """Test deleting a vertex and its connected edges (DETACH DELETE)."""
        # Create vertices and edges
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
        tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        tx.execute(
            "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) "
            "CREATE (b)-[:KNOWS]->(c)"
        )
        tx.commit()
        social_db.flush()

        # Verify Bob has edges
        results = social_session.query(
            "MATCH (b:Person {name: 'Bob'})-[k:KNOWS]-() RETURN count(k) AS count"
        )
        # Bob has incoming edge from Alice and outgoing edge to Charlie
        assert results[0]["count"] >= 1

        # Delete Bob and cascade edges
        tx = social_session.tx()
        result = tx.execute("MATCH (p:Person {name: 'Bob'}) DETACH DELETE p")
        assert result.nodes_deleted >= 1  # At least the vertex
        tx.commit()
        social_db.flush()

        # Verify Bob is deleted
        results = social_session.query(
            "MATCH (p:Person {name: 'Bob'}) RETURN count(p) AS count"
        )
        assert results[0]["count"] == 0

        # Verify Alice and Charlie still exist
        results = social_session.query("MATCH (p:Person) RETURN count(p) AS count")
        assert results[0]["count"] == 2


class TestMergeOperations:
    """Tests for MERGE operations (create if not exists)."""

    def test_merge_vertex_creates_when_not_exists(self, social_db, social_session):
        """Test MERGE creates vertex when it doesn't exist."""
        # Merge (should create)
        tx = social_session.tx()
        result = tx.execute("MERGE (p:Person {name: 'Alice', age: 30})")
        assert result.nodes_created >= 1
        tx.commit()
        social_db.flush()

        # Verify created
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age"
        )
        assert len(results) == 1
        assert results[0]["age"] == 30

    def test_merge_vertex_matches_when_exists(self, social_db, social_session):
        """Test MERGE matches existing vertex instead of creating duplicate."""
        # Create vertex
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.commit()
        social_db.flush()

        # Merge same vertex (should match, not create)
        tx = social_session.tx()
        tx.execute("MERGE (p:Person {name: 'Alice', age: 30})")
        tx.commit()
        social_db.flush()

        # Verify only one vertex exists
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN count(p) AS count"
        )
        assert results[0]["count"] == 1

    def test_merge_edge(self, social_db, social_session):
        """Test MERGE on edges."""
        # Create vertices
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.commit()
        social_db.flush()

        # First merge - should create
        tx = social_session.tx()
        tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "MERGE (a)-[:KNOWS]->(b)"
        )
        tx.commit()
        social_db.flush()

        # Second merge - should match existing
        tx = social_session.tx()
        tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "MERGE (a)-[:KNOWS]->(b)"
        )
        tx.commit()
        social_db.flush()

        # Verify only one edge exists
        results = social_session.query(
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) "
            "RETURN count(k) AS count"
        )
        assert results[0]["count"] == 1


class TestParameterizedQueries:
    """Tests for parameterized queries using $param syntax."""

    def test_create_with_string_parameter(self, social_db, social_session):
        """Test CREATE with string parameter."""
        # Create with parameter
        params = {"name": "Alice", "age": 30}
        tx = social_session.tx()
        result = tx.execute("CREATE (p:Person {name: $name, age: $age})", params)
        assert result.nodes_created >= 1
        tx.commit()
        social_db.flush()

        # Verify
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age"
        )
        assert len(results) == 1
        assert results[0]["age"] == 30

    def test_query_with_parameters(self, social_db, social_session):
        """Test query with parameterized WHERE clause."""
        # Create test data
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
        tx.commit()
        social_db.flush()

        # Query with parameter
        params = {"min_age": 28}
        results = social_session.query(
            "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS name ORDER BY p.age",
            params,
        )
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Charlie"

    def test_create_edge_with_parameters(self, social_db, social_session):
        """Test creating edge with parameterized properties."""
        # Create vertices
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        tx.commit()
        social_db.flush()

        # Create edge with parameters
        params = {"name1": "Alice", "name2": "Bob", "since": 2015}
        tx = social_session.tx()
        result = tx.execute(
            "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) "
            "CREATE (a)-[:KNOWS {since: $since}]->(b)",
            params,
        )
        assert result.relationships_created >= 1
        tx.commit()
        social_db.flush()

        # Verify
        results = social_session.query(
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) "
            "RETURN k.since AS since"
        )
        assert len(results) == 1
        assert results[0]["since"] == 2015

    def test_update_with_parameters(self, social_db, social_session):
        """Test SET operation with parameters."""
        # Create vertex
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.commit()
        social_db.flush()

        # Update with parameter
        params = {"name": "Alice", "new_age": 31}
        tx = social_session.tx()
        result = tx.execute(
            "MATCH (p:Person {name: $name}) SET p.age = $new_age", params
        )
        assert result.properties_set >= 1
        tx.commit()
        social_db.flush()

        # Verify
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age"
        )
        assert len(results) == 1
        assert results[0]["age"] == 31

    def test_delete_with_parameters(self, social_db, social_session):
        """Test DELETE operation with parameters."""
        # Create vertex
        tx = social_session.tx()
        tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        tx.commit()
        social_db.flush()

        # Delete with parameter
        params = {"name": "Alice"}
        tx = social_session.tx()
        result = tx.execute("MATCH (p:Person {name: $name}) DELETE p", params)
        assert result.nodes_deleted >= 1
        tx.commit()
        social_db.flush()

        # Verify deleted
        results = social_session.query(
            "MATCH (p:Person {name: 'Alice'}) RETURN count(p) AS count"
        )
        assert results[0]["count"] == 0


@pytest.mark.parametrize("label", ["Person", "Loose"])
@pytest.mark.parametrize("flushed", [False, True])
def test_underscore_property_reads_back(social_db, label, flushed):
    """A `_`-prefixed property that is not a storage column reads like any other.

    `Person` is declared (so `_foo` is undeclared on it), `Loose` is
    schemaless; both are probed before and after a flush, through the
    converted `Node` / `Edge` objects as well as scalar projections.
    """
    session = social_db.session()
    props = "name: 'A', age: 1, " if label == "Person" else ""
    tx = session.tx()
    tx.execute(
        f"CREATE (a:{label} {{{props}id: 1, _foo: 5}}), (b:{label} {{{props}id: 2}}), "
        f"(a)-[:LINK {{_bar: 7}}]->(b)"
    )
    tx.commit()
    if flushed:
        social_db.flush()

    rows = session.query(
        f"MATCH (n:{label} {{id: 1}}) RETURN n, n._foo AS f, keys(n) AS k"
    )
    assert len(rows) == 1
    node = rows[0]["n"]
    assert node["_foo"] == 5
    assert "_foo" in node
    assert rows[0]["f"] == 5
    assert "_foo" in rows[0]["k"]
    assert (
        session.query(f"MATCH (n:{label}) WHERE n._foo = 5 RETURN n.id AS id")[0]["id"]
        == 1
    )

    tx = session.tx()
    tx.execute(f"MATCH (n:{label} {{id: 1}}) SET n._id = 'u1'")
    tx.commit()
    node = session.query(f"MATCH (n:{label} {{id: 1}}) RETURN n")[0]["n"]
    assert node.get("_id") == "u1"
    assert node["_id"] == "u1"
    assert "_id" in node

    rows = session.query(f"MATCH (:{label})-[r:LINK]->() RETURN r, r._bar AS b")
    assert len(rows) == 1
    assert rows[0]["r"]["_bar"] == 7
    assert rows[0]["b"] == 7
