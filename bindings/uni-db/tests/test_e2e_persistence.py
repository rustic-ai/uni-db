# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for database persistence in sync API.

Tests that data, schema, and indexes persist across database close/reopen cycles.
Uses on-disk databases with tmp_path fixture.
"""

import uni_db


def test_data_persists_across_reopens(tmp_path):
    """Write data, flush, reopen database, verify data persists."""
    db_path = tmp_path / "persist_db"

    # Create and populate database
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()

    # Create schema
    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .apply()
    )

    # Insert data
    db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    db.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    db.execute("CREATE (p:Person {name: 'Charlie', age: 35})")

    # Flush to disk
    db.flush()

    # Drop reference to close database
    del db

    # Reopen database
    db2 = uni_db.DatabaseBuilder.open(str(db_path)).build()

    # Query and verify data exists
    results = db2.query(
        "MATCH (p:Person) RETURN p.name as name, p.age as age ORDER BY p.name"
    )

    assert len(results) == 3
    assert results[0]["name"] == "Alice"
    assert results[0]["age"] == 30
    assert results[1]["name"] == "Bob"
    assert results[1]["age"] == 25
    assert results[2]["name"] == "Charlie"
    assert results[2]["age"] == 35


def test_schema_persists_across_reopens(tmp_path):
    """Schema (labels, edge types, properties) persists across reopens."""
    db_path = tmp_path / "schema_db"

    # Create database with schema
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .property_nullable("email", "string")
        .done()
        .label("Company")
        .property("name", "string")
        .property_nullable("founded", "int")
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .property_nullable("role", "string")
        .done()
        .apply()
    )

    # Insert some data using the schema
    db.execute("CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})")
    db.execute("CREATE (c:Company {name: 'TechCorp', founded: 2010})")
    db.execute(
        "MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) "
        "CREATE (p)-[:WORKS_AT {role: 'Engineer'}]->(c)"
    )

    db.flush()
    del db

    # Reopen and verify schema works
    db2 = uni_db.DatabaseBuilder.open(str(db_path)).build()

    # Should be able to insert using the schema without redefining it
    db2.execute("CREATE (p:Person {name: 'Bob', age: 25})")

    # Query to verify both old and new data
    person_results = db2.query("MATCH (p:Person) RETURN p.name as name ORDER BY p.name")
    assert len(person_results) == 2
    assert person_results[0]["name"] == "Alice"
    assert person_results[1]["name"] == "Bob"

    # Verify edge type works
    edge_results = db2.query(
        "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
        "RETURN p.name as person, c.name as company, r.role as role"
    )
    assert len(edge_results) == 1
    assert edge_results[0]["person"] == "Alice"
    assert edge_results[0]["company"] == "TechCorp"
    assert edge_results[0]["role"] == "Engineer"


def test_indexes_persist_across_reopens(tmp_path):
    """Indexes persist across database reopens."""
    db_path = tmp_path / "index_db"

    # Create database with indexed schema
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()

    (
        db.schema()
        .label("Item")
        .property("sku", "string")
        .property("name", "string")
        .property("price", "float")
        .index("sku", "btree")
        .index("name", "hash")
        .done()
        .apply()
    )

    # Insert data
    db.execute("CREATE (i:Item {sku: 'SKU001', name: 'Widget', price: 19.99})")
    db.execute("CREATE (i:Item {sku: 'SKU002', name: 'Gadget', price: 29.99})")
    db.execute("CREATE (i:Item {sku: 'SKU003', name: 'Doohickey', price: 9.99})")

    db.flush()
    del db

    # Reopen database
    db2 = uni_db.DatabaseBuilder.open(str(db_path)).build()

    # Query using indexed properties - should work efficiently
    results = db2.query(
        "MATCH (i:Item {sku: 'SKU002'}) RETURN i.name as name, i.price as price"
    )
    assert len(results) == 1
    assert results[0]["name"] == "Gadget"
    assert results[0]["price"] == 29.99

    # Query by name (also indexed)
    results2 = db2.query(
        "MATCH (i:Item {name: 'Widget'}) RETURN i.sku as sku, i.price as price"
    )
    assert len(results2) == 1
    assert results2[0]["sku"] == "SKU001"
    assert results2[0]["price"] == 19.99


def test_vector_indexes_persist_across_reopens(tmp_path):
    """Vector indexes persist across database reopens."""
    db_path = tmp_path / "vector_db"

    # Create database with vector property
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()

    (
        db.schema()
        .label("Document")
        .property("title", "string")
        .vector("embedding", 4)
        .done()
        .apply()
    )

    # Insert data with vectors
    db.execute("CREATE (d:Document {title: 'Doc1', embedding: [1.0, 0.0, 0.0, 0.0]})")
    db.execute("CREATE (d:Document {title: 'Doc2', embedding: [0.9, 0.1, 0.0, 0.0]})")
    db.execute("CREATE (d:Document {title: 'Doc3', embedding: [0.0, 0.0, 1.0, 0.0]})")

    # Create vector index
    db.create_vector_index("Document", "embedding", "l2")

    db.flush()
    del db

    # Reopen database
    db2 = uni_db.DatabaseBuilder.open(str(db_path)).build()

    # Vector search should still work via Cypher
    results = db2.query("""
        CALL uni.vector.query('Document', 'embedding', [1.0, 0.0, 0.0, 0.0], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    # Should return top 2 most similar (Doc1 and Doc2)
    assert len(results) >= 1
    # First result should be Doc1 (exact match or very close)
    assert results[0]["distance"] < 0.01, "First match should be near-exact"


def test_multiple_reopen_cycles(tmp_path):
    """Database can be reopened multiple times with data persisting."""
    db_path = tmp_path / "multi_reopen_db"

    # First cycle: create and add data
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()
    (db.schema().label("Counter").property("value", "int").done().apply())
    db.execute("CREATE (c:Counter {value: 1})")
    db.flush()
    del db

    # Second cycle: reopen and add more data
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()
    db.execute("CREATE (c:Counter {value: 2})")
    db.flush()
    del db

    # Third cycle: reopen and add more data
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()
    db.execute("CREATE (c:Counter {value: 3})")
    db.flush()
    del db

    # Fourth cycle: reopen and verify all data
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()
    results = db.query("MATCH (c:Counter) RETURN c.value as value ORDER BY c.value")

    assert len(results) == 3
    assert results[0]["value"] == 1
    assert results[1]["value"] == 2
    assert results[2]["value"] == 3


def test_relationships_persist_across_reopens(tmp_path):
    """Relationships and their properties persist across reopens."""
    db_path = tmp_path / "rel_persist_db"

    # Create database with nodes and relationships
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .done()
        .edge_type("KNOWS", ["Person"], ["Person"])
        .property_nullable("since", "int")
        .done()
        .apply()
    )

    db.execute("CREATE (p:Person {name: 'Alice'})")
    db.execute("CREATE (p:Person {name: 'Bob'})")
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
        "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
    )

    db.flush()
    del db

    # Reopen and verify relationships
    db2 = uni_db.DatabaseBuilder.open(str(db_path)).build()

    results = db2.query(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
        "RETURN a.name as from, b.name as to, r.since as since"
    )

    assert len(results) == 1
    assert results[0]["from"] == "Alice"
    assert results[0]["to"] == "Bob"
    assert results[0]["since"] == 2020


def test_complex_graph_persists(tmp_path):
    """Complex graph with multiple labels and edge types persists."""
    db_path = tmp_path / "complex_graph_db"

    # Create complex schema
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()

    (
        db.schema()
        .label("User")
        .property("name", "string")
        .done()
        .label("Product")
        .property("name", "string")
        .property("price", "float")
        .done()
        .label("Category")
        .property("name", "string")
        .done()
        .edge_type("PURCHASED", ["User"], ["Product"])
        .done()
        .edge_type("IN_CATEGORY", ["Product"], ["Category"])
        .done()
        .apply()
    )

    # Create complex graph
    db.execute("CREATE (u:User {name: 'Alice'})")
    db.execute("CREATE (p:Product {name: 'Laptop', price: 999.99})")
    db.execute("CREATE (c:Category {name: 'Electronics'})")
    db.execute(
        "MATCH (u:User {name: 'Alice'}), (p:Product {name: 'Laptop'}) "
        "CREATE (u)-[:PURCHASED]->(p)"
    )
    db.execute(
        "MATCH (p:Product {name: 'Laptop'}), (c:Category {name: 'Electronics'}) "
        "CREATE (p)-[:IN_CATEGORY]->(c)"
    )

    db.flush()
    del db

    # Reopen and verify complex queries work
    db2 = uni_db.DatabaseBuilder.open(str(db_path)).build()

    results = db2.query(
        "MATCH (u:User)-[:PURCHASED]->(p:Product)-[:IN_CATEGORY]->(c:Category) "
        "RETURN u.name as user, p.name as product, p.price as price, c.name as category"
    )

    assert len(results) == 1
    assert results[0]["user"] == "Alice"
    assert results[0]["product"] == "Laptop"
    assert results[0]["price"] == 999.99
    assert results[0]["category"] == "Electronics"


def test_empty_database_persists(tmp_path):
    """Empty database (with schema but no data) persists."""
    db_path = tmp_path / "empty_persist_db"

    # Create database with schema but no data
    db = uni_db.DatabaseBuilder.open(str(db_path)).build()

    (db.schema().label("Node").property("value", "int").done().apply())

    db.flush()
    del db

    # Reopen and verify schema exists
    db2 = uni_db.DatabaseBuilder.open(str(db_path)).build()

    # Should return empty results but not error
    results = db2.query("MATCH (n:Node) RETURN n.value as value")
    assert len(results) == 0

    # Should be able to insert data using persisted schema
    db2.execute("CREATE (n:Node {value: 42})")
    results2 = db2.query("MATCH (n:Node) RETURN n.value as value")
    assert len(results2) == 1
    assert results2[0]["value"] == 42
