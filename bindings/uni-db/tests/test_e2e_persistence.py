# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for database persistence in sync API.

Tests that data, schema, and indexes persist across database close/reopen cycles.
Uses on-disk databases with tmp_path fixture.
"""

import pytest

import uni_db


def test_data_persists_across_reopens(tmp_path):
    """Write data, flush, reopen database, verify data persists."""
    db_path = tmp_path / "persist_db"

    # Create and populate database
    db = uni_db.UniBuilder.open(str(db_path)).build()

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
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
    tx.commit()

    # Flush to disk
    db.flush()

    # Drop reference to close database
    del db

    # Reopen database
    db2 = uni_db.UniBuilder.open(str(db_path)).build()
    session2 = db2.session()

    # Query and verify data exists
    results = session2.query(
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
    db = uni_db.UniBuilder.open(str(db_path)).build()

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
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})")
    tx.execute("CREATE (c:Company {name: 'TechCorp', founded: 2010})")
    tx.execute(
        "MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) "
        "CREATE (p)-[:WORKS_AT {role: 'Engineer'}]->(c)"
    )
    tx.commit()

    db.flush()
    del db

    # Reopen and verify schema works
    db2 = uni_db.UniBuilder.open(str(db_path)).build()
    session2 = db2.session()

    # Should be able to insert using the schema without redefining it
    tx2 = session2.tx()
    tx2.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    tx2.commit()

    # Query to verify both old and new data
    person_results = session2.query(
        "MATCH (p:Person) RETURN p.name as name ORDER BY p.name"
    )
    assert len(person_results) == 2
    assert person_results[0]["name"] == "Alice"
    assert person_results[1]["name"] == "Bob"

    # Verify edge type works
    edge_results = session2.query(
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
    db = uni_db.UniBuilder.open(str(db_path)).build()

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
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (i:Item {sku: 'SKU001', name: 'Widget', price: 19.99})")
    tx.execute("CREATE (i:Item {sku: 'SKU002', name: 'Gadget', price: 29.99})")
    tx.execute("CREATE (i:Item {sku: 'SKU003', name: 'Doohickey', price: 9.99})")
    tx.commit()

    db.flush()
    del db

    # Reopen database
    db2 = uni_db.UniBuilder.open(str(db_path)).build()
    session2 = db2.session()

    # Query using indexed properties - should work efficiently
    results = session2.query(
        "MATCH (i:Item {sku: 'SKU002'}) RETURN i.name as name, i.price as price"
    )
    assert len(results) == 1
    assert results[0]["name"] == "Gadget"
    assert results[0]["price"] == 29.99

    # Query by name (also indexed)
    results2 = session2.query(
        "MATCH (i:Item {name: 'Widget'}) RETURN i.sku as sku, i.price as price"
    )
    assert len(results2) == 1
    assert results2[0]["sku"] == "SKU001"
    assert results2[0]["price"] == 19.99


def test_vector_indexes_persist_across_reopens(tmp_path):
    """Vector indexes persist across database reopens."""
    db_path = tmp_path / "vector_db"

    # Create database with vector property
    db = uni_db.UniBuilder.open(str(db_path)).build()

    (
        db.schema()
        .label("Document")
        .property("title", "string")
        .vector("embedding", 4)
        .done()
        .apply()
    )

    # Insert data with vectors
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (d:Document {title: 'Doc1', embedding: [1.0, 0.0, 0.0, 0.0]})")
    tx.execute("CREATE (d:Document {title: 'Doc2', embedding: [0.9, 0.1, 0.0, 0.0]})")
    tx.execute("CREATE (d:Document {title: 'Doc3', embedding: [0.0, 0.0, 1.0, 0.0]})")
    tx.commit()

    # Create vector index
    db.schema().label("Document").index(
        "embedding", {"type": "vector", "metric": "l2"}
    ).apply()

    db.flush()
    del db

    # Reopen database
    db2 = uni_db.UniBuilder.open(str(db_path)).build()
    session2 = db2.session()

    # Vector search should still work via Cypher
    results = session2.query("""
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
    db = uni_db.UniBuilder.open(str(db_path)).build()
    (db.schema().label("Counter").property("value", "int").done().apply())
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (c:Counter {value: 1})")
    tx.commit()
    db.flush()
    del db

    # Second cycle: reopen and add more data
    db = uni_db.UniBuilder.open(str(db_path)).build()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (c:Counter {value: 2})")
    tx.commit()
    db.flush()
    del db

    # Third cycle: reopen and add more data
    db = uni_db.UniBuilder.open(str(db_path)).build()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (c:Counter {value: 3})")
    tx.commit()
    db.flush()
    del db

    # Fourth cycle: reopen and verify all data
    db = uni_db.UniBuilder.open(str(db_path)).build()
    session = db.session()
    results = session.query(
        "MATCH (c:Counter) RETURN c.value as value ORDER BY c.value"
    )

    assert len(results) == 3
    assert results[0]["value"] == 1
    assert results[1]["value"] == 2
    assert results[2]["value"] == 3


def test_relationships_persist_across_reopens(tmp_path):
    """Relationships and their properties persist across reopens."""
    db_path = tmp_path / "rel_persist_db"

    # Create database with nodes and relationships
    db = uni_db.UniBuilder.open(str(db_path)).build()

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

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (p:Person {name: 'Alice'})")
    tx.execute("CREATE (p:Person {name: 'Bob'})")
    tx.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
        "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
    )
    tx.commit()

    db.flush()
    del db

    # Reopen and verify relationships
    db2 = uni_db.UniBuilder.open(str(db_path)).build()
    session2 = db2.session()

    results = session2.query(
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
    db = uni_db.UniBuilder.open(str(db_path)).build()

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
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (u:User {name: 'Alice'})")
    tx.execute("CREATE (p:Product {name: 'Laptop', price: 999.99})")
    tx.execute("CREATE (c:Category {name: 'Electronics'})")
    tx.execute(
        "MATCH (u:User {name: 'Alice'}), (p:Product {name: 'Laptop'}) "
        "CREATE (u)-[:PURCHASED]->(p)"
    )
    tx.execute(
        "MATCH (p:Product {name: 'Laptop'}), (c:Category {name: 'Electronics'}) "
        "CREATE (p)-[:IN_CATEGORY]->(c)"
    )
    tx.commit()

    db.flush()
    del db

    # Reopen and verify complex queries work
    db2 = uni_db.UniBuilder.open(str(db_path)).build()
    session2 = db2.session()

    results = session2.query(
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
    db = uni_db.UniBuilder.open(str(db_path)).build()

    (db.schema().label("Node").property("value", "int").done().apply())

    db.flush()
    del db

    # Reopen and verify schema exists
    db2 = uni_db.UniBuilder.open(str(db_path)).build()
    session2 = db2.session()

    # Should return empty results but not error
    results = session2.query("MATCH (n:Node) RETURN n.value as value")
    assert len(results) == 0

    # Should be able to insert data using persisted schema
    tx2 = session2.tx()
    tx2.execute("CREATE (n:Node {value: 42})")
    tx2.commit()
    results2 = session2.query("MATCH (n:Node) RETURN n.value as value")
    assert len(results2) == 1
    assert results2[0]["value"] == 42


def test_restore_snapshot_rewinds_data(tmp_path):
    """restore_snapshot rewinds flushed and unflushed writes, now and on reopen."""
    db_path = str(tmp_path / "restore_db")
    db = uni_db.UniBuilder.open(db_path).build()
    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .done()
        .edge_type("KNOWS", ["Person"], ["Person"])
        .apply()
    )
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'A'})-[:KNOWS]->(:Person {name: 'B'})")
    tx.commit()
    snap = db.create_snapshot("two")

    tx = s.tx()
    tx.execute("MATCH (b:Person {name: 'B'}) DETACH DELETE b")
    tx.execute("CREATE (:Person {name: 'C'})")
    tx.commit()
    db.flush()
    tx = s.tx()
    tx.execute(
        "MATCH (a:Person {name: 'A'}) CREATE (a)-[:KNOWS]->(:Person {name: 'D'})"
    )
    tx.commit()

    def state(database):
        rows = database.session().query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b"
        )
        people = database.session().query("MATCH (p:Person) RETURN count(p) AS c")
        return [(r["a"], r["b"]) for r in rows], people[0]["c"]

    db.restore_snapshot(snap)
    assert state(db) == ([("A", "B")], 2)
    db.shutdown()

    db = uni_db.UniBuilder.open(db_path).build()
    assert state(db) == ([("A", "B")], 2)
    db.shutdown()


def test_restore_snapshot_refused_while_forks_exist(tmp_path):
    """restore_snapshot raises UniRestoreBlockedByForksError naming the forks."""
    db_path = str(tmp_path / "restore_forks_db")
    db = uni_db.UniBuilder.open(db_path).disable_fork_sweeper(True).build()
    db.schema().label("Person").property("name", "string").apply()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'A'})")
    tx.commit()
    snap = db.create_snapshot("one")
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'B'})")
    tx.commit()

    fork = s.fork("what_if").build()
    del fork

    with pytest.raises(uni_db.UniRestoreBlockedByForksError) as excinfo:
        db.restore_snapshot(snap)
    assert excinfo.value.forks == ["what_if"]
    assert s.query("MATCH (p:Person) RETURN count(p) AS c")[0]["c"] == 2

    db.drop_fork("what_if")
    db.restore_snapshot(snap)
    assert s.query("MATCH (p:Person) RETURN count(p) AS c")[0]["c"] == 1
    db.shutdown()


def _store_with_snapshot(db_path):
    """A store holding A, a snapshot of it, then B."""
    db = uni_db.UniBuilder.open(db_path).build()
    db.schema().label("Person").property("name", "string").apply()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'A'})")
    tx.commit()
    snap = db.create_snapshot("a-only")
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'B'})")
    tx.commit()
    return db, snap


def test_admin_procedures_refused_by_query_run_by_run(tmp_path):
    """session.query is read-only: it refuses a mutating admin procedure;
    session.run runs it outside a transaction."""
    db, snap = _store_with_snapshot(str(tmp_path / "admin_db"))
    s = db.session()
    restore = f"CALL uni.admin.snapshot.restore('{snap}')"
    for stmt in (
        restore,
        "CALL uni.admin.compact()",
        "CALL uni.admin.snapshot.create('x')",
        "CALL uni.schema.dropLabel('Person')",
    ):
        with pytest.raises(uni_db.UniQueryError, match="read-only"):
            s.query(stmt)
    assert s.query("MATCH (p:Person) RETURN count(p) AS c")[0]["c"] == 2
    s.query("CALL uni.admin.compactionStatus() YIELD l1_runs RETURN l1_runs")

    s.run(restore)
    assert s.query("MATCH (p:Person) RETURN count(p) AS c")[0]["c"] == 1
    s.run("CALL uni.admin.compact() YIELD tables_optimized RETURN tables_optimized")
    assert s.run("MATCH (p:Person) RETURN p.name AS n")[0]["n"] == "A"
    db.shutdown()


@pytest.mark.asyncio
async def test_async_session_run_restores(tmp_path):
    """AsyncSession.run runs an administrative procedure."""
    db_path = str(tmp_path / "async_admin_db")
    db, snap = _store_with_snapshot(db_path)
    db.shutdown()
    adb = await uni_db.AsyncUni.open(db_path)
    s = adb.session()
    with pytest.raises(uni_db.UniQueryError, match="read-only"):
        await s.query(f"CALL uni.admin.snapshot.restore('{snap}')")
    await s.run(f"CALL uni.admin.snapshot.restore('{snap}')")
    rows = await s.query("MATCH (p:Person) RETURN count(p) AS c")
    assert rows[0]["c"] == 1


def test_delete_snapshot_releases_and_keeps_others(tmp_path):
    """delete_snapshot removes one snapshot; another still restores."""
    db_path = str(tmp_path / "delete_snapshot_db")
    db, first = _store_with_snapshot(db_path)
    second = db.create_snapshot("two")
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'C'})")
    tx.commit()

    db.delete_snapshot(first)
    assert first not in {m.snapshot_id for m in db.list_snapshots()}
    with pytest.raises(uni_db.UniSnapshotNotFoundError) as excinfo:
        db.restore_snapshot(first)
    assert excinfo.value.snapshot_id == first
    with pytest.raises(uni_db.UniSnapshotNotFoundError) as excinfo:
        db.delete_snapshot(first)
    assert excinfo.value.snapshot_id == first

    db.restore_snapshot(second)
    names = s.query("MATCH (p:Person) RETURN p.name AS n ORDER BY n")
    assert [r["n"] for r in names] == ["A", "B"]

    s.run(f"CALL uni.admin.snapshot.delete('{second}')")
    with pytest.raises(uni_db.UniSnapshotNotFoundError):
        s.run(f"CALL uni.admin.snapshot.delete('{second}')")
    db.shutdown()


def test_restore_snapshot_rewinds_the_schema(tmp_path):
    """A label declared after the snapshot is gone after the restore."""
    db_path = str(tmp_path / "restore_schema_db")
    db, snap = _store_with_snapshot(db_path)
    db.schema().label("Pet").property("age", "string").apply()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Pet {age: 'old'})")
    tx.commit()
    assert db.label_exists("Pet")

    db.restore_snapshot(snap)
    assert not db.label_exists("Pet")
    db.schema().label("Pet").property("age", "int").apply()
    tx = s.tx()
    tx.execute("CREATE (:Pet {age: 4})")
    tx.commit()
    db.flush()
    assert s.query("MATCH (p:Pet) RETURN p.age AS a")[0]["a"] == 4
    db.shutdown()


@pytest.mark.asyncio
async def test_async_delete_snapshot(tmp_path):
    """AsyncUni.delete_snapshot deletes, and raises the typed not-found."""
    db_path = str(tmp_path / "async_delete_db")
    db, snap = _store_with_snapshot(db_path)
    db.shutdown()
    adb = await uni_db.AsyncUni.open(db_path)
    await adb.delete_snapshot(snap)
    assert snap not in {m.snapshot_id for m in await adb.list_snapshots()}
    with pytest.raises(uni_db.UniSnapshotNotFoundError) as excinfo:
        await adb.delete_snapshot(snap)
    assert excinfo.value.snapshot_id == snap
