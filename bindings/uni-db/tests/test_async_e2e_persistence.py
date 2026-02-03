# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for database persistence."""

import pytest

import uni_db


@pytest.mark.asyncio
async def test_data_persists_after_reopen(tmp_path):
    """Test that data persists after flushing and reopening the database."""
    db_path = tmp_path / "test_persistence_db"

    db = await uni_db.AsyncDatabase.open(str(db_path))

    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)
    await db.add_property("Person", "age", "int", False)
    await db.create_edge_type("KNOWS", ["Person"], ["Person"])

    await db.execute("CREATE (:Person {name: 'Alice', age: 30})")
    await db.execute("CREATE (:Person {name: 'Bob', age: 25})")
    await db.execute("CREATE (:Person {name: 'Carol', age: 35})")

    await db.execute("""
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS]->(b)
    """)
    await db.execute("""
        MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
        CREATE (b)-[:KNOWS]->(c)
    """)

    await db.flush()

    result = await db.query("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name")
    assert len(result) == 3
    assert result[0]["n.name"] == "Alice"
    assert result[0]["n.age"] == 30

    edges = await db.query(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r) AS cnt"
    )
    assert edges[0]["cnt"] == 2

    del db

    db2 = await uni_db.AsyncDatabase.open(str(db_path))

    result = await db2.query("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name")
    assert len(result) == 3
    assert result[0]["n.name"] == "Alice"
    assert result[0]["n.age"] == 30
    assert result[1]["n.name"] == "Bob"
    assert result[1]["n.age"] == 25
    assert result[2]["n.name"] == "Carol"
    assert result[2]["n.age"] == 35

    edges = await db2.query(
        "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r) AS cnt"
    )
    assert edges[0]["cnt"] == 2

    edge_query = await db2.query("""
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
        RETURN a.name, b.name
    """)
    assert len(edge_query) == 1
    assert edge_query[0]["a.name"] == "Alice"
    assert edge_query[0]["b.name"] == "Bob"


@pytest.mark.asyncio
async def test_schema_persists_across_reopens(tmp_path):
    """Test that schema definitions persist across database reopens."""
    db_path = tmp_path / "test_schema_persistence_db"

    db = await uni_db.AsyncDatabase.open(str(db_path))

    await db.create_label("User")
    await db.add_property("User", "username", "string", False)
    await db.add_property("User", "email", "string", True)

    await db.create_label("Post")
    await db.add_property("Post", "title", "string", False)
    await db.add_property("Post", "content", "string", True)
    await db.add_property("Post", "published", "bool", True)

    await db.create_edge_type("AUTHORED", ["User"], ["Post"])

    await db.create_edge_type("FOLLOWS", ["User"], ["User"])

    await db.flush()

    labels = await db.list_labels()
    assert "User" in labels
    assert "Post" in labels

    edge_types = await db.list_edge_types()
    assert "AUTHORED" in edge_types
    assert "FOLLOWS" in edge_types

    del db

    db2 = await uni_db.AsyncDatabase.open(str(db_path))

    labels = await db2.list_labels()
    assert "User" in labels
    assert "Post" in labels

    edge_types = await db2.list_edge_types()
    assert "AUTHORED" in edge_types
    assert "FOLLOWS" in edge_types

    await db2.execute(
        "CREATE (:User {username: 'testuser', email: 'test@example.com'})"
    )
    await db2.execute(
        "CREATE (:Post {title: 'Test Post', content: 'This is a test', published: true})"
    )

    result = await db2.query("MATCH (u:User) RETURN u.username")
    assert len(result) == 1
    assert result[0]["u.username"] == "testuser"

    posts = await db2.query("MATCH (p:Post) RETURN p.title")
    assert len(posts) == 1
    assert posts[0]["p.title"] == "Test Post"


@pytest.mark.asyncio
async def test_indexes_persist_across_reopens(tmp_path):
    """Test that indexes persist across database reopens."""
    db_path = tmp_path / "test_index_persistence_db"

    db = await uni_db.AsyncDatabase.open(str(db_path))

    await db.create_label("Product")
    await db.add_property("Product", "sku", "string", False)
    await db.add_property("Product", "name", "string", False)
    await db.add_property("Product", "price", "float", False)

    await db.create_scalar_index("Product", "sku", "btree")

    await db.execute("CREATE (:Product {sku: 'SKU001', name: 'Widget A', price: 9.99})")
    await db.execute(
        "CREATE (:Product {sku: 'SKU002', name: 'Widget B', price: 19.99})"
    )
    await db.execute(
        "CREATE (:Product {sku: 'SKU003', name: 'Widget C', price: 29.99})"
    )

    await db.flush()

    label_info = await db.get_label_info("Product")
    assert label_info is not None

    del db

    db2 = await uni_db.AsyncDatabase.open(str(db_path))

    label_info = await db2.get_label_info("Product")
    assert label_info is not None

    result = await db2.query("""
        MATCH (p:Product {sku: 'SKU002'})
        RETURN p.name, p.price
    """)
    assert len(result) == 1
    assert result[0]["p.name"] == "Widget B"
    assert result[0]["p.price"] == 19.99

    await db2.execute(
        "CREATE (:Product {sku: 'SKU004', name: 'Widget D', price: 39.99})"
    )

    result = await db2.query("""
        MATCH (p:Product {sku: 'SKU004'})
        RETURN p.name
    """)
    assert len(result) == 1
    assert result[0]["p.name"] == "Widget D"


@pytest.mark.asyncio
async def test_multiple_reopen_cycles(tmp_path):
    """Test that data remains consistent across multiple reopen cycles."""
    db_path = tmp_path / "test_multi_reopen_db"

    # First cycle: create and insert
    db1 = await uni_db.AsyncDatabase.open(str(db_path))
    await db1.create_label("Counter")
    await db1.add_property("Counter", "value", "int", False)
    await db1.execute("CREATE (:Counter {value: 1})")
    await db1.flush()
    del db1

    # Second cycle: read and update
    db2 = await uni_db.AsyncDatabase.open(str(db_path))
    result = await db2.query("MATCH (c:Counter) RETURN c.value")
    assert result[0]["c.value"] == 1
    await db2.execute("MATCH (c:Counter) SET c.value = 2")
    await db2.flush()
    del db2

    # Third cycle: verify update
    db3 = await uni_db.AsyncDatabase.open(str(db_path))
    result = await db3.query("MATCH (c:Counter) RETURN c.value")
    assert result[0]["c.value"] == 2
    await db3.execute("MATCH (c:Counter) SET c.value = 3")
    await db3.flush()
    del db3

    # Fourth cycle: final verification
    db4 = await uni_db.AsyncDatabase.open(str(db_path))
    result = await db4.query("MATCH (c:Counter) RETURN c.value")
    assert result[0]["c.value"] == 3


@pytest.mark.asyncio
async def test_large_dataset_persistence(tmp_path):
    """Test persistence with a larger dataset."""
    db_path = tmp_path / "test_large_persistence_db"

    db = await uni_db.AsyncDatabase.open(str(db_path))

    await db.create_label("Item")
    await db.add_property("Item", "id", "int", False)
    await db.add_property("Item", "data", "string", False)

    for i in range(1000):
        await db.execute(f"CREATE (:Item {{id: {i}, data: 'item_{i}'}})")

    await db.flush()

    result = await db.query("MATCH (i:Item) RETURN count(i) AS cnt")
    assert result[0]["cnt"] == 1000

    del db

    db2 = await uni_db.AsyncDatabase.open(str(db_path))
    result = await db2.query("MATCH (i:Item) RETURN count(i) AS cnt")
    assert result[0]["cnt"] == 1000

    result = await db2.query("MATCH (i:Item {id: 500}) RETURN i.data")
    assert len(result) == 1
    assert result[0]["i.data"] == "item_500"

    result = await db2.query("MATCH (i:Item {id: 999}) RETURN i.data")
    assert len(result) == 1
    assert result[0]["i.data"] == "item_999"
