# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for AsyncDatabaseBuilder and admin operations."""

import pytest

import uni_db


@pytest.mark.asyncio
async def test_async_database_builder_temporary():
    """Test creating a temporary database using AsyncDatabaseBuilder."""
    db = await uni_db.AsyncDatabaseBuilder.temporary().build()

    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)

    await db.execute("CREATE (:Person {name: 'Alice'})")

    result = await db.query("MATCH (n:Person) RETURN n.name")
    assert len(result) == 1
    assert result[0]["n.name"] == "Alice"


@pytest.mark.asyncio
async def test_async_database_builder_in_memory():
    """Test creating an in-memory database using AsyncDatabaseBuilder."""
    db = await uni_db.AsyncDatabaseBuilder.in_memory().build()

    await db.create_label("Product")
    await db.add_property("Product", "sku", "string", False)
    await db.add_property("Product", "price", "float", False)

    await db.execute("CREATE (:Product {sku: 'SKU001', price: 9.99})")

    result = await db.query("MATCH (p:Product) RETURN p.sku, p.price")
    assert len(result) == 1
    assert result[0]["p.sku"] == "SKU001"
    assert result[0]["p.price"] == 9.99


@pytest.mark.asyncio
async def test_async_database_builder_open_new_path(tmp_path):
    """Test opening/creating a database at a new path using AsyncDatabaseBuilder.open()."""
    db_path = tmp_path / "test_open_db"

    db = await uni_db.AsyncDatabaseBuilder.open(str(db_path)).build()

    await db.create_label("User")
    await db.add_property("User", "username", "string", False)

    await db.execute("CREATE (:User {username: 'testuser'})")

    result = await db.query("MATCH (u:User) RETURN u.username")
    assert len(result) == 1
    assert result[0]["u.username"] == "testuser"


@pytest.mark.asyncio
async def test_async_database_builder_create_new_path(tmp_path):
    """Test creating a new database using AsyncDatabaseBuilder.create()."""
    db_path = tmp_path / "test_create_db"

    db = await uni_db.AsyncDatabaseBuilder.create(str(db_path)).build()

    await db.create_label("Item")
    await db.add_property("Item", "id", "int", False)

    await db.execute("CREATE (:Item {id: 42})")

    result = await db.query("MATCH (i:Item) RETURN i.id")
    assert len(result) == 1
    assert result[0]["i.id"] == 42


@pytest.mark.asyncio
async def test_async_database_builder_create_fails_on_existing(tmp_path):
    """Test that AsyncDatabaseBuilder.create() fails on existing database."""
    db_path = tmp_path / "test_existing_db"

    db1 = await uni_db.AsyncDatabaseBuilder.create(str(db_path)).build()
    await db1.create_label("Test")
    await db1.flush()
    del db1

    with pytest.raises(Exception):
        await uni_db.AsyncDatabaseBuilder.create(str(db_path)).build()


@pytest.mark.asyncio
async def test_async_database_builder_open_existing_fails_on_missing(tmp_path):
    """Test that AsyncDatabaseBuilder.open_existing() fails on non-existent database."""
    db_path = tmp_path / "test_nonexistent_db"

    with pytest.raises(Exception):
        await uni_db.AsyncDatabaseBuilder.open_existing(str(db_path)).build()


@pytest.mark.asyncio
async def test_async_database_builder_open_existing_succeeds(tmp_path):
    """Test that AsyncDatabaseBuilder.open_existing() succeeds on existing database."""
    db_path = tmp_path / "test_open_existing_db"

    db1 = await uni_db.AsyncDatabaseBuilder.create(str(db_path)).build()
    await db1.create_label("Person")
    await db1.add_property("Person", "name", "string", False)
    await db1.execute("CREATE (:Person {name: 'Alice'})")
    await db1.flush()
    del db1

    db2 = await uni_db.AsyncDatabaseBuilder.open_existing(str(db_path)).build()

    result = await db2.query("MATCH (p:Person) RETURN p.name")
    assert len(result) == 1
    assert result[0]["p.name"] == "Alice"


@pytest.mark.asyncio
async def test_async_database_builder_with_cache_size(tmp_path):
    """Test AsyncDatabaseBuilder with custom cache size."""
    db_path = tmp_path / "test_cache_size_db"

    cache_size = 10 * 1024 * 1024
    db = (
        await uni_db.AsyncDatabaseBuilder.open(str(db_path))
        .cache_size(cache_size)
        .build()
    )

    await db.create_label("Data")
    await db.add_property("Data", "value", "int", False)

    await db.execute("CREATE (:Data {value: 123})")

    result = await db.query("MATCH (d:Data) RETURN d.value")
    assert len(result) == 1
    assert result[0]["d.value"] == 123


@pytest.mark.asyncio
async def test_async_database_builder_with_parallelism(tmp_path):
    """Test AsyncDatabaseBuilder with custom parallelism setting."""
    db_path = tmp_path / "test_parallelism_db"

    db = await uni_db.AsyncDatabaseBuilder.open(str(db_path)).parallelism(4).build()

    await db.create_label("Task")
    await db.add_property("Task", "name", "string", False)

    await db.execute("CREATE (:Task {name: 'task1'})")

    result = await db.query("MATCH (t:Task) RETURN t.name")
    assert len(result) == 1
    assert result[0]["t.name"] == "task1"


@pytest.mark.asyncio
async def test_async_database_builder_chained_options(tmp_path):
    """Test AsyncDatabaseBuilder with multiple chained options."""
    db_path = tmp_path / "test_chained_db"

    db = (
        await uni_db.AsyncDatabaseBuilder.open(str(db_path))
        .cache_size(5 * 1024 * 1024)
        .parallelism(2)
        .build()
    )

    await db.create_label("Config")
    await db.add_property("Config", "key", "string", False)
    await db.add_property("Config", "value", "string", False)

    await db.execute("CREATE (:Config {key: 'setting1', value: 'value1'})")

    result = await db.query("MATCH (c:Config) RETURN c.key, c.value")
    assert len(result) == 1
    assert result[0]["c.key"] == "setting1"
    assert result[0]["c.value"] == "value1"


@pytest.mark.asyncio
async def test_async_database_convenience_open(tmp_path):
    """Test AsyncDatabase.open() convenience method."""
    db_path = tmp_path / "test_convenience_open_db"

    db = await uni_db.AsyncDatabase.open(str(db_path))

    await db.create_label("Note")
    await db.add_property("Note", "content", "string", False)

    await db.execute("CREATE (:Note {content: 'test note'})")

    result = await db.query("MATCH (n:Note) RETURN n.content")
    assert len(result) == 1
    assert result[0]["n.content"] == "test note"


@pytest.mark.asyncio
async def test_async_database_convenience_temporary():
    """Test AsyncDatabase.temporary() convenience method."""
    db = await uni_db.AsyncDatabase.temporary()

    await db.create_label("TempData")
    await db.add_property("TempData", "id", "int", False)

    await db.execute("CREATE (:TempData {id: 999})")

    result = await db.query("MATCH (t:TempData) RETURN t.id")
    assert len(result) == 1
    assert result[0]["t.id"] == 999


@pytest.mark.asyncio
async def test_explain():
    """Test the explain method for query plans."""
    db = await uni_db.AsyncDatabase.temporary()

    await db.create_label("Person")
    await db.add_property("Person", "name", "string", False)
    await db.add_property("Person", "age", "int", False)

    await db.execute("CREATE (:Person {name: 'Alice', age: 30})")
    await db.execute("CREATE (:Person {name: 'Bob', age: 25})")

    plan = await db.explain("MATCH (n:Person) WHERE n.age > 20 RETURN n.name, n.age")

    assert isinstance(plan, dict)


@pytest.mark.asyncio
async def test_profile():
    """Test the profile method for query execution profiling."""
    db = await uni_db.AsyncDatabase.temporary()

    await db.create_label("Product")
    await db.add_property("Product", "name", "string", False)
    await db.add_property("Product", "price", "float", False)

    await db.execute("CREATE (:Product {name: 'Widget A', price: 9.99})")
    await db.execute("CREATE (:Product {name: 'Widget B', price: 19.99})")
    await db.execute("CREATE (:Product {name: 'Widget C', price: 29.99})")

    results, stats = await db.profile(
        "MATCH (p:Product) WHERE p.price < 25.0 RETURN p.name, p.price"
    )

    assert isinstance(results, list)
    assert isinstance(stats, dict)

    assert len(results) == 2  # Products with price < 25.0


@pytest.mark.asyncio
async def test_explain_with_complex_query():
    """Test explain with a more complex query involving joins."""
    db = await uni_db.AsyncDatabase.temporary()

    await db.create_label("User")
    await db.add_property("User", "username", "string", False)

    await db.create_label("Post")
    await db.add_property("Post", "title", "string", False)

    await db.create_edge_type("AUTHORED", ["User"], ["Post"])

    await db.execute("CREATE (:User {username: 'alice'})")
    await db.execute("CREATE (:User {username: 'bob'})")
    await db.execute("CREATE (:Post {title: 'Post 1'})")
    await db.execute("CREATE (:Post {title: 'Post 2'})")

    await db.execute("""
        MATCH (u:User {username: 'alice'}), (p:Post {title: 'Post 1'})
        CREATE (u)-[:AUTHORED]->(p)
    """)

    plan = await db.explain("""
        MATCH (u:User)-[:AUTHORED]->(p:Post)
        RETURN u.username, p.title
    """)

    assert isinstance(plan, dict)


@pytest.mark.asyncio
async def test_profile_with_aggregation():
    """Test profile with an aggregation query."""
    db = await uni_db.AsyncDatabase.temporary()

    await db.create_label("Order")
    await db.add_property("Order", "amount", "float", False)

    for i in range(10):
        await db.execute(f"CREATE (:Order {{amount: {(i + 1) * 10.0}}})")

    results, stats = await db.profile("""
        MATCH (o:Order)
        RETURN count(o) AS total_orders, sum(o.amount) AS total_amount
    """)

    assert isinstance(results, list)
    assert isinstance(stats, dict)
    assert len(results) == 1
    assert results[0]["total_orders"] == 10
    assert results[0]["total_amount"] == 550.0  # 10+20+30+...+100
