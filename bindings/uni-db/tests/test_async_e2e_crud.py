# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for CRUD operations."""

import pytest


@pytest.mark.asyncio
async def test_create_single_vertex_and_read(async_social_db):
    """Test creating a single vertex and reading it back with MATCH."""

    await async_social_db.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.age"
    )

    assert len(result) == 1
    assert result[0]["p.name"] == "Alice"
    assert result[0]["p.age"] == 30


@pytest.mark.asyncio
async def test_create_vertex_with_all_properties(async_social_db):
    """Test creating a vertex with all properties including optional ones."""

    await async_social_db.execute(
        "CREATE (p:Person {name: 'Bob', age: 25, email: 'bob@example.com'})"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Bob'}) RETURN p.name, p.age, p.email"
    )

    assert len(result) == 1
    assert result[0]["p.name"] == "Bob"
    assert result[0]["p.age"] == 25
    assert result[0]["p.email"] == "bob@example.com"


@pytest.mark.asyncio
async def test_create_vertex_with_nullable_property_omitted(async_social_db):
    """Test creating a vertex with optional property omitted."""

    await async_social_db.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Charlie'}) RETURN p.name, p.age, p.email"
    )

    assert len(result) == 1
    assert result[0]["p.name"] == "Charlie"
    assert result[0]["p.age"] == 35
    assert result[0].get("p.email") is None


@pytest.mark.asyncio
async def test_create_multiple_vertices(async_social_db):
    """Test creating multiple vertices in one operation."""

    await async_social_db.execute("CREATE (p1:Person {name: 'Diana', age: 28})")
    await async_social_db.execute("CREATE (p2:Person {name: 'Eve', age: 32})")
    await async_social_db.execute("CREATE (p3:Person {name: 'Frank', age: 40})")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person) WHERE p.name IN ['Diana', 'Eve', 'Frank'] RETURN p.name ORDER BY p.name"
    )

    assert len(result) == 3
    assert result[0]["p.name"] == "Diana"
    assert result[1]["p.name"] == "Eve"
    assert result[2]["p.name"] == "Frank"


@pytest.mark.asyncio
async def test_create_edge_between_vertices(async_social_db):
    """Test creating an edge between two vertices."""

    await async_social_db.execute("CREATE (p:Person {name: 'Grace', age: 29})")
    await async_social_db.execute("CREATE (p:Person {name: 'Henry', age: 31})")
    await async_social_db.flush()

    await async_social_db.execute(
        "MATCH (a:Person {name: 'Grace'}), (b:Person {name: 'Henry'}) CREATE (a)-[:KNOWS]->(b)"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (a:Person {name: 'Grace'})-[:KNOWS]->(b:Person {name: 'Henry'}) RETURN a.name, b.name"
    )

    assert len(result) == 1
    assert result[0]["a.name"] == "Grace"
    assert result[0]["b.name"] == "Henry"


@pytest.mark.asyncio
async def test_create_edge_with_properties(async_social_db):
    """Test creating an edge with properties."""

    await async_social_db.execute("CREATE (p:Person {name: 'Ivy', age: 27})")
    await async_social_db.execute("CREATE (p:Person {name: 'Jack', age: 33})")
    await async_social_db.flush()

    await async_social_db.execute(
        "MATCH (a:Person {name: 'Ivy'}), (b:Person {name: 'Jack'}) "
        "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (a:Person {name: 'Ivy'})-[k:KNOWS]->(b:Person {name: 'Jack'}) "
        "RETURN a.name, b.name, k.since"
    )

    assert len(result) == 1
    assert result[0]["a.name"] == "Ivy"
    assert result[0]["b.name"] == "Jack"
    assert result[0]["k.since"] == 2020


@pytest.mark.asyncio
async def test_match_vertex_by_property(async_social_db):
    """Test matching a vertex by a specific property."""

    await async_social_db.execute("CREATE (p:Person {name: 'Kelly', age: 26})")
    await async_social_db.execute("CREATE (p:Person {name: 'Leo', age: 26})")
    await async_social_db.execute("CREATE (p:Person {name: 'Mia', age: 30})")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {age: 26}) RETURN p.name ORDER BY p.name"
    )

    assert len(result) == 2
    assert result[0]["p.name"] == "Kelly"
    assert result[1]["p.name"] == "Leo"


@pytest.mark.asyncio
async def test_match_with_where_clause(async_social_db):
    """Test matching vertices with WHERE clause."""

    await async_social_db.execute("CREATE (p:Person {name: 'Nina', age: 24})")
    await async_social_db.execute("CREATE (p:Person {name: 'Oscar', age: 35})")
    await async_social_db.execute("CREATE (p:Person {name: 'Paul', age: 42})")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person) WHERE p.age >= 30 AND p.age < 40 RETURN p.name, p.age ORDER BY p.name"
    )

    assert len(result) == 1
    assert result[0]["p.name"] == "Oscar"
    assert result[0]["p.age"] == 35


@pytest.mark.asyncio
async def test_set_property_on_vertex(async_social_db):
    """Test updating a property on a vertex using SET."""

    await async_social_db.execute("CREATE (p:Person {name: 'Quinn', age: 28})")
    await async_social_db.flush()

    await async_social_db.execute("MATCH (p:Person {name: 'Quinn'}) SET p.age = 29")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Quinn'}) RETURN p.age"
    )

    assert len(result) == 1
    assert result[0]["p.age"] == 29


@pytest.mark.asyncio
async def test_set_property_on_edge(async_social_db):
    """Test updating a property on an edge using SET."""

    await async_social_db.execute("CREATE (p:Person {name: 'Rachel', age: 30})")
    await async_social_db.execute("CREATE (p:Person {name: 'Sam', age: 32})")
    await async_social_db.execute(
        "MATCH (a:Person {name: 'Rachel'}), (b:Person {name: 'Sam'}) "
        "CREATE (a)-[:KNOWS {since: 2015}]->(b)"
    )
    await async_social_db.flush()

    await async_social_db.execute(
        "MATCH (a:Person {name: 'Rachel'})-[k:KNOWS]->(b:Person {name: 'Sam'}) "
        "SET k.since = 2018"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (a:Person {name: 'Rachel'})-[k:KNOWS]->(b:Person {name: 'Sam'}) "
        "RETURN k.since"
    )

    assert len(result) == 1
    assert result[0]["k.since"] == 2018


@pytest.mark.asyncio
async def test_delete_vertex(async_social_db):
    """Test deleting a vertex."""

    await async_social_db.execute("CREATE (p:Person {name: 'Tina', age: 27})")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Tina'}) RETURN p.name"
    )
    assert len(result) == 1

    await async_social_db.execute("MATCH (p:Person {name: 'Tina'}) DELETE p")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Tina'}) RETURN p.name"
    )
    assert len(result) == 0


@pytest.mark.asyncio
async def test_delete_edge(async_social_db):
    """Test deleting an edge."""

    await async_social_db.execute("CREATE (p:Person {name: 'Uma', age: 29})")
    await async_social_db.execute("CREATE (p:Person {name: 'Victor', age: 31})")
    await async_social_db.execute(
        "MATCH (a:Person {name: 'Uma'}), (b:Person {name: 'Victor'}) "
        "CREATE (a)-[:KNOWS]->(b)"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (a:Person {name: 'Uma'})-[:KNOWS]->(b:Person {name: 'Victor'}) "
        "RETURN a.name, b.name"
    )
    assert len(result) == 1

    await async_social_db.execute(
        "MATCH (a:Person {name: 'Uma'})-[k:KNOWS]->(b:Person {name: 'Victor'}) DELETE k"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (a:Person {name: 'Uma'})-[:KNOWS]->(b:Person {name: 'Victor'}) "
        "RETURN a.name"
    )
    assert len(result) == 0

    result = await async_social_db.query(
        "MATCH (p:Person) WHERE p.name IN ['Uma', 'Victor'] RETURN p.name"
    )
    assert len(result) == 2


@pytest.mark.asyncio
async def test_delete_vertex_with_cascading_edge_removal(async_social_db):
    """Test deleting a vertex with its connected edges."""

    await async_social_db.execute("CREATE (p:Person {name: 'Wendy', age: 28})")
    await async_social_db.execute("CREATE (p:Person {name: 'Xavier', age: 30})")
    await async_social_db.execute("CREATE (p:Person {name: 'Yara', age: 26})")
    await async_social_db.execute(
        "MATCH (a:Person {name: 'Wendy'}), (b:Person {name: 'Xavier'}) "
        "CREATE (a)-[:KNOWS]->(b)"
    )
    await async_social_db.execute(
        "MATCH (a:Person {name: 'Wendy'}), (b:Person {name: 'Yara'}) "
        "CREATE (a)-[:KNOWS]->(b)"
    )
    await async_social_db.flush()

    await async_social_db.execute("MATCH (p:Person {name: 'Wendy'}) DETACH DELETE p")
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Wendy'}) RETURN p.name"
    )
    assert len(result) == 0

    result = await async_social_db.query(
        "MATCH (p:Person) WHERE p.name IN ['Xavier', 'Yara'] RETURN p.name"
    )
    assert len(result) == 2


@pytest.mark.asyncio
async def test_merge_vertex_upsert(async_social_db):
    """Test MERGE for upserting a vertex."""

    await async_social_db.execute("MERGE (p:Person {name: 'Zara', age: 25})")
    await async_social_db.flush()

    result = await async_social_db.query("MATCH (p:Person {name: 'Zara'}) RETURN p.age")
    assert len(result) == 1
    assert result[0]["p.age"] == 25

    await async_social_db.execute("MERGE (p:Person {name: 'Zara', age: 25})")
    await async_social_db.flush()

    result = await async_social_db.query("MATCH (p:Person {name: 'Zara'}) RETURN p.age")
    assert len(result) == 1
    assert result[0]["p.age"] == 25  # Should still be original value


@pytest.mark.asyncio
async def test_merge_edge(async_social_db):
    """Test MERGE for upserting an edge."""

    await async_social_db.execute("CREATE (p:Person {name: 'Adam', age: 33})")
    await async_social_db.execute("CREATE (p:Person {name: 'Beth', age: 29})")
    await async_social_db.flush()

    await async_social_db.execute(
        "MATCH (a:Person {name: 'Adam'}), (b:Person {name: 'Beth'}) "
        "MERGE (a)-[k:KNOWS]->(b) ON CREATE SET k.since = 2021"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (a:Person {name: 'Adam'})-[k:KNOWS]->(b:Person {name: 'Beth'}) "
        "RETURN k.since"
    )
    assert len(result) == 1
    assert result[0]["k.since"] == 2021

    await async_social_db.execute(
        "MATCH (a:Person {name: 'Adam'}), (b:Person {name: 'Beth'}) "
        "MERGE (a)-[k:KNOWS]->(b) ON CREATE SET k.since = 2023"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (a:Person {name: 'Adam'})-[k:KNOWS]->(b:Person {name: 'Beth'}) "
        "RETURN k.since"
    )
    assert len(result) == 1
    assert result[0]["k.since"] == 2021  # Should still be original value


@pytest.mark.asyncio
async def test_match_and_return_multiple_properties(async_social_db):
    """Test matching and returning multiple properties."""

    await async_social_db.execute(
        "CREATE (p:Person {name: 'Carol', age: 34, email: 'carol@example.com'})"
    )
    await async_social_db.execute(
        "CREATE (c:Company {name: 'TechCorp', founded: 2010})"
    )
    await async_social_db.execute(
        "MATCH (p:Person {name: 'Carol'}), (c:Company {name: 'TechCorp'}) "
        "CREATE (p)-[:WORKS_AT {role: 'Engineer'}]->(c)"
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: 'Carol'})-[w:WORKS_AT]->(c:Company) "
        "RETURN p.name, p.age, p.email, c.name, c.founded, w.role"
    )

    assert len(result) == 1
    assert result[0]["p.name"] == "Carol"
    assert result[0]["p.age"] == 34
    assert result[0]["p.email"] == "carol@example.com"
    assert result[0]["c.name"] == "TechCorp"
    assert result[0]["c.founded"] == 2010
    assert result[0]["w.role"] == "Engineer"


@pytest.mark.asyncio
async def test_create_with_parameterized_queries(async_social_db):
    """Test CREATE with parameterized queries using $param syntax."""

    await async_social_db.execute(
        "CREATE (p:Person {name: $name, age: $age})",
        params={"name": "David", "age": 36},
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (p:Person {name: $name}) RETURN p.name, p.age", params={"name": "David"}
    )

    assert len(result) == 1
    assert result[0]["p.name"] == "David"
    assert result[0]["p.age"] == 36

    await async_social_db.execute(
        "CREATE (p:Person {name: $name, age: $age})", params={"name": "Emma", "age": 28}
    )
    await async_social_db.execute(
        "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) "
        "CREATE (a)-[:KNOWS {since: $since}]->(b)",
        params={"name1": "David", "name2": "Emma", "since": 2019},
    )
    await async_social_db.flush()

    result = await async_social_db.query(
        "MATCH (a:Person {name: $name1})-[k:KNOWS]->(b:Person {name: $name2}) "
        "RETURN a.name, b.name, k.since",
        params={"name1": "David", "name2": "Emma"},
    )

    assert len(result) == 1
    assert result[0]["a.name"] == "David"
    assert result[0]["b.name"] == "Emma"
    assert result[0]["k.since"] == 2019
