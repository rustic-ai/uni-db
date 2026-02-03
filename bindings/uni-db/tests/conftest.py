# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Shared test fixtures for both sync and async E2E tests.

Provides predefined schema fixtures for common test scenarios:
- social_db: Person, Company, KNOWS, WORKS_AT
- ecommerce_db: User, Product, Category, Order, VIEWED, PURCHASED, IN_CATEGORY, PLACED
- document_db: Document, Author, Tag, AUTHORED_BY, TAGGED, CITES
- indexed_db: Item with scalar + vector indexes, RELATED_TO
- empty_db: No schema (bare temporary db)
"""

import pytest

import uni_db

# =============================================================================
# Async Fixtures
# =============================================================================


@pytest.fixture
async def async_empty_db():
    """Async temporary database with no schema."""
    return await uni_db.AsyncDatabase.temporary()


@pytest.fixture
async def async_social_db():
    """Async database with social graph schema.

    Labels: Person(name, age, email?), Company(name, founded?)
    Edges: KNOWS(since?), WORKS_AT(role?)
    """
    db = await uni_db.AsyncDatabase.temporary()
    await (
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
        .edge_type("KNOWS", ["Person"], ["Person"])
        .property_nullable("since", "int")
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .property_nullable("role", "string")
        .done()
        .apply()
    )
    return db


@pytest.fixture
async def async_social_db_populated(async_social_db):
    """Async social database pre-populated with test data.

    People: Alice(30), Bob(25), Charlie(35), Diana(28), Eve(32)
    Companies: TechCorp(2010), StartupInc(2020)
    Relationships: Alice-KNOWS->Bob, Bob-KNOWS->Charlie, Alice-KNOWS->Charlie,
                   Diana-KNOWS->Eve, Alice-WORKS_AT->TechCorp, Bob-WORKS_AT->TechCorp,
                   Charlie-WORKS_AT->StartupInc
    """
    db = async_social_db
    await db.execute(
        "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})"
    )
    await db.execute(
        "CREATE (p:Person {name: 'Bob', age: 25, email: 'bob@example.com'})"
    )
    await db.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
    await db.execute(
        "CREATE (p:Person {name: 'Diana', age: 28, email: 'diana@example.com'})"
    )
    await db.execute("CREATE (p:Person {name: 'Eve', age: 32})")
    await db.execute("CREATE (c:Company {name: 'TechCorp', founded: 2010})")
    await db.execute("CREATE (c:Company {name: 'StartupInc', founded: 2020})")

    await db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
        "CREATE (a)-[:KNOWS {since: 2015}]->(b)"
    )
    await db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) "
        "CREATE (b)-[:KNOWS {since: 2018}]->(c)"
    )
    await db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'}) "
        "CREATE (a)-[:KNOWS {since: 2020}]->(c)"
    )
    await db.execute(
        "MATCH (d:Person {name: 'Diana'}), (e:Person {name: 'Eve'}) "
        "CREATE (d)-[:KNOWS]->(e)"
    )
    await db.execute(
        "MATCH (a:Person {name: 'Alice'}), (t:Company {name: 'TechCorp'}) "
        "CREATE (a)-[:WORKS_AT {role: 'Engineer'}]->(t)"
    )
    await db.execute(
        "MATCH (b:Person {name: 'Bob'}), (t:Company {name: 'TechCorp'}) "
        "CREATE (b)-[:WORKS_AT {role: 'Designer'}]->(t)"
    )
    await db.execute(
        "MATCH (c:Person {name: 'Charlie'}), (s:Company {name: 'StartupInc'}) "
        "CREATE (c)-[:WORKS_AT {role: 'CTO'}]->(s)"
    )
    await db.flush()
    return db


@pytest.fixture
async def async_ecommerce_db():
    """Async database with e-commerce schema.

    Labels: User(name), Product(name, price, embedding:vec4), Category(name), Order(amount)
    Edges: VIEWED, PURCHASED, IN_CATEGORY, PLACED
    """
    db = await uni_db.AsyncDatabase.temporary()
    await (
        db.schema()
        .label("User")
        .property("name", "string")
        .done()
        .label("Product")
        .property("name", "string")
        .property("price", "float")
        .vector("embedding", 4)
        .done()
        .label("Category")
        .property("name", "string")
        .done()
        .label("Order")
        .property("amount", "float")
        .done()
        .edge_type("VIEWED", ["User"], ["Product"])
        .done()
        .edge_type("PURCHASED", ["User"], ["Product"])
        .done()
        .edge_type("IN_CATEGORY", ["Product"], ["Category"])
        .done()
        .edge_type("PLACED", ["User"], ["Order"])
        .done()
        .apply()
    )
    return db


@pytest.fixture
async def async_ecommerce_db_populated(async_ecommerce_db):
    """Async e-commerce database pre-populated with data including embeddings."""
    db = async_ecommerce_db
    await db.execute("CREATE (u:User {name: 'Alice'})")
    await db.execute("CREATE (u:User {name: 'Bob'})")
    await db.execute(
        "CREATE (p:Product {name: 'Laptop', price: 999.99, embedding: [1.0, 0.0, 0.0, 0.0]})"
    )
    await db.execute(
        "CREATE (p:Product {name: 'Phone', price: 699.99, embedding: [0.9, 0.1, 0.0, 0.0]})"
    )
    await db.execute(
        "CREATE (p:Product {name: 'Book', price: 19.99, embedding: [0.0, 0.0, 1.0, 0.0]})"
    )
    await db.execute(
        "CREATE (p:Product {name: 'Headphones', price: 149.99, embedding: [0.8, 0.2, 0.0, 0.0]})"
    )
    await db.execute("CREATE (c:Category {name: 'Electronics'})")
    await db.execute("CREATE (c:Category {name: 'Books'})")
    await db.execute("CREATE (o:Order {amount: 999.99})")

    # Edges
    await db.execute(
        "MATCH (u:User {name: 'Alice'}), (p:Product {name: 'Laptop'}) CREATE (u)-[:VIEWED]->(p)"
    )
    await db.execute(
        "MATCH (u:User {name: 'Alice'}), (p:Product {name: 'Laptop'}) CREATE (u)-[:PURCHASED]->(p)"
    )
    await db.execute(
        "MATCH (u:User {name: 'Bob'}), (p:Product {name: 'Book'}) CREATE (u)-[:VIEWED]->(p)"
    )
    await db.execute(
        "MATCH (p:Product {name: 'Laptop'}), (c:Category {name: 'Electronics'}) "
        "CREATE (p)-[:IN_CATEGORY]->(c)"
    )
    await db.execute(
        "MATCH (p:Product {name: 'Phone'}), (c:Category {name: 'Electronics'}) "
        "CREATE (p)-[:IN_CATEGORY]->(c)"
    )
    await db.execute(
        "MATCH (p:Product {name: 'Book'}), (c:Category {name: 'Books'}) "
        "CREATE (p)-[:IN_CATEGORY]->(c)"
    )
    await db.execute(
        "MATCH (u:User {name: 'Alice'}), (o:Order {amount: 999.99}) CREATE (u)-[:PLACED]->(o)"
    )

    await db.create_vector_index("Product", "embedding", "l2")
    await db.flush()
    return db


@pytest.fixture
async def async_document_db():
    """Async database with document/RAG schema.

    Labels: Document(title, text, embedding:vec4), Author(name), Tag(name)
    Edges: AUTHORED_BY, TAGGED, CITES
    """
    db = await uni_db.AsyncDatabase.temporary()
    await (
        db.schema()
        .label("Document")
        .property("title", "string")
        .property("text", "string")
        .vector("embedding", 4)
        .done()
        .label("Author")
        .property("name", "string")
        .done()
        .label("Tag")
        .property("name", "string")
        .done()
        .edge_type("AUTHORED_BY", ["Document"], ["Author"])
        .done()
        .edge_type("TAGGED", ["Document"], ["Tag"])
        .done()
        .edge_type("CITES", ["Document"], ["Document"])
        .done()
        .apply()
    )
    return db


@pytest.fixture
async def async_indexed_db():
    """Async database with indexed schema.

    Labels: Item(sku, name, price, active, embedding:vec4) with btree on sku, hash on name,
            vector index on embedding
    Edges: RELATED_TO(weight?)
    """
    db = await uni_db.AsyncDatabase.temporary()
    await (
        db.schema()
        .label("Item")
        .property("sku", "string")
        .property("name", "string")
        .property("price", "float")
        .property("active", "bool")
        .vector("embedding", 4)
        .index("sku", "btree")
        .index("name", "hash")
        .done()
        .edge_type("RELATED_TO", ["Item"], ["Item"])
        .property_nullable("weight", "float")
        .done()
        .apply()
    )
    await db.create_vector_index("Item", "embedding", "l2")
    return db


# =============================================================================
# Sync Fixtures
# =============================================================================


@pytest.fixture
def empty_db():
    """Sync temporary database with no schema."""
    return uni_db.DatabaseBuilder.temporary().build()


@pytest.fixture
def social_db():
    """Sync database with social graph schema.

    Labels: Person(name, age, email?), Company(name, founded?)
    Edges: KNOWS(since?), WORKS_AT(role?)
    """
    db = uni_db.DatabaseBuilder.temporary().build()
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
        .edge_type("KNOWS", ["Person"], ["Person"])
        .property_nullable("since", "int")
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .property_nullable("role", "string")
        .done()
        .apply()
    )
    return db


@pytest.fixture
def social_db_populated(social_db):
    """Sync social database pre-populated with test data."""
    db = social_db
    db.execute("CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})")
    db.execute("CREATE (p:Person {name: 'Bob', age: 25, email: 'bob@example.com'})")
    db.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
    db.execute("CREATE (p:Person {name: 'Diana', age: 28, email: 'diana@example.com'})")
    db.execute("CREATE (p:Person {name: 'Eve', age: 32})")
    db.execute("CREATE (c:Company {name: 'TechCorp', founded: 2010})")
    db.execute("CREATE (c:Company {name: 'StartupInc', founded: 2020})")

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
        "CREATE (a)-[:KNOWS {since: 2015}]->(b)"
    )
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) "
        "CREATE (b)-[:KNOWS {since: 2018}]->(c)"
    )
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'}) "
        "CREATE (a)-[:KNOWS {since: 2020}]->(c)"
    )
    db.execute(
        "MATCH (d:Person {name: 'Diana'}), (e:Person {name: 'Eve'}) "
        "CREATE (d)-[:KNOWS]->(e)"
    )
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (t:Company {name: 'TechCorp'}) "
        "CREATE (a)-[:WORKS_AT {role: 'Engineer'}]->(t)"
    )
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (t:Company {name: 'TechCorp'}) "
        "CREATE (b)-[:WORKS_AT {role: 'Designer'}]->(t)"
    )
    db.execute(
        "MATCH (c:Person {name: 'Charlie'}), (s:Company {name: 'StartupInc'}) "
        "CREATE (c)-[:WORKS_AT {role: 'CTO'}]->(s)"
    )
    db.flush()
    return db


@pytest.fixture
def ecommerce_db():
    """Sync database with e-commerce schema."""
    db = uni_db.DatabaseBuilder.temporary().build()
    (
        db.schema()
        .label("User")
        .property("name", "string")
        .done()
        .label("Product")
        .property("name", "string")
        .property("price", "float")
        .vector("embedding", 4)
        .done()
        .label("Category")
        .property("name", "string")
        .done()
        .label("Order")
        .property("amount", "float")
        .done()
        .edge_type("VIEWED", ["User"], ["Product"])
        .done()
        .edge_type("PURCHASED", ["User"], ["Product"])
        .done()
        .edge_type("IN_CATEGORY", ["Product"], ["Category"])
        .done()
        .edge_type("PLACED", ["User"], ["Order"])
        .done()
        .apply()
    )
    return db


@pytest.fixture
def ecommerce_db_populated(ecommerce_db):
    """Sync e-commerce database pre-populated with data including embeddings."""
    db = ecommerce_db
    db.execute("CREATE (u:User {name: 'Alice'})")
    db.execute("CREATE (u:User {name: 'Bob'})")
    db.execute(
        "CREATE (p:Product {name: 'Laptop', price: 999.99, embedding: [1.0, 0.0, 0.0, 0.0]})"
    )
    db.execute(
        "CREATE (p:Product {name: 'Phone', price: 699.99, embedding: [0.9, 0.1, 0.0, 0.0]})"
    )
    db.execute(
        "CREATE (p:Product {name: 'Book', price: 19.99, embedding: [0.0, 0.0, 1.0, 0.0]})"
    )
    db.execute(
        "CREATE (p:Product {name: 'Headphones', price: 149.99, embedding: [0.8, 0.2, 0.0, 0.0]})"
    )
    db.execute("CREATE (c:Category {name: 'Electronics'})")
    db.execute("CREATE (c:Category {name: 'Books'})")
    db.execute("CREATE (o:Order {amount: 999.99})")

    db.execute(
        "MATCH (u:User {name: 'Alice'}), (p:Product {name: 'Laptop'}) CREATE (u)-[:VIEWED]->(p)"
    )
    db.execute(
        "MATCH (u:User {name: 'Alice'}), (p:Product {name: 'Laptop'}) CREATE (u)-[:PURCHASED]->(p)"
    )
    db.execute(
        "MATCH (u:User {name: 'Bob'}), (p:Product {name: 'Book'}) CREATE (u)-[:VIEWED]->(p)"
    )
    db.execute(
        "MATCH (p:Product {name: 'Laptop'}), (c:Category {name: 'Electronics'}) "
        "CREATE (p)-[:IN_CATEGORY]->(c)"
    )
    db.execute(
        "MATCH (p:Product {name: 'Phone'}), (c:Category {name: 'Electronics'}) "
        "CREATE (p)-[:IN_CATEGORY]->(c)"
    )
    db.execute(
        "MATCH (p:Product {name: 'Book'}), (c:Category {name: 'Books'}) "
        "CREATE (p)-[:IN_CATEGORY]->(c)"
    )
    db.execute(
        "MATCH (u:User {name: 'Alice'}), (o:Order {amount: 999.99}) CREATE (u)-[:PLACED]->(o)"
    )

    db.create_vector_index("Product", "embedding", "l2")
    db.flush()
    return db


@pytest.fixture
def document_db():
    """Sync database with document/RAG schema."""
    db = uni_db.DatabaseBuilder.temporary().build()
    (
        db.schema()
        .label("Document")
        .property("title", "string")
        .property("text", "string")
        .vector("embedding", 4)
        .done()
        .label("Author")
        .property("name", "string")
        .done()
        .label("Tag")
        .property("name", "string")
        .done()
        .edge_type("AUTHORED_BY", ["Document"], ["Author"])
        .done()
        .edge_type("TAGGED", ["Document"], ["Tag"])
        .done()
        .edge_type("CITES", ["Document"], ["Document"])
        .done()
        .apply()
    )
    return db


@pytest.fixture
def indexed_db():
    """Sync database with indexed schema."""
    db = uni_db.DatabaseBuilder.temporary().build()
    (
        db.schema()
        .label("Item")
        .property("sku", "string")
        .property("name", "string")
        .property("price", "float")
        .property("active", "bool")
        .vector("embedding", 4)
        .index("sku", "btree")
        .index("name", "hash")
        .done()
        .edge_type("RELATED_TO", ["Item"], ["Item"])
        .property_nullable("weight", "float")
        .done()
        .apply()
    )
    db.create_vector_index("Item", "embedding", "l2")
    return db
