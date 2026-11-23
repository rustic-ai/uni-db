# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async versions of use-case integration tests."""

import pytest

import uni_db


@pytest.fixture
async def db():
    """Create a temporary async database for each test."""
    return await uni_db.AsyncUni.temporary()


@pytest.mark.asyncio
async def test_supply_chain(db):
    await (
        db.schema()
        .label("Part")
        .property("sku", "string")
        .property("cost", "float64")
        .done()
        .label("Supplier")
        .done()
        .label("Product")
        .property("name", "string")
        .property("price", "float64")
        .done()
        .edge_type("ASSEMBLED_FROM", ["Product", "Part"], ["Part"])
        .done()
        .edge_type("SUPPLIED_BY", ["Part"], ["Supplier"])
        .done()
        .apply()
    )

    await db.schema().label("Part").index("sku", "hash").apply()

    p1_props = {
        "sku": "RES-10K",
        "cost": 0.05,
        "_doc": {"type": "resistor", "compliance": ["RoHS"]},
    }
    p2_props = {"sku": "MB-X1", "cost": 50.0}
    p3_props = {"sku": "SCR-OLED", "cost": 30.0}

    session = db.session()

    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    vids = await writer.insert_vertices("Part", [p1_props, p2_props, p3_props])
    p1, p2, p3 = vids

    prod_props = {"name": "Smartphone X", "price": 500.0}
    phone_vids = await writer.insert_vertices("Product", [prod_props])
    phone = phone_vids[0]

    await writer.insert_edges(
        "ASSEMBLED_FROM", [(phone, p2, {}), (phone, p3, {}), (p2, p1, {})]
    )
    await writer.commit()
    await tx.commit()

    await db.flush()

    # Warm-up query
    await session.query("MATCH (a:Part)-[:ASSEMBLED_FROM]->(b:Part) RETURN a.sku")

    # BOM Explosion
    results = await session.query("""
        MATCH (defective:Part {sku: 'RES-10K'})
        MATCH (product:Product)-[:ASSEMBLED_FROM*1..5]->(defective)
        RETURN product.name as name, product.price as price
    """)
    names = [r.get("name") for r in results]
    assert "Smartphone X" in names

    # Cost Rollup
    results_cost = await session.query("""
        MATCH (p:Product {name: 'Smartphone X'})
        MATCH (p)-[:ASSEMBLED_FROM*1..5]->(part:Part)
        RETURN SUM(part.cost) AS total_bom_cost
    """)
    assert len(results_cost) == 1
    assert abs(results_cost[0]["total_bom_cost"] - 80.05) < 0.01


@pytest.mark.asyncio
async def test_recommendation(db):
    await (
        db.schema()
        .label("User")
        .done()
        .label("Product")
        .property("name", "string")
        .property("price", "float64")
        .done()
        .label("Category")
        .done()
        .edge_type("VIEWED", ["User"], ["Product"])
        .done()
        .edge_type("PURCHASED", ["User"], ["Product"])
        .done()
        .edge_type("IN_CATEGORY", ["Product"], ["Category"])
        .done()
        .apply()
    )

    await (
        db.schema()
        .label("Product")
        .property("embedding", "vector:4")
        .index("embedding", {"type": "vector", "metric": "cosine"})
        .apply()
    )

    p1_vec = [1.0, 0.0, 0.0, 0.0]
    p2_vec = [0.9, 0.1, 0.0, 0.0]

    session = db.session()

    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    vids = await writer.insert_vertices(
        "Product",
        [
            {"name": "Running Shoes", "price": 100.0, "embedding": p1_vec},
            {"name": "Socks", "price": 10.0, "embedding": p2_vec},
        ],
    )
    p1, p2 = vids

    u_vids = await writer.insert_vertices("User", [{}, {}, {}])
    u1, u2, u3 = u_vids

    await writer.insert_edges("PURCHASED", [(u1, p1, {}), (u2, p1, {}), (u3, p1, {})])
    await writer.commit()
    await tx.commit()

    await db.flush()

    # Collaborative filter
    results = await session.query(
        """
        MATCH (u1:User)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User)
        WHERE u1._vid = $uid AND other._vid <> u1._vid
        RETURN count(DISTINCT other) as count
    """,
        {"uid": u1},
    )
    assert results[0]["count"] == 2


@pytest.mark.asyncio
async def test_rag(db):
    await (
        db.schema()
        .label("Chunk")
        .property("text", "string")
        .done()
        .label("Entity")
        .done()
        .edge_type("MENTIONS", ["Chunk"], ["Entity"])
        .done()
        .apply()
    )

    await (
        db.schema()
        .label("Chunk")
        .property("embedding", "vector:4")
        .index("embedding", {"type": "vector", "metric": "cosine"})
        .apply()
    )

    c1_vec = [1.0, 0.0, 0.0, 0.0]
    c2_vec = [0.9, 0.1, 0.0, 0.0]

    session = db.session()

    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    c_vids = await writer.insert_vertices(
        "Chunk",
        [
            {"text": "Function verify() checks signatures.", "embedding": c1_vec},
            {"text": "Other text about verify.", "embedding": c2_vec},
        ],
    )
    c1, c2 = c_vids

    e_vids = await writer.insert_vertices(
        "Entity", [{"name": "verify", "type": "function"}]
    )
    e1 = e_vids[0]

    await writer.insert_edges("MENTIONS", [(c1, e1, {}), (c2, e1, {})])
    await writer.commit()
    await tx.commit()
    await db.flush()

    # Hybrid RAG query
    results = await session.query(
        """
        MATCH (c:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)
        WHERE c._vid = $cid AND related._vid <> c._vid
        RETURN related.text as text
    """,
        {"cid": c1},
    )
    assert len(results) == 1
    assert results[0]["text"] == "Other text about verify."


@pytest.mark.asyncio
async def test_fraud_detection(db):
    await (
        db.schema()
        .label("User")
        .property_nullable("risk_score", "float32")
        .done()
        .label("Device")
        .done()
        .edge_type("SENT_MONEY", ["User"], ["User"])
        .property("amount", "float64")
        .done()
        .edge_type("USED_DEVICE", ["User"], ["Device"])
        .done()
        .apply()
    )

    session = db.session()

    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    u_vids = await writer.insert_vertices(
        "User",
        [
            {"risk_score": 0.1},
            {"risk_score": 0.2},
            {"risk_score": 0.3},
            {"risk_score": 0.9},
        ],
    )
    ua, ub, uc, ud = u_vids

    d_vids = await writer.insert_vertices("Device", [{}])
    d1 = d_vids[0]

    await writer.insert_edges(
        "SENT_MONEY",
        [
            (ua, ub, {"amount": 5000.0}),
            (ub, uc, {"amount": 5000.0}),
            (uc, ua, {"amount": 5000.0}),
        ],
    )
    await writer.insert_edges("USED_DEVICE", [(ua, d1, {}), (ud, d1, {})])
    await writer.commit()
    await tx.commit()
    await db.flush()

    # Cycle detection
    results = await session.query("""
        MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)
        RETURN count(*) as count
    """)
    assert results[0]["count"] == 3

    # Shared device with fraudster
    results = await session.query("""
        MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)
        WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid
        RETURN u._vid as uid
    """)
    assert len(results) == 1
    assert results[0]["uid"] == ua


@pytest.mark.asyncio
async def test_regional_sales_analytics(db):
    await (
        db.schema()
        .label("Region")
        .property("name", "string")
        .done()
        .label("Order")
        .property("amount", "float64")
        .done()
        .edge_type("SHIPPED_TO", ["Order"], ["Region"])
        .done()
        .apply()
    )

    session = db.session()

    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    vids_region = await writer.insert_vertices("Region", [{"name": "North"}])
    north = vids_region[0]

    orders = [{"amount": 10.0 * (i + 1)} for i in range(100)]
    vids_orders = await writer.insert_vertices("Order", orders)

    edges = [(v, north, {}) for v in vids_orders]
    await writer.insert_edges("SHIPPED_TO", edges)
    await writer.commit()
    await tx.commit()
    await db.flush()

    results = await session.query("""
        MATCH (r:Region {name: 'North'})<-[:SHIPPED_TO]-(o:Order)
        RETURN SUM(o.amount) as total
    """)
    assert abs(results[0]["total"] - 50500.0) < 0.01


@pytest.mark.asyncio
async def test_document_knowledge_graph(db):
    await (
        db.schema()
        .label("Paper")
        .property("topic", "string")
        .property("title", "string")
        .done()
        .edge_type("CITES", ["Paper"], ["Paper"])
        .done()
        .apply()
    )

    session = db.session()

    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    vids = await writer.insert_vertices(
        "Paper",
        [
            {"topic": "AI", "title": "Paper 1"},
            {"topic": "DB", "title": "Paper 2"},
            {"topic": "AI", "title": "Paper 3"},
        ],
    )
    p1, p2, p3 = vids

    await writer.insert_edges("CITES", [(p1, p3, {})])
    await writer.commit()
    await tx.commit()
    await db.flush()

    results = await session.query("""
        MATCH (a:Paper {topic: 'AI'})-[:CITES]->(b:Paper {topic: 'AI'})
        RETURN a.title as src, b.title as dst
    """)
    assert len(results) == 1
    assert results[0]["src"] == "Paper 1"
    assert results[0]["dst"] == "Paper 3"


@pytest.mark.asyncio
async def test_ecommerce_recommendation(db):
    await (
        db.schema()
        .label("User")
        .property("name", "string")
        .done()
        .label("Product")
        .property("name", "string")
        .property("embedding", "vector:2")
        .index("embedding", {"type": "vector", "metric": "l2"})
        .done()
        .edge_type("VIEWED", ["User"], ["Product"])
        .done()
        .apply()
    )

    session = db.session()

    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    vids_u = await writer.insert_vertices("User", [{"name": "Alice"}])
    alice = vids_u[0]

    vids_p = await writer.insert_vertices(
        "Product",
        [
            {"name": "Laptop", "embedding": [1.0, 0.0]},
            {"name": "Mouse", "embedding": [0.9, 0.1]},
            {"name": "Shampoo", "embedding": [0.0, 1.0]},
        ],
    )
    laptop, mouse, shampoo = vids_p

    await writer.insert_edges("VIEWED", [(alice, laptop, {})])
    await writer.commit()
    await tx.commit()
    await db.flush()

    # Find Alice's viewed products
    res = await session.query(
        "MATCH (u:User {name: 'Alice'})-[:VIEWED]->(p:Product) "
        "RETURN p.embedding as emb"
    )
    assert len(res) == 1
    emb = res[0]["emb"]

    # Find similar products
    res_sim = await session.query(
        """
        MATCH (p:Product)
        WHERE vector_similarity(p.embedding, $emb) > 0.9
        RETURN p.name as name
    """,
        {"emb": emb},
    )
    names = [r["name"] for r in res_sim]
    assert "Laptop" in names
    assert "Mouse" in names
    assert "Shampoo" not in names


@pytest.mark.asyncio
async def test_identity_provenance(db):
    await (
        db.schema()
        .label("Node")
        .property("name", "string")
        .done()
        .edge_type("DERIVED_FROM", ["Node"], ["Node"])
        .done()
        .apply()
    )

    session = db.session()

    tx = await session.tx()
    await tx.execute("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'})")
    await tx.execute(
        "MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) "
        "CREATE (b)-[:DERIVED_FROM]->(a)"
    )
    await tx.commit()
    await db.flush()

    res = await session.query(
        "MATCH (b:Node {name: 'B'})-[:DERIVED_FROM]->(a:Node) RETURN a.name as name"
    )
    assert len(res) == 1
    assert res[0]["name"] == "A"
