#!/usr/bin/env python3
"""Generate example Jupyter notebooks for uni-pydantic."""

import json
import os
import uuid


def generate_cell_id():
    """Generate a unique cell ID."""
    return str(uuid.uuid4()).replace("-", "")[:32]


def create_notebook(cells):
    """Create a notebook structure with cells."""
    for cell in cells:
        if "id" not in cell:
            cell["id"] = generate_cell_id()
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.10.0",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def md_cell(source):
    """Create a markdown cell."""
    return {
        "id": generate_cell_id(),
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in source],
    }


def code_cell(source):
    """Create a code cell."""
    return {
        "id": generate_cell_id(),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in source],
    }


# Common imports for all pydantic notebooks
common_imports = [
    "import os",
    "import shutil",
    "import tempfile",
    "",
    "import uni_db",
    "from uni_pydantic import UniNode, UniEdge, UniSession, Field, Relationship",
]

common_imports_with_vector = [
    "import os",
    "import shutil",
    "import tempfile",
    "",
    "import uni_db",
    "from uni_pydantic import UniNode, UniEdge, UniSession, Field, Relationship, Vector",
]


def db_setup(name):
    """Generate database setup code."""
    return [
        f'db_path = os.path.join(tempfile.gettempdir(), "{name}_pydantic_db")',
        "if os.path.exists(db_path):",
        "    shutil.rmtree(db_path)",
        "db = uni_db.Database(db_path)",
        "",
        "# Create session and register models",
        "session = UniSession(db)",
    ]


# =============================================================================
# 1. Fraud Detection Notebook
# =============================================================================
fraud_nb = create_notebook(
    [
        md_cell(
            [
                "# Fraud Detection with uni-pydantic",
                "",
                "Detecting money laundering rings (cycles) and shared device anomalies using Pydantic models.",
            ]
        ),
        code_cell(common_imports),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Using Pydantic models to define our graph schema with type safety.",
            ]
        ),
        code_cell(
            [
                "class User(UniNode):",
                '    """A user in the fraud detection system."""',
                '    __label__ = "User"',
                "    ",
                "    risk_score: float | None = Field(default=None)",
                "    ",
                "    # Relationships",
                '    sent_to: list["User"] = Relationship("SENT_MONEY", direction="outgoing")',
                '    received_from: list["User"] = Relationship("SENT_MONEY", direction="incoming")',
                '    devices: list["Device"] = Relationship("USED_DEVICE", direction="outgoing")',
                "",
                "",
                "class Device(UniNode):",
                '    """A device used by users."""',
                '    __label__ = "Device"',
                "    ",
                "    # Relationships",
                '    users: list[User] = Relationship("USED_DEVICE", direction="incoming")',
                "",
                "",
                "class SentMoney(UniEdge):",
                '    """Edge representing money transfer between users."""',
                '    __edge_type__ = "SENT_MONEY"',
                "    __from__ = User",
                "    __to__ = User",
                "    ",
                "    amount: float",
                "",
                "",
                "class UsedDevice(UniEdge):",
                '    """Edge representing user-device association."""',
                '    __edge_type__ = "USED_DEVICE"',
                "    __from__ = User",
                "    __to__ = Device",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("fraud")
            + [
                "session.register(User, Device, SentMoney, UsedDevice)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "Creating a cycle A->B->C->A and a shared device scenario using Pydantic models.",
            ]
        ),
        code_cell(
            [
                "# Create users with type-safe models",
                "user_a = User(risk_score=0.1)",
                "user_b = User(risk_score=0.2)",
                "user_c = User(risk_score=0.3)",
                "fraudster = User(risk_score=0.9)  # High risk user",
                "",
                "# Create device",
                "device = Device()",
                "",
                "# Add all nodes to session",
                "session.add_all([user_a, user_b, user_c, fraudster, device])",
                "session.commit()",
                "",
                'print(f"Created users: A(vid={user_a.vid}), B(vid={user_b.vid}), C(vid={user_c.vid}), Fraudster(vid={fraudster.vid})")',
                'print(f"Created device: vid={device.vid}")',
            ]
        ),
        code_cell(
            [
                "# Create money transfer cycle: A -> B -> C -> A",
                'session.create_edge(user_a, "SENT_MONEY", user_b, {"amount": 5000.0})',
                'session.create_edge(user_b, "SENT_MONEY", user_c, {"amount": 5000.0})',
                'session.create_edge(user_c, "SENT_MONEY", user_a, {"amount": 5000.0})',
                "",
                "# Create shared device scenario: User A and Fraudster share device",
                'session.create_edge(user_a, "USED_DEVICE", device)',
                'session.create_edge(fraudster, "USED_DEVICE", device)',
                "",
                "session.commit()",
                'print("Created money transfer cycle and shared device relationships")',
            ]
        ),
        md_cell(
            [
                "## 4. Cycle Detection",
                "",
                "Identifying circular money flow using Cypher queries.",
            ]
        ),
        code_cell(
            [
                "# Detect cycles using raw Cypher",
                'query_cycle = """',
                "MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)",
                "RETURN count(*) as count",
                '"""',
                "results = session.cypher(query_cycle)",
                "print(f\"Cycles detected: {results[0]['count']}\")",
            ]
        ),
        md_cell(
            [
                "## 5. Shared Device Analysis",
                "",
                "Identifying users who share devices with high-risk users.",
            ]
        ),
        code_cell(
            [
                "# Find users sharing devices with fraudsters",
                'query_shared = """',
                "MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)",
                "WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid",
                "RETURN u._vid as uid, u.risk_score as risk_score",
                '"""',
                "results = session.cypher(query_shared)",
                'print("Users sharing device with fraudster:")',
                "for r in results:",
                "    print(f\"  User vid={r['uid']}, risk_score={r['risk_score']}\")",
            ]
        ),
        md_cell(
            [
                "## 6. Query Builder Demo",
                "",
                "Using the type-safe query builder to find high-risk users.",
            ]
        ),
        code_cell(
            [
                "# Find all high-risk users using the query builder",
                "high_risk_users = (",
                "    session.query(User)",
                "    .filter(User.risk_score >= 0.5)",
                "    .all()",
                ")",
                "",
                'print(f"High-risk users found: {len(high_risk_users)}")',
                "for user in high_risk_users:",
                '    print(f"  User vid={user.vid}, risk_score={user.risk_score}")',
            ]
        ),
    ]
)


# =============================================================================
# 2. Recommendation Notebook
# =============================================================================
rec_nb = create_notebook(
    [
        md_cell(
            [
                "# Recommendation Engine with uni-pydantic",
                "",
                "This notebook demonstrates two approaches to recommendation: Collaborative Filtering (Graph) and Vector Similarity (Semantic) using Pydantic models.",
            ]
        ),
        code_cell(common_imports_with_vector),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Users view and purchase products. Products have vector embeddings for semantic similarity.",
            ]
        ),
        code_cell(
            [
                "class User(UniNode):",
                '    """A user who views and purchases products."""',
                '    __label__ = "User"',
                "    ",
                "    name: str",
                "    ",
                "    # Relationships",
                '    viewed: list["Product"] = Relationship("VIEWED", direction="outgoing")',
                '    purchased: list["Product"] = Relationship("PURCHASED", direction="outgoing")',
                "",
                "",
                "class Product(UniNode):",
                '    """A product with semantic embedding."""',
                '    __label__ = "Product"',
                "    ",
                "    name: str",
                "    price: float",
                '    embedding: Vector[4] = Field(metric="cosine")  # 4-dim vector with cosine similarity',
                "    ",
                "    # Relationships",
                '    viewed_by: list[User] = Relationship("VIEWED", direction="incoming")',
                '    purchased_by: list[User] = Relationship("PURCHASED", direction="incoming")',
                "",
                "",
                "class Viewed(UniEdge):",
                '    """Edge representing a user viewing a product."""',
                '    __edge_type__ = "VIEWED"',
                "    __from__ = User",
                "    __to__ = Product",
                "",
                "",
                "class Purchased(UniEdge):",
                '    """Edge representing a user purchasing a product."""',
                '    __edge_type__ = "PURCHASED"',
                "    __from__ = User",
                "    __to__ = Product",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("recommendation")
            + [
                "session.register(User, Product, Viewed, Purchased)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "Create products with embeddings and users with purchase/view history.",
            ]
        ),
        code_cell(
            [
                "# Create products with semantic embeddings",
                "# Similar products have similar vectors",
                "running_shoes = Product(",
                '    name="Running Shoes",',
                "    price=100.0,",
                "    embedding=[1.0, 0.0, 0.0, 0.0]  # Sports category",
                ")",
                "socks = Product(",
                '    name="Socks",',
                "    price=10.0,",
                "    embedding=[0.9, 0.1, 0.0, 0.0]  # Similar to shoes",
                ")",
                "shampoo = Product(",
                '    name="Shampoo",',
                "    price=5.0,",
                "    embedding=[0.0, 1.0, 0.0, 0.0]  # Different category",
                ")",
                "",
                "# Create users",
                'alice = User(name="Alice")',
                'bob = User(name="Bob")',
                'charlie = User(name="Charlie")',
                "",
                "# Add all to session",
                "session.add_all([running_shoes, socks, shampoo, alice, bob, charlie])",
                "session.commit()",
                "",
                'print(f"Created products: {running_shoes.name}, {socks.name}, {shampoo.name}")',
                'print(f"Created users: {alice.name}, {bob.name}, {charlie.name}")',
            ]
        ),
        code_cell(
            [
                "# Purchase history: Alice, Bob, Charlie all bought Running Shoes",
                'session.create_edge(alice, "PURCHASED", running_shoes)',
                'session.create_edge(bob, "PURCHASED", running_shoes)',
                'session.create_edge(charlie, "PURCHASED", running_shoes)',
                "",
                "# View history: Alice viewed Socks and Shampoo",
                'session.create_edge(alice, "VIEWED", socks)',
                'session.create_edge(alice, "VIEWED", shampoo)',
                "",
                "session.commit()",
                'print("Created purchase and view relationships")',
            ]
        ),
        md_cell(
            [
                "## 4. Collaborative Filtering",
                "",
                "Who else bought what Alice bought? Using graph traversal for recommendations.",
            ]
        ),
        code_cell(
            [
                "# Find users with similar purchase history to Alice",
                'query = """',
                "MATCH (alice:User {name: 'Alice'})-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User)",
                "WHERE other._vid <> alice._vid",
                "RETURN DISTINCT other.name as name",
                '"""',
                "results = session.cypher(query)",
                'print("Users with similar purchase history to Alice:")',
                "for r in results:",
                "    print(f\"  - {r['name']}\")",
            ]
        ),
        md_cell(
            [
                "## 5. Vector-Based Recommendation",
                "",
                "Find products semantically similar to what Alice viewed using vector similarity.",
            ]
        ),
        code_cell(
            [
                "# Get embeddings of products Alice viewed",
                "res = session.cypher(",
                "    \"MATCH (u:User {name: 'Alice'})-[:VIEWED]->(p:Product) RETURN p.embedding as emb, p.name as name\"",
                ")",
                "",
                "for row in res:",
                '    emb = row["emb"]',
                '    viewed_name = row["name"]',
                "    print(f\"Finding products similar to '{viewed_name}'...\")",
                "",
                "    # Find similar products using vector similarity",
                '    query_sim = """',
                "    MATCH (p:Product)",
                "    WHERE vector_similarity(p.embedding, $emb) > 0.8",
                "    RETURN p.name as name, p.price as price",
                '    """',
                '    sim_products = session.cypher(query_sim, {"emb": emb})',
                "    for p in sim_products:",
                "        print(f\"  -> {p['name']} (${p['price']})\")",
            ]
        ),
        md_cell(
            [
                "## 6. Query Builder Demo",
                "",
                "Using the type-safe query builder to find products.",
            ]
        ),
        code_cell(
            [
                "# Find all products under $50 using the query builder",
                "affordable_products = (",
                "    session.query(Product)",
                "    .filter(Product.price < 50.0)",
                "    .order_by(Product.price)",
                "    .all()",
                ")",
                "",
                'print("Affordable products (under $50):")',
                "for product in affordable_products:",
                '    print(f"  - {product.name}: ${product.price}")',
            ]
        ),
    ]
)


# =============================================================================
# 3. RAG Notebook
# =============================================================================
rag_nb = create_notebook(
    [
        md_cell(
            [
                "# Retrieval-Augmented Generation (RAG) with uni-pydantic",
                "",
                "Combining Vector Search with Knowledge Graph traversal for better context using Pydantic models.",
            ]
        ),
        code_cell(common_imports_with_vector),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Chunks of text with embeddings, linked to named Entities for knowledge graph traversal.",
            ]
        ),
        code_cell(
            [
                "class Chunk(UniNode):",
                '    """A chunk of text with semantic embedding."""',
                '    __label__ = "Chunk"',
                "    ",
                "    text: str",
                '    embedding: Vector[4] = Field(metric="cosine")  # 4-dim vector for demo',
                "    ",
                "    # Relationships",
                '    entities: list["Entity"] = Relationship("MENTIONS", direction="outgoing")',
                "",
                "",
                "class Entity(UniNode):",
                '    """A named entity extracted from text."""',
                '    __label__ = "Entity"',
                "    ",
                "    name: str",
                '    entity_type: str = Field(default="unknown")  # function, class, variable, etc.',
                "    ",
                "    # Relationships",
                '    mentioned_in: list[Chunk] = Relationship("MENTIONS", direction="incoming")',
                "",
                "",
                "class Mentions(UniEdge):",
                '    """Edge representing a chunk mentioning an entity."""',
                '    __edge_type__ = "MENTIONS"',
                "    __from__ = Chunk",
                "    __to__ = Entity",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("rag")
            + [
                "session.register(Chunk, Entity, Mentions)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "Ingest text chunks with embeddings and link them to entities.",
            ]
        ),
        code_cell(
            [
                "# Create chunks with embeddings",
                "chunk1 = Chunk(",
                '    text="Function verify() checks cryptographic signatures.",',
                "    embedding=[1.0, 0.0, 0.0, 0.0]",
                ")",
                "chunk2 = Chunk(",
                '    text="The verify function validates input before processing.",',
                "    embedding=[0.9, 0.1, 0.0, 0.0]  # Similar to chunk1",
                ")",
                "chunk3 = Chunk(",
                '    text="Database connections are pooled for efficiency.",',
                "    embedding=[0.0, 0.0, 1.0, 0.0]  # Different topic",
                ")",
                "",
                "# Create entities",
                'verify_entity = Entity(name="verify", entity_type="function")',
                'database_entity = Entity(name="database", entity_type="concept")',
                "",
                "# Add all to session",
                "session.add_all([chunk1, chunk2, chunk3, verify_entity, database_entity])",
                "session.commit()",
                "",
                'print(f"Created 3 chunks and 2 entities")',
            ]
        ),
        code_cell(
            [
                "# Link chunks to entities",
                'session.create_edge(chunk1, "MENTIONS", verify_entity)',
                'session.create_edge(chunk2, "MENTIONS", verify_entity)',
                'session.create_edge(chunk3, "MENTIONS", database_entity)',
                "",
                "session.commit()",
                'print("Created entity mention relationships")',
            ]
        ),
        md_cell(
            [
                "## 4. Vector Search",
                "",
                "Find semantically similar chunks using vector similarity.",
            ]
        ),
        code_cell(
            [
                "# Query vector (similar to chunk1)",
                "query_vec = [0.95, 0.05, 0.0, 0.0]",
                "",
                "# Find similar chunks",
                'query = """',
                "MATCH (c:Chunk)",
                "WHERE vector_similarity(c.embedding, $query_vec) > 0.8",
                "RETURN c.text as text",
                '"""',
                'results = session.cypher(query, {"query_vec": query_vec})',
                'print("Chunks similar to query:")',
                "for r in results:",
                "    print(f\"  - {r['text']}\")",
            ]
        ),
        md_cell(
            [
                "## 5. Hybrid Retrieval",
                "",
                "Find chunks related to a specific chunk via shared entities (knowledge graph traversal).",
            ]
        ),
        code_cell(
            [
                "# Find chunks that share entities with chunk1",
                'query = """',
                "MATCH (c:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)",
                "WHERE c._vid = $cid AND related._vid <> c._vid",
                "RETURN related.text as text, e.name as shared_entity",
                '"""',
                'results = session.cypher(query, {"cid": chunk1.vid})',
                'print("Chunks related to chunk1 via shared entities:")',
                "for r in results:",
                "    print(f\"  - '{r['text']}' (via entity: {r['shared_entity']})\")",
            ]
        ),
        md_cell(
            [
                "## 6. Query Builder Demo",
                "",
                "Using the type-safe query builder to find entities.",
            ]
        ),
        code_cell(
            [
                "# Find all function entities",
                "function_entities = (",
                "    session.query(Entity)",
                '    .filter(Entity.entity_type == "function")',
                "    .all()",
                ")",
                "",
                'print("Function entities:")',
                "for entity in function_entities:",
                '    print(f"  - {entity.name} (type: {entity.entity_type})")',
            ]
        ),
        code_cell(
            [
                "# Count total chunks",
                "total_chunks = session.query(Chunk).count()",
                'print(f"Total chunks in knowledge base: {total_chunks}")',
            ]
        ),
    ]
)


# =============================================================================
# 4. Sales Analytics Notebook
# =============================================================================
sales_nb = create_notebook(
    [
        md_cell(
            [
                "# Regional Sales Analytics with uni-pydantic",
                "",
                "Combining Graph Traversal with Columnar Aggregation using Pydantic models.",
            ]
        ),
        code_cell(common_imports),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Orders shipped to regions with type-safe Pydantic models.",
            ]
        ),
        code_cell(
            [
                "class Region(UniNode):",
                '    """A geographic region for sales tracking."""',
                '    __label__ = "Region"',
                "    ",
                '    name: str = Field(index="btree")',
                "    ",
                "    # Relationships",
                '    orders: list["Order"] = Relationship("SHIPPED_TO", direction="incoming")',
                "",
                "",
                "class Order(UniNode):",
                '    """A sales order."""',
                '    __label__ = "Order"',
                "    ",
                "    amount: float",
                "    ",
                "    # Relationships",
                '    region: "Region | None" = Relationship("SHIPPED_TO", direction="outgoing")',
                "",
                "",
                "class ShippedTo(UniEdge):",
                '    """Edge representing order shipped to region."""',
                '    __edge_type__ = "SHIPPED_TO"',
                "    __from__ = Order",
                "    __to__ = Region",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("sales")
            + [
                "session.register(Region, Order, ShippedTo)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "Create regions and orders using Pydantic models.",
            ]
        ),
        code_cell(
            [
                "# Create regions",
                'north = Region(name="North")',
                'south = Region(name="South")',
                'east = Region(name="East")',
                'west = Region(name="West")',
                "",
                "session.add_all([north, south, east, west])",
                "session.commit()",
                "",
                'print("Created regions: North, South, East, West")',
            ]
        ),
        code_cell(
            [
                "# Create 100 orders for North region",
                "orders = [Order(amount=10.0 * (i + 1)) for i in range(100)]",
                "session.add_all(orders)",
                "session.commit()",
                "",
                "# Ship all orders to North",
                "for order in orders:",
                '    session.create_edge(order, "SHIPPED_TO", north)',
                "",
                "# Create some orders for other regions",
                "south_orders = [Order(amount=50.0 * (i + 1)) for i in range(20)]",
                "session.add_all(south_orders)",
                "session.commit()",
                "",
                "for order in south_orders:",
                '    session.create_edge(order, "SHIPPED_TO", south)',
                "",
                "session.commit()",
                'print("Created 100 orders for North and 20 orders for South")',
            ]
        ),
        md_cell(
            [
                "## 4. Analytical Queries",
                "",
                "Aggregation queries combining graph traversal with columnar analytics.",
            ]
        ),
        code_cell(
            [
                "# Sum of amounts for orders in North region",
                'query = """',
                "MATCH (r:Region {name: 'North'})<-[:SHIPPED_TO]-(o:Order)",
                "RETURN SUM(o.amount) as total",
                '"""',
                "results = session.cypher(query)",
                "print(f\"Total Sales for North Region: ${results[0]['total']:,.2f}\")",
            ]
        ),
        code_cell(
            [
                "# Sum of amounts for orders in South region",
                'query = """',
                "MATCH (r:Region {name: 'South'})<-[:SHIPPED_TO]-(o:Order)",
                "RETURN SUM(o.amount) as total, COUNT(o) as order_count, AVG(o.amount) as avg_order",
                '"""',
                "results = session.cypher(query)",
                "r = results[0]",
                'print("South Region Analytics:")',
                "print(f\"  Total Sales: ${r['total']:,.2f}\")",
                "print(f\"  Order Count: {r['order_count']}\")",
                "print(f\"  Average Order: ${r['avg_order']:,.2f}\")",
            ]
        ),
        md_cell(
            [
                "## 5. Query Builder Demo",
                "",
                "Using the type-safe query builder for analytics.",
            ]
        ),
        code_cell(
            [
                "# Find high-value orders using query builder",
                "high_value_orders = (",
                "    session.query(Order)",
                "    .filter(Order.amount >= 500.0)",
                "    .order_by(Order.amount, descending=True)",
                "    .limit(10)",
                "    .all()",
                ")",
                "",
                'print("Top 10 High-Value Orders (>=$500):")',
                "for i, order in enumerate(high_value_orders, 1):",
                '    print(f"  {i}. Order vid={order.vid}: ${order.amount:,.2f}")',
            ]
        ),
        code_cell(
            [
                "# Count orders by value range",
                "small_orders = session.query(Order).filter(Order.amount < 100).count()",
                "medium_orders = session.query(Order).filter(Order.amount >= 100).filter(Order.amount < 500).count()",
                "large_orders = session.query(Order).filter(Order.amount >= 500).count()",
                "",
                'print("Order Distribution:")',
                'print(f"  Small (<$100):     {small_orders}")',
                'print(f"  Medium ($100-499): {medium_orders}")',
                'print(f"  Large (>=$500):    {large_orders}")',
            ]
        ),
    ]
)


# =============================================================================
# 5. Supply Chain Notebook
# =============================================================================
supply_chain_nb = create_notebook(
    [
        md_cell(
            [
                "# Supply Chain Management with uni-pydantic",
                "",
                "This notebook demonstrates how to model a supply chain graph to perform BOM (Bill of Materials) explosion and cost rollup using Pydantic models.",
            ]
        ),
        code_cell(common_imports),
        md_cell(
            [
                "## 1. Define Models",
                "",
                "Parts, Suppliers, and Products with assembly relationships using type-safe Pydantic models.",
            ]
        ),
        code_cell(
            [
                "class Part(UniNode):",
                '    """A component part in the supply chain."""',
                '    __label__ = "Part"',
                "    ",
                '    sku: str = Field(index="hash", unique=True)',
                "    cost: float",
                "    ",
                "    # Relationships",
                '    used_in: list["Part"] = Relationship("ASSEMBLED_FROM", direction="incoming")',
                '    components: list["Part"] = Relationship("ASSEMBLED_FROM", direction="outgoing")',
                '    suppliers: list["Supplier"] = Relationship("SUPPLIED_BY", direction="outgoing")',
                "",
                "",
                "class Supplier(UniNode):",
                '    """A supplier of parts."""',
                '    __label__ = "Supplier"',
                "    ",
                '    name: str = Field(index="btree")',
                "    country: str | None = None",
                "    ",
                "    # Relationships",
                '    supplies: list[Part] = Relationship("SUPPLIED_BY", direction="incoming")',
                "",
                "",
                "class Product(UniNode):",
                '    """A finished product assembled from parts."""',
                '    __label__ = "Product"',
                "    ",
                '    name: str = Field(index="btree")',
                "    price: float",
                "    ",
                "    # Relationships",
                '    components: list[Part] = Relationship("ASSEMBLED_FROM", direction="outgoing")',
                "",
                "",
                "class AssembledFrom(UniEdge):",
                '    """Edge representing assembly relationship."""',
                '    __edge_type__ = "ASSEMBLED_FROM"',
                "    __from__ = (Product, Part)",
                "    __to__ = Part",
                "",
                "",
                "class SuppliedBy(UniEdge):",
                '    """Edge representing supplier relationship."""',
                '    __edge_type__ = "SUPPLIED_BY"',
                "    __from__ = Part",
                "    __to__ = Supplier",
            ]
        ),
        md_cell(["## 2. Setup Database and Session"]),
        code_cell(
            db_setup("supply_chain")
            + [
                "session.register(Part, Supplier, Product, AssembledFrom, SuppliedBy)",
                "session.sync_schema()",
                "",
                'print(f"Opened database at {db_path}")',
            ]
        ),
        md_cell(
            [
                "## 3. Create Data",
                "",
                "Create parts, suppliers, and products with assembly hierarchy.",
            ]
        ),
        code_cell(
            [
                "# Create parts",
                'resistor = Part(sku="RES-10K", cost=0.05)',
                'motherboard = Part(sku="MB-X1", cost=50.0)',
                'screen = Part(sku="SCR-OLED", cost=30.0)',
                'battery = Part(sku="BAT-5000", cost=15.0)',
                'case = Part(sku="CASE-ALU", cost=20.0)',
                "",
                "# Create suppliers",
                'electronics_co = Supplier(name="ElectronicsCo", country="China")',
                'display_inc = Supplier(name="DisplayInc", country="South Korea")',
                "",
                "# Create product",
                'smartphone = Product(name="Smartphone X", price=500.0)',
                "",
                "# Add all nodes",
                "session.add_all([",
                "    resistor, motherboard, screen, battery, case,",
                "    electronics_co, display_inc,",
                "    smartphone",
                "])",
                "session.commit()",
                "",
                'print("Created parts, suppliers, and product")',
            ]
        ),
        code_cell(
            [
                "# Create assembly hierarchy",
                "# Smartphone is assembled from motherboard, screen, battery, case",
                'session.create_edge(smartphone, "ASSEMBLED_FROM", motherboard)',
                'session.create_edge(smartphone, "ASSEMBLED_FROM", screen)',
                'session.create_edge(smartphone, "ASSEMBLED_FROM", battery)',
                'session.create_edge(smartphone, "ASSEMBLED_FROM", case)',
                "",
                "# Motherboard contains resistors",
                'session.create_edge(motherboard, "ASSEMBLED_FROM", resistor)',
                "",
                "# Supplier relationships",
                'session.create_edge(resistor, "SUPPLIED_BY", electronics_co)',
                'session.create_edge(motherboard, "SUPPLIED_BY", electronics_co)',
                'session.create_edge(screen, "SUPPLIED_BY", display_inc)',
                "",
                "session.commit()",
                'print("Created assembly and supplier relationships")',
            ]
        ),
        md_cell(
            [
                "## 4. BOM Explosion",
                "",
                "Find all products affected by a defective part, traversing up the assembly hierarchy.",
            ]
        ),
        code_cell(
            [
                "# Warm-up to ensure adjacency is loaded",
                'session.cypher("MATCH (a:Part)-[:ASSEMBLED_FROM]->(b:Part) RETURN a.sku")',
            ]
        ),
        code_cell(
            [
                "# Find products affected by defective resistor",
                'query = """',
                "MATCH (defective:Part {sku: 'RES-10K'})",
                "MATCH (product:Product)-[:ASSEMBLED_FROM*1..5]->(defective)",
                "RETURN product.name as name, product.price as price",
                '"""',
                "results = session.cypher(query)",
                'print("Products affected by defective RES-10K resistor:")',
                "for r in results:",
                "    print(f\"  - {r['name']} (${r['price']})\")",
            ]
        ),
        md_cell(
            [
                "## 5. Cost Rollup",
                "",
                "Calculate the total cost of parts for a product by traversing down the assembly tree.",
            ]
        ),
        code_cell(
            [
                "# Calculate total BOM cost for Smartphone X",
                'query_cost = """',
                "MATCH (p:Product {name: 'Smartphone X'})",
                "MATCH (p)-[:ASSEMBLED_FROM*1..5]->(part:Part)",
                "RETURN SUM(part.cost) AS total_bom_cost",
                '"""',
                "results_cost = session.cypher(query_cost)",
                "total_cost = results_cost[0]['total_bom_cost']",
                'print(f"Total BOM Cost for Smartphone X: ${total_cost:,.2f}")',
                'print(f"Product Price: ${smartphone.price:,.2f}")',
                'print(f"Gross Margin: ${smartphone.price - total_cost:,.2f} ({(smartphone.price - total_cost) / smartphone.price * 100:.1f}%)")',
            ]
        ),
        md_cell(
            [
                "## 6. Supplier Analysis",
                "",
                "Analyze supplier exposure and risk.",
            ]
        ),
        code_cell(
            [
                "# Find all parts from a specific supplier used in Smartphone X",
                'query = """',
                "MATCH (p:Product {name: 'Smartphone X'})-[:ASSEMBLED_FROM*1..5]->(part:Part)-[:SUPPLIED_BY]->(s:Supplier)",
                "RETURN s.name as supplier, s.country as country, collect(part.sku) as parts, SUM(part.cost) as total_cost",
                '"""',
                "results = session.cypher(query)",
                'print("Supplier Exposure for Smartphone X:")',
                'print("-" * 50)',
                "for r in results:",
                "    print(f\"\\nSupplier: {r['supplier']} ({r['country']})\")",
                "    print(f\"  Parts: {', '.join(r['parts'])}\")",
                "    print(f\"  Total Cost: ${r['total_cost']:,.2f}\")",
            ]
        ),
        md_cell(
            [
                "## 7. Query Builder Demo",
                "",
                "Using the type-safe query builder for supply chain queries.",
            ]
        ),
        code_cell(
            [
                "# Find all expensive parts using query builder",
                "expensive_parts = (",
                "    session.query(Part)",
                "    .filter(Part.cost >= 20.0)",
                "    .order_by(Part.cost, descending=True)",
                "    .all()",
                ")",
                "",
                'print("Expensive Parts (>=$20):")',
                "for part in expensive_parts:",
                '    print(f"  - {part.sku}: ${part.cost:,.2f}")',
            ]
        ),
        code_cell(
            [
                "# Find a specific part by SKU",
                'found_part = session.query(Part).filter(Part.sku == "MB-X1").first()',
                "if found_part:",
                '    print(f"Found part: {found_part.sku}")',
                '    print(f"  Cost: ${found_part.cost:,.2f}")',
                '    print(f"  VID: {found_part.vid}")',
            ]
        ),
    ]
)


# =============================================================================
# Write all notebooks
# =============================================================================
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))

    notebooks = {
        "fraud_detection.ipynb": fraud_nb,
        "recommendation.ipynb": rec_nb,
        "rag.ipynb": rag_nb,
        "sales_analytics.ipynb": sales_nb,
        "supply_chain.ipynb": supply_chain_nb,
    }

    for filename, notebook in notebooks.items():
        filepath = os.path.join(script_dir, filename)
        with open(filepath, "w") as f:
            json.dump(notebook, f, indent=2)
        print(f"Generated {filename}")

    print("\nAll notebooks generated successfully.")
