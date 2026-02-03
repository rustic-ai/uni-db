import json
import os
import uuid


def generate_cell_id():
    """Generate a unique cell ID."""
    return str(uuid.uuid4()).replace("-", "")[:32]


def create_notebook(cells):
    # Add IDs to all cells if missing
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
    return {
        "id": generate_cell_id(),
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in source],
    }


def code_cell(source):
    return {
        "id": generate_cell_id(),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in source],
    }


common_setup = [
    "import os",
    "import shutil",
    "import tempfile",
    "",
    "import uni_db",
]


def db_setup(name):
    return [
        f'db_path = os.path.join(tempfile.gettempdir(), "{name}_db")',
        "if os.path.exists(db_path):",
        "    shutil.rmtree(db_path)",
        "db = uni_db.Database(db_path)",
        'print(f"Opened database at {db_path}")',
    ]


# 1. Supply Chain
supply_chain_nb = create_notebook(
    [
        md_cell(
            [
                "# Supply Chain Management with Uni",
                "",
                "This notebook demonstrates how to model a supply chain graph to perform BOM (Bill of Materials) explosion and cost rollup.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("supply_chain")),
        md_cell(
            [
                "## 1. Define Schema",
                "We define Parts, Suppliers, and Products, along with relationships for Assembly and Supply.",
            ]
        ),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("Part")',
                '        .property("sku", "string")',
                '        .property("cost", "float64")',
                '        .index("sku", "hash")',
                "    .done()",
                '    .label("Supplier")',
                "    .done()",
                '    .label("Product")',
                '        .property("name", "string")',
                '        .property("price", "float64")',
                "    .done()",
                '    .edge_type("ASSEMBLED_FROM", ["Product", "Part"], ["Part"])',
                "    .done()",
                '    .edge_type("SUPPLIED_BY", ["Part"], ["Supplier"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(
            [
                "## 2. Ingest Data",
                "We insert parts and products using bulk insertion for performance.",
            ]
        ),
        code_cell(
            [
                'p1_props = {"sku": "RES-10K", "cost": 0.05, "_doc": {"type": "resistor", "compliance": ["RoHS"]}}',
                'p2_props = {"sku": "MB-X1", "cost": 50.0}',
                'p3_props = {"sku": "SCR-OLED", "cost": 30.0}',
                "",
                'vids = db.bulk_insert_vertices("Part", [p1_props, p2_props, p3_props])',
                "p1, p2, p3 = vids",
                "",
                'prod_props = {"name": "Smartphone X", "price": 500.0}',
                'phone_vids = db.bulk_insert_vertices("Product", [prod_props])',
                "phone = phone_vids[0]",
                "",
                'db.bulk_insert_edges("ASSEMBLED_FROM", [(phone, p2, {}), (phone, p3, {}), (p2, p1, {})])',
                "",
                "db.flush()",
            ]
        ),
        md_cell(
            [
                "## 3. BOM Explosion",
                "Find all products that contain a specific defective part (RES-10K), traversing up the assembly hierarchy.",
            ]
        ),
        code_cell(
            [
                "# Warm-up to ensure adjacency is loaded",
                'db.query("MATCH (a:Part)-[:ASSEMBLED_FROM]->(b:Part) RETURN a.sku")',
                "",
                "query = \"MATCH (defective:Part {sku: 'RES-10K'}) MATCH (product:Product)-[:ASSEMBLED_FROM*1..5]->(defective) RETURN product.name as name, product.price as price\"",
                "results = db.query(query)",
                'print("Products affected:")',
                "for r in results:",
                "    print(r)",
            ]
        ),
        md_cell(
            [
                "## 4. Cost Rollup",
                "Calculate the total cost of parts for a product by traversing down the assembly tree.",
            ]
        ),
        code_cell(
            [
                "query_cost = \"MATCH (p:Product {name: 'Smartphone X'}) MATCH (p)-[:ASSEMBLED_FROM*1..5]->(part:Part) RETURN SUM(part.cost) AS total_bom_cost\"",
                "results_cost = db.query(query_cost)",
                "print(f\"Total BOM Cost: {results_cost[0]['total_bom_cost']}\")",
            ]
        ),
    ]
)

# 2. Recommendation
rec_nb = create_notebook(
    [
        md_cell(
            [
                "# Recommendation Engine",
                "",
                "This notebook demonstrates two approaches to recommendation: Collaborative Filtering (Graph) and Vector Similarity (Semantic).",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("recommendation")),
        md_cell(
            [
                "## 1. Schema",
                "Users view and purchase products. Products have vector embeddings.",
            ]
        ),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("User")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Product")',
                '        .property("name", "string")',
                '        .property("price", "float64")',
                '        .vector("embedding", 4)',
                '        .index("embedding", "vector")',
                "    .done()",
                '    .edge_type("VIEWED", ["User"], ["Product"])',
                "    .done()",
                '    .edge_type("PURCHASED", ["User"], ["Product"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(["## 2. Ingest Data"]),
        code_cell(
            [
                "p1_vec = [1.0, 0.0, 0.0, 0.0]",
                "p2_vec = [0.9, 0.1, 0.0, 0.0]",
                "p3_vec = [0.0, 1.0, 0.0, 0.0]",
                "",
                "# Using single quotes for inner dictionary keys to avoid escape hell",
                "vids = db.bulk_insert_vertices('Product', [",
                "    {'name': 'Running Shoes', 'price': 100.0, 'embedding': p1_vec},",
                "    {'name': 'Socks', 'price': 10.0, 'embedding': p2_vec},",
                "    {'name': 'Shampoo', 'price': 5.0, 'embedding': p3_vec}",
                "]) ",
                "p1, p2, p3 = vids",
                "",
                "u_vids = db.bulk_insert_vertices('User', [{'name': 'Alice'}, {'name': 'Bob'}, {'name': 'Charlie'}])",
                "u1, u2, u3 = u_vids",
                "",
                "# Purchase History: Alice, Bob, Charlie all bought Shoes",
                "db.bulk_insert_edges('PURCHASED', [(u1, p1, {}), (u2, p1, {}), (u3, p1, {})])",
                "",
                "# View History: Alice viewed Socks (similar to Shoes) and Shampoo (different)",
                "db.bulk_insert_edges('VIEWED', [(u1, p2, {}), (u1, p3, {})])",
                "",
                "db.flush()",
            ]
        ),
        md_cell(
            ["## 3. Collaborative Filtering", "Who else bought what Alice bought?"]
        ),
        code_cell(
            [
                "query = \"MATCH (u1:User {name: 'Alice'})-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User) WHERE other._vid <> u1._vid RETURN count(DISTINCT other) as count\"",
                "results = db.query(query)",
                "print(f\"Users with similar purchase history: {results[0]['count']}\")",
            ]
        ),
        md_cell(
            [
                "## 4. Vector-Based Recommendation",
                "Find products semantically similar to what Alice viewed.",
            ]
        ),
        code_cell(
            [
                "# First, get embeddings of products Alice viewed",
                "res = db.query(\"MATCH (u:User {name: 'Alice'})-[:VIEWED]->(p:Product) RETURN p.embedding as emb, p.name as name\")",
                "",
                "for row in res:",
                "    emb = row['emb']",
                "    viewed_name = row['name']",
                '    print(f"Finding products similar to {viewed_name}...")',
                "    ",
                "    # Find similar products",
                '    query_sim = "MATCH (p:Product) WHERE vector_similarity(p.embedding, $emb) > 0.8 RETURN p.name as name"',
                '    sim_products = db.query(query_sim, {"emb": emb})',
                "    names = [r['name'] for r in sim_products]",
                '    print(f"  -> Found: {names}")',
            ]
        ),
    ]
)

# 3. RAG
rag_nb = create_notebook(
    [
        md_cell(
            [
                "# Retrieval-Augmented Generation (RAG)",
                "",
                "Combining Vector Search with Knowledge Graph traversal for better context.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("rag")),
        md_cell(
            [
                "## 1. Schema",
                "Chunks of text with embeddings, linked to named Entities.",
            ]
        ),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("Chunk")',
                '        .property("text", "string")',
                '        .vector("embedding", 4)',
                '        .index("embedding", "vector")',
                "    .done()",
                '    .label("Entity")',
                '        .property("name", "string")',
                '        .property("type", "string")',
                "    .done()",
                '    .edge_type("MENTIONS", ["Chunk"], ["Entity"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(["## 2. Ingest Data"]),
        code_cell(
            [
                "c1_vec = [1.0, 0.0, 0.0, 0.0]",
                "c2_vec = [0.9, 0.1, 0.0, 0.0]",
                "",
                "c_vids = db.bulk_insert_vertices('Chunk', [",
                "    {'text': 'Function verify() checks signatures.', 'embedding': c1_vec},",
                "    {'text': 'Other text about verify.', 'embedding': c2_vec}",
                "]) ",
                "c1, c2 = c_vids",
                "",
                "e_vids = db.bulk_insert_vertices('Entity', [{'name': 'verify', 'type': 'function'}])",
                "e1 = e_vids[0]",
                "",
                "db.bulk_insert_edges('MENTIONS', [(c1, e1, {}), (c2, e1, {})])",
                "db.flush()",
            ]
        ),
        md_cell(
            [
                "## 3. Hybrid Retrieval",
                "Find chunks related to a specific chunk via shared entities.",
            ]
        ),
        code_cell(
            [
                'query = "MATCH (c:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk) WHERE c._vid = $cid AND related._vid <> c._vid RETURN related.text as text"',
                'results = db.query(query, {"cid": c1})',
                "for r in results:",
                "    print(f\"Related text: {r['text']}\")",
            ]
        ),
    ]
)

# 4. Fraud Detection
fraud_nb = create_notebook(
    [
        md_cell(
            [
                "# Fraud Detection",
                "",
                "Detecting money laundering rings (cycles) and shared device anomalies.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("fraud")),
        md_cell(["## 1. Schema"]),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("User")',
                '        .property_nullable("risk_score", "float32")',
                "    .done()",
                '    .label("Device")',
                "    .done()",
                '    .edge_type("SENT_MONEY", ["User"], ["User"])',
                '        .property("amount", "float64")',
                "    .done()",
                '    .edge_type("USED_DEVICE", ["User"], ["Device"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(
            [
                "## 2. Ingestion",
                "Creating a cycle A->B->C->A and a shared device scenario.",
            ]
        ),
        code_cell(
            [
                "u_vids = db.bulk_insert_vertices('User', [",
                "    {'risk_score': 0.1}, # A",
                "    {'risk_score': 0.2}, # B",
                "    {'risk_score': 0.3}, # C",
                "    {'risk_score': 0.9}  # D (Fraudster)",
                "]) ",
                "ua, ub, uc, ud = u_vids",
                "",
                "d_vids = db.bulk_insert_vertices('Device', [{}])",
                "d1 = d_vids[0]",
                "",
                "db.bulk_insert_edges('SENT_MONEY', [",
                "    (ua, ub, {'amount': 5000.0}),",
                "    (ub, uc, {'amount': 5000.0}),",
                "    (uc, ua, {'amount': 5000.0})",
                "]) ",
                "",
                "db.bulk_insert_edges('USED_DEVICE', [(ua, d1, {}), (ud, d1, {})])",
                "db.flush()",
            ]
        ),
        md_cell(["## 3. Cycle Detection", "Identifying circular money flow."]),
        code_cell(
            [
                'query_cycle = "MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a) RETURN count(*) as count"',
                "results = db.query(query_cycle)",
                "print(f\"Cycles detected: {results[0]['count']}\")",
            ]
        ),
        md_cell(
            [
                "## 4. Shared Device Analysis",
                "Identifying users who share devices with high-risk users.",
            ]
        ),
        code_cell(
            [
                'query_shared = "MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User) WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid RETURN u._vid as uid"',
                "results = db.query(query_shared)",
                "print(f\"User sharing device with fraudster: {results[0]['uid']}\")",
            ]
        ),
    ]
)

# 5. Sales Analytics
sales_nb = create_notebook(
    [
        md_cell(
            [
                "# Regional Sales Analytics",
                "",
                "Combining Graph Traversal with Columnar Aggregation.",
            ]
        ),
        code_cell(common_setup),
        code_cell(db_setup("sales")),
        md_cell(["## 1. Schema"]),
        code_cell(
            [
                "(",
                "    db.schema()",
                '    .label("Region")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Order")',
                '        .property("amount", "float64")',
                "    .done()",
                '    .edge_type("SHIPPED_TO", ["Order"], ["Region"])',
                "    .done()",
                "    .apply()",
                ")",
                "",
                'print("Schema created")',
            ]
        ),
        md_cell(["## 2. Ingest Data", "One region, 100 orders."]),
        code_cell(
            [
                "vids_region = db.bulk_insert_vertices(\"Region\", [{'name': 'North'}])",
                "north = vids_region[0]",
                "",
                "orders = [{'amount': 10.0 * (i + 1)} for i in range(100)]",
                'vids_orders = db.bulk_insert_vertices("Order", orders)',
                "",
                "edges = [(v, north, {}) for v in vids_orders]",
                'db.bulk_insert_edges("SHIPPED_TO", edges)',
                "db.flush()",
            ]
        ),
        md_cell(["## 3. Analytical Query", "Sum of amounts for orders in a region."]),
        code_cell(
            [
                "query = \"MATCH (r:Region {name: 'North'})<-[:SHIPPED_TO]-(o:Order) RETURN SUM(o.amount) as total\"",
                "results = db.query(query)",
                "print(f\"Total Sales for North Region: {results[0]['total']}\")",
            ]
        ),
    ]
)

# Write notebooks to the same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(script_dir, "supply_chain.ipynb"), "w") as f:
    json.dump(supply_chain_nb, f, indent=2)

with open(os.path.join(script_dir, "recommendation.ipynb"), "w") as f:
    json.dump(rec_nb, f, indent=2)

with open(os.path.join(script_dir, "rag.ipynb"), "w") as f:
    json.dump(rag_nb, f, indent=2)

with open(os.path.join(script_dir, "fraud_detection.ipynb"), "w") as f:
    json.dump(fraud_nb, f, indent=2)

with open(os.path.join(script_dir, "sales_analytics.ipynb"), "w") as f:
    json.dump(sales_nb, f, indent=2)

print("Notebooks created successfully.")
