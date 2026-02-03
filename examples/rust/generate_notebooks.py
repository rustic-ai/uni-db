#!/usr/bin/env python3
"""Generate Rust Jupyter notebooks for Uni examples.

These notebooks use evcxr_jupyter as the Rust kernel for Jupyter.
Install with: cargo install evcxr_jupyter && evcxr_jupyter --install
"""

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
                "display_name": "Rust",
                "language": "rust",
                "name": "rust",
            },
            "language_info": {
                "codemirror_mode": "rust",
                "file_extension": ".rs",
                "mimetype": "text/rust",
                "name": "Rust",
                "pygment_lexer": "rust",
                "version": "",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def md_cell(source):
    if isinstance(source, list):
        source = "\n".join(source)
    return {
        "id": generate_cell_id(),
        "cell_type": "markdown",
        "metadata": {},
        "source": source,
    }


def code_cell(source):
    if isinstance(source, list):
        source = "\n".join(source)
    return {
        "id": generate_cell_id(),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source,
    }


# Common setup for all notebooks - load the uni crate
common_deps = """:dep uni-db = { path = "../../../crates/uni" }
:dep tokio = { version = "1", features = ["full"] }
:dep serde_json = "1"
"""

common_imports = """use uni::{Uni, DataType, IndexType, ScalarType, VectorMetric, VectorAlgo, VectorIndexCfg};
use std::collections::HashMap;
use serde_json::json;

// Helper macro to run async code in evcxr
macro_rules! run {
    ($e:expr) => {
        tokio::runtime::Runtime::new().unwrap().block_on($e)
    };
}
"""


def db_setup(name):
    return f"""let db_path = "./{name}_db";

// Clean up any existing database
if std::path::Path::new(db_path).exists() {{
    std::fs::remove_dir_all(db_path).unwrap();
}}

let db = run!(Uni::open(db_path).build()).unwrap();
println!("Opened database at {{}}", db_path);
"""


# 1. Supply Chain
supply_chain_nb = create_notebook(
    [
        md_cell(
            [
                "# Supply Chain Management with Uni (Rust)",
                "",
                "This notebook demonstrates how to model a supply chain graph to perform BOM (Bill of Materials) explosion and cost rollup using Uni's native Rust API.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("supply_chain")),
        md_cell(
            [
                "## 1. Define Schema",
                "",
                "We define Parts, Suppliers, and Products, along with relationships for Assembly and Supply.",
            ]
        ),
        code_cell(
            """run!(async {
    db.schema()
        .label("Part")
            .property("sku", DataType::String)
            .property("cost", DataType::Float64)
            .index("sku", IndexType::Scalar(ScalarType::Hash))
        .label("Supplier")
        .label("Product")
            .property("name", DataType::String)
            .property("price", DataType::Float64)
        .edge_type("ASSEMBLED_FROM", &["Product", "Part"], &["Part"])
        .edge_type("SUPPLIED_BY", &["Part"], &["Supplier"])
        .apply()
        .await
}).unwrap();

println!("Schema created successfully");"""
        ),
        md_cell(
            [
                "## 2. Ingest Data",
                "",
                "We insert parts and products using bulk insertion for performance.",
            ]
        ),
        code_cell(
            """// Insert Parts
let part_props = vec![
    HashMap::from([
        ("sku".to_string(), json!("RES-10K")),
        ("cost".to_string(), json!(0.05)),
    ]),
    HashMap::from([
        ("sku".to_string(), json!("MB-X1")),
        ("cost".to_string(), json!(50.0)),
    ]),
    HashMap::from([
        ("sku".to_string(), json!("SCR-OLED")),
        ("cost".to_string(), json!(30.0)),
    ]),
];

let part_vids = run!(db.bulk_insert_vertices("Part", part_props)).unwrap();
let (p1, p2, p3) = (part_vids[0], part_vids[1], part_vids[2]);
println!("Inserted parts: {:?}", part_vids);

// Insert Product
let prod_props = vec![
    HashMap::from([
        ("name".to_string(), json!("Smartphone X")),
        ("price".to_string(), json!(500.0)),
    ]),
];

let phone_vids = run!(db.bulk_insert_vertices("Product", prod_props)).unwrap();
let phone = phone_vids[0];
println!("Inserted product: {:?}", phone);

// Create assembly relationships
// Phone is assembled from MB-X1 and SCR-OLED
// MB-X1 is assembled from RES-10K
run!(db.bulk_insert_edges("ASSEMBLED_FROM", vec![
    (phone, p2, HashMap::new()),  // phone <- MB-X1
    (phone, p3, HashMap::new()),  // phone <- SCR-OLED
    (p2, p1, HashMap::new()),     // MB-X1 <- RES-10K
])).unwrap();

run!(db.flush()).unwrap();
println!("Data ingested and flushed");"""
        ),
        md_cell(
            [
                "## 3. BOM Explosion",
                "",
                "Find all products that contain a specific defective part (RES-10K), traversing up the assembly hierarchy.",
            ]
        ),
        code_cell(
            """// First, warm up adjacency cache
run!(db.query("MATCH (a:Part)-[:ASSEMBLED_FROM]->(b:Part) RETURN a.sku")).unwrap();

// BOM explosion query - find products affected by defective part
let query = r#"
    MATCH (defective:Part {sku: 'RES-10K'})
    MATCH (product:Product)-[:ASSEMBLED_FROM*1..5]->(defective)
    RETURN product.name as name, product.price as price
"#;

let results = run!(db.query(query)).unwrap();
println!("Products affected by defective part:");
for row in results.rows {
    println!("  {:?}", row);
}"""
        ),
        md_cell(
            [
                "## 4. Cost Rollup",
                "",
                "Calculate the total cost of parts for a product by traversing down the assembly tree.",
            ]
        ),
        code_cell(
            """let query_cost = r#"
    MATCH (p:Product {name: 'Smartphone X'})
    MATCH (p)-[:ASSEMBLED_FROM*1..5]->(part:Part)
    RETURN SUM(part.cost) AS total_bom_cost
"#;

let results = run!(db.query(query_cost)).unwrap();
println!("Total BOM Cost: {:?}", results.rows[0]);"""
        ),
    ]
)

# 2. Recommendation Engine
recommendation_nb = create_notebook(
    [
        md_cell(
            [
                "# Recommendation Engine (Rust)",
                "",
                "This notebook demonstrates two approaches to recommendation: Collaborative Filtering (Graph) and Vector Similarity (Semantic) using Uni's native Rust API.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("recommendation")),
        md_cell(
            [
                "## 1. Schema",
                "",
                "Users view and purchase products. Products have vector embeddings.",
            ]
        ),
        code_cell(
            """run!(async {
    db.schema()
        .label("User")
            .property("name", DataType::String)
        .label("Product")
            .property("name", DataType::String)
            .property("price", DataType::Float64)
            .property("embedding", DataType::Vector { dimensions: 4 })
            .index("embedding", IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
            }))
        .edge_type("VIEWED", &["User"], &["Product"])
        .edge_type("PURCHASED", &["User"], &["Product"])
        .apply()
        .await
}).unwrap();

println!("Schema created");"""
        ),
        md_cell("## 2. Ingest Data"),
        code_cell(
            """// Product embeddings (simplified 4D vectors)
let p1_vec = vec![1.0, 0.0, 0.0, 0.0];  // Running Shoes
let p2_vec = vec![0.9, 0.1, 0.0, 0.0];  // Socks (similar to shoes)
let p3_vec = vec![0.0, 1.0, 0.0, 0.0];  // Shampoo (different)

let products = vec![
    HashMap::from([
        ("name".to_string(), json!("Running Shoes")),
        ("price".to_string(), json!(100.0)),
        ("embedding".to_string(), json!(p1_vec)),
    ]),
    HashMap::from([
        ("name".to_string(), json!("Socks")),
        ("price".to_string(), json!(10.0)),
        ("embedding".to_string(), json!(p2_vec)),
    ]),
    HashMap::from([
        ("name".to_string(), json!("Shampoo")),
        ("price".to_string(), json!(5.0)),
        ("embedding".to_string(), json!(p3_vec)),
    ]),
];

let prod_vids = run!(db.bulk_insert_vertices("Product", products)).unwrap();
let (p1, p2, p3) = (prod_vids[0], prod_vids[1], prod_vids[2]);

// Users
let users = vec![
    HashMap::from([("name".to_string(), json!("Alice"))]),
    HashMap::from([("name".to_string(), json!("Bob"))]),
    HashMap::from([("name".to_string(), json!("Charlie"))]),
];

let user_vids = run!(db.bulk_insert_vertices("User", users)).unwrap();
let (u1, u2, u3) = (user_vids[0], user_vids[1], user_vids[2]);

// Purchase history: Alice, Bob, Charlie all bought Shoes
run!(db.bulk_insert_edges("PURCHASED", vec![
    (u1, p1, HashMap::new()),
    (u2, p1, HashMap::new()),
    (u3, p1, HashMap::new()),
])).unwrap();

// View history: Alice viewed Socks and Shampoo
run!(db.bulk_insert_edges("VIEWED", vec![
    (u1, p2, HashMap::new()),
    (u1, p3, HashMap::new()),
])).unwrap();

run!(db.flush()).unwrap();
println!("Data ingested");"""
        ),
        md_cell(
            ["## 3. Collaborative Filtering", "", "Who else bought what Alice bought?"]
        ),
        code_cell(
            """let query = r#"
    MATCH (u1:User {name: 'Alice'})-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User)
    WHERE other._vid <> u1._vid
    RETURN count(DISTINCT other) as count
"#;

let results = run!(db.query(query)).unwrap();
println!("Users with similar purchase history: {:?}", results.rows[0]);"""
        ),
        md_cell(
            [
                "## 4. Vector-Based Recommendation",
                "",
                "Find products semantically similar to what Alice viewed.",
            ]
        ),
        code_cell(
            """// Get embeddings of products Alice viewed
let query = r#"
    MATCH (u:User {name: 'Alice'})-[:VIEWED]->(p:Product)
    RETURN p.embedding as emb, p.name as name
"#;

let results = run!(db.query(query)).unwrap();

for row in &results.rows {
    let viewed_name = &row.values[1];  // name column
    println!("Finding products similar to {:?}...", viewed_name);

    // Note: In a full implementation, you would use vector_similarity function
    // For now, we show the structure
}"""
        ),
    ]
)

# 3. RAG (Retrieval-Augmented Generation)
rag_nb = create_notebook(
    [
        md_cell(
            [
                "# Retrieval-Augmented Generation (RAG) with Uni (Rust)",
                "",
                "Combining Vector Search with Knowledge Graph traversal for better context.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("rag")),
        md_cell(
            [
                "## 1. Schema",
                "",
                "Chunks of text with embeddings, linked to named Entities.",
            ]
        ),
        code_cell(
            """run!(async {
    db.schema()
        .label("Chunk")
            .property("text", DataType::String)
            .property("embedding", DataType::Vector { dimensions: 4 })
            .index("embedding", IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
            }))
        .label("Entity")
            .property("name", DataType::String)
            .property("type", DataType::String)
        .edge_type("MENTIONS", &["Chunk"], &["Entity"])
        .apply()
        .await
}).unwrap();

println!("RAG schema created");"""
        ),
        md_cell("## 2. Ingest Data"),
        code_cell(
            """// Chunk embeddings
let c1_vec = vec![1.0, 0.0, 0.0, 0.0];
let c2_vec = vec![0.9, 0.1, 0.0, 0.0];

let chunks = vec![
    HashMap::from([
        ("text".to_string(), json!("Function verify() checks signatures.")),
        ("embedding".to_string(), json!(c1_vec)),
    ]),
    HashMap::from([
        ("text".to_string(), json!("Other text about verify.")),
        ("embedding".to_string(), json!(c2_vec)),
    ]),
];

let chunk_vids = run!(db.bulk_insert_vertices("Chunk", chunks)).unwrap();
let (c1, c2) = (chunk_vids[0], chunk_vids[1]);

// Entities
let entities = vec![
    HashMap::from([
        ("name".to_string(), json!("verify")),
        ("type".to_string(), json!("function")),
    ]),
];

let entity_vids = run!(db.bulk_insert_vertices("Entity", entities)).unwrap();
let e1 = entity_vids[0];

// Link chunks to entities
run!(db.bulk_insert_edges("MENTIONS", vec![
    (c1, e1, HashMap::new()),
    (c2, e1, HashMap::new()),
])).unwrap();

run!(db.flush()).unwrap();
println!("RAG data ingested");"""
        ),
        md_cell(
            [
                "## 3. Hybrid Retrieval",
                "",
                "Find chunks related to a specific chunk via shared entities.",
            ]
        ),
        code_cell(
            """// Find related chunks through shared entity mentions
let query = format!(r#"
    MATCH (c:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)
    WHERE c._vid = {} AND related._vid <> c._vid
    RETURN related.text as text
"#, c1.as_u64());  // Get the raw vid value

let results = run!(db.query(&query)).unwrap();
println!("Related chunks:");
for row in results.rows {
    println!("  {:?}", row);
}"""
        ),
    ]
)

# 4. Fraud Detection
fraud_nb = create_notebook(
    [
        md_cell(
            [
                "# Fraud Detection with Uni (Rust)",
                "",
                "Detecting money laundering rings (cycles) and shared device anomalies using Uni's native Rust API.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("fraud")),
        md_cell("## 1. Schema"),
        code_cell(
            """run!(async {
    db.schema()
        .label("User")
            .property_nullable("risk_score", DataType::Float32)
        .label("Device")
        .edge_type("SENT_MONEY", &["User"], &["User"])
            .property("amount", DataType::Float64)
        .edge_type("USED_DEVICE", &["User"], &["Device"])
        .apply()
        .await
}).unwrap();

println!("Fraud detection schema created");"""
        ),
        md_cell(
            [
                "## 2. Ingestion",
                "",
                "Creating a cycle A->B->C->A and a shared device scenario.",
            ]
        ),
        code_cell(
            """// Users with risk scores
let users = vec![
    HashMap::from([("risk_score".to_string(), json!(0.1))]),  // A
    HashMap::from([("risk_score".to_string(), json!(0.2))]),  // B
    HashMap::from([("risk_score".to_string(), json!(0.3))]),  // C
    HashMap::from([("risk_score".to_string(), json!(0.9))]),  // D (Fraudster)
];

let user_vids = run!(db.bulk_insert_vertices("User", users)).unwrap();
let (ua, ub, uc, ud) = (user_vids[0], user_vids[1], user_vids[2], user_vids[3]);

// Device
let devices = vec![HashMap::new()];
let device_vids = run!(db.bulk_insert_vertices("Device", devices)).unwrap();
let d1 = device_vids[0];

// Money transfer cycle: A -> B -> C -> A
run!(db.bulk_insert_edges("SENT_MONEY", vec![
    (ua, ub, HashMap::from([("amount".to_string(), json!(5000.0))])),
    (ub, uc, HashMap::from([("amount".to_string(), json!(5000.0))])),
    (uc, ua, HashMap::from([("amount".to_string(), json!(5000.0))])),
])).unwrap();

// Shared device: User A and Fraudster D share device
run!(db.bulk_insert_edges("USED_DEVICE", vec![
    (ua, d1, HashMap::new()),
    (ud, d1, HashMap::new()),
])).unwrap();

run!(db.flush()).unwrap();
println!("Fraud data ingested");"""
        ),
        md_cell(["## 3. Cycle Detection", "", "Identifying circular money flow."]),
        code_cell(
            """let query_cycle = r#"
    MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)
    RETURN count(*) as count
"#;

let results = run!(db.query(query_cycle)).unwrap();
println!("Cycles detected: {:?}", results.rows[0]);"""
        ),
        md_cell(
            [
                "## 4. Shared Device Analysis",
                "",
                "Identifying users who share devices with high-risk users.",
            ]
        ),
        code_cell(
            """let query_shared = r#"
    MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)
    WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid
    RETURN u._vid as uid
"#;

let results = run!(db.query(query_shared)).unwrap();
println!("User sharing device with fraudster: {:?}", results.rows[0]);"""
        ),
    ]
)

# 5. Sales Analytics
sales_nb = create_notebook(
    [
        md_cell(
            [
                "# Regional Sales Analytics with Uni (Rust)",
                "",
                "Combining Graph Traversal with Columnar Aggregation using Uni's native Rust API.",
            ]
        ),
        code_cell(common_deps),
        code_cell(common_imports),
        code_cell(db_setup("sales")),
        md_cell("## 1. Schema"),
        code_cell(
            """run!(async {
    db.schema()
        .label("Region")
            .property("name", DataType::String)
        .label("Order")
            .property("amount", DataType::Float64)
        .edge_type("SHIPPED_TO", &["Order"], &["Region"])
        .apply()
        .await
}).unwrap();

println!("Sales analytics schema created");"""
        ),
        md_cell(["## 2. Ingest Data", "", "One region, 100 orders."]),
        code_cell(
            """// Create region
let regions = vec![
    HashMap::from([("name".to_string(), json!("North"))]),
];

let region_vids = run!(db.bulk_insert_vertices("Region", regions)).unwrap();
let north = region_vids[0];

// Create 100 orders with varying amounts
let orders: Vec<HashMap<String, serde_json::Value>> = (0..100)
    .map(|i| HashMap::from([
        ("amount".to_string(), json!(10.0 * (i + 1) as f64))
    ]))
    .collect();

let order_vids = run!(db.bulk_insert_vertices("Order", orders)).unwrap();

// Ship all orders to North region
let edges: Vec<_> = order_vids.iter()
    .map(|vid| (*vid, north, HashMap::new()))
    .collect();

run!(db.bulk_insert_edges("SHIPPED_TO", edges)).unwrap();
run!(db.flush()).unwrap();

println!("Inserted {} orders shipped to North region", order_vids.len());"""
        ),
        md_cell(
            ["## 3. Analytical Query", "", "Sum of amounts for orders in a region."]
        ),
        code_cell(
            """let query = r#"
    MATCH (r:Region {name: 'North'})<-[:SHIPPED_TO]-(o:Order)
    RETURN SUM(o.amount) as total
"#;

let results = run!(db.query(query)).unwrap();
println!("Total Sales for North Region: {:?}", results.rows[0]);

// Expected: 10 * (1 + 2 + ... + 100) = 10 * 5050 = 50500"""
        ),
    ]
)

# Write notebooks
script_dir = os.path.dirname(os.path.abspath(__file__))

notebooks = [
    ("supply_chain.ipynb", supply_chain_nb),
    ("recommendation.ipynb", recommendation_nb),
    ("rag.ipynb", rag_nb),
    ("fraud_detection.ipynb", fraud_nb),
    ("sales_analytics.ipynb", sales_nb),
]

for filename, nb in notebooks:
    path = os.path.join(script_dir, filename)
    with open(path, "w") as f:
        json.dump(nb, f, indent=2)
    print(f"Created {filename}")

print("\nAll Rust notebooks created successfully!")
print("\nTo use these notebooks:")
print("1. Install evcxr_jupyter: cargo install evcxr_jupyter && evcxr_jupyter --install")
print("2. Run: jupyter notebook")
print("3. Open any .ipynb file and select the Rust kernel")
