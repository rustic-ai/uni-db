// Integration tests for Rust notebook examples
// These tests mirror the Jupyter notebook examples to verify they work correctly.

use std::collections::HashMap;
use tempfile::tempdir;
use uni_db::unival;
use uni_db::{DataType, IndexType, ScalarType, Uni, VectorAlgo, VectorIndexCfg, VectorMetric};

// ============================================================================
// Supply Chain Example
// ============================================================================
#[tokio::test]
async fn test_supply_chain() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().to_str().unwrap();

    let db = Uni::open(db_path).build().await.unwrap();

    // Schema
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
        .unwrap();

    // Insert Parts
    let part_props: Vec<uni_db::common::Properties> = vec![
        HashMap::from([
            ("sku".to_string(), unival!("RES-10K")),
            ("cost".to_string(), unival!(0.05)),
        ]),
        HashMap::from([
            ("sku".to_string(), unival!("MB-X1")),
            ("cost".to_string(), unival!(50.0)),
        ]),
        HashMap::from([
            ("sku".to_string(), unival!("SCR-OLED")),
            ("cost".to_string(), unival!(30.0)),
        ]),
    ];

    let part_vids = db.bulk_insert_vertices("Part", part_props).await.unwrap();
    let (p1, p2, p3) = (part_vids[0], part_vids[1], part_vids[2]);

    // Insert Product
    let prod_props: Vec<uni_db::common::Properties> = vec![HashMap::from([
        ("name".to_string(), unival!("Smartphone X")),
        ("price".to_string(), unival!(500.0)),
    ])];

    let phone_vids = db
        .bulk_insert_vertices("Product", prod_props)
        .await
        .unwrap();
    let phone = phone_vids[0];

    // Create assembly relationships
    db.bulk_insert_edges(
        "ASSEMBLED_FROM",
        vec![
            (phone, p2, HashMap::new()), // phone <- MB-X1
            (phone, p3, HashMap::new()), // phone <- SCR-OLED
            (p2, p1, HashMap::new()),    // MB-X1 <- RES-10K
        ],
    )
    .await
    .unwrap();

    db.flush().await.unwrap();

    // Warm up adjacency cache
    db.query("MATCH (a:Part)-[:ASSEMBLED_FROM]->(b:Part) RETURN a.sku")
        .await
        .unwrap();

    // BOM explosion query
    let query = r#"
        MATCH (defective:Part {sku: 'RES-10K'})
        MATCH (product:Product)-[:ASSEMBLED_FROM*1..5]->(defective)
        RETURN product.name as name, product.price as price
    "#;

    let results = db.query(query).await.unwrap();
    println!("Products affected: {:?}", results.rows);
    assert!(!results.rows.is_empty(), "Should find affected products");

    // Cost rollup
    let query_cost = r#"
        MATCH (p:Product {name: 'Smartphone X'})
        MATCH (p)-[:ASSEMBLED_FROM*1..5]->(part:Part)
        RETURN SUM(part.cost) AS total_bom_cost
    "#;

    let results = db.query(query_cost).await.unwrap();
    println!("Total BOM Cost: {:?}", results.rows[0]);

    Ok(())
}

// ============================================================================
// Recommendation Example
// ============================================================================
#[tokio::test]
async fn test_recommendation() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().to_str().unwrap();

    let db = Uni::open(db_path).build().await.unwrap();

    // Schema
    db.schema()
        .label("User")
        .property("name", DataType::String)
        .label("Product")
        .property("name", DataType::String)
        .property("price", DataType::Float64)
        .property("embedding", DataType::Vector { dimensions: 4 })
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .edge_type("VIEWED", &["User"], &["Product"])
        .edge_type("PURCHASED", &["User"], &["Product"])
        .apply()
        .await
        .unwrap();

    // Product embeddings
    let p1_vec = vec![1.0, 0.0, 0.0, 0.0]; // Running Shoes
    let p2_vec = vec![0.9, 0.1, 0.0, 0.0]; // Socks (similar)
    let p3_vec = vec![0.0, 1.0, 0.0, 0.0]; // Shampoo (different)

    let products: Vec<uni_db::common::Properties> = vec![
        HashMap::from([
            ("name".to_string(), unival!("Running Shoes")),
            ("price".to_string(), unival!(100.0)),
            ("embedding".to_string(), unival!(p1_vec)),
        ]),
        HashMap::from([
            ("name".to_string(), unival!("Socks")),
            ("price".to_string(), unival!(10.0)),
            ("embedding".to_string(), unival!(p2_vec)),
        ]),
        HashMap::from([
            ("name".to_string(), unival!("Shampoo")),
            ("price".to_string(), unival!(5.0)),
            ("embedding".to_string(), unival!(p3_vec)),
        ]),
    ];

    let prod_vids = db.bulk_insert_vertices("Product", products).await.unwrap();
    let (p1, p2, p3) = (prod_vids[0], prod_vids[1], prod_vids[2]);

    // Users
    let users: Vec<uni_db::common::Properties> = vec![
        HashMap::from([("name".to_string(), unival!("Alice"))]),
        HashMap::from([("name".to_string(), unival!("Bob"))]),
        HashMap::from([("name".to_string(), unival!("Charlie"))]),
    ];

    let user_vids = db.bulk_insert_vertices("User", users).await.unwrap();
    let (u1, u2, u3) = (user_vids[0], user_vids[1], user_vids[2]);

    // Purchase history
    db.bulk_insert_edges(
        "PURCHASED",
        vec![
            (u1, p1, HashMap::new()),
            (u2, p1, HashMap::new()),
            (u3, p1, HashMap::new()),
        ],
    )
    .await
    .unwrap();

    // View history
    db.bulk_insert_edges(
        "VIEWED",
        vec![(u1, p2, HashMap::new()), (u1, p3, HashMap::new())],
    )
    .await
    .unwrap();

    db.flush().await.unwrap();

    // Collaborative filtering
    let query = r#"
        MATCH (u1:User {name: 'Alice'})-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User)
        WHERE other._vid <> u1._vid
        RETURN count(DISTINCT other) as count
    "#;

    let results = db.query(query).await.unwrap();
    println!("Users with similar purchase history: {:?}", results.rows[0]);

    Ok(())
}

// ============================================================================
// RAG Example
// ============================================================================
#[tokio::test]
async fn test_rag() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().to_str().unwrap();

    let db = Uni::open(db_path).build().await.unwrap();

    // Schema
    db.schema()
        .label("Chunk")
        .property("text", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 4 })
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .label("Entity")
        .property("name", DataType::String)
        .property("type", DataType::String)
        .edge_type("MENTIONS", &["Chunk"], &["Entity"])
        .apply()
        .await
        .unwrap();

    // Chunk embeddings
    let c1_vec = vec![1.0, 0.0, 0.0, 0.0];
    let c2_vec = vec![0.9, 0.1, 0.0, 0.0];

    let chunks: Vec<uni_db::common::Properties> = vec![
        HashMap::from([
            (
                "text".to_string(),
                unival!("Function verify() checks signatures."),
            ),
            ("embedding".to_string(), unival!(c1_vec)),
        ]),
        HashMap::from([
            ("text".to_string(), unival!("Other text about verify.")),
            ("embedding".to_string(), unival!(c2_vec)),
        ]),
    ];

    let chunk_vids = db.bulk_insert_vertices("Chunk", chunks).await.unwrap();
    let (c1, c2) = (chunk_vids[0], chunk_vids[1]);

    // Entities
    let entities: Vec<uni_db::common::Properties> = vec![HashMap::from([
        ("name".to_string(), unival!("verify")),
        ("type".to_string(), unival!("function")),
    ])];

    let entity_vids = db.bulk_insert_vertices("Entity", entities).await.unwrap();
    let e1 = entity_vids[0];

    // Link chunks to entities
    db.bulk_insert_edges(
        "MENTIONS",
        vec![(c1, e1, HashMap::new()), (c2, e1, HashMap::new())],
    )
    .await
    .unwrap();

    db.flush().await.unwrap();

    // Hybrid retrieval
    let query = format!(
        r#"
        MATCH (c:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(related:Chunk)
        WHERE c._vid = {} AND related._vid <> c._vid
        RETURN related.text as text
    "#,
        c1.as_u64()
    );

    let results = db.query(&query).await.unwrap();
    println!("Related chunks: {:?}", results.rows);
    assert!(!results.rows.is_empty(), "Should find related chunks");

    Ok(())
}

// ============================================================================
// Fraud Detection Example
// ============================================================================
#[tokio::test]
async fn test_fraud_detection() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().to_str().unwrap();

    let db = Uni::open(db_path).build().await.unwrap();

    // Schema
    db.schema()
        .label("User")
        .property_nullable("risk_score", DataType::Float32)
        .label("Device")
        .edge_type("SENT_MONEY", &["User"], &["User"])
        .property("amount", DataType::Float64)
        .edge_type("USED_DEVICE", &["User"], &["Device"])
        .apply()
        .await
        .unwrap();

    // Users with risk scores
    let users: Vec<uni_db::common::Properties> = vec![
        HashMap::from([("risk_score".to_string(), unival!(0.1))]), // A
        HashMap::from([("risk_score".to_string(), unival!(0.2))]), // B
        HashMap::from([("risk_score".to_string(), unival!(0.3))]), // C
        HashMap::from([("risk_score".to_string(), unival!(0.9))]), // D (Fraudster)
    ];

    let user_vids = db.bulk_insert_vertices("User", users).await.unwrap();
    let (ua, ub, uc, ud) = (user_vids[0], user_vids[1], user_vids[2], user_vids[3]);

    // Device
    let devices = vec![HashMap::new()];
    let device_vids = db.bulk_insert_vertices("Device", devices).await.unwrap();
    let d1 = device_vids[0];

    // Money transfer cycle: A -> B -> C -> A
    db.bulk_insert_edges(
        "SENT_MONEY",
        vec![
            (
                ua,
                ub,
                HashMap::from([("amount".to_string(), unival!(5000.0))]),
            ),
            (
                ub,
                uc,
                HashMap::from([("amount".to_string(), unival!(5000.0))]),
            ),
            (
                uc,
                ua,
                HashMap::from([("amount".to_string(), unival!(5000.0))]),
            ),
        ],
    )
    .await
    .unwrap();

    // Shared device
    db.bulk_insert_edges(
        "USED_DEVICE",
        vec![(ua, d1, HashMap::new()), (ud, d1, HashMap::new())],
    )
    .await
    .unwrap();

    db.flush().await.unwrap();

    // Cycle detection
    let query_cycle = r#"
        MATCH (a:User)-[:SENT_MONEY]->(b:User)-[:SENT_MONEY]->(c:User)-[:SENT_MONEY]->(a)
        RETURN count(*) as count
    "#;

    let results = db.query(query_cycle).await.unwrap();
    println!("Cycles detected: {:?}", results.rows[0]);

    // Shared device analysis
    let query_shared = r#"
        MATCH (u:User)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(fraudster:User)
        WHERE fraudster.risk_score > 0.8 AND u._vid <> fraudster._vid
        RETURN u._vid as uid
    "#;

    let results = db.query(query_shared).await.unwrap();
    println!("User sharing device with fraudster: {:?}", results.rows[0]);
    assert!(!results.rows.is_empty(), "Should find user sharing device");

    Ok(())
}

// ============================================================================
// Sales Analytics Example
// ============================================================================
#[tokio::test]
async fn test_sales_analytics() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().to_str().unwrap();

    let db = Uni::open(db_path).build().await.unwrap();

    // Schema
    db.schema()
        .label("Region")
        .property("name", DataType::String)
        .label("ORDER")
        .property("amount", DataType::Float64)
        .edge_type("SHIPPED_TO", &["ORDER"], &["Region"])
        .apply()
        .await
        .unwrap();

    // Create region
    let regions: Vec<uni_db::common::Properties> =
        vec![HashMap::from([("name".to_string(), unival!("North"))])];

    let region_vids = db.bulk_insert_vertices("Region", regions).await.unwrap();
    let north = region_vids[0];

    // Create 100 orders
    let orders: Vec<uni_db::common::Properties> = (0..100)
        .map(|i| HashMap::from([("amount".to_string(), unival!(10.0 * (i + 1) as f64))]))
        .collect();

    let order_vids = db.bulk_insert_vertices("ORDER", orders).await.unwrap();

    // Ship all orders to North region
    let edges: Vec<_> = order_vids
        .iter()
        .map(|vid| (*vid, north, HashMap::new()))
        .collect();

    db.bulk_insert_edges("SHIPPED_TO", edges).await.unwrap();
    db.flush().await.unwrap();

    // Analytical query
    let query = r#"
        MATCH (r:Region {name: 'North'})<-[:SHIPPED_TO]-(o:Order)
        RETURN SUM(o.amount) as total
    "#;

    let results = db.query(query).await.unwrap();
    println!("Total Sales for North Region: {:?}", results.rows[0]);

    // Verify: 10 * (1 + 2 + ... + 100) = 10 * 5050 = 50500
    // The result should contain 50500.0

    Ok(())
}
