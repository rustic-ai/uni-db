// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{
    LargeBinaryArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, UniId, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::storage::manager::StorageManager;

// ... existing tests ...

#[tokio::test]
async fn test_regional_sales_analytics() -> anyhow::Result<()> {
    // Scenario: Region <-[:SHIPPED_TO]- Order.
    // 1. Find Region
    // 2. Traverse to Orders
    // 3. Columnar Scan of "amount" for these orders (Batch Read)
    // 4. Compute Sum (Analytics)

    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _region_lbl = schema_manager.add_label("Region")?;
    let _order_lbl = schema_manager.add_label("Order")?;
    let shipped_type =
        schema_manager.add_edge_type("SHIPPED_TO", vec!["Order".into()], vec!["Region".into()])?;

    schema_manager.add_property("Region", "name", DataType::String, false)?;
    schema_manager.add_property("Order", "amount", DataType::Float64, false)?; // Float64 for currency

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );
    let lancedb_store = storage.lancedb_store();

    // Data:
    // Region: "North" (Vid 0)
    // Orders: 0..100. All shipped to "North".
    // Amount = 10.0 * (i + 1)

    // Insert Region
    let region_ds = storage.vertex_dataset("Region")?;
    let region_batch = RecordBatch::try_new(
        region_ds.get_arrow_schema(&schema_manager.schema())?,
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(0).as_u64()])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false])),
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![None::<&str>; 1])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                );
                lb.values().append_value("Region");
                lb.append(true);
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 1]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 1]).with_timezone("UTC")), // _updated_at
            Arc::new(StringArray::from(vec!["North"])),
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 1])), // overflow_json
        ],
    )?;
    region_ds
        .write_batch_lancedb(lancedb_store, region_batch, &schema_manager.schema())
        .await?;

    // Insert Orders (Batch of 100)
    let order_ds = storage.vertex_dataset("Order")?;
    let num_orders = 100;

    let mut vid_builder = arrow_array::builder::UInt64Builder::new();
    let mut amount_builder = arrow_array::builder::Float64Builder::new();
    let mut uid_builder = arrow_array::builder::FixedSizeBinaryBuilder::new(32);
    let mut bool_builder = arrow_array::builder::BooleanBuilder::new();
    let mut ver_builder = arrow_array::builder::UInt64Builder::new();

    let dummy_uid = vec![0u8; 32];

    for i in 0..num_orders {
        vid_builder.append_value(Vid::new(i as u64).as_u64());
        amount_builder.append_value(10.0 * (i as f64 + 1.0));
        uid_builder.append_value(&dummy_uid).unwrap();
        bool_builder.append_value(false);
        ver_builder.append_value(1);
    }

    let order_batch = RecordBatch::try_new(
        order_ds.get_arrow_schema(&schema_manager.schema())?,
        vec![
            Arc::new(vid_builder.finish()),
            Arc::new(uid_builder.finish()),
            Arc::new(bool_builder.finish()),
            Arc::new(ver_builder.finish()),
            Arc::new(StringArray::from(vec![None::<&str>; num_orders])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                );
                for _ in 0..num_orders {
                    lb.values().append_value("Order");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(
                TimestampNanosecondArray::from(vec![None::<i64>; num_orders]).with_timezone("UTC"),
            ), // _created_at
            Arc::new(
                TimestampNanosecondArray::from(vec![None::<i64>; num_orders]).with_timezone("UTC"),
            ), // _updated_at
            Arc::new(amount_builder.finish()),
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; num_orders])), // overflow_json
        ],
    )?;
    order_ds
        .write_batch_lancedb(lancedb_store, order_batch, &schema_manager.schema())
        .await?;

    // Edges: Order(i) -> Region(0)
    // Adjacency: Region(0) <- Orders(0..99) (Incoming)
    // To traverse efficiently from Region -> Orders, we need "SHIPPED_TO" "bwd" (incoming) adjacency on Region.
    // Or "fwd" adjacency on Orders.
    // Query: MATCH (r:Region {name: "North"})<-[:SHIPPED_TO]-(o:Order) ...
    // Direction: Incoming from Region perspective.

    // We populate `adj_bwd_SHIPPED_TO_Region`.
    // Format: dst_vid (Region) -> [src_vids (Orders)]
    let adj_ds = storage.adjacency_dataset("SHIPPED_TO", "Region", "bwd")?; // bwd partition by dst_label

    let mut n_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());
    let mut e_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());

    // One row for Region 0
    for i in 0..num_orders {
        n_builder.values().append_value(Vid::new(i as u64).as_u64());
        e_builder.values().append_value(Eid::new(i as u64).as_u64());
    }
    n_builder.append(true);
    e_builder.append(true);

    let adj_batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(0).as_u64()])),
            Arc::new(n_builder.finish()),
            Arc::new(e_builder.finish()),
        ],
    )?;
    adj_ds.write_chunk_lancedb(lancedb_store, adj_batch).await?;

    // Warm the adjacency cache
    use uni_db::storage::direction::Direction as CacheDir;
    storage
        .warm_adjacency(shipped_type, CacheDir::Incoming, None)
        .await?;

    // Execution:
    // 1. Load subgraph (Region -> incoming Orders)
    let region_vid = Vid::new(0);
    // Load incoming edges
    let graph = storage
        .load_subgraph(
            &[region_vid],
            &[shipped_type],
            1,
            uni_db::runtime::Direction::Incoming,
            None,
        )
        .await?;

    // 2. Collect Order VIDs
    let mut order_vids = Vec::new();
    for e in graph.edges() {
        let u_vid = e.src_vid;
        order_vids.push(u_vid);
    }
    assert_eq!(order_vids.len(), 100);

    // 3. Fetch amounts using PropertyManager
    // Use PropertyManager to read the "amount" property for each order
    use uni_db::runtime::property_manager::PropertyManager;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 1000);

    let mut total_sales = 0.0f64;
    for vid in &order_vids {
        let amount_val = prop_mgr.get_vertex_prop(*vid, "amount").await?;
        if let Some(val) = amount_val.as_f64() {
            total_sales += val;
        }
    }

    // Expected: 10 * (1 + 2 + ... + 100)
    // Sum 1..100 = 100*101/2 = 5050
    // Total = 50500.0
    assert_eq!(total_sales, 50500.0);

    Ok(())
}

#[tokio::test]
async fn test_ecommerce_recommendation() -> anyhow::Result<()> {
    // Scenario: User -> VIEWED -> Product. Find products similar to what User viewed.
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _user_lbl = schema_manager.add_label("User")?;
    let _prod_lbl = schema_manager.add_label("Product")?;
    let _viewed_type =
        schema_manager.add_edge_type("VIEWED", vec!["User".into()], vec!["Product".into()])?;

    schema_manager.add_property("User", "name", DataType::String, false)?;
    schema_manager.add_property("Product", "name", DataType::String, false)?;
    schema_manager.add_property(
        "Product",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;

    schema_manager.save().await?;

    // Create database using high-level API
    use uni_db::Uni;
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // 2. Create data using high-level API
    db.execute("CREATE (alice:User {name: 'Alice'})").await?;
    db.execute("CREATE (laptop:Product {name: 'Laptop', embedding: [1.0, 0.0]})")
        .await?;
    db.execute("CREATE (mouse:Product {name: 'Mouse', embedding: [0.9, 0.1]})")
        .await?;
    db.execute("CREATE (shampoo:Product {name: 'Shampoo', embedding: [0.0, 1.0]})")
        .await?;

    // Create the VIEWED edge: Alice -> Laptop
    db.execute(
        "MATCH (u:User {name: 'Alice'}), (p:Product {name: 'Laptop'}) CREATE (u)-[:VIEWED]->(p)",
    )
    .await?;

    // Flush to storage
    db.flush().await?;

    // 3. Execution Logic
    // Step A: Find products Alice viewed
    let result = db
        .query("MATCH (u:User)-[:VIEWED]->(p:Product) RETURN p.embedding, p.name")
        .await?;
    assert_eq!(result.len(), 1);

    // Get embedding from result
    let embedding: Vec<f32> = result.rows()[0].get("p.embedding")?;
    assert_eq!(embedding, vec![1.0, 0.0]); // Laptop's embedding

    // Step B: Vector Search using that embedding
    // Find top 2 (should be Laptop itself and Mouse)
    let similar = db
        .query("CALL uni.vector.query('Product', 'embedding', [1.0, 0.0], 2) YIELD node RETURN node.name AS name")
        .await?;

    // Verify we got Laptop and Mouse (both have similar embeddings to [1.0, 0.0])
    let names: Vec<String> = similar
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(similar.len(), 2);
    assert!(names.contains(&"Laptop".to_string()));
    assert!(names.contains(&"Mouse".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_document_knowledge_graph() -> anyhow::Result<()> {
    // Scenario: Document (Paper) -> Graph (CITES) -> Document.
    // JSON Index on $.topic
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _paper_lbl = schema_manager.add_label("Paper")?;
    let cites_type =
        schema_manager.add_edge_type("CITES", vec!["Paper".into()], vec!["Paper".into()])?;

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );
    let lancedb_store = storage.lancedb_store();

    // Insert Papers using vertex dataset directly
    let paper_ds = storage.vertex_dataset("Paper")?;
    let paper_batch = RecordBatch::try_new(
        paper_ds.get_arrow_schema(&schema_manager.schema())?,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(0).as_u64(),
                Vid::new(1).as_u64(),
                Vid::new(2).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 96].into(),
                None,
            )),
            Arc::new(arrow_array::BooleanArray::from(vec![false, false, false])),
            Arc::new(UInt64Array::from(vec![1, 1, 1])),
            Arc::new(StringArray::from(vec![None::<&str>; 3])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                );
                for _ in 0..3 {
                    lb.values().append_value("Paper");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 3]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 3]).with_timezone("UTC")), // _updated_at
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 3])), // overflow_json
        ],
    )?;
    paper_ds
        .write_batch_lancedb(lancedb_store, paper_batch, &schema_manager.schema())
        .await?;

    // Edge: 0 -> CITES -> 2
    let adj_ds = storage.adjacency_dataset("CITES", "Paper", "fwd")?;
    let mut n_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());
    let mut e_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());

    n_builder.values().append_value(Vid::new(2).as_u64());
    n_builder.append(true);
    e_builder.values().append_value(Eid::new(0).as_u64());
    e_builder.append(true);

    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(0).as_u64()])),
            Arc::new(n_builder.finish()),
            Arc::new(e_builder.finish()),
        ],
    )?;
    adj_ds.write_chunk_lancedb(lancedb_store, batch).await?;

    // Warm the adjacency cache
    use uni_db::storage::direction::Direction as CacheDir3;
    storage
        .warm_adjacency(cites_type, CacheDir3::Outgoing, None)
        .await?;

    // Traverse from Paper 0
    let vid_0 = Vid::new(0);
    let graph = storage
        .load_subgraph(
            &[vid_0],
            &[cites_type],
            1,
            uni_db::runtime::Direction::Outgoing,
            None,
        )
        .await?;

    // Check neighbor
    let mut neighbors = Vec::new();
    for e in graph.edges() {
        neighbors.push(e.dst_vid);
    }

    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0], Vid::new(2));

    Ok(())
}

#[tokio::test]
async fn test_identity_provenance() -> anyhow::Result<()> {
    // Scenario: Resolve UID -> VID, then traverse
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let derived_type =
        schema_manager.add_edge_type("DERIVED_FROM", vec!["Node".into()], vec!["Node".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );
    let lancedb_store = storage.lancedb_store();

    // Node A (VID 0) -> UID A
    // Node B (VID 1) -> UID B
    // B -> DERIVED_FROM -> A

    // UIDs
    let uid_bytes_a = [0xA; 32];
    let uid_a = UniId::from_bytes(uid_bytes_a);

    let uid_bytes_b = [0xB; 32];
    let uid_b = UniId::from_bytes(uid_bytes_b);

    // Insert mappings
    storage
        .insert_vertex_with_uid("Node", Vid::new(0), uid_a)
        .await?;
    storage
        .insert_vertex_with_uid("Node", Vid::new(1), uid_b)
        .await?;

    // Create Adjacency B -> A
    let adj_ds = storage.adjacency_dataset("DERIVED_FROM", "Node", "fwd")?;
    let mut n_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());
    let mut e_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::UInt64Builder::new());

    n_builder.values().append_value(Vid::new(0).as_u64());
    n_builder.append(true); // Row 0 (VID B?? No, adjacency dataset rows are sorted by src_vid.
    e_builder.values().append_value(Eid::new(0).as_u64());
    e_builder.append(true);

    // We need to write row for VID 1 (B).
    // Adjacency Chunk format: [src_vid, neighbors, eids].
    // We just write one row for B.
    let batch = RecordBatch::try_new(
        adj_ds.get_arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(1).as_u64()])),
            Arc::new(n_builder.finish()),
            Arc::new(e_builder.finish()),
        ],
    )?;
    adj_ds.write_chunk_lancedb(lancedb_store, batch).await?;

    // Warm the adjacency cache
    use uni_db::storage::direction::Direction as CacheDir4;
    storage
        .warm_adjacency(derived_type, CacheDir4::Outgoing, None)
        .await?;

    // Logic:
    // 1. Resolve UID B
    let vid_b_resolved = storage
        .get_vertex_by_uid(&uid_b, "Node")
        .await?
        .expect("UID B not found");
    assert_eq!(vid_b_resolved, Vid::new(1));

    // 2. Load subgraph
    let graph = storage
        .load_subgraph(
            &[vid_b_resolved],
            &[derived_type],
            1,
            uni_db::runtime::Direction::Outgoing,
            None,
        )
        .await?;

    // 3. Verify edge to A
    let edges: Vec<_> = graph.edges().collect();
    assert_eq!(edges.len(), 1);

    let target_vid = edges[0].dst_vid;

    assert_eq!(target_vid, Vid::new(0));

    Ok(())
}
