// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use uni_db::Uni;
use uni_db::unival;

#[tokio::test]
async fn test_rag_use_case() -> anyhow::Result<()> {
    // 1. Setup
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().to_str().unwrap();
    let schema_path = temp_dir.path().join("schema.json");

    let schema_json = r#"{
      "schema_version": 1,
      "labels": {
        "Chunk": { "id": 1, "created_at": "2024-01-01T00:00:00Z", "state": "Active" },
        "Entity": { "id": 2, "created_at": "2024-01-01T00:00:00Z", "state": "Active" }
      },
      "edge_types": {
        "MENTIONS": { "id": 1, "src_labels": ["Chunk"], "dst_labels": ["Entity"], "state": "Active" },
        "RELATED_TO": { "id": 2, "src_labels": ["Entity"], "dst_labels": ["Entity"], "state": "Active" },
        "NEXT_CHUNK": { "id": 3, "src_labels": ["Chunk"], "dst_labels": ["Chunk"], "state": "Active" }
      },
      "properties": {
        "Chunk": {
          "text": { "type": "String", "nullable": false, "added_in": 1, "state": "Active" },
          "embedding": { "type": { "Vector": { "dimensions": 4 } }, "nullable": false, "added_in": 1, "state": "Active" }
        },
        "Entity": {
          "name": { "type": "String", "nullable": false, "added_in": 1, "state": "Active" },
          "type": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" }
        }
      },
      "indexes": [
        {
          "type": "Vector",
          "name": "chunk_embeddings",
          "label": "Chunk",
          "property": "embedding",
          "index_type": { "Hnsw": { "m": 32, "ef_construction": 200, "ef_search": 100 } },
          "metric": "Cosine",
          "embedding_config": null
        }
      ]
    }"#;
    std::fs::write(&schema_path, schema_json)?;

    let db = Uni::open(path).schema_file(&schema_path).build().await?;

    // 2. Data Ingestion
    // Chunks: C1, C2, C3
    // C1: "Function verify() checks signatures." (Vec: [1.0, 0.0, 0.0, 0.0])
    // C2: "Other text about verify." (Vec: [0.9, 0.1, 0.0, 0.0]) -> Close to C1
    // C3: "Bananas are yellow." (Vec: [0.0, 0.0, 1.0, 0.0]) -> Far

    // Entities: E1 ("verify", "function")

    // Edges: C1 -> MENTIONS -> E1
    //        C2 -> MENTIONS -> E1

    let c1_vec = vec![1.0, 0.0, 0.0, 0.0];
    let c2_vec = vec![0.9, 0.1, 0.0, 0.0];
    let c3_vec = vec![0.0, 0.0, 1.0, 0.0];

    let chunks = vec![
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
        HashMap::from([
            ("text".to_string(), unival!("Bananas are yellow.")),
            ("embedding".to_string(), unival!(c3_vec)),
        ]),
    ];
    let chunk_vids = db.bulk_insert_vertices("Chunk", chunks).await?;
    let c1 = chunk_vids[0];
    let c2 = chunk_vids[1];
    let _c3 = chunk_vids[2];

    let entities = vec![HashMap::from([
        ("name".to_string(), unival!("verify")),
        ("type".to_string(), unival!("function")),
    ])];
    let entity_vids = db.bulk_insert_vertices("Entity", entities).await?;
    let e1 = entity_vids[0];

    // Insert Edges
    let edges_mentions = vec![(c1, e1, HashMap::new()), (c2, e1, HashMap::new())];
    db.bulk_insert_edges("MENTIONS", edges_mentions).await?;

    // Flush to ensure data is in Lance (Vector query usually reads from L1/L2)
    // Note: Vector query might NOT read from L0?
    // Implementation detail: Uni's vector query usually leverages Lance's vector search which works on flushed files.
    // L0 is in-memory buffer. Does Vector query scan L0?
    // Let's assume we need to flush for reliable vector search if it uses Lance native search.
    db.flush().await?;

    // Trigger compaction to create vector index?
    // Usually Lance auto-indexes or we might need explicit index build.
    // But for small data, brute force search should work even without index.

    // 3. Query
    // Find chunks similar to [1.0, 0.0, 0.0, 0.0] (C1 itself)
    // Should find C1 (dist 0) and C2 (dist small)
    // Then expand to E1 and find related C2.

    let query_vec = vec![1.0, 0.0, 0.0, 0.0];

    // Debug: Run vector query only
    let debug_query = "CALL uni.vector.query('Chunk', 'embedding', $query_vec, 5) YIELD node, distance RETURN node, distance";
    let debug_result = db
        .query_with(debug_query)
        .param("query_vec", unival!(query_vec.clone()))
        .fetch_all()
        .await?;
    println!("Debug Vector Query: {} rows", debug_result.rows.len());
    assert!(
        !debug_result.rows.is_empty(),
        "Vector search should return results"
    );

    // Note: When using a variable from YIELD in a MATCH pattern,
    // we need to specify the label explicitly for the planner to recognize it.
    let query = "
        CALL uni.vector.query('Chunk', 'embedding', $query_vec, 5)
        YIELD node, distance
        MATCH (node:Chunk)-[:MENTIONS]->(topic:Entity)
        MATCH (related_chunk:Chunk)-[:MENTIONS]->(topic)
        WHERE related_chunk._vid <> node._vid
        RETURN node.text, topic.name, related_chunk.text, distance
    ";

    let result = db
        .query_with(query)
        .param("query_vec", unival!(query_vec))
        .fetch_all()
        .await?;

    println!("Columns: {:?}", result.columns);
    for (i, row) in result.rows.iter().enumerate() {
        println!("Row {}: {:?}", i, row.values);
    }

    // Expect:
    // primary=C1, topic=E1, related=C2
    // primary=C2, topic=E1, related=C1 (maybe, depending on vector search result order)

    // We expect at least one row.
    assert!(
        !result.rows.is_empty(),
        "Should find related chunks via graph"
    );

    let row = &result.rows[0];
    let p_text: String = String::try_from(&row.values[0]).unwrap();
    let topic: String = String::try_from(&row.values[1]).unwrap();
    let r_text: String = String::try_from(&row.values[2]).unwrap();

    println!("Row: {} | {} | {}", p_text, topic, r_text);

    Ok(())
}
