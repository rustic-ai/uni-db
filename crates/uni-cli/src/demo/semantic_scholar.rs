// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::sync::RwLock;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

pub async fn import_semantic_scholar(
    papers_path: &Path,
    citations_path: &Path,
    output_path: &Path,
) -> Result<()> {
    println!("Initializing storage at {:?}", output_path);

    // 1. Setup Schema
    let schema_file = output_path.join("schema.json");
    // Ensure parent dir exists
    if let Some(p) = output_path.parent() {
        tokio::fs::create_dir_all(p).await?;
    }
    tokio::fs::create_dir_all(output_path).await?;

    let schema_manager = SchemaManager::load(&schema_file).await?;

    // Define Schema
    // Paper
    let _paper_lbl = if let Ok(id) = schema_manager.add_label("Paper") {
        id
    } else {
        schema_manager.schema().labels["Paper"].id
    };

    // Ensure properties
    let _ = schema_manager.add_property("Paper", "title", DataType::String, false);
    let _ = schema_manager.add_property("Paper", "year", DataType::Int32, false);
    let _ = schema_manager.add_property("Paper", "citation_count", DataType::Int32, false);
    let _ = schema_manager.add_property(
        "Paper",
        "embedding",
        DataType::Vector { dimensions: 768 },
        false,
    );
    // Additional properties
    let _ = schema_manager.add_property("Paper", "venue", DataType::String, true);

    // CITES Edge
    let cites_type = if let Ok(id) =
        schema_manager.add_edge_type("CITES", vec!["Paper".into()], vec!["Paper".into()])
    {
        id
    } else {
        schema_manager.schema().edge_types["CITES"].id
    };

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage_dir = output_path.join("storage");
    let storage =
        Arc::new(StorageManager::new(storage_dir.to_str().unwrap(), schema_manager.clone()).await?);

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    // 2. Load Papers
    println!("Loading papers from {:?}", papers_path);
    let file = File::open(papers_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut count = 0;

    {
        let mut w = writer.write().await;

        while let Some(line) = lines.next_line().await? {
            let json: Value = serde_json::from_str(&line)?;

            // Extract fields
            let vid_u64 = json
                .get("vid")
                .and_then(|v| v.as_u64())
                .ok_or(anyhow!("Missing vid"))?;
            let vid = Vid::new(vid_u64);

            let mut props = HashMap::new();
            if let Some(t) = json.get("title") {
                props.insert("title".to_string(), t.clone());
            }
            if let Some(y) = json.get("year") {
                props.insert("year".to_string(), y.clone());
            }
            if let Some(c) = json.get("citation_count") {
                props.insert("citation_count".to_string(), c.clone());
            }
            if let Some(e) = json.get("embedding") {
                props.insert("embedding".to_string(), e.clone());
            }

            // Insert vertex — convert serde_json values to uni_common values
            let uni_props: uni_common::Properties =
                props.into_iter().map(|(k, v)| (k, v.into())).collect();
            w.insert_vertex(vid, uni_props).await?;

            count += 1;
            if count % 1000 == 0 {
                print!("\rProcessed {} papers", count);
            }
        }
    }
    println!("\nFlushing papers...");
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // 3. Load Citations
    println!("Loading citations from {:?}", citations_path);
    let file = File::open(citations_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut count = 0;
    {
        let mut w = writer.write().await;

        while let Some(line) = lines.next_line().await? {
            let json: Value = serde_json::from_str(&line)?;

            let src_u64 = json
                .get("src_vid")
                .and_then(|v| v.as_u64())
                .ok_or(anyhow!("Missing src_vid"))?;
            let dst_u64 = json
                .get("dst_vid")
                .and_then(|v| v.as_u64())
                .ok_or(anyhow!("Missing dst_vid"))?;

            let src_vid = Vid::new(src_u64);
            let dst_vid = Vid::new(dst_u64);

            // Generate EID based on count (simple)
            let eid = Eid::new(count);

            w.insert_edge(src_vid, dst_vid, cites_type, eid, HashMap::new())
                .await?;

            count += 1;
            if count % 1000 == 0 {
                print!("\rProcessed {} citations", count);
            }
        }
    }
    println!("\nFlushing citations...");
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    println!("Import complete!");
    Ok(())
}
