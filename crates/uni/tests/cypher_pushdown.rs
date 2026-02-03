// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::{LogicalPlan, QueryPlanner};
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_inline_property_pushdown_logic() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int32, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Case 1: Simple inline property
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;

    // Verify plan has Scan with filter
    if let LogicalPlan::Project { input, .. } = plan {
        if let LogicalPlan::Scan {
            filter, variable, ..
        } = *input
        {
            assert_eq!(variable, "n");
            assert!(
                filter.is_some(),
                "Scan should have a filter for inline properties"
            );

            // Should be n.name = 'Alice'
            if let Some(Expr::BinaryOp { left, op, right }) = filter {
                assert_eq!(op, BinaryOp::Eq);
                if let Expr::Property(_, prop) = *left {
                    assert_eq!(prop, "name");
                } else {
                    panic!("Expected property on left");
                }
                assert_eq!(*right, Expr::Literal(CypherLiteral::String("Alice".into())));
            } else {
                panic!("Expected binary op filter");
            }
        } else {
            panic!("Expected Scan as Project input, found {:?}", input);
        }
    } else {
        panic!("Expected Project at top, found {:?}", plan);
    }

    // Case 2: Inline property + WHERE clause
    // With predicate pushdown optimization, both are combined into Scan's filter
    let sql = "MATCH (n:Person {name: 'Alice'}) WHERE n.age > 30 RETURN n";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;

    // Optimized plan structure: Project -> Scan (with combined filter)
    // Both inline properties AND pushable WHERE predicates go into Scan filter
    if let LogicalPlan::Project { input, .. } = plan {
        if let LogicalPlan::Scan {
            filter, variable, ..
        } = *input
        {
            assert_eq!(variable, "n");
            assert!(
                filter.is_some(),
                "Scan should have combined filter for inline props and WHERE"
            );

            // Filter should be (name = 'Alice') AND (age > 30)
            if let Some(Expr::BinaryOp { op, .. }) = &filter {
                assert_eq!(
                    *op,
                    BinaryOp::And,
                    "Filter should be AND of inline and WHERE predicates"
                );
            } else {
                panic!("Expected combined binary op filter, found {:?}", filter);
            }
        } else {
            panic!(
                "Expected Scan under Project (predicates pushed down), found {:?}",
                input
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_pushdown_execution() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // Seed data
    {
        let mut w = writer.write().await;
        let v1 = w.next_vid().await?;
        let mut p1 = std::collections::HashMap::new();
        p1.insert("name".to_string(), json!("Alice"));
        w.insert_vertex_with_labels(v1, p1, vec!["Person".to_string()])
            .await?;

        let v2 = w.next_vid().await?;
        let mut p2 = std::collections::HashMap::new();
        p2.insert("name".to_string(), json!("Bob"));
        w.insert_vertex_with_labels(v2, p2, vec!["Person".to_string()])
            .await?;

        w.flush_to_l1(None).await?;
    }

    // Query with inline filter
    let sql = "MATCH (n:Person {name: 'Alice'}) RETURN n.name";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("n.name"), Some(&json!("Alice")));

    Ok(())
}

#[tokio::test]
async fn test_or_pushdown_execution() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "status", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // Seed data
    {
        let mut w = writer.write().await;
        let v1 = w.next_vid().await?;
        let mut p1 = std::collections::HashMap::new();
        p1.insert("status".to_string(), json!("active"));
        w.insert_vertex_with_labels(v1, p1, vec!["Person".to_string()])
            .await?;

        let v2 = w.next_vid().await?;
        let mut p2 = std::collections::HashMap::new();
        p2.insert("status".to_string(), json!("pending"));
        w.insert_vertex_with_labels(v2, p2, vec!["Person".to_string()])
            .await?;

        let v3 = w.next_vid().await?;
        let mut p3 = std::collections::HashMap::new();
        p3.insert("status".to_string(), json!("archived"));
        w.insert_vertex_with_labels(v3, p3, vec!["Person".to_string()])
            .await?;

        w.flush_to_l1(None).await?;
    }

    // Query: WHERE status = 'active' OR status = 'pending'
    let sql = "MATCH (n:Person) WHERE n.status = 'active' OR n.status = 'pending' RETURN n.status ORDER BY n.status";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;

    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);
    // Ordered results
    assert_eq!(results[0].get("n.status"), Some(&json!("active")));
    assert_eq!(results[1].get("n.status"), Some(&json!("pending")));

    Ok(())
}

#[tokio::test]
async fn test_is_null_execution() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "email", DataType::String, true)?; // nullable
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // Seed data
    {
        let mut w = writer.write().await;
        // v1: has email
        let v1 = w.next_vid().await?;
        let mut p1 = std::collections::HashMap::new();
        p1.insert("email".to_string(), json!("alice@example.com"));
        w.insert_vertex_with_labels(v1, p1, vec!["Person".to_string()])
            .await?;

        // v2: null email (explicit null or missing property?)
        // Missing property is treated as null in lookups
        let v2 = w.next_vid().await?;
        let p2 = std::collections::HashMap::new();
        w.insert_vertex_with_labels(v2, p2, vec!["Person".to_string()])
            .await?;

        w.flush_to_l1(None).await?;
    }

    // Test IS NOT NULL
    let sql = "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.email";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("n.email"), Some(&json!("alice@example.com")));

    // Test IS NULL
    let sql = "MATCH (n:Person) WHERE n.email IS NULL RETURN n";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    // n is returned, can check id or whatever, but count 1 is enough

    Ok(())
}

#[tokio::test]
async fn test_traverse_target_pushdown() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    let knows_type = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.add_property("Person", "name", DataType::String, true)?;
    schema_manager.add_property("Person", "age", DataType::Int32, true)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // Seed data: v1 (source) -> v2 (age 25), v1 -> v3 (age 35)
    {
        let mut w = writer.write().await;
        let v1 = w.next_vid().await?; // Source
        let v2 = w.next_vid().await?; // Target 1 (age 25)
        let v3 = w.next_vid().await?; // Target 2 (age 35)

        // Insert ALL vertices - v1 was missing before!
        w.insert_vertex_with_labels(
            v1,
            [("name".to_string(), json!("Source"))].into(),
            vec!["Person".to_string()],
        )
        .await?;
        w.insert_vertex_with_labels(
            v2,
            [("age".to_string(), json!(25))].into(),
            vec!["Person".to_string()],
        )
        .await?;
        w.insert_vertex_with_labels(
            v3,
            [("age".to_string(), json!(35))].into(),
            vec!["Person".to_string()],
        )
        .await?;

        let eid1 = w.next_eid(knows_type).await?;
        w.insert_edge(v1, v2, knows_type, eid1, std::collections::HashMap::new())
            .await?;

        let eid2 = w.next_eid(knows_type).await?;
        w.insert_edge(v1, v3, knows_type, eid2, std::collections::HashMap::new())
            .await?;

        w.flush_to_l1(None).await?;
    }

    // Query: MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 30 RETURN b.age
    // This should trigger target filter pushdown
    let sql = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 30 RETURN b.age";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;

    // Verify plan structure
    if let LogicalPlan::Project { input, .. } = &plan {
        let traverse_op = match &**input {
            LogicalPlan::Traverse { target_filter, .. } => {
                assert!(
                    target_filter.is_some(),
                    "Traverse should have target_filter"
                );
                true
            }
            LogicalPlan::Filter {
                input: filter_input,
                ..
            } => {
                if let LogicalPlan::Traverse { target_filter, .. } = &**filter_input {
                    assert!(
                        target_filter.is_some(),
                        "Traverse should have target_filter"
                    );
                    true
                } else {
                    false
                }
            }
            _ => false,
        };
        assert!(traverse_op, "Expected Traverse operator with pushdown");
    }

    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("b.age"), Some(&json!(35)));

    Ok(())
}

/// Test that outer predicates are pushed into Apply node's input_filter
/// to reduce subquery executions (using CALL {} syntax which creates Apply nodes).
#[tokio::test]
async fn test_apply_input_filter_pushdown() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "status", DataType::String, false)?;
    schema_manager.add_property("Person", "score", DataType::Int32, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // Seed data: active and inactive people with scores
    {
        let mut w = writer.write().await;
        let alice = w.next_vid().await?;
        let bob = w.next_vid().await?;
        let charlie = w.next_vid().await?;

        w.insert_vertex_with_labels(
            alice,
            [
                ("name".to_string(), json!("Alice")),
                ("status".to_string(), json!("active")),
                ("score".to_string(), json!(100)),
            ]
            .into(),
            vec!["Person".to_string()],
        )
        .await?;
        w.insert_vertex_with_labels(
            bob,
            [
                ("name".to_string(), json!("Bob")),
                ("status".to_string(), json!("inactive")),
                ("score".to_string(), json!(200)),
            ]
            .into(),
            vec!["Person".to_string()],
        )
        .await?;
        w.insert_vertex_with_labels(
            charlie,
            [
                ("name".to_string(), json!("Charlie")),
                ("status".to_string(), json!("active")),
                ("score".to_string(), json!(150)),
            ]
            .into(),
            vec!["Person".to_string()],
        )
        .await?;

        w.flush_to_l1(None).await?;
    }

    // Query with outer predicate and CALL subquery
    // p.status = 'active' should be pushed down to filter the Apply input
    // Only active people (Alice, Charlie) should have the CALL subquery executed
    let sql = "MATCH (p:Person) WHERE p.status = 'active' CALL { WITH p RETURN p.score * 2 AS doubled } RETURN p.name, doubled ORDER BY p.name";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;

    // Verify plan structure: The predicate should be pushed down
    // It can be pushed either to:
    // 1. Apply.input_filter (for residual predicates), OR
    // 2. Scan.filter (more optimal - all the way to storage)
    fn find_scan_filter(plan: &LogicalPlan) -> Option<&Expr> {
        match plan {
            LogicalPlan::Scan { filter, .. } => filter.as_ref(),
            LogicalPlan::Apply { input, .. } => find_scan_filter(input),
            LogicalPlan::Project { input, .. } => find_scan_filter(input),
            LogicalPlan::Filter { input, .. } => find_scan_filter(input),
            LogicalPlan::Sort { input, .. } => find_scan_filter(input),
            LogicalPlan::Limit { input, .. } => find_scan_filter(input),
            LogicalPlan::SubqueryCall { input, .. } => find_scan_filter(input),
            _ => None,
        }
    }

    // The outer predicate p.status = 'active' should be pushed to Scan filter
    // (the most aggressive/optimal pushdown)
    let scan_filter = find_scan_filter(&plan);
    assert!(
        scan_filter.is_some(),
        "Scan should have filter set with pushed predicate. Plan: {:?}",
        plan
    );

    // Execute and verify results
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    // Only active people: Alice and Charlie
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("p.name"), Some(&json!("Alice")));
    assert_eq!(results[0].get("doubled"), Some(&json!(200))); // 100 * 2
    assert_eq!(results[1].get("p.name"), Some(&json!("Charlie")));
    assert_eq!(results[1].get("doubled"), Some(&json!(300))); // 150 * 2

    Ok(())
}

/// Test subquery predicate pushdown with CALL {} syntax
#[tokio::test]
async fn test_call_subquery_input_filter_pushdown() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int32, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    // Seed data
    {
        let mut w = writer.write().await;
        let alice = w.next_vid().await?;
        let bob = w.next_vid().await?;
        let charlie = w.next_vid().await?;

        w.insert_vertex_with_labels(
            alice,
            [
                ("name".to_string(), json!("Alice")),
                ("age".to_string(), json!(25)),
            ]
            .into(),
            vec!["Person".to_string()],
        )
        .await?;
        w.insert_vertex_with_labels(
            bob,
            [
                ("name".to_string(), json!("Bob")),
                ("age".to_string(), json!(35)),
            ]
            .into(),
            vec!["Person".to_string()],
        )
        .await?;
        w.insert_vertex_with_labels(
            charlie,
            [
                ("name".to_string(), json!("Charlie")),
                ("age".to_string(), json!(45)),
            ]
            .into(),
            vec!["Person".to_string()],
        )
        .await?;

        w.flush_to_l1(None).await?;
    }

    // Query with outer predicate that should be pushed to input_filter
    // p.age > 30 should filter to Bob and Charlie before CALL subquery runs
    let sql = "MATCH (p:Person) WHERE p.age > 30 CALL { WITH p RETURN p.age * 2 AS doubled } RETURN p.name, doubled ORDER BY p.name";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;

    // Execute and verify results
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    // Bob (35) and Charlie (45) have age > 30
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("p.name"), Some(&json!("Bob")));
    assert_eq!(results[0].get("doubled"), Some(&json!(70))); // 35 * 2
    assert_eq!(results[1].get("p.name"), Some(&json!("Charlie")));
    assert_eq!(results[1].get("doubled"), Some(&json!(90))); // 45 * 2

    Ok(())
}
