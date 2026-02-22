// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

/// Helper: create a test environment with Person+Company labels and KNOWS+WORKS_AT edge types.
async fn setup_qpp_graph() -> anyhow::Result<(
    Arc<StorageManager>,
    Arc<RwLock<Writer>>,
    Executor,
    PropertyManager,
    QueryPlanner,
)> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_path_buf();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_label("Company")?;
    schema_manager.add_property("Company", "name", DataType::String, false)?;

    // KNOWS: Person -> Person
    schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    // WORKS_AT: Person -> Company
    schema_manager.add_edge_type(
        "WORKS_AT",
        vec!["Person".to_string()],
        vec!["Company".to_string()],
    )?;
    // PARTNER: Company -> Company
    schema_manager.add_edge_type(
        "PARTNER",
        vec!["Company".to_string()],
        vec!["Company".to_string()],
    )?;
    // LINK: Person -> Person (generic)
    schema_manager.add_edge_type(
        "LINK",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;

    schema_manager.save().await?;
    let schema = Arc::new(schema_manager.schema().clone());

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            Arc::new(schema_manager),
        )
        .await?,
    );
    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), storage.schema_manager_arc(), 0)
            .await
            .unwrap(),
    ));
    let prop_manager = PropertyManager::new(storage.clone(), storage.schema_manager_arc(), 1024);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema);

    // Leak temp_dir so it doesn't get cleaned up during the test
    std::mem::forget(temp_dir);

    Ok((storage, writer, executor, prop_manager, planner))
}

/// Helper to run a query and return sorted result names.
#[allow(dead_code)]
async fn run_query(
    executor: &Executor,
    planner: &QueryPlanner,
    prop_manager: &PropertyManager,
    query: &str,
) -> anyhow::Result<Vec<String>> {
    let params = HashMap::new();
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let results = executor.execute(plan, prop_manager, &params).await?;
    let mut names: Vec<String> = results
        .iter()
        .filter_map(|row| {
            row.values()
                .next()
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        })
        .collect();
    names.sort();
    Ok(names)
}

/// Helper to run a query and return the number of rows.
async fn run_query_count(
    executor: &Executor,
    planner: &QueryPlanner,
    prop_manager: &PropertyManager,
    query: &str,
) -> anyhow::Result<usize> {
    let params = HashMap::new();
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let results = executor.execute(plan, prop_manager, &params).await?;
    Ok(results.len())
}

#[tokio::test]
async fn test_qpp_single_hop_regression() -> anyhow::Result<()> {
    // Single-hop QPP should behave identically to VLP
    // ((a)-[:LINK]->(b)){2,4} == (a)-[:LINK*2..4]->(b)
    let (_storage, _writer, executor, prop_manager, planner) = setup_qpp_graph().await?;
    let params = HashMap::new();

    // Create a chain: P1 -> P2 -> P3 -> P4 -> P5
    let setup = r#"
        CREATE (p1:Person {name: 'P1'})
        CREATE (p2:Person {name: 'P2'})
        CREATE (p3:Person {name: 'P3'})
        CREATE (p4:Person {name: 'P4'})
        CREATE (p5:Person {name: 'P5'})
        CREATE (p1)-[:LINK]->(p2)
        CREATE (p2)-[:LINK]->(p3)
        CREATE (p3)-[:LINK]->(p4)
        CREATE (p4)-[:LINK]->(p5)
    "#;
    let ast = uni_query::parse_cypher(setup)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // QPP: ((a)-[:LINK]->(b)){2,4} from P1
    let count = run_query_count(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (a:Person {name: 'P1'}) MATCH (a)((x)-[:LINK]->(y)){2,4}(b) RETURN b.name",
    )
    .await?;

    // P1 can reach:
    // 2 hops: P3
    // 3 hops: P4
    // 4 hops: P5
    assert_eq!(
        count, 3,
        "QPP single-hop should find 3 endpoints (2,3,4 hops)"
    );

    Ok(())
}

#[tokio::test]
async fn test_qpp_two_hop_basic() -> anyhow::Result<()> {
    // Two-hop QPP: ((a)-[:KNOWS]->(b)-[:WORKS_AT]->(c)){1,2}
    // Each iteration: Person -KNOWS-> Person -WORKS_AT-> Company
    let (_storage, _writer, executor, prop_manager, planner) = setup_qpp_graph().await?;
    let params = HashMap::new();

    // Graph: Alice -KNOWS-> Bob -WORKS_AT-> AcmeCo
    //        Bob -KNOWS-> Charlie -WORKS_AT-> WidgetCo
    let setup = r#"
        CREATE (a:Person {name: 'Alice'})
        CREATE (b:Person {name: 'Bob'})
        CREATE (c:Person {name: 'Charlie'})
        CREATE (co1:Company {name: 'AcmeCo'})
        CREATE (co2:Company {name: 'WidgetCo'})
        CREATE (a)-[:KNOWS]->(b)
        CREATE (b)-[:WORKS_AT]->(co1)
        CREATE (b)-[:KNOWS]->(c)
        CREATE (c)-[:WORKS_AT]->(co2)
    "#;
    let ast = uni_query::parse_cypher(setup)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 1 iteration from Alice: Alice -KNOWS-> Bob -WORKS_AT-> AcmeCo → target = AcmeCo
    let count = run_query_count(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (a:Person {name: 'Alice'}) MATCH (a)((x)-[:KNOWS]->(y)-[:WORKS_AT]->(z)){1,1}(target) RETURN target.name",
    )
    .await?;
    assert_eq!(count, 1, "1 iteration should find AcmeCo");

    Ok(())
}

#[tokio::test]
async fn test_qpp_with_label_constraint() -> anyhow::Result<()> {
    // QPP with label constraint on intermediate node:
    // ((a)-[:KNOWS]->(b:Person)){1,3}
    // This should filter intermediate nodes to only Person labels
    let (_storage, _writer, executor, prop_manager, planner) = setup_qpp_graph().await?;
    let params = HashMap::new();

    // Chain: P1 -> P2 -> P3 -> P4
    let setup = r#"
        CREATE (p1:Person {name: 'P1'})
        CREATE (p2:Person {name: 'P2'})
        CREATE (p3:Person {name: 'P3'})
        CREATE (p4:Person {name: 'P4'})
        CREATE (p1)-[:KNOWS]->(p2)
        CREATE (p2)-[:KNOWS]->(p3)
        CREATE (p3)-[:KNOWS]->(p4)
    "#;
    let ast = uni_query::parse_cypher(setup)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // QPP: ((a)-[:KNOWS]->(b:Person)){1,3} from P1
    // All intermediate nodes are Person, so all paths should match
    let count = run_query_count(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (a:Person {name: 'P1'}) MATCH (a)((x)-[:KNOWS]->(y:Person)){1,3}(b) RETURN b.name",
    )
    .await?;
    assert_eq!(count, 3, "Should find P2, P3, P4 (all Person)");

    Ok(())
}

#[tokio::test]
async fn test_qpp_no_match() -> anyhow::Result<()> {
    // QPP with non-existent edge type should return empty
    let (_storage, _writer, executor, prop_manager, planner) = setup_qpp_graph().await?;
    let params = HashMap::new();

    let setup = r#"
        CREATE (p1:Person {name: 'P1'})
        CREATE (p2:Person {name: 'P2'})
        CREATE (p1)-[:KNOWS]->(p2)
    "#;
    let ast = uni_query::parse_cypher(setup)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // QPP using PARTNER edge type (no Person->Person PARTNER edges exist)
    let count = run_query_count(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (a:Person {name: 'P1'}) MATCH (a)((x)-[:PARTNER]->(y)){1,3}(b) RETURN b.name",
    )
    .await?;
    assert_eq!(count, 0, "No PARTNER edges from Person, should be empty");

    Ok(())
}

#[tokio::test]
async fn test_qpp_zero_iterations() -> anyhow::Result<()> {
    // QPP with {0,2} should include the source as a zero-length result
    let (_storage, _writer, executor, prop_manager, planner) = setup_qpp_graph().await?;
    let params = HashMap::new();

    let setup = r#"
        CREATE (p1:Person {name: 'P1'})
        CREATE (p2:Person {name: 'P2'})
        CREATE (p1)-[:LINK]->(p2)
    "#;
    let ast = uni_query::parse_cypher(setup)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // QPP: ((a)-[:LINK]->(b)){0,2} from P1
    // 0 iterations: P1 (source = target)
    // 1 iteration: P2
    let count = run_query_count(
        &executor,
        &planner,
        &prop_manager,
        "MATCH (a:Person {name: 'P1'}) MATCH (a)((x)-[:LINK]->(y)){0,2}(b) RETURN b.name",
    )
    .await?;
    assert_eq!(
        count, 2,
        "{{0,2}} should find P1 (zero-length) and P2 (1 hop)"
    );

    Ok(())
}
