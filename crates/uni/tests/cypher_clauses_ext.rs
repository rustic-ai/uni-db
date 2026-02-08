// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
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

/// Shared test harness for Cypher clause tests that need a Person schema with name+age.
struct PersonTestHarness {
    executor: Executor,
    planner: QueryPlanner,
    prop_mgr: PropertyManager,
    writer: Arc<RwLock<Writer>>,
}

impl PersonTestHarness {
    /// Create a test harness with a Person label having `name` (String) and `age` (Int32).
    async fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
        schema_manager.add_label("Person")?;
        schema_manager.add_property("Person", "name", DataType::String, false)?;
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

        Ok(Self {
            executor,
            planner,
            prop_mgr,
            writer,
        })
    }

    /// Insert Person vertices from (name, age) pairs and flush to L1.
    async fn insert_people(&self, people: &[(&str, i32)]) -> anyhow::Result<()> {
        let mut w = self.writer.write().await;
        for (name, age) in people {
            let vid = w.next_vid().await?;
            let mut props = HashMap::new();
            props.insert("name".to_string(), json!(name).into());
            props.insert("age".to_string(), json!(age).into());
            w.insert_vertex_with_labels(vid, props, vec!["Person".to_string()])
                .await?;
        }
        w.flush_to_l1(None).await?;
        Ok(())
    }

    /// Parse, plan, and execute a Cypher query, returning the result rows.
    async fn run_query(
        &self,
        cypher: &str,
    ) -> anyhow::Result<Vec<HashMap<String, serde_json::Value>>> {
        let query = uni_query::parse_cypher(cypher)?;
        let plan = self.planner.plan(query)?;
        self.executor
            .execute(plan, &self.prop_mgr, &HashMap::new())
            .await
    }
}

#[tokio::test]
async fn test_cypher_unwind() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );
    let executor = Executor::new(storage.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let sql = "UNWIND [1, 2, 3] AS x RETURN x";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("x"), Some(&json!(1)));
    assert_eq!(results[1].get("x"), Some(&json!(2)));
    assert_eq!(results[2].get("x"), Some(&json!(3)));

    Ok(())
}

#[tokio::test]
async fn test_cypher_set_remove() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let h = PersonTestHarness::new(temp_dir.path()).await?;

    // 1. CREATE with properties
    h.run_query("CREATE (n:Person {name: 'Alice', age: 30})")
        .await?;

    let vid;
    {
        let w = h.writer.read().await;
        let l0 = w.l0_manager.get_current();
        let l0 = l0.read();
        assert_eq!(l0.vertex_properties.len(), 1);
        vid = *l0.vertex_properties.keys().next().unwrap();
        let props = &l0.vertex_properties[&vid];
        assert_eq!(props.get("name"), Some(&json!("Alice").into()));
        assert_eq!(props.get("age"), Some(&json!(30).into()));
    }

    // 2. SET property — flush first so MATCH can find it
    {
        let mut w = h.writer.write().await;
        w.flush_to_l1(None).await?;
    }

    h.run_query("MATCH (n:Person) SET n.age = 31").await?;

    {
        let w = h.writer.read().await;
        let l0 = w.l0_manager.get_current();
        let l0 = l0.read();
        let props = &l0.vertex_properties[&vid];
        assert_eq!(props.get("age"), Some(&json!(31).into()));
        assert_eq!(
            props.get("name"),
            Some(&json!("Alice").into()),
            "Name should be preserved"
        );
    }

    // 3. REMOVE property
    h.run_query("MATCH (n:Person) REMOVE n.age").await?;

    {
        let w = h.writer.read().await;
        let l0 = w.l0_manager.get_current();
        let l0 = l0.read();
        let props = &l0.vertex_properties[&vid];
        assert_eq!(props.get("age"), Some(&json!(null).into()));
    }

    Ok(())
}

#[tokio::test]
async fn test_cypher_with() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
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

    // Create a node (name-only schema, no age property)
    {
        let mut w = writer.write().await;
        let vid = w.next_vid().await?;
        let mut props = HashMap::new();
        props.insert("name".to_string(), json!("Alice").into());
        w.insert_vertex_with_labels(vid, props, vec!["Person".to_string()])
            .await?;
        w.flush_to_l1(None).await?;
    }

    let sql = "MATCH (n:Person) WITH n RETURN n.name";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor.execute(plan, &prop_mgr, &HashMap::new()).await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("n.name"), Some(&json!("Alice")));

    Ok(())
}

#[tokio::test]
async fn test_with_aggregation_where() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let h = PersonTestHarness::new(temp_dir.path()).await?;

    h.insert_people(&[("Alice", 30), ("Bob", 25), ("Charlie", 35), ("Diana", 25)])
        .await?;

    let sql = "MATCH (p:Person) WITH p.age AS age, count(p) AS cnt WHERE age > 28 RETURN age, cnt ORDER BY age";
    let results = h.run_query(sql).await?;

    // age=30 (count=1), age=35 (count=1)
    assert_eq!(
        results.len(),
        2,
        "Expected 2 groups with age > 28, got {:?}",
        results
    );
    assert_eq!(results[0].get("age"), Some(&json!(30)));
    assert_eq!(results[0].get("cnt"), Some(&json!(1)));
    assert_eq!(results[1].get("age"), Some(&json!(35)));
    assert_eq!(results[1].get("cnt"), Some(&json!(1)));

    Ok(())
}

#[tokio::test]
async fn test_with_aggregation_return() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let h = PersonTestHarness::new(temp_dir.path()).await?;

    h.insert_people(&[("Alice", 30), ("Bob", 25), ("Charlie", 30)])
        .await?;

    let sql = "MATCH (p:Person) WITH p.age AS age, count(p) AS cnt RETURN age, cnt ORDER BY age";
    let results = h.run_query(sql).await?;

    // age=25 (count=1), age=30 (count=2)
    assert_eq!(results.len(), 2, "Expected 2 groups, got {:?}", results);
    assert_eq!(results[0].get("age"), Some(&json!(25)));
    assert_eq!(results[0].get("cnt"), Some(&json!(1)));
    assert_eq!(results[1].get("age"), Some(&json!(30)));
    assert_eq!(results[1].get("cnt"), Some(&json!(2)));

    Ok(())
}

#[tokio::test]
async fn test_return_distinct() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let h = PersonTestHarness::new(temp_dir.path()).await?;

    h.insert_people(&[("Alice", 30), ("Bob", 25), ("Charlie", 30), ("Diana", 25)])
        .await?;

    let sql = "MATCH (n:Person) RETURN DISTINCT n.age AS age ORDER BY age";
    let results = h.run_query(sql).await?;

    assert_eq!(
        results.len(),
        2,
        "Expected 2 distinct ages, got {:?}",
        results
    );
    assert_eq!(results[0].get("age"), Some(&json!(25)));
    assert_eq!(results[1].get("age"), Some(&json!(30)));

    Ok(())
}

#[tokio::test]
async fn test_return_distinct_multiple_columns() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let h = PersonTestHarness::new(temp_dir.path()).await?;

    // (Alice, 30), (Bob, 25), (Alice2, 30) -- two share age=30 but names differ
    h.insert_people(&[("Alice", 30), ("Bob", 25), ("Alice2", 30)])
        .await?;

    let sql = "MATCH (n:Person) RETURN DISTINCT n.name AS name, n.age AS age ORDER BY name";
    let results = h.run_query(sql).await?;

    assert_eq!(
        results.len(),
        3,
        "All 3 rows are distinct by (name, age), got {:?}",
        results
    );

    Ok(())
}

#[tokio::test]
async fn test_with_distinct() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let h = PersonTestHarness::new(temp_dir.path()).await?;

    h.insert_people(&[("Alice", 30), ("Bob", 25), ("Charlie", 30), ("Diana", 25)])
        .await?;

    let sql = "MATCH (n:Person) WITH DISTINCT n.age AS age RETURN age ORDER BY age";
    let results = h.run_query(sql).await?;

    assert_eq!(
        results.len(),
        2,
        "Expected 2 distinct ages via WITH DISTINCT, got {:?}",
        results
    );
    assert_eq!(results[0].get("age"), Some(&json!(25)));
    assert_eq!(results[1].get("age"), Some(&json!(30)));

    Ok(())
}
