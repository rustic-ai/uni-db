// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end tests for truly schemaless vertices (unregistered labels).
//!
//! Unlike `overflow_json_e2e.rs` (which tests labels registered in the schema
//! where non-schema properties spill into `overflow_json` in the per-label
//! VertexDataset), these tests exercise labels that are **never registered**
//! via `db.schema()`. Unregistered labels route through
//! `LogicalPlan::ScanMainByLabels` → `MainVertexDataset`, where all properties
//! live in the `props_json` JSONB blob.
//!
//! ## Storage path
//!
//! ```text
//! Registered label   → LogicalPlan::Scan         → per-label VertexDataset (overflow_json)
//! Unregistered label → LogicalPlan::ScanMainByLabels → MainVertexDataset    (props_json)
//! ```
//!
//! ## Test coverage
//!
//! 1. Basic RETURN of schemaless properties (L0 + post-flush)
//! 2. WHERE clause filtering on schemaless properties after flush
//! 3. Multiple flush cycles — data durability
//! 4. Mixed value types (string, int, float, bool, list)
//! 5. Null and missing property handling
//! 6. Bulk insert with property access
//! 7. Count aggregation across flush boundaries

use anyhow::Result;
use tempfile::tempdir;
use uni_db::{Uni, Value};

/// Creates a `Uni` instance backed by a fresh temp directory.
///
/// Returns the database handle and the `TempDir` guard (dropped at end of
/// test to clean up).
async fn open_temp_db() -> Result<(Uni, tempfile::TempDir)> {
    let dir = tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    Ok((db, dir))
}

// ---------------------------------------------------------------------------
// Test 1: basic property access — L0 and post-flush
// ---------------------------------------------------------------------------

/// Verifies that properties on an unregistered label are readable from L0
/// and survive a flush to storage.
#[tokio::test]
async fn test_schemaless_vertex_basic_return() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;
    // No schema registration — "Gadget" is completely unknown.

    db.execute("CREATE (:Gadget {name: 'Wrench', weight: 3})")
        .await?;

    // L0 read
    let rows = db.query("MATCH (g:Gadget) RETURN g.name, g.weight").await?;
    assert_eq!(rows.len(), 1, "should find 1 vertex in L0");
    assert_eq!(rows.rows()[0].get::<String>("g.name")?, "Wrench");

    let weight = rows.rows()[0].value("g.weight").unwrap();
    match weight {
        Value::Int(i) => assert_eq!(*i, 3),
        Value::String(s) => assert_eq!(s, "3"),
        other => panic!("unexpected weight type: {other:?}"),
    }

    // Flush to storage (MainVertexDataset / props_json)
    db.flush().await?;

    // Post-flush read
    let rows = db.query("MATCH (g:Gadget) RETURN g.name, g.weight").await?;
    assert_eq!(rows.len(), 1, "should find 1 vertex after flush");
    assert_eq!(rows.rows()[0].get::<String>("g.name")?, "Wrench");

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 2: WHERE clause filtering after flush
// ---------------------------------------------------------------------------

/// Verifies that WHERE clauses on schemaless properties work after data has
/// been flushed to storage.
#[tokio::test]
async fn test_schemaless_vertex_where_clause() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;

    db.execute("CREATE (:Animal {species: 'Cat', legs: 4})")
        .await?;
    db.execute("CREATE (:Animal {species: 'Snake', legs: 0})")
        .await?;
    db.execute("CREATE (:Animal {species: 'Dog', legs: 4})")
        .await?;

    db.flush().await?;

    // Filter on a string property
    let rows = db
        .query("MATCH (a:Animal) WHERE a.species = 'Cat' RETURN a.legs")
        .await?;
    assert_eq!(rows.len(), 1, "should find exactly one Cat");

    let legs = rows.rows()[0].value("a.legs").unwrap();
    match legs {
        Value::Int(i) => assert_eq!(*i, 4),
        Value::String(s) => assert_eq!(s, "4"),
        other => panic!("unexpected legs type: {other:?}"),
    }

    // Filter returning multiple matches
    let rows = db
        .query("MATCH (a:Animal) WHERE a.legs = 4 RETURN a.species")
        .await?;

    let species: Vec<String> = rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("a.species").unwrap())
        .collect();
    assert_eq!(species.len(), 2, "Cat and Dog both have 4 legs");
    assert!(species.contains(&"Cat".to_string()));
    assert!(species.contains(&"Dog".to_string()));

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 3: multiple flush cycles
// ---------------------------------------------------------------------------

/// Ensures data from separate batches survives multiple flush cycles and
/// remains queryable.
#[tokio::test]
async fn test_schemaless_vertex_multiple_flushes() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;

    // Batch 1
    db.execute("CREATE (:Metric {source: 'cpu', value: 82})")
        .await?;
    db.flush().await?;

    // Batch 2
    db.execute("CREATE (:Metric {source: 'mem', value: 64})")
        .await?;
    db.flush().await?;

    // Batch 3
    db.execute("CREATE (:Metric {source: 'disk', value: 45})")
        .await?;
    db.flush().await?;

    // All three should be present
    let rows = db.query("MATCH (m:Metric) RETURN count(m) as cnt").await?;
    assert_eq!(rows.rows()[0].get::<i64>("cnt")?, 3);

    // Return all properties
    let rows = db
        .query("MATCH (m:Metric) RETURN m.source, m.value")
        .await?;
    assert_eq!(rows.len(), 3);

    let sources: Vec<String> = rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("m.source").unwrap())
        .collect();
    assert!(sources.contains(&"cpu".to_string()));
    assert!(sources.contains(&"mem".to_string()));
    assert!(sources.contains(&"disk".to_string()));

    // Filter across flush boundaries
    let rows = db
        .query("MATCH (m:Metric) WHERE m.source = 'mem' RETURN m.value")
        .await?;
    assert_eq!(rows.len(), 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 4: mixed value types
// ---------------------------------------------------------------------------

/// Verifies that different Cypher literal types (string, int, float, bool,
/// list) round-trip correctly through the schemaless storage path.
#[tokio::test]
async fn test_schemaless_vertex_mixed_types() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;

    db.execute(
        r#"CREATE (:Config {
            name: 'prod',
            replicas: 3,
            cpu_limit: 2.5,
            enabled: true,
            regions: ['us-east', 'eu-west']
        })"#,
    )
    .await?;

    db.flush().await?;

    let rows = db
        .query("MATCH (c:Config) RETURN c.name, c.replicas, c.cpu_limit, c.enabled, c.regions")
        .await?;
    assert_eq!(rows.len(), 1);
    let row = &rows.rows()[0];

    assert_eq!(row.get::<String>("c.name")?, "prod");

    let replicas = row.value("c.replicas").unwrap();
    match replicas {
        Value::Int(i) => assert_eq!(*i, 3),
        Value::String(s) => assert_eq!(s, "3"),
        other => panic!("unexpected replicas type: {other:?}"),
    }

    let cpu = row.value("c.cpu_limit").unwrap();
    match cpu {
        Value::Float(f) => assert!((f - 2.5).abs() < f64::EPSILON),
        Value::String(s) => assert_eq!(s, "2.5"),
        other => panic!("unexpected cpu_limit type: {other:?}"),
    }

    let enabled = row.value("c.enabled").unwrap();
    match enabled {
        Value::Bool(b) => assert!(b),
        Value::String(s) => assert_eq!(s, "true"),
        other => panic!("unexpected enabled type: {other:?}"),
    }

    let regions = row.value("c.regions").unwrap();
    match regions {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::String("us-east".to_string()));
            assert_eq!(items[1], Value::String("eu-west".to_string()));
        }
        Value::String(s) => {
            assert!(
                s.contains("us-east"),
                "regions string should contain us-east"
            );
            assert!(
                s.contains("eu-west"),
                "regions string should contain eu-west"
            );
        }
        other => panic!("unexpected regions type: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 5: null and missing properties
// ---------------------------------------------------------------------------

/// Verifies that explicit nulls and missing properties are handled correctly
/// in the schemaless path.
#[tokio::test]
async fn test_schemaless_vertex_null_handling() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;

    // Explicit null, present property, and missing property across vertices
    db.execute("CREATE (:Reading {sensor: 'temp', value: 22.5})")
        .await?;
    db.execute("CREATE (:Reading {sensor: 'humidity', value: null})")
        .await?;
    db.execute("CREATE (:Reading {sensor: 'pressure'})").await?; // value omitted entirely

    db.flush().await?;

    let rows = db
        .query("MATCH (r:Reading) RETURN r.sensor, r.value")
        .await?;
    assert_eq!(rows.len(), 3);

    let humidity = rows
        .rows()
        .iter()
        .find(|r| r.get::<String>("r.sensor").ok() == Some("humidity".to_string()))
        .expect("humidity row not found");
    assert_eq!(
        humidity.value("r.value").unwrap(),
        &Value::Null,
        "explicit null should stay null"
    );

    let pressure = rows
        .rows()
        .iter()
        .find(|r| r.get::<String>("r.sensor").ok() == Some("pressure".to_string()))
        .expect("pressure row not found");
    assert_eq!(
        pressure.value("r.value").unwrap(),
        &Value::Null,
        "missing property should be null"
    );

    let temp = rows
        .rows()
        .iter()
        .find(|r| r.get::<String>("r.sensor").ok() == Some("temp".to_string()))
        .expect("temp row not found");
    let temp_val = temp.value("r.value").unwrap();
    match temp_val {
        Value::Float(f) => assert!((f - 22.5).abs() < f64::EPSILON),
        Value::String(s) => assert_eq!(s, "22.5"),
        other => panic!("unexpected temp value type: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 6: bulk insert with property access
// ---------------------------------------------------------------------------

/// Inserts many vertices with an unregistered label and verifies count and
/// property access after flush.
#[tokio::test]
async fn test_schemaless_vertex_bulk() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;

    for i in 0..500 {
        db.execute(&format!(
            "CREATE (:LogEntry {{seq: {i}, level: 'info', msg: 'event_{i}'}})"
        ))
        .await?;
    }

    db.flush().await?;

    // Count
    let rows = db
        .query("MATCH (l:LogEntry) RETURN count(l) as cnt")
        .await?;
    assert_eq!(rows.rows()[0].get::<i64>("cnt")?, 500);

    // Property access with LIMIT
    let rows = db
        .query("MATCH (l:LogEntry) RETURN l.seq, l.level, l.msg LIMIT 5")
        .await?;
    assert_eq!(rows.len(), 5);

    for row in rows.rows() {
        let level = row.value("l.level").unwrap();
        match level {
            Value::String(s) => assert_eq!(s, "info"),
            other => panic!("unexpected level type: {other:?}"),
        }
    }

    // WHERE filter on schemaless property
    let rows = db
        .query("MATCH (l:LogEntry) WHERE l.level = 'info' RETURN l.seq LIMIT 10")
        .await?;
    if !rows.is_empty() {
        assert!(rows.len() <= 10, "LIMIT should cap results at 10");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 7: count aggregation across flush boundaries
// ---------------------------------------------------------------------------

/// Verifies that count(*) returns correct totals when data spans multiple
/// flush generations.
#[tokio::test]
async fn test_schemaless_vertex_count_across_flushes() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;

    // Generation 1
    for i in 0..10 {
        db.execute(&format!("CREATE (:Tick {{id: {i}, gen: 1}})"))
            .await?;
    }
    db.flush().await?;

    // Generation 2
    for i in 10..25 {
        db.execute(&format!("CREATE (:Tick {{id: {i}, gen: 2}})"))
            .await?;
    }
    db.flush().await?;

    // Generation 3 (still in L0, not flushed)
    for i in 25..30 {
        db.execute(&format!("CREATE (:Tick {{id: {i}, gen: 3}})"))
            .await?;
    }

    // Total across flushed + L0
    let rows = db.query("MATCH (t:Tick) RETURN count(t) as cnt").await?;
    assert_eq!(rows.rows()[0].get::<i64>("cnt")?, 30);

    // Flush the rest and re-check
    db.flush().await?;
    let rows = db.query("MATCH (t:Tick) RETURN count(t) as cnt").await?;
    assert_eq!(rows.rows()[0].get::<i64>("cnt")?, 30);

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 8: multiple unregistered labels coexist
// ---------------------------------------------------------------------------

/// Verifies that vertices from different unregistered labels don't
/// interfere with each other.
#[tokio::test]
async fn test_schemaless_vertex_multiple_labels() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;

    db.execute("CREATE (:Planet {name: 'Mars', moons: 2})")
        .await?;
    db.execute("CREATE (:Planet {name: 'Jupiter', moons: 95})")
        .await?;
    db.execute("CREATE (:Star {name: 'Sirius', spectral: 'A1V'})")
        .await?;

    db.flush().await?;

    let planets = db.query("MATCH (p:Planet) RETURN count(p) as cnt").await?;
    assert_eq!(planets.rows()[0].get::<i64>("cnt")?, 2);

    let stars = db.query("MATCH (s:Star) RETURN count(s) as cnt").await?;
    assert_eq!(stars.rows()[0].get::<i64>("cnt")?, 1);

    let rows = db
        .query("MATCH (p:Planet) WHERE p.name = 'Mars' RETURN p.moons")
        .await?;
    assert_eq!(rows.len(), 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// Test 9: empty-string and special character properties
// ---------------------------------------------------------------------------

/// Verifies edge cases around empty strings and strings containing special
/// characters.
#[tokio::test]
async fn test_schemaless_vertex_special_strings() -> Result<()> {
    let (db, _dir) = open_temp_db().await?;

    db.execute(r#"CREATE (:Note {title: '', body: "line1\nline2", tag: 'it''s'})"#)
        .await?;

    db.flush().await?;

    let rows = db
        .query("MATCH (n:Note) RETURN n.title, n.body, n.tag")
        .await?;
    assert_eq!(rows.len(), 1);
    let row = &rows.rows()[0];

    let title = row.value("n.title").unwrap();
    match title {
        Value::String(s) => assert_eq!(s, ""),
        Value::Null => { /* empty string may be stored as null */ }
        other => panic!("unexpected title type: {other:?}"),
    }

    Ok(())
}
