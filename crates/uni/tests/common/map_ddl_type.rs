// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end tests for parameterized `MAP<K,V>` property types declared via Cypher DDL
//! (issue #105). Storage already round-trips `Map(String, {String|Int64|Bytes})`; these
//! cover the new DDL grammar + parsers (`MAP<…>` on the `CREATE LABEL` path and the
//! `uni.schema.createLabel` procedure path), the extended `build_map_column` value types
//! (scalars typed, nested values via a CypherValue fallback child), STRING-key enforcement,
//! the parser/writer boundary (`LIST<MAP<…>>` rejected, not panicked), and reopen
//! persistence. Keys are always STRING (`Value::Map` is string-keyed).

use std::collections::HashMap;
use uni_db::Uni;

/// Create a temp DB and declare `Doc` via a DDL `CREATE LABEL` statement.
async fn db_with_ddl(decl: &str) -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(decl).await?;
    tx.commit().await?;
    Ok(db)
}

async fn insert(db: &Uni, cypher: &str) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

async fn read_attrs_json(db: &Uni) -> anyhow::Result<serde_json::Value> {
    let res = db
        .session()
        .query("MATCH (d:Doc) RETURN d.attrs AS m")
        .await?;
    Ok(res.rows()[0].value("m").unwrap().clone().into())
}

// ---------------------------------------------------------------------------
// Scalar value types (typed columns)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn map_string_float_roundtrips() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (name STRING, attrs MAP<STRING, FLOAT>)").await?;
    insert(&db, "CREATE (:Doc {name:'a', attrs: {x: 1.5, y: 2.25}})").await?;
    let m: HashMap<String, f64> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("x"), Some(&1.5));
    assert_eq!(m.get("y"), Some(&2.25));
    Ok(())
}

#[tokio::test]
async fn map_string_double_roundtrips() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (attrs MAP<STRING, DOUBLE>)").await?;
    insert(&db, "CREATE (:Doc {attrs: {p: 1.234567890123456}})").await?;
    let m: HashMap<String, f64> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("p"), Some(&1.234567890123456));
    Ok(())
}

#[tokio::test]
async fn map_string_int_roundtrips() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (attrs MAP<STRING, INT>)").await?;
    insert(&db, "CREATE (:Doc {attrs: {a: 10, b: -20}})").await?;
    let m: HashMap<String, i64> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("a"), Some(&10));
    assert_eq!(m.get("b"), Some(&-20));
    Ok(())
}

#[tokio::test]
async fn map_string_bool_roundtrips() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (attrs MAP<STRING, BOOL>)").await?;
    insert(&db, "CREATE (:Doc {attrs: {t: true, f: false}})").await?;
    let m: HashMap<String, bool> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("t"), Some(&true));
    assert_eq!(m.get("f"), Some(&false));
    Ok(())
}

#[tokio::test]
async fn map_string_string_roundtrips() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (attrs MAP<STRING, STRING>)").await?;
    insert(&db, "CREATE (:Doc {attrs: {k: 'v', k2: 'w'}})").await?;
    let m: HashMap<String, String> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("k"), Some(&"v".to_string()));
    Ok(())
}

// ---------------------------------------------------------------------------
// Nested value types (CypherValue fallback child)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn map_string_list_int_roundtrips() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (attrs MAP<STRING, LIST<INT>>)").await?;
    insert(&db, "CREATE (:Doc {attrs: {a: [1, 2, 3], b: [4]}})").await?;
    let m: HashMap<String, Vec<i64>> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("a"), Some(&vec![1, 2, 3]));
    assert_eq!(m.get("b"), Some(&vec![4]));
    Ok(())
}

#[tokio::test]
async fn map_string_nested_map_roundtrips() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (attrs MAP<STRING, MAP<STRING, INT>>)").await?;
    insert(&db, "CREATE (:Doc {attrs: {a: {x: 1, y: 2}}})").await?;
    let m: HashMap<String, HashMap<String, i64>> =
        serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("a").and_then(|inner| inner.get("x")), Some(&1));
    assert_eq!(m.get("a").and_then(|inner| inner.get("y")), Some(&2));
    Ok(())
}

#[tokio::test]
async fn map_string_vector_roundtrips() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (attrs MAP<STRING, VECTOR(2)>)").await?;
    insert(&db, "CREATE (:Doc {attrs: {a: [1.0, 2.0]}})").await?;
    let m: HashMap<String, Vec<f64>> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("a"), Some(&vec![1.0, 2.0]));
    Ok(())
}

// ---------------------------------------------------------------------------
// Edge cases + persistence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn map_empty_and_absent() -> anyhow::Result<()> {
    let db = db_with_ddl("CREATE LABEL Doc (name STRING, attrs MAP<STRING, INT>)").await?;
    insert(&db, "CREATE (:Doc {name:'empty', attrs: {}})").await?;
    let m: HashMap<String, i64> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert!(m.is_empty(), "empty map should round-trip empty: {m:?}");
    Ok(())
}

#[tokio::test]
async fn map_persists_across_reopen() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    {
        let db = Uni::open(path).build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL Doc (attrs MAP<STRING, FLOAT>)")
            .await?;
        tx.commit().await?;
        insert(&db, "CREATE (:Doc {attrs: {x: 9.5}})").await?;
        drop(db);
    }
    let db = Uni::open(path).build().await?;
    let m: HashMap<String, f64> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("x"), Some(&9.5), "map must survive reopen");
    Ok(())
}

// ---------------------------------------------------------------------------
// Boundary / negative behavior
// ---------------------------------------------------------------------------

/// Non-STRING keys are rejected at DDL parse time (fail fast at CREATE LABEL).
#[tokio::test]
async fn map_non_string_key_rejected() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    let res = tx
        .execute("CREATE LABEL Bad (attrs MAP<INT, STRING>)")
        .await;
    assert!(res.is_err(), "MAP<INT,STRING> should be rejected");
    let msg = format!("{:#}", res.unwrap_err());
    assert!(
        msg.to_uppercase().contains("MAP KEY") || msg.to_uppercase().contains("STRING"),
        "error should explain the STRING-key constraint: {msg}"
    );
    Ok(())
}

/// `LIST<MAP<…>>` round-trips, before and after the flush and after a reopen.
///
/// The parser accepted it while the List writer could not store a Map
/// element, so the write used to fail at the flush (this test asserted that
/// it failed cleanly rather than panicking). The writer now builds any element
/// type with the scalar builder for it, which closes the asymmetry.
#[tokio::test]
async fn list_of_map_round_trips() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let expected = serde_json::json!([{"a": 1}, {"b": 2, "c": 3}]);
    {
        let db = Uni::open(&uri).build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL Doc (attrs LIST<MAP<STRING, INT>>)")
            .await?;
        tx.execute("CREATE (:Doc {attrs: [{a: 1}, {b: 2, c: 3}]})")
            .await?;
        tx.commit().await?;
        assert_eq!(read_attrs_json(&db).await?, expected, "unflushed");
        db.flush().await?;
        assert_eq!(read_attrs_json(&db).await?, expected, "flushed");
        db.shutdown().await?;
    }
    let db = Uni::open(&uri).build().await?;
    assert_eq!(read_attrs_json(&db).await?, expected, "reopened");
    Ok(())
}

// ---------------------------------------------------------------------------
// Cross-surface: the uni.schema.createLabel procedure path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn map_via_create_label_procedure() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    db.session()
        .run(
            r#"CALL uni.schema.createLabel('Doc', {
                "properties": { "attrs": { "type": "MAP<STRING, FLOAT>" } }
            })"#,
        )
        .await?;
    insert(&db, "CREATE (:Doc {attrs: {x: 4.5}})").await?;
    let m: HashMap<String, f64> = serde_json::from_value(read_attrs_json(&db).await?)?;
    assert_eq!(m.get("x"), Some(&4.5), "procedure-path MAP must round-trip");
    Ok(())
}
