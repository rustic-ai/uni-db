// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_ddl_execution() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. CREATE LABEL
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING NOT NULL, age INT UNIQUE)")
        .await?;
    tx.commit().await?;

    let schema = db.schema().current();
    assert!(schema.labels.contains_key("Person"));
    let person_props = schema.properties.get("Person").unwrap();
    assert!(person_props.contains_key("name"));
    assert!(person_props.contains_key("age"));
    assert!(!person_props.get("name").unwrap().nullable);

    // Check auto-created unique constraint for age
    assert!(
        schema
            .constraints
            .iter()
            .any(|c| c.name == "Person_age_unique")
    );

    // 2. CREATE EDGE TYPE
    let tx = db.session().tx().await?;
    tx.execute("CREATE EDGE TYPE FOLLOWS (since STRING) FROM Person TO Person")
        .await?;
    tx.commit().await?;

    let schema = db.schema().current();
    assert!(schema.edge_types.contains_key("FOLLOWS"));
    let follows_meta = schema.edge_types.get("FOLLOWS").unwrap();
    assert_eq!(follows_meta.src_labels, &["Person".to_string()]);

    // 3. ALTER LABEL
    let tx = db.session().tx().await?;
    tx.execute("ALTER LABEL Person ADD PROPERTY bio STRING")
        .await?;
    tx.commit().await?;
    let schema = db.schema().current();
    assert!(schema.properties.get("Person").unwrap().contains_key("bio"));

    // 4. CREATE CONSTRAINT
    let tx = db.session().tx().await?;
    tx.execute("CREATE CONSTRAINT name_unique ON (p:Person) ASSERT p.name IS UNIQUE")
        .await?;
    tx.commit().await?;
    let schema = db.schema().current();
    assert!(schema.constraints.iter().any(|c| c.name == "name_unique"));

    // 5. SHOW CONSTRAINTS
    let result = db.session().query("SHOW CONSTRAINTS").await?;
    // Person_age_unique and name_unique
    assert_eq!(result.len(), 2);

    // 6. DROP LABEL
    let tx = db.session().tx().await?;
    tx.execute("DROP LABEL Person").await?;
    tx.commit().await?;
    let schema = db.schema().current();
    assert!(schema.labels.contains_key("Person"));
    assert!(matches!(
        schema.labels.get("Person").unwrap().state,
        uni_common::core::schema::SchemaElementState::Tombstone { .. }
    ));
    // Properties are kept for historical snapshots but should be considered part of tombstoned label
    assert!(schema.properties.contains_key("Person"));

    Ok(())
}

/// The single value `query` returns in column `v`.
async fn scalar(db: &Uni, query: &str) -> Result<uni_db::Value> {
    let rows = db.session().query(query).await?;
    assert_eq!(rows.len(), 1, "{query}: expected one row");
    Ok(rows.rows()[0]
        .value("v")
        .cloned()
        .unwrap_or(uni_db::Value::Null))
}

/// `DEFAULT` in `CREATE LABEL` fills an absent property on every insert path:
/// Cypher CREATE (and the bound node it returns), MERGE, and bulk ingest. An
/// explicit `NULL` is kept as NULL: the default stands in only for a property
/// the write does not name.
#[tokio::test]
async fn test_create_label_default_applies_on_insert() -> Result<()> {
    use std::collections::HashMap;
    use uni_db::Value;
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE LABEL Item (name STRING, qty INT DEFAULT 1, \
         tag STRING NOT NULL DEFAULT 'x', score FLOAT DEFAULT 2, \
         tags LIST<STRING> DEFAULT ['a', 'b'], born DATE DEFAULT date('2020-01-02'))",
    )
    .await?;
    tx.commit().await?;

    let meta = db.schema().current();
    let qty = &meta.properties["Item"]["qty"];
    assert_eq!(qty.default, Some(Value::Int(1)));
    // Stored in the declared form: an INT literal for a FLOAT column is a float.
    assert_eq!(
        meta.properties["Item"]["score"].default,
        Some(Value::Float(2.0))
    );
    assert_eq!(meta.properties["Item"]["name"].default, None);

    let tx = db.session().tx().await?;
    let created = tx
        .query("CREATE (n:Item {name: 'a'}) RETURN n.qty AS q, n.tag AS t")
        .await?;
    assert_eq!(created.rows()[0].value("q"), Some(&Value::Int(1)));
    assert_eq!(
        created.rows()[0].value("t"),
        Some(&Value::String("x".into()))
    );
    tx.execute("CREATE (:Item {name: 'b', qty: 7})").await?;
    tx.execute("CREATE (:Item {name: 'n', qty: null})").await?;
    tx.execute("MERGE (:Item {name: 'm'})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices(
        "Item",
        vec![HashMap::from([(
            "name".to_string(),
            Value::String("bulk".into()),
        )])],
    )
    .await?;
    bulk.commit().await?;
    tx.commit().await?;

    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        for (name, want) in [
            ("a", Value::Int(1)),
            ("b", Value::Int(7)),
            ("n", Value::Null),
            ("m", Value::Int(1)),
            ("bulk", Value::Int(1)),
        ] {
            let got = scalar(
                &db,
                &format!("MATCH (n:Item {{name: '{name}'}}) RETURN n.qty AS v"),
            )
            .await?;
            assert_eq!(got, want, "qty of {name} (flushed: {flush})");
        }
        assert_eq!(
            scalar(&db, "MATCH (n:Item {name: 'bulk'}) RETURN n.tag AS v").await?,
            Value::String("x".into())
        );
        assert_eq!(
            scalar(&db, "MATCH (n:Item {name: 'a'}) RETURN n.score AS v").await?,
            Value::Float(2.0)
        );
        assert_eq!(
            scalar(&db, "MATCH (n:Item {name: 'a'}) RETURN size(n.tags) AS v").await?,
            Value::Int(2)
        );
        assert_eq!(
            scalar(
                &db,
                "MATCH (n:Item {name: 'a'}) RETURN toString(n.born) AS v"
            )
            .await?,
            Value::String("2020-01-02".into())
        );
    }

    // A NOT NULL property with a default is still refused when written NULL.
    let tx = db.session().tx().await?;
    assert!(
        tx.execute("CREATE (:Item {name: 'z', tag: null})")
            .await
            .is_err()
    );
    Ok(())
}

/// `DEFAULT` on an edge type and on `ALTER ... ADD PROPERTY`. The added
/// property's default fills new writes only: existing rows are not backfilled.
#[tokio::test]
async fn test_default_on_edge_type_and_alter() -> Result<()> {
    use uni_db::Value;
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE R (w FLOAT DEFAULT 0.5, note STRING) FROM P TO P")
        .await?;
    tx.execute("CREATE (:P {name: 'a'})-[:R]->(:P {name: 'b'})")
        .await?;
    let bound = tx
        .query("MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) CREATE (a)-[r:R {note: 'n'}]->(b) RETURN r.w AS w")
        .await?;
    assert_eq!(bound.rows()[0].value("w"), Some(&Value::Float(0.5)));
    tx.commit().await?;
    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        let rows = db
            .session()
            .query("MATCH ()-[r:R]->() RETURN r.w AS w")
            .await?;
        assert_eq!(rows.len(), 2);
        for row in rows.rows() {
            assert_eq!(row.value("w"), Some(&Value::Float(0.5)), "flushed: {flush}");
        }
    }

    let tx = db.session().tx().await?;
    tx.execute("ALTER LABEL P ADD PROPERTY color STRING DEFAULT 'red'")
        .await?;
    tx.execute("CREATE (:P {name: 'c'})").await?;
    tx.commit().await?;
    assert_eq!(
        scalar(&db, "MATCH (n:P {name: 'a'}) RETURN n.color AS v").await?,
        Value::Null,
        "no backfill of existing rows"
    );
    assert_eq!(
        scalar(&db, "MATCH (n:P {name: 'c'}) RETURN n.color AS v").await?,
        Value::String("red".into())
    );
    Ok(())
}

/// A default survives a reopen, and a non-constant or ill-typed DEFAULT is
/// refused at DDL time without changing the schema.
#[tokio::test]
async fn test_default_persists_and_rejects_non_constant() -> Result<()> {
    use uni_db::Value;
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(path.clone()).build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL Doc (title STRING, rank INT DEFAULT 3 + 4)")
            .await?;
        tx.commit().await?;
        for bad in [
            "CREATE LABEL Bad1 (x FLOAT DEFAULT rand())",
            "CREATE LABEL Bad2 (x INT DEFAULT $p)",
            "CREATE LABEL Bad3 (x INT DEFAULT 'abc')",
            "CREATE LABEL Bad4 (x DATETIME DEFAULT datetime())",
            "CREATE LABEL Bad5 (x INT DEFAULT n.y)",
        ] {
            let tx = db.session().tx().await?;
            let err = tx.execute(bad).await;
            assert!(err.is_err(), "{bad} must be refused");
            drop(tx);
        }
        let schema = db.schema().current();
        for name in ["Bad1", "Bad2", "Bad3", "Bad4", "Bad5"] {
            assert!(
                !schema.labels.contains_key(name),
                "{name} must not be declared"
            );
        }
        db.shutdown().await?;
    }
    let db = Uni::open(path).build().await?;
    assert_eq!(
        db.schema().current().properties["Doc"]["rank"].default,
        Some(Value::Int(7))
    );
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't'})").await?;
    tx.commit().await?;
    assert_eq!(
        scalar(&db, "MATCH (d:Doc) RETURN d.rank AS v").await?,
        Value::Int(7)
    );
    Ok(())
}

/// `DROP LABEL` and `DROP EDGE TYPE` are refused while data carries them,
/// counting flushed, unflushed and the current transaction's writes, through
/// DDL and the `uni.schema.*` procedures alike. Once the data is gone the
/// drop succeeds.
#[tokio::test]
async fn test_drop_label_refused_while_populated() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (name STRING)").await?;
    tx.execute("CREATE LABEL Q (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE R () FROM P TO P").await?;
    tx.execute("CREATE (:P {name: 'a'})-[:R]->(:P {name: 'b'})")
        .await?;
    tx.commit().await?;

    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        // A refused statement leaves its transaction rollback-only, so each
        // attempt gets its own.
        let err = db
            .session()
            .tx()
            .await?
            .execute("DROP LABEL P")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("'P'") && err.contains("2 vertices"),
            "error must name the label and the count: {err}"
        );
        let err = db
            .session()
            .tx()
            .await?
            .execute("DROP EDGE TYPE R")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("'R'") && err.contains("1 edge"),
            "error must name the edge type and the count: {err}"
        );
        for call in [
            "CALL uni.schema.dropLabel('P') YIELD success RETURN success",
            "CALL uni.schema.dropEdgeType('R') YIELD success RETURN success",
        ] {
            let res = db.session().tx().await?.query(call).await;
            assert!(res.is_err(), "{call} must be refused (flushed: {flush})");
        }
    }
    let schema = db.schema().current();
    assert!(matches!(
        schema.labels["P"].state,
        uni_common::core::schema::SchemaElementState::Active
    ));

    // The current transaction's own write counts.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Q {name: 'q'})").await?;
    assert!(tx.execute("DROP LABEL Q").await.is_err());
    tx.rollback();

    // Once the data is gone, the drops succeed.
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:R]->() DELETE r").await?;
    tx.execute("DROP EDGE TYPE R").await?;
    tx.execute("MATCH (n:P) DETACH DELETE n").await?;
    tx.execute("DROP LABEL P").await?;
    tx.execute("DROP LABEL Q").await?;
    tx.commit().await?;
    let schema = db.schema().current();
    assert!(matches!(
        schema.labels["P"].state,
        uni_common::core::schema::SchemaElementState::Tombstone { .. }
    ));
    Ok(())
}
