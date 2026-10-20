// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A user property named like an entity's own field — `label`, `labels`,
//! `type`, `properties` — is an ordinary property on every read path.
//!
//! The result normalizer used to pick an entity's properties out of a
//! flattened entity map by name: it dropped inline keys called `label`, `type`
//! and `properties`. `MATCH (n:F) CALL { WITH n SET ... } RETURN n` lost all
//! three while `n.label` still read them. An entity's own fields are told
//! apart structurally now (reserved names plus the property container), never
//! by a colliding name.
//!
//! The matrix covers declared and schemaless labels and edge types, each in
//! L0 and after a flush, read through `RETURN n`, `properties()`, `keys()`, a
//! map projection, property access, a collected list and a path.

use std::collections::{BTreeSet, HashMap};

use uni_db::{DataType, Uni, Value};

/// Property names that collide with an entity field in some encoding.
const COLLIDING: &[&str] = &["label", "labels", "type", "properties"];

/// The property map every entity under test carries.
fn expected_props() -> HashMap<String, Value> {
    let mut m: HashMap<String, Value> = COLLIDING
        .iter()
        .map(|k| ((*k).to_string(), Value::String(format!("{k}-value"))))
        .collect();
    m.insert("name".to_string(), Value::String("x".to_string()));
    m
}

/// The Cypher map literal for [`expected_props`].
fn props_literal() -> String {
    let mut parts: Vec<String> = COLLIDING
        .iter()
        .map(|k| format!("{k}: '{k}-value'"))
        .collect();
    parts.push("name: 'x'".to_string());
    format!("{{{}}}", parts.join(", "))
}

async fn setup(db: &Uni) -> anyhow::Result<()> {
    let mut builder = db.schema().label("D");
    for k in COLLIDING {
        builder = builder.property_nullable(k, DataType::String);
    }
    let mut edge = builder
        .property_nullable("name", DataType::String)
        .done()
        .edge_type("E", &["D"], &["D"]);
    for k in COLLIDING {
        edge = edge.property_nullable(k, DataType::String);
    }
    edge.property_nullable("name", DataType::String)
        .apply()
        .await?;

    let p = props_literal();
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE (a:D {p})-[:E {p}]->(b:D {p}), (c:S {p})-[:R {p}]->(d:S {p})"
    ))
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn first_value(db: &Uni, cypher: &str) -> anyhow::Result<Value> {
    let res = db.session().query(cypher).await?;
    let row = res
        .rows()
        .first()
        .ok_or_else(|| anyhow::anyhow!("no rows for `{cypher}`"))?;
    Ok(row.values()[0].clone())
}

fn map_of(v: &Value) -> Option<HashMap<String, Value>> {
    match v {
        Value::Map(m) => Some(m.clone()),
        Value::Node(n) => Some(n.properties.clone()),
        Value::Edge(e) => Some(e.properties.clone()),
        _ => None,
    }
}

/// Every read surface for one entity variable; returns one line per defect.
async fn check_entity(db: &Uni, pattern: &str, var: &str, what: &str) -> Vec<String> {
    let want = expected_props();
    let want_keys: BTreeSet<String> = want.keys().cloned().collect();
    let mut defects = Vec::new();

    let probes: Vec<(String, String)> = vec![
        (
            "RETURN".into(),
            format!("MATCH {pattern} RETURN {var} LIMIT 1"),
        ),
        (
            "properties()".into(),
            format!("MATCH {pattern} RETURN properties({var}) LIMIT 1"),
        ),
        (
            "map projection".into(),
            format!("MATCH {pattern} RETURN {var} {{.*}} LIMIT 1"),
        ),
        (
            "collect".into(),
            format!("MATCH {pattern} RETURN collect({var})[0] LIMIT 1"),
        ),
    ];
    for (surface, q) in probes {
        match first_value(db, &q).await {
            Ok(v) => match map_of(&v) {
                Some(got) if got == want => {}
                _ => defects.push(format!("{what} {surface}: got {v:?}")),
            },
            Err(e) => defects.push(format!("{what} {surface}: error {e}")),
        }
    }

    match first_value(db, &format!("MATCH {pattern} RETURN keys({var}) LIMIT 1")).await {
        Ok(Value::List(ks)) => {
            let got: BTreeSet<String> = ks
                .iter()
                .filter_map(|k| match k {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            if got != want_keys {
                defects.push(format!("{what} keys(): got {got:?}"));
            }
        }
        other => defects.push(format!("{what} keys(): got {other:?}")),
    }

    for k in COLLIDING {
        let q = format!("MATCH {pattern} RETURN {var}.{k} LIMIT 1");
        match first_value(db, &q).await {
            Ok(v) if Some(&v) == want.get(*k) => {}
            other => defects.push(format!("{what} {var}.{k}: got {other:?}")),
        }
    }
    defects
}

async fn check_path(db: &Uni, pattern: &str, what: &str) -> Vec<String> {
    let want = expected_props();
    let mut defects = Vec::new();
    match first_value(db, &format!("MATCH p = {pattern} RETURN p LIMIT 1")).await {
        Ok(Value::Path(p)) => {
            for n in &p.nodes {
                if n.properties != want {
                    defects.push(format!("{what} path node: got {:?}", n.properties));
                }
            }
            for e in &p.edges {
                if e.properties != want {
                    defects.push(format!("{what} path edge: got {:?}", e.properties));
                }
            }
        }
        other => defects.push(format!("{what} path: got {other:?}")),
    }
    defects
}

async fn check_all(db: &Uni, state: &str) -> Vec<String> {
    let mut defects = Vec::new();
    for (pattern, var, what) in [
        ("(n:D)", "n", "declared node"),
        ("(n:S)", "n", "schemaless node"),
        ("(:D)-[r:E]->(:D)", "r", "declared edge"),
        ("(:S)-[r:R]->(:S)", "r", "schemaless edge"),
    ] {
        defects.extend(
            check_entity(db, pattern, var, &format!("[{state}] {what}"))
                .await
                .into_iter(),
        );
    }
    for (pattern, what) in [
        ("(:D)-[:E]->(:D)", "declared"),
        ("(:S)-[:R]->(:S)", "schemaless"),
    ] {
        defects.extend(check_path(db, pattern, &format!("[{state}] {what}")).await);
    }
    defects
}

#[tokio::test]
async fn colliding_property_names_survive_every_read_surface() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup(&db).await?;

    let mut defects = check_all(&db, "L0").await;
    db.flush().await?;
    defects.extend(check_all(&db, "flushed").await);

    assert!(
        defects.is_empty(),
        "{} defect(s):\n{}",
        defects.len(),
        defects.join("\n")
    );
    Ok(())
}

/// A declared `MAP` property named `properties` is a property, not the
/// entity's property container.
#[tokio::test]
async fn map_valued_properties_property_is_not_the_container() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL M (name STRING, properties MAP<STRING, INT64>)")
        .await?;
    tx.execute("CREATE EDGE TYPE MR (name STRING, properties MAP<STRING, INT64>) FROM M TO M")
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:M {name: 'x', properties: {inner: 1}})-[:MR {name: 'y', properties: {inner: 2}}]->(:M {name: 'z'})")
        .await?;
    tx.commit().await?;

    for state in ["L0", "flushed"] {
        if state == "flushed" {
            db.flush().await?;
        }
        let n = first_value(&db, "MATCH (n:M {name: 'x'}) RETURN n").await?;
        let Value::Node(n) = n else {
            panic!("[{state}] expected a node, got {n:?}");
        };
        assert_eq!(
            n.properties.get("name"),
            Some(&Value::String("x".into())),
            "[{state}] node lost `name`: {:?}",
            n.properties
        );
        assert!(
            matches!(n.properties.get("properties"), Some(Value::Map(m)) if m.get("inner") == Some(&Value::Int(1))),
            "[{state}] node lost `properties`: {:?}",
            n.properties
        );

        let e = first_value(&db, "MATCH ()-[r:MR]->() RETURN r").await?;
        let Value::Edge(e) = e else {
            panic!("[{state}] expected an edge, got {e:?}");
        };
        assert_eq!(
            e.properties.get("name"),
            Some(&Value::String("y".into())),
            "[{state}] edge lost `name`: {:?}",
            e.properties
        );
        assert!(
            matches!(e.properties.get("properties"), Some(Value::Map(m)) if m.get("inner") == Some(&Value::Int(2))),
            "[{state}] edge lost `properties`: {:?}",
            e.properties
        );
    }
    Ok(())
}

/// A unit subquery that writes re-emits its entity as a flattened map — user
/// properties as top-level keys beside the system fields. The normalizer's
/// name-based split dropped `label`, `type` and `properties` there (both in L0
/// and after a flush); property access through a search procedure's yielded
/// node is checked alongside.
#[tokio::test]
async fn flattened_entity_maps_keep_colliding_properties() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE LABEL F (content STRING, label STRING, labels STRING, type STRING, \
         properties STRING)",
    )
    .await?;
    tx.execute("CREATE FULLTEXT INDEX f_fts FOR (d:F) ON EACH [d.content]")
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:F {content: 'apple pie', label: 'l', labels: 'ls', type: 't', \
         properties: 'p'})",
    )
    .await?;
    tx.commit().await?;

    let want: HashMap<String, Value> = HashMap::from([
        ("content".to_string(), Value::String("apple pie".into())),
        ("label".to_string(), Value::String("l".into())),
        ("labels".to_string(), Value::String("ls".into())),
        ("type".to_string(), Value::String("t".into())),
        ("properties".to_string(), Value::String("p".into())),
    ]);

    let mut defects = Vec::new();
    for state in ["L0", "flushed"] {
        if state == "flushed" {
            db.flush().await?;
        }
        // A unit subquery that writes re-emits the entity as a flattened map.
        let tx = db.session().tx().await?;
        let res = tx
            .query("MATCH (n:F) CALL { WITH n SET n.content = 'apple pie' } RETURN n")
            .await?;
        let v = res.rows()[0].values()[0].clone();
        tx.commit().await?;
        match map_of(&v) {
            Some(got) if got == want => {}
            _ => defects.push(format!("[{state}] unit subquery RETURN n: got {v:?}")),
        }
        for k in ["label", "labels", "type", "properties"] {
            let q = format!(
                "CALL uni.fts.query('F', 'content', 'apple', 10) YIELD node RETURN node.{k}"
            );
            match first_value(&db, &q).await {
                Ok(v) if Some(&v) == want.get(k) => {}
                other => defects.push(format!("[{state}] fts node.{k}: got {other:?}")),
            }
        }
    }
    assert!(
        defects.is_empty(),
        "{} defect(s):\n{}",
        defects.len(),
        defects.join("\n")
    );
    Ok(())
}
