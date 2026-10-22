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

/// A writing unit subquery re-emits its entity with its identity: before,
/// `properties(n)` and `collect(n)` after `CALL { WITH n SET ... }` read null
/// (the re-emitted struct carried a null `_vid`), whatever the properties.
#[tokio::test]
async fn unit_subquery_reemits_the_entity_with_its_identity() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {name: 'p', w: 1})-[:PR {k: 1}]->(:P {name: 'q'})")
        .await?;
    tx.commit().await?;
    let want_n: HashMap<String, Value> = HashMap::from([
        ("name".to_string(), Value::String("p".into())),
        ("w".to_string(), Value::Int(2)),
    ]);
    let want_r: HashMap<String, Value> = HashMap::from([("k".to_string(), Value::Int(2))]);
    let mut defects = Vec::new();
    for state in ["L0", "flushed"] {
        if state == "flushed" {
            db.flush().await?;
        }
        for (q, want) in [
            (
                "MATCH (n:P {name: 'p'}) CALL { WITH n SET n.w = 2 } RETURN properties(n)",
                &want_n,
            ),
            (
                "MATCH (n:P {name: 'p'}) CALL { WITH n SET n.w = 2 } RETURN collect(n)[0]",
                &want_n,
            ),
            (
                "MATCH (n:P {name: 'p'}) CALL { WITH n SET n.w = 2 } RETURN n",
                &want_n,
            ),
            (
                "MATCH ()-[r:PR]->() CALL { WITH r SET r.k = 2 } RETURN properties(r)",
                &want_r,
            ),
            (
                "MATCH ()-[r:PR]->() CALL { WITH r SET r.k = 2 } RETURN r",
                &want_r,
            ),
        ] {
            let tx = db.session().tx().await?;
            let got = tx.query(q).await?.rows()[0].values()[0].clone();
            tx.rollback();
            if map_of(&got).as_ref() != Some(want) {
                defects.push(format!("[{state}] {q}: got {got:?}"));
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

/// A flattened entity map — user properties as top-level keys, no
/// `_all_props` — whose map-valued user property is named `properties`.
///
/// Two user-reachable producers make one: a writing unit subquery
/// re-emitting its entity, and a parameter (or `UNWIND`ed map) spelling an
/// entity with `_vid` and `_labels`. The `properties` map was taken for the
/// serialised entity's property container, so every other property vanished
/// (`$m` came back as a node with only the nested map's keys) or the nested
/// keys leaked in as properties (`RETURN n` gained `inner`), and `n.properties`
/// read `NULL`.
#[tokio::test]
async fn flattened_entity_with_a_map_valued_properties_property() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL M (name STRING, properties MAP<STRING, INT64>)")
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:M {name: 'x', properties: {inner: 1}})")
        .await?;
    tx.commit().await?;

    let inner = Value::Map(HashMap::from([("inner".to_string(), Value::Int(1))]));
    let want: HashMap<String, Value> = HashMap::from([
        ("name".to_string(), Value::String("x".into())),
        ("properties".to_string(), inner.clone()),
    ]);
    let want_keys = Value::List(vec![
        Value::String("name".into()),
        Value::String("properties".into()),
    ]);
    let mut defects = Vec::new();
    let mut identity_defects = Vec::new();
    let mut check = |what: String, got: Vec<Value>| {
        let ok = got.len() == 5
            && got[0] == Value::String("x".into())
            && got[1] == inner
            && got[2] == want_keys
            && map_of(&got[3]).as_ref() == Some(&want)
            && got[4].is_null();
        if !ok {
            defects.push(format!("{what}: got {got:?}"));
        }
    };

    for state in ["L0", "flushed"] {
        if state == "flushed" {
            db.flush().await?;
        }
        let tx = db.session().tx().await?;
        let res = tx
            .query(
                "MATCH (n:M) CALL { WITH n SET n.name = 'x' } \
                 RETURN n.name, n.properties, keys(n), n, n.inner",
            )
            .await?;
        tx.rollback();
        check(
            format!("[{state}] unit subquery"),
            res.rows()[0].values().to_vec(),
        );

        // The re-emitted entity keeps its identity: `properties(n)` and
        // `collect(n)` read it as an entity, not as a null one.
        for q in [
            "MATCH (n:M) CALL { WITH n SET n.name = 'x' } RETURN properties(n)",
            "MATCH (n:M) CALL { WITH n SET n.name = 'x' } RETURN collect(n)[0]",
        ] {
            let tx = db.session().tx().await?;
            let got = tx.query(q).await?.rows()[0].values()[0].clone();
            tx.rollback();
            if map_of(&got).as_ref() != Some(&want) {
                identity_defects.push(format!("[{state}] {q}: got {got:?}"));
            }
        }
    }

    let lit = "{_vid: 0, _labels: ['A'], name: 'x', properties: {inner: 1}}";
    let res = db
        .session()
        .query(&format!(
            "UNWIND [{lit}] AS m RETURN m.name, m.properties, keys(m), m, m.inner"
        ))
        .await?;
    check(
        "UNWIND of a map literal".into(),
        res.rows()[0].values().to_vec(),
    );

    let mut param = want.clone();
    param.insert("_vid".to_string(), Value::Int(0));
    param.insert(
        "_labels".to_string(),
        Value::List(vec![Value::String("A".into())]),
    );
    let res = db
        .session()
        .query_with("RETURN $m.name, $m.properties, keys($m), $m, $m.inner")
        .param("m", Value::Map(param))
        .fetch_all()
        .await?;
    check("parameter".into(), res.rows()[0].values().to_vec());
    defects.extend(identity_defects);

    assert!(
        defects.is_empty(),
        "{} defect(s):\n{}",
        defects.len(),
        defects.join("\n")
    );
    Ok(())
}
