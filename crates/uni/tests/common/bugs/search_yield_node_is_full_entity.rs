// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A search procedure's `YIELD node` is a full node entity.
//!
//! `uni.fts.query`, `uni.vector.query` and `uni.search` yield `node`. Used
//! wholesale -- `RETURN node`, `properties(node)`, `keys(node)`,
//! `labels(node)`, `node {.*}` -- it came back with no properties at all, and
//! `labels(node)` failed outright. Only a property the query named
//! (`node.id`) was fetched, because the planner dropped the whole-entity
//! marker before building the procedure's column list, and the yield was a
//! VID string rather than a node struct inside the query.
//!
//! openCypher gives a yielded node ordinary entity semantics; Neo4j's
//! `db.index.fulltext.queryNodes` yields full nodes.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(search_yield_node_is_full_entity)'

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

/// The three node-yielding search procedures, each finding `id: 0` first.
const SEARCHES: &[(&str, &str)] = &[
    (
        "fts",
        "CALL uni.fts.query('Doc', 'content', 'hello', 1) YIELD node",
    ),
    (
        "vector",
        "CALL uni.vector.query('Doc', 'emb', $q, 1) YIELD node",
    ),
    (
        "search",
        "CALL uni.search('Doc', {vector: 'emb', fts: 'content'}, 'hello', $q, 1) YIELD node",
    ),
];

async fn open() -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::Int)
        .property("content", DataType::String)
        .property("emb", DataType::Vector { dimensions: 2 })
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.content]")
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    // Only `id: 0` mentions "hello" and sits on the query vector, so every
    // search ranks it first with k = 1.
    tx.execute(
        "CREATE (:Doc {id: 0, content: 'hello world', emb: [1.0, 0.0], extra: 'x'}), \
                (:Doc {id: 1, content: 'quiet meadow', emb: [0.0, 1.0]})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn one(db: &Uni, search: &str, tail: &str) -> Result<HashMap<String, Value>> {
    let cypher = format!("{search} {tail}");
    let result = db
        .session()
        .query_with(&cypher)
        .param("q", Value::List(vec![Value::Float(1.0), Value::Float(0.0)]))
        .fetch_all()
        .await
        .map_err(|e| anyhow::anyhow!("{cypher}: {e}"))?;
    assert_eq!(result.rows().len(), 1, "{cypher}: one hit expected");
    let row = &result.rows()[0];
    Ok(result
        .columns()
        .iter()
        .map(|c| (c.clone(), row.value(c).cloned().unwrap_or(Value::Null)))
        .collect())
}

fn props_of(v: &Value, ctx: &str) -> HashMap<String, Value> {
    match v {
        Value::Node(n) => n.properties.clone(),
        Value::Map(m) => m.clone(),
        other => panic!("{ctx}: expected a node or map, got {other:?}"),
    }
}

/// Every whole-entity use of the yielded node sees all of its properties.
async fn assert_full_entity(db: &Uni, phase: &str) -> Result<()> {
    for (name, search) in SEARCHES {
        let ctx = format!("{phase}/{name}");

        let r = one(db, search, "RETURN node").await?;
        let node = &r["node"];
        let Value::Node(n) = node else {
            panic!("{ctx}: RETURN node must be a node, got {node:?}");
        };
        assert_eq!(n.labels, vec!["Doc".to_string()], "{ctx}: labels");
        assert_eq!(n.properties.get("id"), Some(&Value::Int(0)), "{ctx}: {n:?}");
        assert_eq!(
            n.properties.get("content"),
            Some(&Value::String("hello world".into())),
            "{ctx}: {n:?}"
        );
        assert!(n.properties.contains_key("emb"), "{ctx}: {n:?}");
        assert_eq!(
            n.properties.get("extra"),
            Some(&Value::String("x".into())),
            "{ctx}: an undeclared property is part of the entity too: {n:?}"
        );

        let r = one(db, search, "RETURN properties(node) AS p").await?;
        let p = props_of(&r["p"], &ctx);
        assert_eq!(
            p.get("id"),
            Some(&Value::Int(0)),
            "{ctx}: properties(): {p:?}"
        );
        assert_eq!(
            p.get("extra"),
            Some(&Value::String("x".into())),
            "{ctx}: properties(): {p:?}"
        );

        let r = one(db, search, "RETURN keys(node) AS k").await?;
        let Value::List(keys) = &r["k"] else {
            panic!("{ctx}: keys() must be a list, got {:?}", r["k"]);
        };
        let mut keys: Vec<String> = keys
            .iter()
            .map(|k| k.as_str().unwrap_or_default().to_string())
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["content", "emb", "extra", "id"], "{ctx}: keys()");

        let r = one(db, search, "RETURN labels(node) AS l").await?;
        assert_eq!(
            r["l"],
            Value::List(vec![Value::String("Doc".into())]),
            "{ctx}: labels()"
        );

        let r = one(db, search, "RETURN node {.*} AS m").await?;
        let m = props_of(&r["m"], &ctx);
        assert_eq!(
            m.get("id"),
            Some(&Value::Int(0)),
            "{ctx}: map projection: {m:?}"
        );
        assert!(m.contains_key("content"), "{ctx}: map projection: {m:?}");

        let r = one(db, search, "WITH node RETURN node").await?;
        let p = props_of(&r["node"], &ctx);
        assert_eq!(
            p.get("id"),
            Some(&Value::Int(0)),
            "{ctx}: through WITH: {p:?}"
        );

        // A named property alongside the whole entity: the narrow request must
        // not narrow the entity.
        let r = one(db, search, "RETURN node.id AS id, node").await?;
        assert_eq!(r["id"], Value::Int(0), "{ctx}");
        let p = props_of(&r["node"], &ctx);
        assert!(
            p.contains_key("content"),
            "{ctx}: entity beside node.id: {p:?}"
        );

        // Identity is preserved: the yielded node equals the matched one.
        let r = one(
            db,
            search,
            "MATCH (m:Doc) WHERE m = node RETURN m.id AS id, labels(node) AS l",
        )
        .await?;
        assert_eq!(r["id"], Value::Int(0), "{ctx}: identity");
    }
    Ok(())
}

#[tokio::test]
async fn search_yield_node_is_full_entity_unflushed() -> Result<()> {
    let db = open().await?;
    assert_full_entity(&db, "L0").await
}

#[tokio::test]
async fn search_yield_node_is_full_entity_flushed() -> Result<()> {
    let db = open().await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    assert_full_entity(&db, "flushed").await
}
