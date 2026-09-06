// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A pattern comprehension over an undeclared edge type yields nothing after the
//! store is reopened.
//!
//! `[ (a)-[e:T]-(x) | e ]` returns `[]` where `T` was never declared with
//! `CREATE EDGE TYPE`, once the store has been closed and reopened. The plain
//! traversal `MATCH (a)-[e:T]-(x)` finds the edge in the same session, so the
//! edge is present and reachable — the comprehension's own expansion is what
//! comes back empty.
//!
//! The bug is invisible in the obvious test: in a single session, before any
//! reopen, the comprehension works. It reproduces through the CLI because every
//! invocation is its own process, so the query always runs against a reopened
//! store.
//!
//! This is a silent wrong answer. An empty list is a legitimate result for a
//! node with no matching edges, so nothing at the call site distinguishes it
//! from the correct answer.
//!
//! Found while fixing #193. Unrelated to it, and present before that fix —
//! confirmed by reverting the #193 planner change and reproducing.
//!
//! ## Root cause, and why the comprehension is only one face
//!
//! An edge type that was never declared is interned at write time by
//! `SchemaManager::get_or_assign_edge_type_id`, which mints a name → id pair
//! into `Schema::schemaless_registry`. That path is synchronous and runs per
//! row, so it cannot `save()`, and nothing else did: the persisted
//! `catalog/schema.json` came back with an empty registry every time.
//!
//! The plain *typed* traversal survives that because a type it cannot resolve
//! falls into `unknown_types` and is planned as `TraverseMainByType`, matching
//! on the name against the main edge table — whose `type` column is a `Utf8`,
//! so storage is name-addressed and nothing on disk was ever corrupt. Every
//! path that needs the *id* had no such fallback and silently answered from an
//! empty registry:
//!
//! - `[ (a)-[e:T]-(x) | e ]` yields `[]` — the reported symptom. The
//!   comprehension resolves id-only and, on a miss, builds a step with no edge
//!   types and a comment saying it "will produce no results".
//! - `MATCH (a)-[e]-(x)` — the *untyped* traversal — finds nothing, because it
//!   asks `all_edge_type_ids()`, which is empty.
//! - `[ (a)-[e]-(x) | e ]` likewise.
//!
//! The fix persists the registry: `SchemaManager::save_if_dirty` writes the
//! schema when its version has moved past what was last written, and the commit
//! path calls it once the transaction is durable.

use uni_db::{Uni, Value};

/// `a` -[:LIKES]-> `b`, with no `CREATE EDGE TYPE`.
async fn write_fixture(db: &Uni) {
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:LIKES]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// Every way of reaching the one edge, counted.
///
/// `(typed traversal rows, typed comprehension items, untyped traversal rows,
/// untyped comprehension items)`. The fixture has exactly one edge, so all four
/// must be 1. The untyped pair is here because it shares the reported symptom's
/// root — `all_edge_type_ids()` reads the same registry — and it fails without
/// any comprehension involved at all.
async fn every_route_to_the_edge(db: &Uni) -> (usize, usize, usize, usize) {
    async fn rows(db: &Uni, q: &str) -> usize {
        db.session().query(q).await.unwrap().rows().len()
    }
    async fn items(db: &Uni, q: &str) -> usize {
        let r = db.session().query(q).await.unwrap();
        match &r.rows()[0].values()[0] {
            Value::List(items) => items.len(),
            other => panic!("expected a List from `{q}`, got {other:?}"),
        }
    }

    (
        rows(db, "MATCH (a:P {name:'b'})-[e:LIKES]-(x) RETURN e").await,
        items(
            db,
            "MATCH (a:P {name:'b'}) RETURN [ (a)-[e:LIKES]-(x) | e ] AS es",
        )
        .await,
        rows(db, "MATCH (a:P {name:'b'})-[e]-(x) RETURN e").await,
        items(
            db,
            "MATCH (a:P {name:'b'}) RETURN [ (a)-[e]-(x) | e ] AS es",
        )
        .await,
    )
}

/// Control: within one session the two agree, even across a flush.
///
/// This is what makes the failure below a finding rather than a broken fixture —
/// the same query pair passes here, so neither the data nor the comprehension
/// syntax is at fault.
#[tokio::test]
async fn schemaless_comprehension_agrees_with_traversal_in_one_session() {
    let db = Uni::in_memory().build().await.unwrap();
    write_fixture(&db).await;
    assert_eq!(every_route_to_the_edge(&db).await, (1, 1, 1, 1));
    db.flush().await.unwrap();
    assert_eq!(
        every_route_to_the_edge(&db).await,
        (1, 1, 1, 1),
        "a flush alone does not break it"
    );
}

/// Every route to the edge survives a close and reopen.
///
/// Before the fix this returned `(1, 0, 0, 0)`: only the typed traversal, which
/// is the one route that never consults the schemaless registry.
#[tokio::test]
async fn schemaless_comprehension_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let path = path.to_str().unwrap();

    {
        let db = Uni::open(path).build().await.unwrap();
        write_fixture(&db).await;
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(path).build().await.unwrap();
    assert_eq!(
        every_route_to_the_edge(&db).await,
        (1, 1, 1, 1),
        "the typed traversal still finds the edge; every other route must too"
    );
}

/// The interned schemaless edge type is on disk after the writer exits.
///
/// Asserted against the persisted catalog rather than through a query, so a
/// future regression is attributed to the registry rather than to whichever
/// read path notices first.
#[tokio::test]
async fn schemaless_edge_type_is_persisted_to_the_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let path = store.to_str().unwrap();

    {
        let db = Uni::open(path).build().await.unwrap();
        write_fixture(&db).await;
        db.shutdown().await.unwrap();
    }

    let catalog = std::fs::read_to_string(store.join("catalog/schema.json"))
        .expect("the catalog must exist after a clean shutdown");
    let schema: serde_json::Value = serde_json::from_str(&catalog).unwrap();
    let names = &schema["schemaless_registry"]["name_to_id"];
    assert!(
        names.get("LIKES").is_some(),
        "LIKES was interned at write time and must survive the process; \
         persisted registry was {names}"
    );
}
