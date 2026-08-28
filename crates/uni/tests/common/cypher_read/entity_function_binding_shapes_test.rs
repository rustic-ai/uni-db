//! Entity functions across the binding shapes the TCK does not reach.
//!
//! `docs/testing/single-shape-coverage-2026-08-27.md` audits how the openCypher
//! TCK covers each function that takes a graph entity, classified by how the
//! entity is bound *in the query under test*:
//!
//! | function | queries | contexts |
//! |---|---|---|
//! | `relationships(` | 7 | MATCH-traversal:7 — one shape |
//! | `nodes(` | 11 | MATCH-traversal:8, MATCH-node:3 |
//! | `type(` | 13 | MATCH-traversal:11, MATCH-node:2 |
//! | `properties(` | 7 | (no clause):4, MATCH-traversal:2, MATCH-node:1 |
//!
//! None of them is exercised on an entity that reached the function through a
//! `collect()`/`UNWIND` round trip, and `relationships(` is exercised on exactly
//! one shape. That is the same position `startNode`/`endNode` was in, where the
//! untested shape turned out to be broken.
//!
//! Here it is not: every case below passed the first time it was run. That is
//! the point of writing them down anyway — an untested shape that happens to
//! work is one refactor away from an untested shape that does not, and the
//! audit is only worth doing if the empty buckets get filled.

use uni_db::{Uni, Value};

/// `a -[:KNOWS {since:'Y'}]-> b`.
async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS (since STRING) FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS {since:'Y'}]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

async fn one(db: &Uni, q: &str) -> Value {
    db.session()
        .query(q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .rows()[0]
        .values()[0]
        .clone()
}

/// A path carried through `collect()`/`UNWIND` before `relationships()` sees it.
#[tokio::test]
async fn relationships_of_a_path_through_collect_and_unwind() {
    let db = fixture().await;
    let got = one(
        &db,
        "MATCH p = (:P)-[:KNOWS]->(:P) WITH collect(p) AS ps UNWIND ps AS q \
         RETURN size(relationships(q)) AS n",
    )
    .await;
    assert_eq!(got, Value::Int(1));
}

/// The same round trip for `nodes()`.
#[tokio::test]
async fn nodes_of_a_path_through_collect_and_unwind() {
    let db = fixture().await;
    let got = one(
        &db,
        "MATCH p = (:P)-[:KNOWS]->(:P) WITH collect(p) AS ps UNWIND ps AS q \
         RETURN size(nodes(q)) AS n",
    )
    .await;
    assert_eq!(got, Value::Int(2));
}

/// `type()` on a relationship that arrived through a collected list rather than
/// straight off a traversal.
#[tokio::test]
async fn type_of_a_relationship_through_collect_and_unwind() {
    let db = fixture().await;
    let got = one(
        &db,
        "MATCH ()-[r:KNOWS]->() WITH collect(r) AS rs UNWIND rs AS r2 RETURN type(r2) AS t",
    )
    .await;
    assert_eq!(got, Value::String("KNOWS".to_string()));
}

/// `type()` on a MERGE-bound relationship — the binding the TCK covers for
/// `startNode`/`endNode` and for nothing else.
#[tokio::test]
async fn type_of_a_merge_bound_relationship() {
    let db = fixture().await;
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let rows = tx
        .query_with("MERGE (x:P {name:'a'})-[r:KNOWS]->(y:P {name:'b'}) RETURN type(r) AS t")
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        rows.rows()[0].values()[0],
        Value::String("KNOWS".to_string())
    );
}

/// `properties()` on a relationship through the same round trip. The property
/// has to survive the encode/decode, not just the identity.
#[tokio::test]
async fn properties_of_a_relationship_through_collect_and_unwind() {
    let db = fixture().await;
    let got = one(
        &db,
        "MATCH ()-[r:KNOWS]->() WITH collect(r) AS rs UNWIND rs AS r2 \
         RETURN properties(r2) AS p",
    )
    .await;
    match got {
        Value::Map(m) => assert_eq!(m.get("since"), Some(&Value::String("Y".to_string()))),
        other => panic!("expected a property map, got {other:?}"),
    }
}

/// The control each of the above is compared against: the one shape the TCK
/// does cover. A failure here is an ordinary regression, not a coverage gap.
#[tokio::test]
async fn the_covered_shapes_still_work() {
    let db = fixture().await;
    assert_eq!(
        one(
            &db,
            "MATCH p = (:P)-[:KNOWS]->(:P) RETURN size(relationships(p)) AS n"
        )
        .await,
        Value::Int(1)
    );
    assert_eq!(
        one(&db, "MATCH ()-[r:KNOWS]->() RETURN type(r) AS t").await,
        Value::String("KNOWS".to_string())
    );
}
