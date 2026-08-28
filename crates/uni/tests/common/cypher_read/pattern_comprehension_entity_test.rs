//! Projecting a whole entity out of a pattern comprehension.
//!
//! `[(n)-[:R]->(x) | x.name]` worked and `[(n)-[:R]->(x) | x]` did not:
//!
//! ```text
//! Schema error: No field named x.
//! Valid fields are "n._vid", "n._labels", "x._vid".
//! ```
//!
//! The vectorized comprehension built its inner batch from the property
//! references it found — `Expr::Property(Variable(x), prop)` and nothing else —
//! so a bare `x` had no column to resolve against. The collector now recognises
//! a bare entity reference and the batch carries the entity itself, encoded as
//! a CypherValue exactly as the non-vectorised path encodes its items. That
//! agreement is the point: which path plans a comprehension is an optimisation
//! decision, and the two must not disagree about what `| x` evaluates to.
//!
//! The relationship form is covered too, and deliberately: it is the case where
//! a half-fix would have been worse than the gap. Adding the column without
//! filling it turns `No field named r` into a silent `null`.

use uni_db::{Uni, Value};

/// `a -[:KNOWS {since:'YEAR'}]-> b`.
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
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS {since:'YEAR'}]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// The list produced for the row anchored at `a`.
async fn list_for_a(db: &Uni, q: &str) -> Vec<Value> {
    let r = db
        .session()
        .query(q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    for row in r.rows() {
        if let Value::List(items) = &row.values()[0]
            && !items.is_empty()
        {
            return items.clone();
        }
    }
    panic!("no non-empty list in the result of {q}");
}

#[tokio::test]
async fn a_whole_node_can_be_projected() {
    let db = fixture().await;
    let items = list_for_a(&db, "MATCH (n:P) RETURN [(n)-[:KNOWS]->(x) | x] AS l").await;
    match &items[0] {
        Value::Node(node) => {
            assert_eq!(node.labels, vec!["P".to_string()]);
            assert_eq!(
                node.properties.get("name"),
                Some(&Value::String("b".to_string())),
                "the node must carry its properties, not just its id"
            );
        }
        other => panic!("expected a node, got {other:?}"),
    }
}

#[tokio::test]
async fn a_whole_relationship_can_be_projected() {
    let db = fixture().await;
    let items = list_for_a(&db, "MATCH (n:P) RETURN [(n)-[r:KNOWS]->(x) | r] AS l").await;
    match &items[0] {
        Value::Edge(edge) => {
            assert_eq!(edge.edge_type, "KNOWS");
            assert_ne!(
                edge.src, edge.dst,
                "endpoints must be the edge's own, in stored orientation"
            );
            assert_eq!(
                edge.properties.get("since"),
                Some(&Value::String("YEAR".to_string()))
            );
        }
        other => panic!("expected a relationship, got {other:?}"),
    }
}

/// The vectorized and fallback paths must agree on the value. The unanchored
/// form takes the fallback; the anchored form takes the vectorized one.
#[tokio::test]
async fn both_comprehension_paths_produce_the_same_node() {
    let db = fixture().await;
    let anchored = list_for_a(&db, "MATCH (n:P) RETURN [(n)-[:KNOWS]->(x) | x] AS l").await;
    let unanchored = list_for_a(&db, "RETURN [(a:P)-[:KNOWS]->(x:P) | x] AS l").await;
    assert_eq!(anchored, unanchored);
}

/// `id()` over the projected entity — the shape LDBC-style correlation uses.
#[tokio::test]
async fn id_of_a_projected_entity_resolves() {
    let db = fixture().await;
    let items = list_for_a(&db, "MATCH (n:P) RETURN [(n)-[:KNOWS]->(x) | id(x)] AS l").await;
    let direct = db
        .session()
        .query("MATCH (n:P {name:'b'}) RETURN id(n) AS v")
        .await
        .unwrap();
    assert_eq!(items[0], direct.rows()[0].values()[0]);
}

/// A property projection must still narrow to that property rather than
/// materialising the whole entity — the projection-pruning contract.
#[tokio::test]
async fn a_property_projection_still_works() {
    let db = fixture().await;
    let items = list_for_a(&db, "MATCH (n:P) RETURN [(n)-[:KNOWS]->(x) | x.name] AS l").await;
    assert_eq!(items[0], Value::String("b".to_string()));
}

/// Still open. A map whose *value* is a property of an inner variable compiles
/// that access as `index(x, 'name')` over a bare `x`, because the comprehension
/// compiles its map expression with the outer `TranslationContext` and the
/// pattern's own variables are not in it — so `is_graph_entity` is false and
/// the column-reference form is never chosen. A list literal of the same
/// property works, which is what makes it a compiler-context bug rather than a
/// missing column. Closing it means giving the inner scope its own context, not
/// widening `x` to the whole entity, which would defeat the projection pruning
/// the test above pins.
#[tokio::test]
#[ignore = "inner pattern variables are absent from the comprehension's \
            TranslationContext, so a map value compiles as index(bare x, …)"]
async fn a_map_literal_over_an_inner_property() {
    let db = fixture().await;
    let items = list_for_a(
        &db,
        "MATCH (n:P) RETURN [(n)-[:KNOWS]->(x) | {name: x.name}] AS l",
    )
    .await;
    match &items[0] {
        Value::Map(m) => assert_eq!(m.get("name"), Some(&Value::String("b".to_string()))),
        other => panic!("expected a map, got {other:?}"),
    }
}

/// Same cause, through map-projection syntax.
#[tokio::test]
#[ignore = "same cause as a_map_literal_over_an_inner_property"]
async fn a_map_projection_over_an_inner_entity() {
    let db = fixture().await;
    let items = list_for_a(
        &db,
        "MATCH (n:P) RETURN [(n)-[:KNOWS]->(x) | x {.name}] AS l",
    )
    .await;
    match &items[0] {
        Value::Map(m) => assert_eq!(m.get("name"), Some(&Value::String("b".to_string()))),
        other => panic!("expected a map, got {other:?}"),
    }
}
