//! An entity that has passed through `collect()` and `UNWIND` is still an entity.
//!
//! A scan-bound entity variable is a *set* of DataFusion columns — `{var}._vid`,
//! `{var}._labels`, `{var}.{prop}`. After `collect()` + `UNWIND` it is a single
//! opaque CypherValue column named `{var}`. Consumers that assume the column form
//! fail against the opaque one.
//!
//! The openCypher TCK does exercise this pipeline — `Unwind1` scenario [12],
//! "Unwind does not remove variables from scope" — and it passes. But it uses the
//! unwound variable as a traversal *target* and returns it *whole*: it never reads
//! a property off it, never uses it as the traversal anchor, and never counts it.
//! Those three are the gap, and each one below was observed failing.
//!
//! Found by LDBC SNB Interactive IC9 (`UNWIND friends AS friend MATCH
//! (friend)<-[:HAS_CREATOR]-(message) ... RETURN friend.id`) and IC10
//! (`size(posts)` where `posts = collect(post)`).

use uni_db::{Uni, Value};

/// Two people, three posts: `a` wrote two, `b` wrote one.
async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE LABEL Post (title STRING)")
        .await
        .unwrap();
    tx.execute("CREATE EDGE TYPE HAS_CREATOR FROM Post TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("CREATE (:Post {title:'p1'}), (:Post {title:'p2'}), (:Post {title:'p3'})")
        .await
        .unwrap();
    for (person, post) in [("a", "p1"), ("a", "p2"), ("b", "p3")] {
        tx.execute(&format!(
            "MATCH (n:P {{name:'{person}'}}), (p:Post {{title:'{post}'}}) \
             CREATE (p)-[:HAS_CREATOR]->(n)"
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    db
}

async fn one(db: &Uni, q: &str) -> Value {
    let r = db.session().query(q).await.unwrap();
    r.rows()[0].values()[0].clone()
}

/// The first two columns of every row, sorted here rather than by `ORDER BY`.
///
/// Deliberate: `ORDER BY` over a traversal is currently non-deterministic — the
/// same binary returns `p1, p2` and `p2, p1` on alternating runs for a query with
/// no ties, and it does so with this file's fixes reverted, so it is a separate
/// pre-existing defect. Ordering is not what these tests are about, and leaving
/// them to depend on it would make them flaky for a reason unrelated to their
/// subject. The row *contents* are still asserted exactly.
fn sorted_pairs(r: &uni_db::QueryResult) -> Vec<(Value, Value)> {
    let mut got: Vec<(Value, Value)> = r
        .rows()
        .iter()
        .map(|x| (x.values()[0].clone(), x.values()[1].clone()))
        .collect();
    got.sort_by_key(|(a, b)| (format!("{a:?}"), format!("{b:?}")));
    got
}

/// The minimal `f._vid` failure: counting an unwound entity.
#[tokio::test]
async fn count_of_an_unwound_entity() {
    let db = fixture().await;
    let v = one(
        &db,
        "MATCH (f:P) WITH collect(DISTINCT f) AS fs UNWIND fs AS f RETURN count(f)",
    )
    .await;
    assert_eq!(v, Value::Int(2));
}

/// A property read directly off an unwound entity.
#[tokio::test]
async fn property_of_an_unwound_entity() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (f:P) WITH collect(DISTINCT f) AS fs UNWIND fs AS f \
             RETURN f.name AS name",
        )
        .await
        .unwrap();
    let mut names: Vec<Value> = r.rows().iter().map(|x| x.values()[0].clone()).collect();
    names.sort_by_key(|v| format!("{v:?}"));
    assert_eq!(
        names,
        vec![Value::String("a".into()), Value::String("b".into())]
    );
}

/// The scan-bound baseline for the test below. It shares every clause except the
/// `collect()`/`UNWIND` round-trip, so if the two disagree the round-trip is what
/// changed the answer — and if this one is itself wrong, the fault is not in the
/// round-trip at all.
#[tokio::test]
async fn scan_bound_baseline_for_the_traversal_shape() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (f:P) \
             MATCH (f)<-[:HAS_CREATOR]-(post:Post) \
             RETURN f.name AS name, post.title AS title",
        )
        .await
        .unwrap();
    let got = sorted_pairs(&r);
    assert_eq!(
        got,
        vec![
            (Value::String("a".into()), Value::String("p1".into())),
            (Value::String("a".into()), Value::String("p2".into())),
            (Value::String("b".into()), Value::String("p3".into())),
        ]
    );
}

/// IC9's shape: the unwound entity anchors a traversal, and a property is read
/// off it *afterwards*. The traversal alone already worked; the property did not.
#[tokio::test]
async fn property_of_an_unwound_entity_after_it_anchors_a_traversal() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (f:P) WITH collect(DISTINCT f) AS fs UNWIND fs AS f \
             MATCH (f)<-[:HAS_CREATOR]-(post:Post) \
             RETURN f.name AS name, post.title AS title",
        )
        .await
        .unwrap();
    let got = sorted_pairs(&r);
    assert_eq!(
        got,
        vec![
            (Value::String("a".into()), Value::String("p1".into())),
            (Value::String("a".into()), Value::String("p2".into())),
            (Value::String("b".into()), Value::String("p3".into())),
        ]
    );
}

/// `id()` of an unwound entity — the same identity-column assumption as `count`.
#[tokio::test]
async fn id_of_an_unwound_entity_matches_the_scan_bound_one() {
    let db = fixture().await;
    let direct = one(&db, "MATCH (f:P {name:'a'}) RETURN id(f)").await;
    let unwound = one(
        &db,
        "MATCH (f:P {name:'a'}) WITH collect(f) AS fs UNWIND fs AS f RETURN id(f)",
    )
    .await;
    assert_eq!(direct, unwound);
}

/// IC10: `size()` over a collected entity list.
#[tokio::test]
async fn size_of_a_collected_entity_list() {
    let db = fixture().await;
    assert_eq!(
        one(&db, "MATCH (f:P) WITH collect(f) AS fs RETURN size(fs)").await,
        Value::Int(2)
    );
}

/// `size()` must agree with `count()` over the same collection.
#[tokio::test]
async fn size_of_a_collected_edge_list() {
    let db = fixture().await;
    assert_eq!(
        one(
            &db,
            "MATCH ()-[r:HAS_CREATOR]->() WITH collect(r) AS rs RETURN size(rs)"
        )
        .await,
        Value::Int(3)
    );
}

/// The shape the TCK already covers (`Unwind1` [12]): the unwound entity used as
/// a traversal *target*, returned whole. This is the guard that the fix does not
/// regress what already works.
#[tokio::test]
async fn unwound_entity_as_a_traversal_target_still_works() {
    let db = fixture().await;
    let v = one(
        &db,
        "MATCH (p:Post)-[:HAS_CREATOR]->(b1:P) WITH collect(b1) AS bees \
         UNWIND bees AS b2 MATCH (post:Post)-[:HAS_CREATOR]->(b2) RETURN count(post)",
    )
    .await;
    assert_eq!(v, Value::Int(5), "3 posts x their creators, re-joined");
}
