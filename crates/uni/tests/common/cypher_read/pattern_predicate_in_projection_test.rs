//! A pattern predicate may appear inside an expression in `RETURN` / `WITH`.
//!
//! What openCypher forbids is a *bare pattern as the projected value* — the
//! openCypher TCK pins exactly that, and only that:
//!
//! - `Pattern1` [22] "Fail on using pattern in RETURN projection":
//!   `MATCH (n) RETURN (n)-[]->()` must raise `SyntaxError: UnexpectedSyntax`
//! - `Pattern1` [23] "Fail on using pattern in WITH projection":
//!   `MATCH (n) WITH (n)-[]->() AS x RETURN x` must do the same
//!
//! A pattern in a *boolean* context is a different thing and is legal —
//! `Pattern1` [19]–[21] cover `WHERE NOT (n)-[:REL2]-()` and conjunctions of
//! pattern predicates. No TCK scenario rejects one nested inside an expression in
//! a projection.
//!
//! The guard was written with `contains_pattern_predicate`, which walks the whole
//! expression tree, so it rejected the legal nested case along with the illegal
//! bare one. Found by LDBC SNB Interactive IC7
//! (`not((liker)-[:KNOWS]-(person)) AS isNew` in `RETURN`) and IC10 (a pattern
//! predicate in a list comprehension's `WHERE`, inside `WITH`).

use uni_db::{Uni, Value};

/// `a` knows `b`; `c` knows nobody.
async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'}), (:P {name:'c'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// Sorted `(name, flag)` pairs, so the assertion does not depend on `ORDER BY`
/// over a traversal, which is separately non-deterministic.
async fn name_flag_pairs(db: &Uni, q: &str) -> Vec<(String, Value)> {
    let r = db.session().query(q).await.unwrap();
    let mut got: Vec<(String, Value)> = r
        .rows()
        .iter()
        .map(|x| match &x.values()[0] {
            Value::String(s) => (s.clone(), x.values()[1].clone()),
            other => panic!("expected a name, got {other:?}"),
        })
        .collect();
    got.sort_by_key(|(n, _)| n.clone());
    got
}

/// IC7's shape: a negated pattern predicate as a projected boolean.
#[tokio::test]
async fn negated_pattern_predicate_in_return() {
    let db = fixture().await;
    let got = name_flag_pairs(
        &db,
        "MATCH (p:P) RETURN p.name AS name, not((p)-[:KNOWS]-(:P)) AS isAlone",
    )
    .await;
    assert_eq!(
        got,
        vec![
            ("a".to_string(), Value::Bool(false)),
            ("b".to_string(), Value::Bool(false)),
            ("c".to_string(), Value::Bool(true)),
        ]
    );
}

/// The same, un-negated, so the fix is not just inverting a boolean somewhere.
#[tokio::test]
async fn bare_pattern_predicate_as_a_boolean_in_return() {
    let db = fixture().await;
    let got = name_flag_pairs(
        &db,
        "MATCH (p:P) RETURN p.name AS name, ((p)-[:KNOWS]-(:P)) AND true AS known",
    )
    .await;
    assert_eq!(
        got,
        vec![
            ("a".to_string(), Value::Bool(true)),
            ("b".to_string(), Value::Bool(true)),
            ("c".to_string(), Value::Bool(false)),
        ]
    );
}

/// The same in a `WITH` projection.
#[tokio::test]
async fn pattern_predicate_in_with_projection() {
    let db = fixture().await;
    let got = name_flag_pairs(
        &db,
        "MATCH (p:P) WITH p.name AS name, not((p)-[:KNOWS]-(:P)) AS isAlone RETURN name, isAlone",
    )
    .await;
    assert_eq!(
        got,
        vec![
            ("a".to_string(), Value::Bool(false)),
            ("b".to_string(), Value::Bool(false)),
            ("c".to_string(), Value::Bool(true)),
        ]
    );
}

/// IC10's shape: a pattern predicate in a list comprehension's `WHERE`, whose
/// result is then measured — inside `WITH`.
#[tokio::test]
async fn pattern_predicate_inside_a_list_comprehension_in_with() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (p:P) WITH collect(p) AS ps \
             WITH size([q IN ps WHERE (q)-[:KNOWS]-(:P)]) AS connected \
             RETURN connected",
        )
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(2), "a and b");
}

/// TCK `Pattern1` [22]. A bare pattern as the projected value stays rejected.
#[tokio::test]
async fn a_bare_pattern_in_return_is_still_a_syntax_error() {
    let db = fixture().await;
    let err = db
        .session()
        .query("MATCH (n:P) RETURN (n)-[:KNOWS]->()")
        .await
        .expect_err("a bare pattern as a projection must be rejected");
    assert!(
        format!("{err}").contains("UnexpectedSyntax"),
        "expected a SyntaxError, got: {err}"
    );
}

/// TCK `Pattern1` [23]. Same for `WITH`.
#[tokio::test]
async fn a_bare_pattern_in_with_is_still_a_syntax_error() {
    let db = fixture().await;
    let err = db
        .session()
        .query("MATCH (n:P) WITH (n)-[:KNOWS]->() AS x RETURN x")
        .await
        .expect_err("a bare pattern as a projection must be rejected");
    assert!(
        format!("{err}").contains("UnexpectedSyntax"),
        "expected a SyntaxError, got: {err}"
    );
}

/// TCK `List6` [6]. A pattern predicate is a boolean, so `size()` of one is not
/// meaningful and stays rejected — the guard is about *position*, not about
/// whether the projection happens to be a bare pattern. Narrowing it to
/// "top-level bare pattern only" let this through, which is what caught it.
#[tokio::test]
async fn size_of_a_pattern_predicate_is_still_a_syntax_error() {
    let db = fixture().await;
    let err = db
        .session()
        .query("MATCH (a:P) RETURN size((a)-[:KNOWS]->())")
        .await
        .expect_err("size() of a pattern predicate must be rejected");
    assert!(
        format!("{err}").contains("UnexpectedSyntax"),
        "expected a SyntaxError, got: {err}"
    );
}

/// The counterpart from TCK `List6` [7]: a pattern *comprehension* is a list, so
/// `size()` of one is legal and must keep working.
///
/// Projects a property rather than the whole node: `[... | x]` fails with
/// `No field named x. Valid fields are "a._vid", "a._labels", "a.name", "x._vid"`
/// — a pattern comprehension's inner schema carries the identity column but not
/// the whole-entity column. That is a separate gap in the same family as the
/// `collect()`/`UNWIND` work, unrelated to the guard under test here.
#[tokio::test]
async fn size_of_a_pattern_comprehension_still_works() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (a:P {name:'a'}) RETURN size([(a)-[:KNOWS]->(x) | x.name]) AS n")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(1));
}
