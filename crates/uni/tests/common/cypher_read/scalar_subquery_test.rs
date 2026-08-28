//! `EXISTS { … }`, `COUNT { … }` and `COLLECT { … }`.
//!
//! Two of the three did not run at all:
//!
//! ```text
//! RETURN COUNT { MATCH (a:P)-[:KNOWS]->(b:P) RETURN b } AS c
//! Expected aggregate function, got: CountSubquery(…)
//! ```
//!
//! `Expr::is_aggregate()` answered `true` for `CountSubquery` and
//! `CollectSubquery`, so the planner put them in a `LogicalPlan::Aggregate`'s
//! aggregate list, where the physical planner expects an `Expr::FunctionCall`
//! and rejects everything else. They are not aggregates: each is a *scalar
//! subquery*, evaluated once per outer row, aggregating over its own result
//! rather than over the outer one — `MATCH (n) RETURN COUNT { … }` returns one
//! row per `n`, not a single grouped row. `EXISTS { … }` was never classified
//! that way, which is the only reason it worked, and it is now the shared
//! operator all three run through.
//!
//! #184's remediation plan listed only `COLLECT { … }`. `COUNT { … }` failed
//! identically and for the same reason.

use uni_db::{Uni, Value};

/// `a` knows `b` and `c`; `b` and `c` know nobody.
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
    for to in ["b", "c"] {
        tx.execute(&format!(
            "MATCH (x:P {{name:'a'}}), (y:P {{name:'{to}'}}) CREATE (x)-[:KNOWS]->(y)"
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    db
}

/// Rows as `(name, value)`, sorted by name.
async fn by_name(db: &Uni, q: &str) -> Vec<(String, Value)> {
    let mut rows: Vec<(String, Value)> = db
        .session()
        .query(q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::String(s) => (s.clone(), r.values()[1].clone()),
            other => panic!("expected a name, got {other:?}"),
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

#[tokio::test]
async fn count_subquery_is_evaluated_per_row() {
    let db = fixture().await;
    // One row per `n`, each with its own count — not one grouped row, which is
    // what treating it as an aggregate would have produced.
    let got = by_name(
        &db,
        "MATCH (n:P) RETURN n.name AS n, COUNT { MATCH (n)-[:KNOWS]->(b:P) RETURN b } AS c",
    )
    .await;
    assert_eq!(
        got,
        vec![
            ("a".to_string(), Value::Int(2)),
            ("b".to_string(), Value::Int(0)),
            ("c".to_string(), Value::Int(0)),
        ]
    );
}

#[tokio::test]
async fn collect_subquery_is_evaluated_per_row() {
    let db = fixture().await;
    let got = by_name(
        &db,
        "MATCH (n:P) RETURN n.name AS n, \
         COLLECT { MATCH (n)-[:KNOWS]->(b:P) RETURN b.name } AS l",
    )
    .await;
    let mut a_list = match &got[0].1 {
        Value::List(items) => items.clone(),
        other => panic!("expected a list, got {other:?}"),
    };
    a_list.sort_by_key(|v| format!("{v:?}"));
    assert_eq!(
        a_list,
        vec![
            Value::String("b".to_string()),
            Value::String("c".to_string())
        ]
    );
    assert_eq!(got[1].1, Value::List(vec![]));
    assert_eq!(got[2].1, Value::List(vec![]));
}

/// Uncorrelated bodies are still per-row, they just give the same answer each
/// time. Pinned because an aggregate would collapse the outer rows instead.
#[tokio::test]
async fn an_uncorrelated_subquery_does_not_collapse_the_outer_rows() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (n:P) RETURN n.name AS n, \
             COUNT { MATCH (a:P)-[:KNOWS]->(b:P) RETURN b } AS c",
        )
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 3, "one row per outer row");
    for row in r.rows() {
        assert_eq!(row.values()[1], Value::Int(2));
    }
}

/// The control: `EXISTS { … }` shares the operator now, so a regression in the
/// shared path shows up here too.
#[tokio::test]
async fn exists_subquery_still_works() {
    let db = fixture().await;
    let got = by_name(
        &db,
        "MATCH (n:P) RETURN n.name AS n, EXISTS { MATCH (n)-[:KNOWS]->(b:P) } AS e",
    )
    .await;
    assert_eq!(
        got,
        vec![
            ("a".to_string(), Value::Bool(true)),
            ("b".to_string(), Value::Bool(false)),
            ("c".to_string(), Value::Bool(false)),
        ]
    );
}

/// A body returning more than one column is a query error, not an arbitrary
/// pick of the first — which would make the answer depend on column order.
#[tokio::test]
async fn a_multi_column_collect_body_is_rejected() {
    let db = fixture().await;
    let res = db
        .session()
        .query("MATCH (n:P) RETURN COLLECT { MATCH (n)-[:KNOWS]->(b:P) RETURN b.name, b } AS l")
        .await;
    assert!(
        res.is_err(),
        "COLLECT {{ … }} must return exactly one column; got {:?}",
        res.map(|r| r.rows().len())
    );
}

/// A subquery in an expression position cannot write.
#[tokio::test]
async fn a_mutating_subquery_body_is_rejected() {
    let db = fixture().await;
    let res = db
        .session()
        .query("MATCH (n:P) RETURN COUNT { MATCH (n) CREATE (:P {name:'x'}) RETURN n } AS c")
        .await;
    assert!(res.is_err(), "an updating clause must be rejected");
}
