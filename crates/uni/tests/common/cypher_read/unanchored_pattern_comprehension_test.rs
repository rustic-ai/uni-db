//! A pattern comprehension whose pattern variables are all fresh.
//!
//! `[(a:P)-[:KNOWS]->(b:P) | a.name]` binds nothing from the outer scope. The
//! vectorized operator anchors on the first pattern node that already has a
//! `{var}._vid` column in the input schema, so with everything fresh it had no
//! anchor and the query failed outright:
//!
//! ```text
//! No anchor node found in pattern comprehension. None of the pattern variables
//! have a corresponding `_vid` column in the input schema.
//! ```
//!
//! All the forms below are legal openCypher. The openCypher TCK's eleven
//! pattern-comprehension scenarios
//! (`crates/uni-tck/tck/features/expressions/pattern/Pattern2.feature`) *all*
//! anchor on a variable bound by an outer `MATCH`, so passing them establishes
//! "anchored pattern comprehensions work" — not "pattern comprehensions work".
//! These tests cover what the suite structurally cannot see.
//!
//! Note the contrast with pattern *predicates*, which already worked:
//! `MATCH (n:P) WHERE (:P)-[:KNOWS]->(:P) RETURN n.name` returns all rows,
//! because `compile_pattern_exists` falls back to a general subquery path when
//! anchoring fails. Comprehensions had no such fallback.

use uni_db::{Uni, Value};

/// `a` knows `b`; `c` knows nobody. One KNOWS edge in total.
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

fn as_list(v: &Value) -> Vec<Value> {
    match v {
        Value::List(items) => items.clone(),
        other => panic!("expected a list, got {other:?}"),
    }
}

/// The anchored form, which has always worked. Guards against a regression in
/// the vectorized path while the unanchored one is added beside it.
#[tokio::test]
async fn anchored_comprehension_still_works() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (n:P) RETURN n.name AS name, [(n)-[:KNOWS]->(b) | b.name] AS l")
        .await
        .unwrap();
    let mut got: Vec<(String, usize)> = r
        .rows()
        .iter()
        .map(|row| match &row.values()[0] {
            Value::String(s) => (s.clone(), as_list(&row.values()[1]).len()),
            other => panic!("expected a name, got {other:?}"),
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 0),
            ("c".to_string(), 0)
        ]
    );
}

/// No outer scope at all.
#[tokio::test]
async fn unanchored_with_no_outer_match() {
    let db = fixture().await;
    let r = db
        .session()
        .query("RETURN [(a:P)-[:KNOWS]->(b:P) | a.name] AS l")
        .await
        .unwrap();
    assert_eq!(
        as_list(&r.rows()[0].values()[0]),
        vec![Value::String("a".to_string())]
    );
}

/// An outer scope exists, but the comprehension references none of it — so the
/// same list must come back for every outer row. This is also the invariant a
/// later "evaluate once and broadcast" rewrite has to preserve.
#[tokio::test]
async fn uncorrelated_yields_the_same_list_for_every_row() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (n:P) RETURN [(a:P)-[:KNOWS]->(b:P) | a.name] AS l")
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 3, "one row per :P node");
    for row in r.rows() {
        assert_eq!(
            as_list(&row.values()[0]),
            vec![Value::String("a".to_string())]
        );
    }
}

/// The IC10-adjacent shape: an unanchored comprehension measured by `size()`.
#[tokio::test]
async fn unanchored_inside_size() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (n:P) RETURN size([(a:P)-[:KNOWS]->(b:P) | 1]) AS c")
        .await
        .unwrap();
    for row in r.rows() {
        assert_eq!(row.values()[0], Value::Int(1));
    }
}

/// Correlated, but not by an equality that could pin an anchor. The result must
/// differ per outer row, which is what distinguishes this from the uncorrelated
/// case above.
#[tokio::test]
async fn correlated_by_a_non_equality_predicate() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (n:P) RETURN n.name AS name, \
             [(a:P)-[:KNOWS]->(b:P) WHERE a.name >= n.name | a.name] AS l",
        )
        .await
        .unwrap();
    let mut got: Vec<(String, usize)> = r
        .rows()
        .iter()
        .map(|row| match &row.values()[0] {
            Value::String(s) => (s.clone(), as_list(&row.values()[1]).len()),
            other => panic!("expected a name, got {other:?}"),
        })
        .collect();
    got.sort();
    // The only match has a.name = 'a', so it survives only for n.name <= 'a'.
    assert_eq!(
        got,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 0),
            ("c".to_string(), 0)
        ]
    );
}

/// An unanchored comprehension that matches nothing yields an empty list, not a
/// dropped row and not null.
#[tokio::test]
async fn unanchored_with_no_matches_is_an_empty_list() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (n:P) RETURN n.name AS name, [(a:P)-[:KNOWS]->(b:P) WHERE a.name = 'zz' | a.name] AS l")
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 3);
    for row in r.rows() {
        assert!(as_list(&row.values()[1]).is_empty());
    }
}

/// Unanchored pattern *predicates* already worked via the EXISTS fallback. Kept
/// as the control that establishes the asymmetry this work removes.
#[tokio::test]
async fn unanchored_pattern_predicate_control() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (n:P) WHERE (:P)-[:KNOWS]->(:P) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 3);
}

/// A pattern variable must shadow an outer column of the same name.
///
/// The fallback declares the outer row's bare columns in the subquery's scope so
/// the comprehension can correlate through any value, not only through a property
/// access. Without excluding the pattern's own bindings, `a` here would resolve to
/// the outer `a` and the pattern would stop being a fresh match.
#[tokio::test]
async fn a_pattern_variable_shadows_an_outer_column_of_the_same_name() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (p:P {name:'c'}) WITH p.name AS a \
             RETURN a, [(a:P)-[:KNOWS]->(b:P) | a.name] AS l",
        )
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::String("c".to_string()));
    // The pattern's own `a` is the KNOWS source, not the outer string 'c'.
    assert_eq!(
        as_list(&r.rows()[1 - 1].values()[1]),
        vec![Value::String("a".to_string())]
    );
}

/// Correlating through an outer variable, which the fallback must resolve by
/// declaring the outer row's bare columns in the subquery's scope.
///
/// LDBC SNB IC14 correlates the same way but reaches its outer relationship via
/// `startNode(r).id`. That form cannot be tested here because `startNode(e).name`
/// does not plan at all — `MATCH ()-[e:KNOWS]->() RETURN startNode(e).name` fails
/// with `Schema error: No field named e`, with no comprehension involved. It is
/// an independent defect, reported separately.
#[tokio::test]
async fn correlates_through_an_outer_variable() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (n:P) RETURN n.name AS name, \
             [(a:P)-[:KNOWS]->(b:P) WHERE a.name = n.name | a.name] AS l",
        )
        .await
        .unwrap();
    let mut got: Vec<(String, usize)> = r
        .rows()
        .iter()
        .map(|row| match &row.values()[0] {
            Value::String(s) => (s.clone(), as_list(&row.values()[1]).len()),
            other => panic!("expected a name, got {other:?}"),
        })
        .collect();
    got.sort();
    // Only 'a' is a KNOWS source, so only that outer row correlates.
    assert_eq!(
        got,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 0),
            ("c".to_string(), 0)
        ]
    );
}
