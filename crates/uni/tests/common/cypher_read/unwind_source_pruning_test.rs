//! `UNWIND` must not carry the list it consumed past itself — #184.
//!
//! `UNWIND xs AS x` expands the list into rows, but every operator above copies
//! its input columns forward, and a traversal copies them once *per fan-out
//! row*. So a collected list of `n` entities, unwound and then traversed, was
//! re-materialised `rows × n` times inside `GraphUnwindStream::build_output_batch`'s
//! `take` over the input columns. On LDBC SNB IC6 at SF1 that arithmetic comes
//! to roughly 14 TB and the allocation aborts the process; inserting a bare
//! `WITH f` after the `UNWIND` makes the identical query answer correctly,
//! because the projection drops the list.
//!
//! The planner now proves the source dead and the operator drops it, so the
//! user does not have to know. These tests are about the half that is easy to
//! get wrong: the cases where the list is **not** dead and pruning it would
//! turn a memory fix into a wrong answer. The pruning itself is pinned by
//! `dead_unwind_source_tests` in the planner, which asserts on the analysis
//! rather than on a memory figure no test can afford to reproduce.

use uni_db::{Uni, Value};

/// `a` knows `b` and `c`; `b` wrote `p1`, `c` wrote `p2`.
async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE LABEL Post (title STRING)")
        .await
        .unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE EDGE TYPE HAS_CREATOR FROM Post TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'}), (:P {name:'c'})")
        .await
        .unwrap();
    tx.execute("CREATE (:Post {title:'p1'}), (:Post {title:'p2'})")
        .await
        .unwrap();
    for (from, to) in [("a", "b"), ("a", "c")] {
        tx.execute(&format!(
            "MATCH (x:P {{name:'{from}'}}), (y:P {{name:'{to}'}}) CREATE (x)-[:KNOWS]->(y)"
        ))
        .await
        .unwrap();
    }
    for (person, post) in [("b", "p1"), ("c", "p2")] {
        tx.execute(&format!(
            "MATCH (x:P {{name:'{person}'}}), (p:Post {{title:'{post}'}}) \
             CREATE (p)-[:HAS_CREATOR]->(x)"
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    db
}

async fn strings(db: &Uni, q: &str) -> Vec<String> {
    let mut out: Vec<String> = db
        .session()
        .query(q)
        .await
        .unwrap_or_else(|e| panic!("{q}\n  failed: {e}"))
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    out.sort();
    out
}

/// LDBC IC6's shape: collect, UNWIND, traverse from the unwound entity. The
/// list is dead after the UNWIND, so it is dropped — and the answer must be
/// exactly what it was when the list rode along.
#[tokio::test]
async fn collect_unwind_traverse_still_answers() {
    let db = fixture().await;
    let got = strings(
        &db,
        "MATCH (person:P {name:'a'})-[:KNOWS]->(friend:P) \
         WITH collect(DISTINCT friend) AS friends \
         UNWIND friends AS f \
         MATCH (f)<-[:HAS_CREATOR]-(post:Post) \
         RETURN post.title AS t",
    )
    .await;
    assert_eq!(got, vec!["p1".to_string(), "p2".to_string()]);
}

/// The list is returned, so it is live and must survive the UNWIND.
#[tokio::test]
async fn a_list_returned_after_unwind_survives() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (person:P {name:'a'})-[:KNOWS]->(friend:P) \
             WITH collect(DISTINCT friend.name) AS names \
             UNWIND names AS n \
             RETURN n AS one, names AS all_of_them",
        )
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 2);
    for row in r.rows() {
        match &row.values()[1] {
            Value::List(items) => assert_eq!(items.len(), 2, "the whole list must still be there"),
            other => panic!("expected the collected list, got {other:?}"),
        }
    }
}

/// `size(names)` reads the list without returning it — still live.
#[tokio::test]
async fn a_list_read_by_a_function_after_unwind_survives() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (person:P {name:'a'})-[:KNOWS]->(friend:P) \
             WITH collect(DISTINCT friend.name) AS names \
             UNWIND names AS n \
             RETURN n AS one, size(names) AS howmany",
        )
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Int(2));
}

/// `RETURN *` names nothing, so the analysis has to stand down rather than
/// conclude the list is unread.
#[tokio::test]
async fn a_wildcard_return_after_unwind_keeps_the_list() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (person:P {name:'a'})-[:KNOWS]->(friend:P) \
             WITH collect(DISTINCT friend.name) AS names \
             UNWIND names AS n \
             RETURN *",
        )
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 2);
    assert!(
        r.columns().iter().any(|c| c == "names"),
        "RETURN * must still carry the list; got columns {:?}",
        r.columns()
    );
}

/// The same list unwound twice: blanking hides each UNWIND from the other, so
/// neither may be treated as the only reader.
#[tokio::test]
async fn a_list_unwound_twice_still_expands_twice() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (person:P {name:'a'})-[:KNOWS]->(friend:P) \
             WITH collect(DISTINCT friend.name) AS names \
             UNWIND names AS n1 \
             UNWIND names AS n2 \
             RETURN count(*) AS c",
        )
        .await
        .unwrap();
    // 2 × 2 — if the first UNWIND had dropped `names`, the second could not run.
    assert_eq!(r.rows()[0].values()[0], Value::Int(4));
}

/// An UNWIND whose source is not a variable has no column to drop.
#[tokio::test]
async fn unwind_over_a_computed_list_is_unaffected() {
    let db = fixture().await;
    let r = db
        .session()
        .query("UNWIND range(1, 4) AS i RETURN count(i) AS c")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(4));
}

/// `count(*)` carries an `Expr::Wildcard` argument. It names nothing extra, so
/// it must not be mistaken for `RETURN *` and stand the analysis down — that
/// would disable pruning for essentially every aggregate query, which is most
/// of the shapes this exists for. LDBC IC6 ends in exactly this form.
#[tokio::test]
async fn count_star_does_not_disable_pruning() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (person:P {name:'a'})-[:KNOWS]->(friend:P) \
             WITH collect(DISTINCT friend) AS friends \
             UNWIND friends AS f \
             MATCH (f)<-[:HAS_CREATOR]-(post:Post) \
             RETURN count(*) AS c",
        )
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(2));
}
