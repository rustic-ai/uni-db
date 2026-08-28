//! `ORDER BY` over strings that begin with `P` — #186.
//!
//! #186 reported that `ORDER BY` over a *traversal* was non-deterministic, and
//! that a plain scan-plus-sort was stable, so "the traversal is what makes the
//! difference". Both halves of that are wrong, and the reproduction that shows
//! it is the last test in this file: a plain scan, no traversal anywhere, over
//! the values `p3, p1, p0, p2`, comes back unsorted.
//!
//! What actually differed was the *spelling of the values*. The original
//! reproduction sorted post titles; the scan control sorted something else.
//! `classify_temporal` treated any string beginning with `P`/`p` as an
//! ISO-8601 duration — `paris`, `p0`, a product code — and both duration
//! parsers accepted whatever followed, silently skipping designators with no
//! number and dropping a trailing number with no designator, so every such
//! string parsed as a *zero duration*. Durations have no ordering, so all of
//! them encoded to one identical sort key:
//!
//! ```text
//! "p0" -> 0705800000000000000080000000000000008000000000000000
//! "p1" -> 0705800000000000000080000000000000008000000000000000   (identical)
//! ```
//!
//! With every key equal the sort is a no-op, so rows came back in whatever
//! order the input happened to have. Over a traversal that input order is
//! itself unstable, which is why the bug looked like a traversal bug and looked
//! non-deterministic; over a scan the input order is stable, which is why it
//! looked like "sometimes it works".
//!
//! The consequences are worse than row order. Leading rank byte `0x07` is
//! Temporal and `0x05` is String, so P-strings also sorted *after* every other
//! string; and `ORDER BY … LIMIT n` returned the wrong **rows**, not merely the
//! right rows in the wrong order.
//!
//! Fixed in three places, each of which was independently fail-open:
//! `classify_temporal` now requires a duration *shape*, and
//! `parse_iso8601_duration` / `parse_iso8601_duration_cypher` now reject a
//! designator with no number, a trailing number with no designator, and a
//! duration with no components at all.

use uni_db::{Uni, Value};

/// `n` vertices carrying `values` in the given order.
async fn fixture(label: &str, values: &[&str]) -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute(&format!("CREATE LABEL {label} (k STRING)"))
        .await
        .unwrap();
    for v in values {
        tx.execute(&format!("CREATE (:{label} {{k:'{v}'}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db
}

async fn sorted_keys(db: &Uni, q: &str) -> Vec<String> {
    db.session()
        .query(q)
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect()
}

/// The decisive reproduction: no traversal, and the input order is not the
/// sorted order. This is what proves the trigger is the value's spelling and
/// not the plan shape.
#[tokio::test]
async fn order_by_sorts_strings_that_begin_with_p() {
    let db = fixture("Pk", &["p3", "p1", "p0", "p2"]).await;
    let got = sorted_keys(&db, "MATCH (s:Pk) RETURN s.k AS k ORDER BY s.k").await;
    assert_eq!(got, vec!["p0", "p1", "p2", "p3"]);
}

/// P-strings must interleave with every other string, not sort after them.
/// A duration outranks a string in Cypher's cross-type order, so a
/// misclassified `paris` sorted after `zebra`.
#[tokio::test]
async fn strings_beginning_with_p_are_not_ranked_as_temporals() {
    let db = fixture("Mix", &["zebra", "paris", "alpha", "prague", "porto"]).await;
    let got = sorted_keys(&db, "MATCH (s:Mix) RETURN s.k AS k ORDER BY s.k").await;
    assert_eq!(got, vec!["alpha", "paris", "porto", "prague", "zebra"]);
}

/// The severity that makes this more than a presentation bug: with every sort
/// key equal, `ORDER BY … LIMIT n` returns the wrong *rows*.
#[tokio::test]
async fn order_by_limit_over_p_strings_returns_the_right_rows() {
    let db = fixture("Lim", &["paris", "prague", "porto"]).await;
    let got = sorted_keys(
        &db,
        "MATCH (s:Lim) RETURN s.k AS k ORDER BY s.k DESC LIMIT 2",
    )
    .await;
    assert_eq!(got, vec!["prague", "porto"]);
}

/// The shape that first surfaced it, kept because it is the one #186 reports.
#[tokio::test]
async fn order_by_over_a_traversal_sorts() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL Person (name STRING)")
        .await
        .unwrap();
    tx.execute("CREATE LABEL Post (title STRING)")
        .await
        .unwrap();
    tx.execute("CREATE EDGE TYPE WROTE FROM Person TO Post")
        .await
        .unwrap();
    tx.execute("CREATE (:Person {name:'a'})").await.unwrap();
    for i in 0..8 {
        tx.execute(&format!("CREATE (:Post {{title:'p{i}'}})"))
            .await
            .unwrap();
        tx.execute(&format!(
            "MATCH (a:Person {{name:'a'}}), (p:Post {{title:'p{i}'}}) CREATE (a)-[:WROTE]->(p)"
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();

    let expected: Vec<String> = (0..8).map(|i| format!("p{i}")).collect();
    // Repeated, because the traversal's own row order varies run to run: a
    // single pass could agree by luck, and it was exactly that variability
    // that made the original bug look like a traversal defect.
    for _ in 0..20 {
        let got = sorted_keys(
            &db,
            "MATCH (a:Person)-[:WROTE]->(p:Post) RETURN p.title AS t ORDER BY p.title",
        )
        .await;
        assert_eq!(got, expected);
    }
}

/// Real durations must still be recognised, or the fix would have traded one
/// wrong answer for another.
#[tokio::test]
async fn genuine_iso_durations_are_still_durations() {
    let db = Uni::in_memory().build().await.unwrap();
    for (expr, expect_months, expect_days) in [
        ("duration('P1Y2M3D')", 14_i64, 3_i64),
        ("duration('PT1H30M')", 0, 0),
        ("duration('P1W')", 0, 7),
    ] {
        let r = db
            .session()
            .query(&format!("RETURN {expr}.months AS m, {expr}.days AS d"))
            .await
            .unwrap_or_else(|e| panic!("{expr} failed: {e}"));
        assert_eq!(r.rows()[0].values()[0], Value::Int(expect_months), "{expr}");
        assert_eq!(r.rows()[0].values()[1], Value::Int(expect_days), "{expr}");
    }
}

/// A string that is not a duration must not be accepted as one. Before the
/// fix every one of these parsed as a zero duration.
#[tokio::test]
async fn strings_shaped_unlike_a_duration_are_rejected() {
    let db = Uni::in_memory().build().await.unwrap();
    for s in ["paris", "p0", "P", "PT", "P1Y2X", "PYD"] {
        let r = db
            .session()
            .query(&format!("RETURN duration('{s}') AS d"))
            .await;
        assert!(
            r.is_err(),
            "duration('{s}') should be rejected, got {:?}",
            r.map(|x| x.rows()[0].values()[0].clone())
        );
    }
}
