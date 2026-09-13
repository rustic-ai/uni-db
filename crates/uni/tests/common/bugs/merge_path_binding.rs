// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `MERGE p = (a)-[:R]->(b)` must bind and measure `p` like `MATCH` does.
//!
//! Two separate defects met here, and the first hid the second.
//!
//! **`length()` measured a path's JSON shape.** `cypher_size_scalar` had arms
//! for strings, lists, maps, nodes and edges, but none for a path, so a path
//! fell to the catch-all that renders it as `{nodes, relationships}` and
//! returns that object's *key count*. Every path, of every length, measured 2.
//! Plausible enough to survive: a one-hop path has two nodes, and 2 came back.
//!
//! **A matched MERGE dropped the relationship from `p`.** The relationship fast
//! path's match branch binds no edge — the relationship is anonymous, so
//! nothing lands in the row — and `bind_path_variables` pushes an edge only for
//! a relationship with a bound variable while requiring just
//! `!nodes.is_empty()`. So `p` came back with its nodes and no relationships.
//! The fast path now declines a pattern carrying a path variable and leaves it
//! to the general path, which materialises the match rows.
//!
//! The first defect masked the second: comparing `length(p)` across the created
//! and matched arms showed 2 and 2, which reads as "no discrepancy" when the
//! underlying paths differ by an entire relationship.

use anyhow::Result;
use uni_db::Uni;

async fn two_nodes() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE R FROM E TO E").await?;
    tx.execute("CREATE (:E {uid: 'a'}), (:E {uid: 'b'})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

const MERGE_PATH: &str = "MATCH (a:E {uid:'a'}), (b:E {uid:'b'}) \
                          MERGE p = (a)-[:R]->(b) \
                          RETURN length(p) AS len";

#[tokio::test]
async fn a_merged_path_measures_and_binds_like_a_matched_one() -> Result<()> {
    let db = two_nodes().await?;

    // First run creates the relationship, second matches it. Both bind `p`.
    let mut seen = Vec::new();
    for _ in 0..2 {
        let tx = db.session().tx().await?;
        let r = tx.query(MERGE_PATH).await?;
        tx.commit().await?;
        seen.push(r.rows()[0].get::<i64>("len")?);
    }

    // The same path bound by MATCH, which never goes through MERGE's binding.
    let reference_rows = db
        .session()
        .query("MATCH p = (:E {uid:'a'})-[:R]->(:E {uid:'b'}) RETURN length(p) AS len")
        .await?;
    let reference = reference_rows.rows()[0].get::<i64>("len")?;

    eprintln!(
        "length(p): created={}  matched={}  match-reference={reference}",
        seen[0], seen[1]
    );

    assert_eq!(
        reference, 1,
        "the MATCH reference itself is wrong, so the comparison below is \
         meaningless: a one-hop path has length 1"
    );
    assert_eq!(
        seen[0], reference,
        "a MERGE that CREATED the relationship bound or measured `p` \
         differently from MATCH"
    );
    assert_eq!(
        seen[1], reference,
        "a MERGE that MATCHED the relationship bound or measured `p` \
         differently from MATCH — the match branch dropped the relationship"
    );

    // Exactly one relationship exists: the second run matched, it did not
    // create a second one.
    let n: i64 = db
        .session()
        .query("MATCH ()-[e:R]->() RETURN count(e) AS n")
        .await?
        .rows()[0]
        .get("n")?;
    assert_eq!(n, 1, "the second MERGE created a duplicate relationship");
    Ok(())
}

/// `length()` counts relationships, on a longer path as well as a one-hop.
///
/// Guards the arm directly. A one-hop path measuring 2 was the original
/// symptom, and a two-hop path measures 2 as well — so the one-hop case alone
/// could be "fixed" by any change that happens to return 1, and the two-hop
/// case is what pins it to the relationship count rather than a coincidence.
#[tokio::test]
async fn length_of_a_path_counts_relationships() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE R FROM E TO E").await?;
    tx.execute(
        "CREATE (a:E {uid:'a'}), (b:E {uid:'b'}), (c:E {uid:'c'}), \
         (a)-[:R]->(b), (b)-[:R]->(c)",
    )
    .await?;
    tx.commit().await?;

    for (cypher, want) in [
        (
            "MATCH p = (:E {uid:'a'})-[:R]->(:E {uid:'b'}) RETURN length(p) AS n",
            1,
        ),
        (
            "MATCH p = (:E {uid:'a'})-[:R]->()-[:R]->(:E {uid:'c'}) RETURN length(p) AS n",
            2,
        ),
    ] {
        let got: i64 = db.session().query(cypher).await?.rows()[0].get("n")?;
        assert_eq!(
            got, want,
            "length() returned {got}, want {want}, for: {cypher}"
        );
    }
    Ok(())
}

/// `nodes(p)` and `relationships(p)` must return a path's parts, not Null.
///
/// Both UDFs handled only the legacy map encoding of a path and fell to
/// `_ => Value::Null` otherwise, so they emptied silently wherever a path
/// arrived as a `Value::Path`. `expr_eval`'s versions have always had the arm;
/// only the DataFusion ones were missing it — the same one-value-two-encodings
/// split that made `length()` measure a path's JSON key count.
///
/// Asserted against a two-hop path so the two counts differ (3 nodes, 2
/// relationships). With a one-hop path both sides of a confusion between them
/// would read 2 and 1 either way round.
#[tokio::test]
async fn nodes_and_relationships_return_a_paths_parts() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE R FROM E TO E").await?;
    tx.execute(
        "CREATE (a:E {uid:'a'}), (b:E {uid:'b'}), (c:E {uid:'c'}), \
         (a)-[:R]->(b), (b)-[:R]->(c)",
    )
    .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query(
            "MATCH p = (:E {uid:'a'})-[:R]->()-[:R]->(:E {uid:'c'}) \
             RETURN size(nodes(p)) AS n, size(relationships(p)) AS r",
        )
        .await?;
    let row = &r.rows()[0];
    assert_eq!(
        row.get::<i64>("n")?,
        3,
        "nodes(p) did not return the path's three nodes"
    );
    assert_eq!(
        row.get::<i64>("r")?,
        2,
        "relationships(p) did not return the path's two relationships"
    );

    // And on a MERGE'd path, which is where the Null was first seen.
    let tx = db.session().tx().await?;
    let merged = tx
        .query(
            "MATCH (a:E {uid:'a'}), (b:E {uid:'b'}) MERGE p = (a)-[:R]->(b) \
             RETURN size(nodes(p)) AS n, size(relationships(p)) AS r",
        )
        .await?;
    tx.commit().await?;
    let row = &merged.rows()[0];
    assert_eq!(row.get::<i64>("n")?, 2, "a MERGE'd path lost its nodes");
    assert_eq!(
        row.get::<i64>("r")?,
        1,
        "a MERGE'd path lost its relationship"
    );
    Ok(())
}

/// `nodes()` / `relationships()` reject a non-path instead of answering Null.
///
/// Returning Null was fail-open in the most misleading way available: it is
/// indistinguishable from "a path with no nodes" and from "this encoding was
/// not recognised". That second reading is not hypothetical — it is exactly
/// what a missing `Value::Path` arm produced here, and why the gap went
/// unnoticed behind a test named after these two functions.
///
/// Null in, null out is kept: that is the Cypher convention for a function
/// applied to a missing value, and the one case where a null answer is an
/// answer rather than a silence.
#[tokio::test]
async fn nodes_and_relationships_reject_a_non_path() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.execute("CREATE (:E {uid: 'a'})").await?;
    tx.commit().await?;

    for call in [
        "nodes(n.uid)",
        "relationships(n.uid)",
        "nodes(1)",
        "relationships(1)",
    ] {
        let err = db
            .session()
            .query(&format!("MATCH (n:E {{uid:'a'}}) RETURN {call} AS x"))
            .await
            .err();
        let msg = err.map(|e| e.to_string()).unwrap_or_default();
        assert!(
            msg.contains("expects a Path"),
            "{call} should be a type error, got: {msg:?}"
        );
    }

    // Null in, null out.
    for call in ["nodes(null)", "relationships(null)"] {
        let r = db
            .session()
            .query(&format!("RETURN {call} IS NULL AS is_null"))
            .await?;
        assert!(
            r.rows()[0].get::<bool>("is_null")?,
            "{call} should be null, not an error"
        );
    }
    Ok(())
}
