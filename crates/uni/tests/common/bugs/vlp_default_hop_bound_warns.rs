// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A variable-length pattern with no written upper bound is planned with a
//! default of 100 hops, and truncating at that bound must say so.
//!
//! The two safety caps inside the search (`MAX_FRONTIER_SIZE`,
//! `MAX_PRED_POOL_SIZE`) already push a `QueryWarning` when they abandon a
//! traversal, precisely because results that look complete and are not are the
//! worst kind of answer. The planner-supplied hop ceiling has the same
//! consequence and was silent: on a 150-vertex chain,
//! `MATCH p = (a)-[:R*]->(b)` returned 100 paths with a maximum length of 100 —
//! the true longest being 149 — and an empty `warnings` list.
//!
//! The noise half matters as much as the signal half. A warning that fires on
//! ordinary bounded traversals trains people to ignore the channel, so the
//! quiet cases are asserted just as hard as the loud one.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// A simple chain of `n` vertices: `n0 -> n1 -> ... -> n(n-1)`.
///
/// A chain rather than a mesh on purpose — the longest path is exactly `n - 1`,
/// so "was the answer truncated" has an arithmetic answer rather than a
/// judgement call.
async fn chain(n: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("uid", DataType::String)
        .edge_type("R", &["N"], &["N"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for i in 0..n {
        tx.execute(&format!("CREATE (:N {{uid: 'n{i}'}})")).await?;
    }
    for i in 0..n - 1 {
        let j = i + 1;
        tx.execute(&format!(
            "MATCH (a:N {{uid: 'n{i}'}}), (b:N {{uid: 'n{j}'}}) CREATE (a)-[:R]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// Truncating at the default bound is reported, and the report says what to do.
#[tokio::test]
async fn default_hop_bound_truncation_is_reported() -> Result<()> {
    let db = chain(150).await?;

    let result = db
        .session()
        .query("MATCH p = (a:N {uid: 'n0'})-[:R*]->(b:N) RETURN max(length(p)) AS deepest")
        .await?;

    let deepest: i64 = result.rows()[0].get("deepest")?;
    assert_eq!(
        deepest, 100,
        "the default bound should cap the answer at 100 hops"
    );

    let warnings = result.warnings();
    assert_eq!(
        warnings.len(),
        1,
        "truncating at a bound the user did not write must be reported, got {warnings:?}"
    );
    let text = format!("{:?}", warnings[0]);
    assert!(
        text.contains("incomplete"),
        "the warning must say the results are incomplete: {text}"
    );
    assert!(
        text.contains("upper bound"),
        "the warning must name the remedy: {text}"
    );
    Ok(())
}

/// An explicitly written bound is the user getting what they asked for.
///
/// Including a bound *above* the default: `*1..140` returns paths the bare `*`
/// form cannot reach, which also pins that an explicit bound is honoured rather
/// than clamped to 100.
#[tokio::test]
async fn an_explicit_bound_is_not_reported() -> Result<()> {
    let db = chain(150).await?;
    let session = db.session();

    for (bound, expected_deepest) in [("*1..3", 3i64), ("*1..140", 140)] {
        let result = session
            .query(&format!(
                "MATCH p = (a:N {{uid: 'n0'}})-[:R{bound}]->(b:N) RETURN max(length(p)) AS deepest"
            ))
            .await?;
        let deepest: i64 = result.rows()[0].get("deepest")?;
        assert_eq!(
            deepest, expected_deepest,
            "an explicit bound must be honoured verbatim"
        );
        assert!(
            result.warnings().is_empty(),
            "a bound the user wrote is not a truncation to report ({bound}): {:?}",
            result.warnings()
        );
    }
    Ok(())
}

/// A search that finishes on its own has nothing to report, even with no bound.
///
/// This is the case that would make the warning noise: the overwhelming
/// majority of `[*]` queries run over graphs shallower than 100 hops.
#[tokio::test]
async fn an_unbounded_pattern_that_completes_is_not_reported() -> Result<()> {
    let db = chain(10).await?;
    let session = db.session();

    for query in [
        "MATCH p = (a:N {uid: 'n0'})-[:R*]->(b:N) RETURN count(p) AS c",
        "MATCH (a:N {uid: 'n0'})-[:R*]->(b:N) RETURN count(DISTINCT b) AS c",
    ] {
        let result = session.query(query).await?;
        let count: i64 = result.rows()[0].get("c")?;
        assert_eq!(count, 9, "the chain from n0 reaches nine vertices");
        assert!(
            result.warnings().is_empty(),
            "a completed search must stay quiet: {:?}",
            result.warnings()
        );
    }
    Ok(())
}

/// The same chain with an edge type no schema declares.
async fn schemaless_chain(n: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    for i in 0..n {
        tx.execute(&format!("CREATE (:S {{uid: 's{i}'}})")).await?;
    }
    for i in 0..n - 1 {
        let j = i + 1;
        tx.execute(&format!(
            "MATCH (a:S {{uid: 's{i}'}}), (b:S {{uid: 's{j}'}}) CREATE (a)-[:LINK]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// A schemaless edge type runs a different traversal, which must report the
/// default bound the same way: for path enumeration and for reachability.
#[tokio::test]
async fn default_hop_bound_truncation_is_reported_for_schemaless_edges() -> Result<()> {
    let db = schemaless_chain(150).await?;
    let session = db.session();

    for (query, want) in [
        (
            "MATCH p = (a:S {uid: 's0'})-[:LINK*]->(b:S) RETURN max(length(p)) AS c",
            100i64,
        ),
        (
            "MATCH (a:S {uid: 's0'})-[:LINK*]->(b:S) RETURN count(DISTINCT b) AS c",
            100,
        ),
    ] {
        let result = session.query(query).await?;
        let got: i64 = result.rows()[0].get("c")?;
        assert_eq!(got, want, "{query}: the default bound caps the answer");
        let warnings = result.warnings();
        assert_eq!(
            warnings.len(),
            1,
            "{query}: truncating at the default bound must be reported, got {warnings:?}"
        );
        let text = format!("{:?}", warnings[0]);
        assert!(
            text.contains("incomplete") && text.contains("upper bound"),
            "{query}: {text}"
        );
    }

    // An explicit bound, and a search that completes, stay quiet.
    for query in [
        "MATCH p = (a:S {uid: 's0'})-[:LINK*1..140]->(b:S) RETURN count(p) AS c",
        "MATCH p = (a:S {uid: 's140'})-[:LINK*]->(b:S) RETURN count(p) AS c",
        "MATCH (a:S {uid: 's140'})-[:LINK*]->(b:S) RETURN count(DISTINCT b) AS c",
    ] {
        let result = session.query(query).await?;
        assert!(
            result.warnings().is_empty(),
            "{query}: nothing to report, got {:?}",
            result.warnings()
        );
    }
    Ok(())
}
