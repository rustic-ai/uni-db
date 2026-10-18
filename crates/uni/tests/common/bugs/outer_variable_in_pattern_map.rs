// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A pattern property map may name a variable bound by an earlier clause.
//!
//! `MATCH (s:S) MATCH (a:N)-[:R]->(b:N {uid: s.name})` failed to plan with
//! `No field named s` (and the `OPTIONAL MATCH` form likewise), while the
//! same constraint spelled `WHERE b.uid = s.name` planned and answered. A
//! pattern map is a predicate over the row, so both spellings must agree.
//!
//! Each probe is differential against its explicit-`WHERE` twin, on
//! unflushed and flushed data, for `MATCH` and `OPTIONAL MATCH`. An absolute
//! anchor pins the twin.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(outer_variable_in_pattern_map)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{Session, Uni};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    for ddl in [
        "CREATE LABEL N (id INT, uid STRING, x INT)",
        "CREATE LABEL S (name STRING, k INT)",
        "CREATE EDGE TYPE R (w INT) FROM N TO N",
        "CREATE INDEX idx_n_uid FOR (n:N) ON (n.uid)",
    ] {
        tx.execute(ddl).await?;
    }
    for i in 0..10 {
        tx.execute(&format!(
            "CREATE (:N {{id: {i}, uid: 'u{i}', x: {}}})",
            i % 2
        ))
        .await?;
    }
    // `R` is declared (typed traversal); `Q` is not (schemaless traversal).
    for i in 0..9 {
        tx.execute(&format!(
            "MATCH (a:N {{id: {i}}}), (b:N {{id: {}}}) \
             CREATE (a)-[:R {{w: {}}}]->(b), (a)-[:Q {{w: {}}}]->(b)",
            i + 1,
            i % 3,
            i % 3
        ))
        .await?;
    }
    tx.execute(
        "CREATE (:S {name: 'u3', k: 1}), (:S {name: 'u5', k: 2}), \
         (:S {name: 'nope', k: 0}), (:S {k: 1})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn bag(session: &Session, q: &str) -> Result<Vec<String>> {
    let r = session
        .query(q)
        .await
        .map_err(|e| anyhow::anyhow!("`{q}` failed: {e}"))?;
    let mut rows: Vec<String> = r
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    rows.sort();
    Ok(rows)
}

/// `(inline-map form, explicit WHERE form)`. `{O}` is `MATCH` or
/// `OPTIONAL MATCH`.
const PAIRS: &[(&str, &str)] = &[
    (
        "MATCH (s:S) {O} (a:N)-[:R]->(b:N {uid: s.name}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[:R]->(b:N) WHERE b.uid = s.name RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N)-[:R]->(b:N {uid: s.name, x: 1}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[:R]->(b:N) WHERE b.uid = s.name AND b.x = 1 \
         RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N)-[:R]->(b {uid: s.name}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[:R]->(b) WHERE b.uid = s.name RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N)-[:R]->(:N)-[:R]->(b:N {uid: s.name}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[:R]->(:N)-[:R]->(b:N) WHERE b.uid = s.name \
         RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N {uid: s.name})-[:R]->(b:N) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[:R]->(b:N) WHERE a.uid = s.name RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N)-[:R {w: s.k}]->(b:N) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[r:R]->(b:N) WHERE r.w = s.k RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N)-[:R*1..2]->(b:N {uid: s.name}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[:R*1..2]->(b:N) WHERE b.uid = s.name RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) WITH s.name AS k {O} (a:N)-[:R]->(b:N {uid: k}) RETURN k, a.id, b.id",
        "MATCH (s:S) WITH s.name AS k {O} (a:N)-[:R]->(b:N) WHERE b.uid = k RETURN k, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N)-[:Q]->(b:N {uid: s.name}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[:Q]->(b:N) WHERE b.uid = s.name RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N)-[:Q*1..2]->(b:N {uid: s.name}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)-[:Q*1..2]->(b:N) WHERE b.uid = s.name RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N) ((x)-[:R]->(y)){1,2} (b:N {uid: s.name}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N) ((x)-[:R]->(y)){1,2} (b:N) WHERE b.uid = s.name \
         RETURN s.name, a.id, b.id",
    ),
    (
        "MATCH (s:S) {O} (a:N)<-[:R]-(b:N {uid: s.name}) RETURN s.name, a.id, b.id",
        "MATCH (s:S) {O} (a:N)<-[:R]-(b:N) WHERE b.uid = s.name RETURN s.name, a.id, b.id",
    ),
];

#[tokio::test]
async fn outer_variable_in_pattern_map_matches_where_form() -> Result<()> {
    let db = open().await?;
    let session = db.session();

    // Anchor the twin of the reported query.
    assert_eq!(
        bag(
            &session,
            "MATCH (s:S) MATCH (a:N)-[:R]->(b:N) WHERE b.uid = s.name RETURN s.name, a.id, b.id"
        )
        .await?,
        vec![
            "[String(\"u3\"), Int(2), Int(3)]",
            "[String(\"u5\"), Int(4), Int(5)]"
        ]
    );

    // ... and its OPTIONAL twin, which must null-extend the two `S` rows
    // that have no such `b`, so the OPTIONAL differentials are not vacuous.
    assert_eq!(
        bag(
            &session,
            "MATCH (s:S) OPTIONAL MATCH (a:N)-[:R]->(b:N) WHERE b.uid = s.name \
             RETURN s.name, a.id, b.id"
        )
        .await?,
        vec![
            "[Null, Null, Null]",
            "[String(\"nope\"), Null, Null]",
            "[String(\"u3\"), Int(2), Int(3)]",
            "[String(\"u5\"), Int(4), Int(5)]"
        ]
    );

    let mut failures = Vec::new();
    for state in ["unflushed", "flushed"] {
        for (inline, explicit) in PAIRS {
            for o in ["MATCH", "OPTIONAL MATCH"] {
                let qi = inline.replace("{O}", o);
                let qe = explicit.replace("{O}", o);
                let want = bag(&session, &qe).await?;
                match bag(&session, &qi).await {
                    Ok(got) if got == want => {}
                    Ok(got) => failures.push(format!(
                        "{state}: `{qi}`\n   inline {got:?}\n   where  {want:?}"
                    )),
                    Err(e) => failures.push(format!("{state}: {e}")),
                }
            }
        }
        db.flush().await?;
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
