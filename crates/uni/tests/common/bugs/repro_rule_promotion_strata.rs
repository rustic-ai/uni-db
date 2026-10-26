#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/transaction.rs:1096 (finding [8]).
//!
//! Commit-time rule promotion copies only `tx_reg.rules` into the session
//! registry (line 1096: `session_reg.rules.insert(name, rule)`) and omits
//! `strata` (and `sources`). A `LocyRuleRegistry`'s `rules`/`strata` are a
//! pure function of `sources`; registered rules execute ONLY via their
//! strata. A promoted rule sits in `.rules` (so `list()`/`count()` report it)
//! but has no stratum in `session_reg.strata`, so `session.locy("QUERY ...")`
//! never evaluates it.
//!
//! We keep ONE session so the tx promotion target and the query source are
//! the same `session_rule_registry`.

use uni_db::Uni;

#[tokio::test]
async fn promoted_rule_missing_strata_never_evaluates() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("name", uni_db::DataType::String)
        .edge_type("EDGE", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;

    // Seed base facts: A -> B -> C.
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    const RULE: &str = "CREATE RULE reach AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b";

    // Register the rule INSIDE a transaction, then commit → promotion.
    let tx = session.tx().await?;
    tx.rules().register(RULE).await?;
    let commit = tx.commit().await?;
    assert_eq!(
        commit.rules_promoted, 1,
        "the tx-registered rule must be promoted on commit"
    );

    // `list()`/`count()` read only `.rules`, so they REPORT the rule present.
    assert!(
        session.rules().list().contains(&"reach".to_string()),
        "session.rules().list() reports the promoted rule"
    );

    // Now evaluate a QUERY over the promoted rule on the SAME session.
    let promoted_result = session.locy("QUERY reach WHERE a.name = 'A'").await;

    // Observe how many `reach` facts the promoted rule produced.
    let promoted_count = match &promoted_result {
        Ok(r) => r
            .derived_rules()
            .await
            .unwrap()
            .get("reach")
            .map(|f| f.len())
            .unwrap_or(0),
        Err(_) => 0,
    };

    // Positive control: register the IDENTICAL rule directly at the session
    // level (rebuilds strata from sources) on a fresh db and confirm it DOES
    // evaluate — isolating the omitted-strata promotion as the cause.
    let db2 = Uni::in_memory().build().await?;
    db2.schema()
        .label("Node")
        .property("name", uni_db::DataType::String)
        .edge_type("EDGE", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;
    let session2 = db2.session();
    let tx2 = session2.tx().await?;
    tx2.execute(
        "CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})",
    )
    .await?;
    tx2.commit().await?;
    db2.flush().await?;
    session2.rules().register(RULE).await?;
    let control = session2.locy("QUERY reach WHERE a.name = 'A'").await?;
    let control_count = control
        .derived_rules()
        .await
        .unwrap()
        .get("reach")
        .map(|f| f.len())
        .unwrap_or(0);

    assert!(
        control_count >= 2,
        "control: session-level registration evaluates the rule (got {control_count} facts)"
    );

    // FIXED (transaction.rs): promotion rebuilds the session registry from the
    // combined sources, so the promoted rule gets its strata and evaluates like
    // the control.
    assert_eq!(
        promoted_count, control_count,
        "promoted rule must evaluate like the control ({control_count} facts), got {promoted_count}"
    );
    assert!(promoted_count >= 2, "promoted rule must produce facts");

    Ok(())
}
