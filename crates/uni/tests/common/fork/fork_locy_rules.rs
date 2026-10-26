// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 9 — Locy rule registry isolation across forks.
//!
//! Locks in the behavior already established by Phase 1's
//! `UniInner::at_fork`, which deep-clones `locy_rule_registry` from
//! primary into the forked inner. Per the pending-work doc, no
//! substrate change is needed; this test exists so a future regression
//! that breaks fork↔primary registry isolation surfaces immediately.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Build a Uni with the schema the test rules reference.
async fn db_with_schema() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema().edge_type("E", &["N"], &["N"]).apply().await?;
    Ok(db)
}

#[tokio::test]
async fn fork_inherits_primary_db_rules_at_fork_point() -> Result<()> {
    let db = db_with_schema().await?;

    // Register on the primary db registry — this is the inheritable state.
    db.rules()
        .register(
            "CREATE RULE rule_a AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
        )
        .await?;

    let primary = db.session();
    let forked = primary.fork("inherit").await?;

    let primary_rules = primary.rules().list();
    let fork_rules = forked.rules().list();

    assert!(
        primary_rules.contains(&"rule_a".to_string()),
        "primary should see rule_a; got {primary_rules:?}"
    );
    assert!(
        fork_rules.contains(&"rule_a".to_string()),
        "fork should inherit rule_a from fork point; got {fork_rules:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_local_rules_do_not_leak_to_primary() -> Result<()> {
    let db = db_with_schema().await?;
    let primary = db.session();
    let forked = primary.fork("isolated").await?;

    // Fork registers its own rule.
    forked
        .rules()
        .register(
            "CREATE RULE fork_only AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
        )
        .await?;

    let fork_rules = forked.rules().list();
    let primary_rules = primary.rules().list();
    let db_rules = db.rules().list();

    assert!(
        fork_rules.contains(&"fork_only".to_string()),
        "fork should see its own rule; got {fork_rules:?}"
    );
    assert!(
        !primary_rules.contains(&"fork_only".to_string()),
        "primary session must not see fork's rule; got {primary_rules:?}"
    );
    assert!(
        !db_rules.contains(&"fork_only".to_string()),
        "primary db registry must not see fork's rule; got {db_rules:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn primary_rules_added_after_fork_are_invisible_to_fork() -> Result<()> {
    let db = db_with_schema().await?;
    let primary = db.session();
    let forked = primary.fork("snapshot").await?;

    // Register on primary db AFTER fork is created — fork must not see it
    // (snapshot semantics at fork point).
    db.rules()
        .register(
            "CREATE RULE post_fork AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
        )
        .await?;

    let fork_rules = forked.rules().list();
    let db_rules = db.rules().list();

    assert!(
        db_rules.contains(&"post_fork".to_string()),
        "primary db should see its own newly-registered rule; got {db_rules:?}"
    );
    assert!(
        !fork_rules.contains(&"post_fork".to_string()),
        "fork must not see primary rules added after fork point; got {fork_rules:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_and_primary_each_see_own_plus_inherited() -> Result<()> {
    let db = db_with_schema().await?;

    // Pre-fork shared rule.
    db.rules()
        .register(
            "CREATE RULE shared AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
        )
        .await?;

    let primary = db.session();
    let forked = primary.fork("each_own").await?;

    // Each side adds its own rule after fork.
    db.rules()
        .register(
            "CREATE RULE primary_only AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
        )
        .await?;
    forked
        .rules()
        .register(
            "CREATE RULE fork_only AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
        )
        .await?;

    let db_rules = db.rules().list();
    let fork_rules = forked.rules().list();

    // Primary db: shared + primary_only, not fork_only.
    assert!(db_rules.contains(&"shared".to_string()));
    assert!(db_rules.contains(&"primary_only".to_string()));
    assert!(
        !db_rules.contains(&"fork_only".to_string()),
        "primary db must not see fork's rule; got {db_rules:?}"
    );

    // Fork: shared (inherited) + fork_only, not primary_only (post-fork).
    assert!(
        fork_rules.contains(&"shared".to_string()),
        "fork should inherit shared rule; got {fork_rules:?}"
    );
    assert!(
        fork_rules.contains(&"fork_only".to_string()),
        "fork should see its own rule; got {fork_rules:?}"
    );
    assert!(
        !fork_rules.contains(&"primary_only".to_string()),
        "fork must not see primary's post-fork rule; got {fork_rules:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// #97 regression: a Locy QUERY on a fork must resolve over data the
/// parent committed but never flushed (L0). Before the fix the fork
/// branched off an empty Lance tip, so the rule matched zero inherited
/// nodes/edges and `derived` came back empty.
#[tokio::test]
async fn fork_locy_derive_over_inherited_unflushed_l0() -> Result<()> {
    let db = db_with_schema().await?;

    // Commit graph data WITHOUT flushing — it lives only in L0.
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute(
        "CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'}), \
         (:N {name: 'B2'})-[:E]->(:N {name: 'C'})",
    )
    .await?;
    tx.commit().await?;

    // Register the rule on the inheritable primary registry, then fork.
    db.rules()
        .register(
            "CREATE RULE connected AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
        )
        .await?;

    let forked = primary.fork("locy_unflushed").await?;
    let result = forked.locy("QUERY connected").await?;

    assert!(
        result
            .derived_rules()
            .await
            .unwrap()
            .contains_key("connected"),
        "fork QUERY should produce the 'connected' relation"
    );
    let facts = &result.derived_rules().await.unwrap()["connected"];
    assert_eq!(
        facts.len(),
        2,
        "rule must match both inherited unflushed edges; got {}",
        facts.len()
    );

    db.shutdown().await?;
    Ok(())
}
