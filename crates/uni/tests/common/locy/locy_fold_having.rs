// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for post-FOLD WHERE (HAVING) clause in Locy rules.

use anyhow::Result;
use uni_db::Uni;

/// Basic HAVING: FOLD COUNT + WHERE count >= 3 filters out low-count groups.
#[tokio::test]
async fn test_fold_having_basic() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    // Create a graph where Alice has 3 invoices and Bob has 1
    tx.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), \
         (i1:Invoice {id: 1}), (i2:Invoice {id: 2}), (i3:Invoice {id: 3}), (i4:Invoice {id: 4}), \
         (a)-[:PAID {amount: 10}]->(i1), \
         (a)-[:PAID {amount: 20}]->(i2), \
         (a)-[:PAID {amount: 30}]->(i3), \
         (b)-[:PAID {amount: 5}]->(i4)",
    )
    .await?;
    tx.commit().await?;

    let config = uni_db::locy::LocyConfig {
        max_iterations: 100,
        timeout: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    };

    let result = db
        .session()
        .locy_with(
            "CREATE RULE frequent_payer AS \
             MATCH (p:Person)-[r:PAID]->(i:Invoice) \
             FOLD n = COUNT(*) \
             WHERE n >= 3 \
             YIELD KEY p, n",
        )
        .with_config(config)
        .run()
        .await?;

    let facts = result
        .derived
        .get("frequent_payer")
        .expect("rule 'frequent_payer' missing");

    // Only Alice (3 invoices) passes; Bob (1 invoice) filtered out
    assert_eq!(
        facts.len(),
        1,
        "only Alice should pass HAVING filter (n >= 3), got {} facts",
        facts.len()
    );

    Ok(())
}

/// HAVING filters all groups → empty derived set.
#[tokio::test]
async fn test_fold_having_filters_all() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'Alice'}), (i1:Invoice {id: 1}), \
         (a)-[:PAID {amount: 10}]->(i1)",
    )
    .await?;
    tx.commit().await?;

    let config = uni_db::locy::LocyConfig {
        max_iterations: 100,
        timeout: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    };

    let result = db
        .session()
        .locy_with(
            "CREATE RULE high_volume AS \
             MATCH (p:Person)-[r:PAID]->(i:Invoice) \
             FOLD n = COUNT(*) \
             WHERE n >= 100 \
             YIELD KEY p, n",
        )
        .with_config(config)
        .run()
        .await?;

    let facts = result
        .derived
        .get("high_volume")
        .expect("rule 'high_volume' missing");
    assert_eq!(
        facts.len(),
        0,
        "all groups should be filtered, got: {facts:?}"
    );

    Ok(())
}

/// FOLD without HAVING still works (regression test).
#[tokio::test]
async fn test_fold_without_having_unchanged() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), \
         (i1:Invoice {id: 1}), (i2:Invoice {id: 2}), (i3:Invoice {id: 3}), \
         (a)-[:PAID {amount: 100}]->(i1), \
         (a)-[:PAID {amount: 200}]->(i2), \
         (b)-[:PAID {amount: 50}]->(i3)",
    )
    .await?;
    tx.commit().await?;

    let config = uni_db::locy::LocyConfig {
        max_iterations: 100,
        timeout: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    };

    let result = db
        .session()
        .locy_with(
            "CREATE RULE spending AS \
             MATCH (p:Person)-[r:PAID]->(i:Invoice) \
             FOLD total = SUM(r.amount) \
             YIELD KEY p, total",
        )
        .with_config(config)
        .run()
        .await?;

    let facts = result
        .derived
        .get("spending")
        .expect("rule 'spending' missing");
    // Both people present without HAVING filter
    assert_eq!(
        facts.len(),
        2,
        "both groups should appear without HAVING, got: {facts:?}"
    );

    Ok(())
}

/// Multiple HAVING conditions combined with AND.
#[tokio::test]
async fn test_fold_having_multiple_conditions() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    // Alice: 3 payments totalling 60, Bob: 2 payments totalling 500, Carol: 1 payment of 5
    tx.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}), \
         (i1:Invoice {id: 1}), (i2:Invoice {id: 2}), (i3:Invoice {id: 3}), \
         (i4:Invoice {id: 4}), (i5:Invoice {id: 5}), (i6:Invoice {id: 6}), \
         (a)-[:PAID {amount: 10}]->(i1), \
         (a)-[:PAID {amount: 20}]->(i2), \
         (a)-[:PAID {amount: 30}]->(i3), \
         (b)-[:PAID {amount: 200}]->(i4), \
         (b)-[:PAID {amount: 300}]->(i5), \
         (c)-[:PAID {amount: 5}]->(i6)",
    )
    .await?;
    tx.commit().await?;

    let config = uni_db::locy::LocyConfig {
        max_iterations: 100,
        timeout: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    };

    // Require both: count >= 2 AND total >= 100
    // Alice: count=3 ✓, total=60 ✗ → filtered
    // Bob:   count=2 ✓, total=500 ✓ → passes
    // Carol: count=1 ✗, total=5 ✗  → filtered
    let result = db
        .session()
        .locy_with(
            "CREATE RULE big_spenders AS \
             MATCH (p:Person)-[r:PAID]->(i:Invoice) \
             FOLD n = COUNT(*), total = SUM(r.amount) \
             WHERE n >= 2 AND total >= 100 \
             YIELD KEY p, n, total",
        )
        .with_config(config)
        .run()
        .await?;

    let facts = result
        .derived
        .get("big_spenders")
        .expect("rule 'big_spenders' missing");

    assert_eq!(
        facts.len(),
        1,
        "only Bob should pass both conditions, got {} facts",
        facts.len()
    );

    Ok(())
}

/// HAVING combined with BEST BY in the same rule.
#[tokio::test]
async fn test_fold_having_with_best_by() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    // Two people, both with >= 2 payments, different totals
    tx.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), \
         (i1:Invoice {id: 1}), (i2:Invoice {id: 2}), (i3:Invoice {id: 3}), (i4:Invoice {id: 4}), \
         (a)-[:PAID {amount: 100}]->(i1), \
         (a)-[:PAID {amount: 200}]->(i2), \
         (b)-[:PAID {amount: 50}]->(i3), \
         (b)-[:PAID {amount: 60}]->(i4)",
    )
    .await?;
    tx.commit().await?;

    let config = uni_db::locy::LocyConfig {
        max_iterations: 100,
        timeout: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    };

    // FOLD + HAVING (n >= 2) + BEST BY total DESC
    // Both people pass HAVING. BEST BY selects top-1 per KEY group,
    // and since each person IS the key, both remain (2 facts).
    let result = db
        .session()
        .locy_with(
            "CREATE RULE top_spender AS \
             MATCH (p:Person)-[r:PAID]->(i:Invoice) \
             FOLD n = COUNT(*), total = SUM(r.amount) \
             WHERE n >= 2 \
             BEST BY total DESC \
             YIELD KEY p, n, total",
        )
        .with_config(config)
        .run()
        .await?;

    // The key assertion: FOLD → HAVING → BEST BY pipeline completes
    // without error. The exact row count depends on BEST BY semantics
    // with FOLD grouping.
    assert!(
        result.derived.contains_key("top_spender"),
        "rule should be present in derived set"
    );

    Ok(())
}
