// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A negated IS-reference whose `TO` target lands on a **scalar** column of the
//! negated rule excluded nothing — the negation failed open.
//!
//! `apply_anti_join_composite` keys on node identity: it builds the banned set
//! from `UInt64` columns and skips any row whose key columns are not VIDs. A
//! `TO` target on a FOLD output is `Float64`, so *every* negated fact was
//! skipped, `banned` came out empty, and the early return handed back every
//! body row unfiltered. `b IS NOT stake` (no target) correctly kept only the
//! nodes with no `stake` fact; adding `TO agg` kept all of them.
//!
//! An unbound `TO` target is existentially quantified — `b IS NOT stake TO agg`
//! reads "no `stake` fact for `b`, whatever `agg` is" — so the fix drops such a
//! component from the composite key rather than comparing it. The node-target
//! case below is the control: there the target *is* a VID and must stay in the
//! key, or the negation over-excludes.

use std::time::Duration;

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::LocyConfig;

fn default_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

async fn rule_rows(db: &Uni, program: &str, rule: &str) -> Result<usize> {
    let result = db
        .session()
        .locy_with(program)
        .with_config(default_config())
        .run()
        .await?;
    Ok(result
        .derived
        .get(rule)
        .map(|rows| rows.len())
        .unwrap_or_default())
}

/// `stake` has a fact for `n1` and `n2` only, so the negation must keep `n0`
/// alone — whether or not the value column is named with `TO`.
#[tokio::test]
async fn is_not_with_scalar_to_target_still_excludes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (n0:E {uid: 'n0'}), (n1:E {uid: 'n1'}), (n2:E {uid: 'n2'}), \
                (n0)-[:L {w: 70.0}]->(n1), (n0)-[:L {w: 30.0}]->(n2)",
    )
    .await?;
    tx.commit().await?;

    const STAKE: &str = "CREATE RULE stake AS \
         MATCH (a:E)-[l:L]->(b:E) \
         FOLD agg = MSUM(l.w) \
         YIELD KEY b, agg \n";

    let no_target = rule_rows(
        &db,
        &format!("{STAKE}CREATE RULE t AS MATCH (b:E) WHERE b IS NOT stake YIELD KEY b"),
        "t",
    )
    .await?;
    let scalar_target = rule_rows(
        &db,
        &format!("{STAKE}CREATE RULE t AS MATCH (b:E) WHERE b IS NOT stake TO agg YIELD KEY b"),
        "t",
    )
    .await?;

    assert_eq!(no_target, 1, "control: only n0 has no stake fact");
    assert_eq!(
        scalar_target, 1,
        "an unbound scalar TO target is existentially quantified — it must not \
         turn the negation into a no-op"
    );
    Ok(())
}

/// Control for the composite-key case the scalar fix must not disturb: when the
/// `TO` target is a node column, it stays in the key, so the negation excludes
/// the specific `(a, b)` pair rather than every `a` with any partner.
#[tokio::test]
async fn is_not_with_node_to_target_keeps_the_composite_key() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    // a0 knows d0 but not d1; a1 knows nothing.
    tx.execute(
        "CREATE (a0:A {uid: 'a0'}), (a1:A {uid: 'a1'}), \
                (d0:D {uid: 'd0'}), (d1:D {uid: 'd1'}), \
                (a0)-[:KNOWS]->(d0)",
    )
    .await?;
    tx.commit().await?;

    let rows = rule_rows(
        &db,
        "CREATE RULE known AS MATCH (a:A)-[:KNOWS]->(d:D) YIELD KEY a, KEY d \n\
         CREATE RULE unknown AS MATCH (a:A), (d:D) \
         WHERE a IS NOT known TO d YIELD KEY a, KEY d",
        "unknown",
    )
    .await?;

    // 4 (a, d) pairs minus the single known pair (a0, d0).
    assert_eq!(
        rows, 3,
        "a node TO target must remain a composite-key component: only the \
         (a0, d0) pair is excluded, not every pair with a0"
    );
    Ok(())
}
