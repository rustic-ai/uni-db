// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #300: each positive IS reference anchored on a bound variable is
//! joined by hash, in every conjunct order.
//!
//! The planner puts every test of a reference — each position whose variable
//! is bound elsewhere — into one Filter directly on the reference's
//! CrossJoin, which the physical planner lowers to a multi-key
//! `HashJoinExec`. A test placed anywhere else (above another join, or split
//! across filters) leaves a `CrossJoinExec` whose row volume is the product of
//! its inputs. P1's two references are both anchored on the MATCH variable
//! `o`, and `e`, which `stake` binds, is materialized by a `ScanAll` joined on
//! its vid; so the clause plans with no cross join at all.
//!
//! The physical plan needs a storage-backed planner, so it is read from the
//! operators `profile()` reports rather than from a planner unit test.

// Rust guideline compliant

use anyhow::{Result, ensure};
use uni_db::{DataType, Uni};

const STAKE: &str = "CREATE RULE stake AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE r.pct IS NOT NULL \
    FOLD pct = MAX(r.pct) YIELD KEY o, KEY e, pct\n";
const SEED: &str = "CREATE RULE seed AS MATCH (e:E) WHERE e.uid = 'x' YIELD KEY e, 100.0 AS agg\n";

/// `x -[60]-> a -[60]-> b`.
async fn ownership() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("uid", DataType::String)
        .done()
        .edge_type("OWNS", &["E"], &["E"])
        .property_nullable("pct", DataType::Float64)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (x:E {uid: 'x'}), (a:E {uid: 'a'}), (b:E {uid: 'b'}), \
                (x)-[:OWNS {pct: 60.0}]->(a), (a)-[:OWNS {pct: 60.0}]->(b)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn anchored_is_refs_join_by_hash_in_every_order() -> Result<()> {
    let db = ownership().await?;
    for conjuncts in [
        ["o IS seed", "(o, e, pct) IS stake"],
        ["(o, e, pct) IS stake", "o IS seed"],
    ] {
        let program = format!(
            "{STAKE}{SEED}CREATE RULE r AS MATCH (o:E) WHERE {} AND {} YIELD KEY e, pct",
            conjuncts[0], conjuncts[1]
        );
        let (result, profile) = db.session().locy_with(&program).profile().await?;
        let operators: Vec<&str> = profile
            .profile
            .strata
            .iter()
            .flat_map(|stratum| &stratum.rules)
            .filter(|rule| rule.name == "r")
            .flat_map(|rule| &rule.iterations)
            .flat_map(|iteration| &iteration.operators)
            .map(|op| op.operator.as_str())
            .collect();
        let hash_joins = operators.iter().filter(|o| **o == "HashJoinExec").count();
        let cross_joins = operators.iter().filter(|o| **o == "CrossJoinExec").count();
        ensure!(
            cross_joins == 0 && hash_joins >= 2,
            "order {conjuncts:?}: expected a HashJoinExec per anchored reference and no \
             CrossJoinExec, got {hash_joins} hash and {cross_joins} cross joins in {operators:?}"
        );
        let facts = result
            .derived_rule("r")
            .await
            .unwrap()
            .map_or(0, |rows| rows.len());
        ensure!(
            facts == 1,
            "order {conjuncts:?}: expected one fact (a, 60.0), got {facts}"
        );
    }
    Ok(())
}
