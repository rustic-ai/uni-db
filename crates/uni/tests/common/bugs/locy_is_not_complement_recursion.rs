// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: Locy `IS NOT` complement must stay correct when a FOLD rule and a
//! recursive rule coexist and the recursive relation exceeds the
//! `DEDUP_ANTI_JOIN_THRESHOLD` (300 facts).
//!
//! Distilled from the flagship predictive-maintenance notebook
//! (`website/docs/examples/python/locy_predictive_maintenance.ipynb`), whose
//! `HEALTHY_ASSETS_COUNT` assertion failed: `healthy_assets` (the complement of
//! `failure_prone`) derived 0 rows instead of 30.
//!
//! ## Root cause (fixed)
//!
//! The semi-naive fixpoint switches its delta-dedup strategy once a rule's fact
//! set reaches `DEDUP_ANTI_JOIN_THRESHOLD` (300). Above the threshold it used a
//! `LeftAnti` hash join (`arrow_left_anti_dedup`), which removes candidate rows
//! matching *existing* facts but did **not** dedup duplicate rows *within* a
//! single iteration's candidate batch — unlike the `RowDedupState` / legacy paths
//! used below the threshold. A transitive-closure rule emits the same `(a, b)`
//! pair via every intermediate `mid`, so above 300 facts the relation exploded
//! (here `58_050` rows for a `1_350`-pair closure). That bloated, with FOLD's
//! provenance tracking active, pushed the recursive stratum past the 300 s program
//! timeout, after which the remaining stratum (`healthy_assets`) was skipped —
//! silently yielding 0 rows. The fix makes `arrow_left_anti_dedup` dedup within
//! the candidate set, restoring set semantics across the threshold.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test bugs \
//!     -E 'test(locy_is_not_complement_partitions_fleet_with_fold_and_recursion)'

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Stage count and per-stage width of the layered UPSTREAM_OF DAG.
///
/// `STAGES * WIDTH` equipment nodes form `STAGES` fully-bipartite layers. The
/// base `UPSTREAM_OF` relation (675 edges) exceeds `DEDUP_ANTI_JOIN_THRESHOLD`,
/// so the recursive iterations exercise the `arrow_left_anti_dedup` delta path —
/// the path that previously failed to dedup. Mirrors the flagship notebook's
/// dataset (60 equipment across 4 process stages).
const STAGES: usize = 4;
const WIDTH: usize = 15;

/// Builds an in-memory database seeded with the layered predictive-maintenance
/// topology: `STAGES * WIDTH` equipment, one component each, and a
/// fully-bipartite `UPSTREAM_OF` graph between consecutive stages.
async fn setup_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Equipment")
        .property("udi", DataType::String)
        .property("actual_failed", DataType::Bool)
        .done()
        .label("Component")
        .property("health", DataType::Float64)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;

    let n = STAGES * WIDTH;
    // Half the fleet is flagged failed (even udi); the other half is healthy.
    for i in 0..n {
        let failed = i % 2 == 0;
        tx.execute(&format!(
            "CREATE (:Equipment {{udi: '{i}', actual_failed: {failed}}})"
        ))
        .await?;
        tx.execute(&format!(
            "MATCH (e:Equipment {{udi: '{i}'}}) \
             CREATE (e)-[:HAS_PART]->(:Component {{health: 0.7}})"
        ))
        .await?;
    }
    // Fully-bipartite edges between consecutive stages → dense multi-path DAG.
    for stage in 0..STAGES - 1 {
        for a in stage * WIDTH..(stage + 1) * WIDTH {
            for b in (stage + 1) * WIDTH..(stage + 2) * WIDTH {
                tx.execute(&format!(
                    "MATCH (u:Equipment {{udi: '{a}'}}), (d:Equipment {{udi: '{b}'}}) \
                     CREATE (u)-[:UPSTREAM_OF]->(d)"
                ))
                .await?;
            }
        }
    }
    tx.commit().await?;
    Ok(db)
}

/// The `IS NOT` complement must partition the fleet with `failure_prone`, and the
/// recursive relation must dedup to its distinct closure — even when a FOLD rule
/// coexists and the relation crosses `DEDUP_ANTI_JOIN_THRESHOLD`.
#[tokio::test]
async fn locy_is_not_complement_partitions_fleet_with_fold_and_recursion() -> Result<()> {
    let db = setup_db().await?;
    let n = STAGES * WIDTH;
    let expected_healthy = (0..n).filter(|i| i % 2 != 0).count();
    // Layer i reaches every node in every later layer, so the transitive closure
    // is WIDTH² pairs per ordered layer pair: WIDTH² * C(STAGES, 2).
    let expected_closure = WIDTH * WIDTH * (STAGES * (STAGES - 1) / 2);

    // All three ingredients in one program: FOLD aggregation, a recursive relation
    // larger than DEDUP_ANTI_JOIN_THRESHOLD, and the IS NOT complement under test.
    let program = "\
        CREATE RULE failure_prone AS \
          MATCH (e:Equipment) WHERE e.actual_failed = true YIELD KEY e \n\
        CREATE RULE healthy_assets AS \
          MATCH (e:Equipment) WHERE e IS NOT failure_prone YIELD KEY e \n\
        CREATE RULE component_risk AS \
          MATCH (e:Equipment)-[:HAS_PART]->(c:Component) \
          FOLD composite_unhealth = MNOR(1.0 - c.health) YIELD KEY e, composite_unhealth \n\
        CREATE RULE upstream_reaches AS \
          MATCH (a:Equipment)-[:UPSTREAM_OF]->(b:Equipment) YIELD KEY a, KEY b \n\
        CREATE RULE upstream_reaches AS \
          MATCH (a:Equipment)-[:UPSTREAM_OF]->(mid:Equipment) \
          WHERE mid IS upstream_reaches TO b YIELD KEY a, KEY b";

    let result = db.session().locy(program).await?;

    // A rule that derives zero facts is absent from the `derived` map; treat a
    // missing key as 0 rows.
    let derived = result.derived_rules().await?;
    let len_of = |rule: &str| derived.get(rule).map_or(0, |rows| rows.len());

    assert_eq!(
        len_of("failure_prone"),
        n - expected_healthy,
        "failure_prone should select the failed half of the fleet"
    );

    // Guards the dedup fix: the recursive relation must be its distinct closure,
    // not the duplicate-inflated bag the >threshold LeftAnti path used to produce.
    assert_eq!(
        len_of("upstream_reaches"),
        expected_closure,
        "upstream_reaches should dedup to the distinct transitive closure"
    );

    // The complement must partition the fleet (the bug derived 0 here).
    assert_eq!(
        len_of("healthy_assets"),
        expected_healthy,
        "IS NOT complement should derive the healthy half ({expected_healthy})"
    );

    Ok(())
}
