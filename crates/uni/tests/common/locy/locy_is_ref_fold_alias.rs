// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: a FOLD aggregate (or YIELD expr / deferred WHERE filter) that
//! references a non-KEY value column brought in by a *non-first* positive
//! IS-ref must resolve to that scan's `__isref{n}_`-prefixed column.
//!
//! The second and later positive IS-refs of a clause have their derived-scan
//! columns aliased with an `__isref{n}_` prefix to avoid colliding with an
//! earlier scan's identically named yield columns (`locy_planner.rs` Step 3).
//! Before the fix, the clause projection that feeds FOLD kept the *bare* column
//! name, so `FOLD score = MPROD(mapping_conf)` over a `mapping_conf` yielded by
//! the second IS-ref planned against a non-existent field and failed with:
//!
//!   DataFusion planning failed: Schema error: No field named mapping_conf.
//!   Did you mean '__isref1_mapping_conf'?
//!
//! This shape is exactly the `claim_infringed` rule of the flagship
//! `locy_patent_fto` notebook, which broke the release docs build.

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

/// Faithful minimal reproduction of the patent-FTO `claim_infringed` rule:
/// product `P1` maps both of claim `C1`'s two elements with confidences
/// 0.5 and 0.4, so the MPROD conjunction over the shared elements is 0.2.
///
/// `celem` (occurrence 0, no prefix) yields only KEY columns; `emap`
/// (occurrence 1, `__isref1_` prefix) yields the non-KEY `mapping_conf` that
/// the FOLD aggregates. The fix rewrites `MPROD(mapping_conf)` to read the
/// aliased `__isref1_mapping_conf` column.
#[tokio::test]
async fn fold_over_non_first_is_ref_value_column() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (p:P {name: 'P1'}), (c:C {name: 'C1'}), \
                (e1:CE {name: 'E1'}), (e2:CE {name: 'E2'}), \
                (c)-[:HAS]->(e1), (c)-[:HAS]->(e2), \
                (p)-[:MAPS {conf: 0.5}]->(e1), (p)-[:MAPS {conf: 0.4}]->(e2)",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE celem AS \
             MATCH (c:C)-[:HAS]->(ce:CE) YIELD KEY c, KEY ce \n\
             CREATE RULE emap AS \
             MATCH (p:P)-[r:MAPS]->(ce:CE) YIELD KEY p, KEY ce, r.conf AS mapping_conf \n\
             CREATE RULE infringed AS \
             MATCH (p:P), (c:C) \
             WHERE c IS celem TO ce, p IS emap TO ce \
             FOLD score = MPROD(mapping_conf) \
             YIELD KEY p, KEY c, score",
        )
        .with_config(default_config())
        .run()
        .await?;

    let infringed = result
        .derived
        .get("infringed")
        .expect("rule 'infringed' missing");
    assert_eq!(
        infringed.len(),
        1,
        "expected exactly one (P1, C1) infringement row, got {}: {:?}",
        infringed.len(),
        infringed
    );

    let score = infringed[0]
        .get("score")
        .expect("infringement row missing 'score' column");
    match score {
        uni_common::Value::Float(f) => assert!(
            (f - 0.2).abs() < 1e-9,
            "MPROD(0.5, 0.4) over both claim elements must be 0.2, got {f}"
        ),
        other => panic!("expected Float score, got {other:?}"),
    }

    Ok(())
}
