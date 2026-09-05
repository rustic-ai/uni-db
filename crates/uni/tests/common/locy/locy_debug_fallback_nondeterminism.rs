// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repros for the two `format!("{v:?}")` fallback arms that render a
//! `std::collections::HashMap` and therefore produce a *run-varying* string.
//!
//! Both tests are `#[ignore]`d because they FAIL today — they are the
//! executable statement of the defect, not a regression guard yet.
//!
//! 1. `locy_validate.rs::canonical_key` has no arm for `Value::Map`, so a
//!    path-valued KEY column (which arrives as a nested `Value::Map`)
//!    becomes a Debug string. Rule side and target side build independent
//!    `HashMap`s, so the two renderings rarely agree and the VALIDATE join
//!    silently drops most rows — the reported metric is computed over a
//!    random subset (GitHub #236).
//!
//! 2. `locy_ast_builder.rs::value_to_string` (used by `generate_skolem_id`)
//!    has no arm for `Value::Node`, whose `properties` field is a `HashMap`.
//!    The "deterministic" Skolem ID of a DERIVE `NEW` node is therefore a
//!    different string on every run.

use anyhow::Result;
use uni_db::Uni;

/// #236: VALIDATE over a path-valued KEY joins a random subset of rows.
///
/// 20 `:Item` nodes, every prediction 0.9 (=> predicted true), 10 labelled
/// true. The correct result is `n_samples == 20` and `accuracy == 0.5`.
/// Observed across 14 separate processes: n_samples 1, 2, 3, 5, or an
/// "empty join" hard error; accuracy 0.0 / 0.333 / 0.4 / 0.5 / 0.667 / 1.0.
#[tokio::test]
#[ignore = "repro for #236: canonical_key Debug-renders Value::Map; currently fails"]
async fn validate_path_key_joins_a_random_subset() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    for i in 0..20 {
        tx.execute(&format!(
            "CREATE (:Item {{label: {}}})",
            if i % 2 == 0 { "true" } else { "false" }
        ))
        .await?;
    }
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE rp AS MATCH p = (s:Item) YIELD KEY p, 0.9 AS risk PROB \n\
             VALIDATE rp ON MATCH p = (s:Item) TARGET s.label METRICS accuracy",
        )
        .await?;

    let uni_db::locy::CommandResult::Validate(v) = &result.command_results[0] else {
        panic!("expected a VALIDATE result");
    };
    assert_eq!(
        v.n_samples, 20,
        "VALIDATE must score every target row; a path-valued KEY silently \
         drops rows whose Debug rendering differs between the two join sides"
    );
    Ok(())
}

/// Sibling defect: the DERIVE Skolem ID varies run to run (and call to call).
///
/// The binding row carries the matched node as `Value::Node`, whose
/// `properties` HashMap is Debug-rendered into the `_skolem_id` string.
/// Five evaluations in one process yield five different IDs.
#[tokio::test]
#[ignore = "repro for the value_to_string sibling defect: currently fails"]
async fn derive_skolem_id_is_not_deterministic() -> Result<()> {
    let mut ids = Vec::new();
    for _ in 0..5 {
        let db = Uni::in_memory().build().await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:P {name: 'A', age: 30, city: 'X', tag: 't', k: 5})")
            .await?;
        tx.commit().await?;

        let tx = session.tx().await?;
        tx.locy("CREATE RULE mk AS MATCH (p:P) DERIVE (p)-[:HAS]->(NEW s:Summary) \n DERIVE mk")
            .await?;
        let rows = tx
            .query("MATCH (s:Summary) RETURN s._skolem_id AS sk")
            .await?;
        ids.push(format!("{:?}", rows.rows()));
        tx.commit().await?;
    }
    assert!(
        ids.windows(2).all(|w| w[0] == w[1]),
        "generate_skolem_id must be deterministic, got {} distinct IDs: {ids:#?}",
        ids.iter().collect::<std::collections::HashSet<_>>().len()
    );
    Ok(())
}
