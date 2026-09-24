// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: a rule that forwards a NON-KEY value column brought in by a
//! (composite) IS-reference must be consumable by a DOWNSTREAM rule.
//!
//! The derived-scan schema for such a forwarding rule is built from
//! `infer_yield_type`. Before the fix, a yield column that merely re-exports an
//! IS-ref value column (e.g. `... WHERE (p,c) IS pc_scored YIELD ... score`)
//! had no resolvable expression type and defaulted to `LargeUtf8`, while the
//! rule's materialized data carried the real `Float64`. Any downstream rule
//! scanning it then failed at plan time with:
//!
//!   Arrow error: Invalid argument error: column types must match schema types,
//!   expected LargeUtf8 but found Float64 at column index N
//!
//! `infer_yield_type` now resolves such a column's type from the referenced
//! rule's schema. This is the engine fix that unblocked the all-elements
//! `claim_infringed` rule in the patent-FTO flagship notebook.

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

/// `pc_scored` folds an edge weight into a Float64 `score`. `forwarded`
/// re-exports that `score` through a composite `(a,b) IS pc_scored`. `rollup`
/// (a separate downstream stratum) then consumes `forwarded` via a single
/// `a IS forwarded TO b` ref under a MATCH pattern and folds it. Before the
/// fix, `forwarded`'s derived scan typed `score` as Utf8 and `rollup` failed
/// with an Arrow schema mismatch; now it computes the correct value.
#[tokio::test]
async fn forwarded_is_ref_value_column_is_consumable_downstream() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    // a --W(0.5)--> b , a --W(0.4)--> b   (two weighted edges a→b)
    // plus a hub h with HAS edge to a, so the downstream MATCH has a node to bind.
    tx.execute(
        "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (h:H {name: 'h'}), \
                (a)-[:W {w: 0.5}]->(b), (a)-[:W {w: 0.4}]->(b), \
                (h)-[:HAS]->(a)",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE pc_scored AS \
             MATCH (a:N)-[r:W]->(b:N) \
             FOLD score = MPROD(r.w) \
             YIELD KEY a, KEY b, score \n\
             CREATE RULE forwarded AS \
             MATCH (a:N), (b:N) \
             WHERE (a, b) IS pc_scored \
             YIELD KEY a, KEY b, score \n\
             CREATE RULE rollup AS \
             MATCH (h:H)-[:HAS]->(a:N), (b:N) \
             WHERE a IS forwarded TO b \
             FOLD total = MNOR(score) \
             YIELD KEY h, KEY a, total",
        )
        .with_config(default_config())
        .run()
        .await?;

    // The downstream rule must materialize (it errored before the fix).
    let rollup = result.derived.get("rollup").expect(
        "rule 'rollup' missing — downstream consumption of a forwarded \
                 IS-ref value column failed",
    );
    assert_eq!(
        rollup.len(),
        1,
        "expected one (h, a) rollup row, got {}: {:?}",
        rollup.len(),
        rollup
    );

    // pc_scored folds the two a→b weights into MPROD(0.5, 0.4) = 0.2; forwarded
    // re-exports 0.2; rollup is MNOR over the single forwarded score = 0.2.
    let total = rollup[0].get("total").expect("rollup row missing 'total'");
    match total {
        uni_common::Value::Float(f) => assert!(
            (f - 0.2).abs() < 1e-9,
            "MNOR over forwarded score 0.2 must be 0.2, got {f}"
        ),
        other => panic!("expected Float total, got {other:?}"),
    }

    Ok(())
}
