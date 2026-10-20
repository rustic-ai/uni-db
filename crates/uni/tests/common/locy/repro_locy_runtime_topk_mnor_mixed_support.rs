// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `TopKProofs` MNOR over proofs that reference one certain fact.
//!
//! # History
//!
//! Under `TopKProofs` an `MNOR` fold used to build a `Proof` per pre-fold row
//! whose only events were the facts the row referenced (`body_support_map`,
//! resolved by the subject's body column), weighted by the largest row reading
//! them. Two edges from one hub that both reference `hub_score(h)` collapsed to
//! one proof of weight 0.7, and a mixed group (supported and unsupported rows)
//! once drove the group to 1.0 (finding [24]).
//!
//! # Now
//!
//! `TopKProofs` evaluates each fact over the lineage of its `k` most probable
//! rows (`locy_lineage::LineageStore::correct`): each row is its own event AND
//! every fact it references. `hub_score` has no PROB column, so its fact is
//! certain and no event, and the two rows are the two edges, independent:
//! `1 - (1 - 0.7)(1 - 0.6) = 0.88`, the possible-worlds probability. The
//! mixed-support case cannot arise: every row is its own event.

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{DataType, Uni, Value};
use uni_locy::SemiringKind;

/// A `risk` rule keyed by `(h, i)`: two edges from the same hub `h` into item
/// `i`, both referencing `hub_score(h)`, a rule without a PROB column.
const RISK_SHARED: &str = "CREATE RULE hub_score AS \
     MATCH (h:Hub) YIELD KEY h \n\
     CREATE RULE risk AS \
     MATCH (h:Hub)-[e:CAUSE]->(i:Item) WHERE h IS hub_score \
     FOLD p = MNOR(e.prob) YIELD KEY h, KEY i, p \n\
     CREATE RULE risk AS \
     MATCH (h:Hub)-[e:PLAIN]->(i:Item) WHERE h IS hub_score \
     FOLD p = MNOR(e.prob) YIELD KEY h, KEY i, p \n\
     QUERY risk RETURN i.name AS item, p";

/// The `TopKProofs` semiring, with room for every proof.
fn topk_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        semiring: SemiringKind::TopKProofs { k: 8 },
        ..Default::default()
    }
}

/// Seed a Hub with two edges into Item `i1`:
///   h -CAUSE 0.7-> i1  (referencing hub_score(h))
///   h -PLAIN 0.6-> i1  (referencing the same hub_score(h))
async fn seed(db: &Uni) -> Result<()> {
    db.schema()
        .label("Hub")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("CAUSE", &["Hub"], &["Item"])
        .property("prob", DataType::Float64)
        .apply()
        .await?;
    db.schema()
        .edge_type("PLAIN", &["Hub"], &["Item"])
        .property("prob", DataType::Float64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Hub {name: 'h'}), (:Item {name: 'i1'})")
        .await?;
    tx.execute(
        "MATCH (h:Hub {name:'h'}), (i:Item {name:'i1'}) CREATE (h)-[:CAUSE {prob: 0.7}]->(i)",
    )
    .await?;
    tx.execute(
        "MATCH (h:Hub {name:'h'}), (i:Item {name:'i1'}) CREATE (h)-[:PLAIN {prob: 0.6}]->(i)",
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

fn prob_of(result: &uni_locy::LocyResult, item: &str) -> Option<f64> {
    let rows = result.rows()?;
    rows.iter()
        .find(|r| r.get("item").and_then(Value::as_str) == Some(item))
        .and_then(|r| r.get("p"))
        .and_then(Value::as_f64)
}

/// Two edges into one item, both referencing the certain `hub_score(h)`, are
/// two independent proofs: `1 - 0.3 · 0.4 = 0.88`.
#[tokio::test]
async fn topk_mnor_over_a_certain_shared_reference_keeps_rows_independent() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(&db).await?;

    let result = db
        .session()
        .locy_with(RISK_SHARED)
        .with_config(topk_config())
        .run()
        .await?;

    let p = prob_of(&result, "i1").expect("risk(h, i1)");
    assert!(
        (p - 0.88).abs() < 1e-12,
        "the two edges are independent events and hub_score is certain: expected 0.88, got {p}"
    );
    Ok(())
}
