// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #272: `WHERE b IS <rule> TO <value column>` returns zero rows and
//! raises nothing, while referencing the same column by name returns the
//! expected rows.
//!
//! The referenced rule yields one KEY column and one value column:
//!
//! ```text
//! CREATE RULE stake AS
//!     MATCH (a:E)-[l:L]->(b:E)
//!     FOLD agg = MSUM(l.w)
//!     YIELD KEY b, agg
//! ```
//!
//! `WHERE b IS stake` (value column by name) and `WHERE b IS stake TO agg`
//! (the documented `TO` form) must return the same rows.

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

const STAKE: &str = "CREATE RULE stake AS \
     MATCH (a:E)-[l:L]->(b:E) \
     FOLD agg = MSUM(l.w) \
     YIELD KEY b, agg \n";

async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (n0:E {uid: 'n0'}), (n1:E {uid: 'n1'}), (n2:E {uid: 'n2'}), \
                (n0)-[:L {w: 70.0}]->(n1), (n0)-[:L {w: 30.0}]->(n2)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn rows(db: &Uni, program: &str) -> Result<Vec<(String, f64)>> {
    let result = db
        .session()
        .locy_with(program)
        .with_config(default_config())
        .run()
        .await?;
    let mut out = Vec::new();
    for row in result.command_results.iter().flat_map(|c| match c {
        uni_db::locy::CommandResult::Query(rows) => rows.as_slice(),
        _ => &[],
    }) {
        let uid = row
            .get("uid")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default();
        let agg = row.get("agg").and_then(|v| v.as_f64()).unwrap_or(f64::NAN);
        out.push((uid, agg));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

#[tokio::test]
async fn is_ref_to_value_column_matches_by_name() -> Result<()> {
    let db = setup().await?;

    let by_name = rows(
        &db,
        &format!(
            "{STAKE}CREATE RULE t AS \
             MATCH (b:E) WHERE b IS stake YIELD KEY b, agg \n\
             QUERY t RETURN b.uid AS uid, agg"
        ),
    )
    .await?;

    let via_to = rows(
        &db,
        &format!(
            "{STAKE}CREATE RULE t AS \
             MATCH (b:E) WHERE b IS stake TO agg YIELD KEY b, agg \n\
             QUERY t RETURN b.uid AS uid, agg"
        ),
    )
    .await?;

    let expected = vec![("n1".to_string(), 70.0), ("n2".to_string(), 30.0)];
    assert_eq!(by_name, expected, "control: by-name spelling");
    assert_eq!(
        via_to, expected,
        "issue #272: the TO form must bind the referenced rule's value column"
    );
    Ok(())
}
