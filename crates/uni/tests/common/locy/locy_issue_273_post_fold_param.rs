// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #273: a query parameter is not resolved in the post-FOLD (HAVING)
//! position. The same `$thr` resolves in the pre-FOLD `WHERE` and fails after
//! `FOLD` with
//!
//! ```text
//! LocyRuntimeError: Internal error: HAVING expression conversion:
//!     Unresolved parameter: $thr
//! ```
//!
//! while interpolating the literal into the program text works.

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

const POST_FOLD: &str = "CREATE RULE t AS \
     MATCH (a:E)-[l:L]->(b:E) \
     FOLD agg = MSUM(l.w) \
     WHERE agg >= $thr \
     YIELD KEY b, agg \n\
     QUERY t RETURN b.uid AS uid, agg";

const PRE_FOLD: &str = "CREATE RULE t AS \
     MATCH (a:E)-[l:L]->(b:E) \
     WHERE l.w >= $thr \
     YIELD KEY b, l.w AS w \n\
     QUERY t RETURN b.uid AS uid, w";

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

async fn row_count(db: &Uni, program: &str, with_param: bool) -> Result<usize> {
    let session = db.session();
    let mut builder = session.locy_with(program).with_config(default_config());
    if with_param {
        builder = builder.param("thr", 50.0);
    }
    let result = builder.run().await?;
    Ok(result
        .command_results
        .iter()
        .map(|c| match c {
            uni_db::locy::CommandResult::Query(rows) => rows.len(),
            _ => 0,
        })
        .sum())
}

#[tokio::test]
async fn param_resolves_in_post_fold_where() -> Result<()> {
    let db = setup().await?;

    assert_eq!(
        row_count(&db, PRE_FOLD, true).await?,
        1,
        "control: the parameter resolves in the pre-FOLD WHERE"
    );
    assert_eq!(
        row_count(&db, &POST_FOLD.replace("$thr", "50.0"), false).await?,
        1,
        "control: the interpolated literal works post-FOLD"
    );
    assert_eq!(
        row_count(&db, POST_FOLD, true).await?,
        1,
        "issue #273: the parameter must resolve in the post-FOLD (HAVING) position"
    );
    Ok(())
}
