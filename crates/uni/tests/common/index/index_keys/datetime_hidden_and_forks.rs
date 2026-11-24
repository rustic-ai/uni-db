// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T4: a key column is the engine's, never a property: `RETURN n`,
//! `properties(n)`, `keys(n)`, `get_label_info` and the index listings do not
//! show it.
//!
//! T5: forks. A fork created before the index reads and writes the label
//! (its branch is widened with the key column, all NULL); the key is not
//! pushed down on the fork's view, so it answers like primary would; diff and
//! promote are unaffected by the key column.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(datetime_hidden) | test(datetime_fork)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::{PromotePattern, Uni, Value};

const SCHEMA: &str = "CREATE LABEL Event (id INT, ts DATETIME)";
const INDEX: &str = "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)";

fn shows_key(text: &str) -> bool {
    text.contains("__key_")
}

#[tokio::test]
async fn datetime_hidden_key_columns() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(&db, SCHEMA).await?;
    run(&db, INDEX).await?;
    run(
        &db,
        "CREATE (:Event {id: 1, ts: datetime('2026-01-01T12:00:00Z')})",
    )
    .await?;
    for stage in ["unflushed", "flushed"] {
        for query in [
            "MATCH (e:Event) RETURN e",
            "MATCH (e:Event) RETURN properties(e) AS p",
            "MATCH (e:Event) RETURN keys(e) AS k",
            "MATCH (e:Event) WHERE e.ts = datetime('2026-01-01T12:00:00Z') RETURN e",
        ] {
            let rows = db.session().query(query).await?;
            assert_eq!(rows.len(), 1, "{stage}: {query}");
            let rendered = format!("{:?}", rows.rows()[0].values());
            assert!(!shows_key(&rendered), "{stage}: {query}: {rendered}");
        }
        db.flush().await?;
    }
    let info = db.get_label_info("Event").await?.expect("label info");
    assert!(
        info.properties.iter().all(|p| !shows_key(&p.name)),
        "get_label_info lists a key column: {:?}",
        info.properties
    );
    let ts = info.properties.iter().find(|p| p.name == "ts").expect("ts");
    assert!(ts.is_indexed, "the property reports its index");
    let idx = info
        .indexes
        .iter()
        .find(|i| i.name == "idx_ts")
        .expect("idx");
    assert_eq!(idx.properties, vec!["ts".to_string()]);
    assert_eq!(idx.status, "ONLINE");

    let listed = db
        .session()
        .query("CALL uni.schema.indexes() YIELD name, properties RETURN name, properties")
        .await?;
    let rendered = format!("{:?}", listed.rows());
    assert!(
        rendered.contains("idx_ts") && !shows_key(&rendered),
        "{rendered}"
    );
    let shown = db.session().query("SHOW INDEXES").await?;
    let rendered = format!("{:?}", shown.rows());
    assert!(!shows_key(&rendered), "{rendered}");
    Ok(())
}

async fn ids(session: &uni_db::Session, query: &str) -> Result<Vec<i64>> {
    let rows = session.query(query).await?;
    let mut out: Vec<i64> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id"))
        .collect::<Result<_, _>>()?;
    out.sort_unstable();
    Ok(out)
}

const AT_NOON: &str =
    "MATCH (e:Event) WHERE e.ts = datetime('2026-01-01T12:00:00Z') RETURN e.id AS id";

#[tokio::test]
async fn datetime_fork_created_before_the_index() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(&db, SCHEMA).await?;
    run(
        &db,
        "CREATE (:Event {id: 1, ts: datetime('2026-01-01T12:00:00Z')}), \
                (:Event {id: 2, ts: datetime('2025-01-01T12:00:00Z')})",
    )
    .await?;
    db.flush().await?;
    let primary = db.session();
    {
        // The fork writes the label, so it has a branch of the table.
        let fork = primary.fork("before_index").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Event {id: 3, ts: datetime('2026-01-01T12:00:00Z')})")
            .await?;
        tx.commit().await?;
        fork.flush().await?;
    }
    run(&db, INDEX).await?;
    assert_eq!(ids(&primary, AT_NOON).await?, vec![1]);

    let fork = primary.fork("before_index").await?;
    assert_eq!(ids(&fork, AT_NOON).await?, vec![1, 3], "fork reads");
    let explain = fork.query_with(AT_NOON).explain().await?;
    let (_, profile) = fork.query_with(AT_NOON).profile().await?;
    let hits: usize = profile
        .runtime_stats
        .iter()
        .filter(|s| s.operator == "GraphScanExec")
        .filter_map(|s| s.index_hits)
        .sum();
    assert_eq!(hits, 0, "no key pushdown on a fork: {explain:?}");
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Event {id: 4, ts: datetime('2026-01-01T12:00:00Z')})")
        .await?;
    tx.execute("MATCH (e:Event {id: 1}) SET e.ts = datetime('2024-01-01T00:00:00Z')")
        .await?;
    tx.commit().await?;
    assert_eq!(
        ids(&fork, AT_NOON).await?,
        vec![3, 4],
        "fork writes, unflushed"
    );
    fork.flush().await?;
    assert_eq!(
        ids(&fork, AT_NOON).await?,
        vec![3, 4],
        "fork writes, flushed"
    );
    assert_eq!(
        ids(&primary, AT_NOON).await?,
        vec![1],
        "primary is isolated"
    );

    // Diff and promote see the rows, not the key column.
    let diff = db.diff_fork_primary("before_index").await?;
    assert!(!shows_key(&format!("{diff:?}")), "{diff:?}");
    drop(fork);
    let report = db
        .promote_from_fork("before_index", &[PromotePattern::label("Event")])
        .await?;
    assert!(report.vertices_inserted >= 1, "{report:?}");
    db.flush().await?;
    let promoted = ids(&primary, AT_NOON).await?;
    assert!(promoted.contains(&4), "{promoted:?}");
    let all = primary
        .query("MATCH (e:Event) WHERE e.id = 4 RETURN e.ts AS ts")
        .await?;
    assert!(matches!(all.rows()[0].values()[0], Value::Temporal(_)));
    Ok(())
}
