//! Tests for <https://github.com/rustic-ai/uni-db/issues/25>
//!
//! `QueryPlanner` previously contained `Cell<usize>` (which is `!Send`),
//! making Locy futures `!Send` and requiring `spawn_blocking` workarounds.
//! After the fix (Cell → AtomicUsize), Locy futures are Send and can be
//! used directly with `tokio::spawn`.

use anyhow::Result;

/// Legacy workaround: spawn_blocking with a nested runtime.
/// Kept as a regression guard — this must continue to work.
#[tokio::test]
async fn test_locy_works_via_spawn_blocking_workaround() -> Result<()> {
    let result = tokio::task::spawn_blocking(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let db = uni_db::Uni::in_memory().build().await?;
            let session = db.session();

            let tx = session.tx().await?;
            tx.execute(
                "CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})-[:KNOWS]->(:Person {name: 'Carol'})",
            )
            .await?;
            tx.commit().await?;

            session
                .locy(
                    "CREATE RULE reachable AS \
                         MATCH (a:Person)-[:KNOWS]->(b:Person) \
                         YIELD KEY a, b \
                     CREATE RULE reachable AS \
                         MATCH (a:Person)-[:KNOWS]->(mid:Person) \
                         WHERE mid IS reachable TO b \
                         YIELD KEY a, b",
                )
                .await
        })
    })
    .await?
    .expect("Locy evaluation failed");

    let reachable = result
        .derived_rules()
        .await
        .unwrap()
        .get("reachable")
        .expect("rule 'reachable' missing");

    // Direct: Alice->Bob, Bob->Carol. Transitive: Alice->Carol.
    assert!(
        reachable.len() >= 3,
        "expected at least 3 reachable facts, got {}",
        reachable.len()
    );

    Ok(())
}

/// Proves Locy futures are Send: `tokio::spawn` requires Send futures.
/// Before the fix (Cell<usize> → AtomicUsize), this would fail to compile.
#[tokio::test]
async fn test_locy_future_is_send() -> Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;

    let handle = tokio::spawn(async move {
        let session = db.session();

        let tx = session.tx().await.unwrap();
        tx.execute(
            "CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})-[:KNOWS]->(:Person {name: 'Carol'})",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        session
            .locy(
                "CREATE RULE reachable AS \
                     MATCH (a:Person)-[:KNOWS]->(b:Person) \
                     YIELD KEY a, b \
                 CREATE RULE reachable AS \
                     MATCH (a:Person)-[:KNOWS]->(mid:Person) \
                     WHERE mid IS reachable TO b \
                     YIELD KEY a, b \
                 QUERY reachable RETURN *",
            )
            .await
    });

    let result = handle.await?.expect("Locy evaluation failed");

    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);
    // QUERY returns the evaluated facts; at minimum the two direct edges.
    assert!(
        !rows.is_empty(),
        "expected reachable rows from Locy evaluation via tokio::spawn",
    );

    Ok(())
}
