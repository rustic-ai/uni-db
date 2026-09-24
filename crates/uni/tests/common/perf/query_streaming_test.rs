// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_query_cursor_streaming() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Insert 100 persons
    let tx = db.session().tx().await?;
    for i in 0..100 {
        tx.execute(&format!("CREATE (:Person {{name: 'Person {}'}})", i))
            .await?;
    }
    tx.commit().await?;

    // Query with cursor
    let mut cursor = db
        .session()
        .query_with("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .cursor()
        .await?;

    assert_eq!(cursor.columns(), &["p.name"]);

    let mut total_rows = 0;
    while let Some(batch_res) = cursor.next_batch().await {
        let batch = batch_res?;
        total_rows += batch.len();
        // Each batch should be <= default batch size (usually 1024 or similar)
        // In our in-memory test, it might be all in one batch if default is 1024.
    }

    assert_eq!(total_rows, 100);

    // Test with smaller batch size in config if possible
    let db2 = Uni::in_memory()
        .config(uni_db::UniConfig {
            batch_size: 10,
            ..Default::default()
        })
        .build()
        .await?;

    db2.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    let tx2 = db2.session().tx().await?;
    for i in 0..100 {
        tx2.execute(&format!("CREATE (:Person {{name: 'Person {}'}})", i))
            .await?;
    }
    tx2.commit().await?;

    let mut cursor2 = db2
        .session()
        .query_with("MATCH (p:Person) RETURN p.name")
        .cursor()
        .await?;
    let first_batch = cursor2.next_batch().await.unwrap()?;
    assert_eq!(first_batch.len(), 10);

    let remaining = cursor2.collect_remaining().await?;
    assert_eq!(remaining.len(), 90);

    Ok(())
}

/// Rows large enough to span more than one DataFusion batch.
///
/// DataFusion's session `batch_size` defaults to 8192 and `GraphScanExec`
/// slices its output to it, so a result of this size arrives as four batches.
/// Anything at or below 8192 is a single batch and cannot tell a stream from a
/// collect.
const MULTI_BATCH_ROWS: i64 = 25_000;

/// Build a store holding [`MULTI_BATCH_ROWS`] single-property vertices.
async fn multi_batch_store(config: uni_db::UniConfig) -> Result<Uni> {
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("P")
        .property("n", DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:P {{n: i}})",
        MULTI_BATCH_ROWS - 1
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// The memory ceiling fires while the result is still arriving, not after.
///
/// `execute_stream` used to drain the whole plan and emit one item, so the
/// cursor's per-item check could only ever run once — after every row was
/// already resident as a `HashMap` per row, the most expensive representation
/// in the pipeline. The guard reported an allocation that had already been
/// paid for (#240).
///
/// Discriminating, and measured rather than argued: with the collecting shape
/// restored, this test sees **0 rows** and a reported figure of 4 075 000 bytes
/// — the whole result. Streaming, it sees 8 192 rows and 2 670 592 bytes, which
/// is only what had actually been built when the ceiling was crossed. Both
/// halves of the assertion move, so neither passes for free.
#[tokio::test]
async fn the_memory_ceiling_fires_while_the_result_is_still_arriving() -> Result<()> {
    // Sized between one batch of expanded rows and the whole result. Note this
    // one knob also sizes the DataFusion memory pool, so a value small enough
    // to trip the cursor on its first batch instead fails inside
    // `GraphScanExec` and never reaches the check under test.
    const CEILING: usize = 2_000_000;

    let db = multi_batch_store(uni_db::UniConfig {
        batch_size: 8192,
        max_query_memory: CEILING,
        ..Default::default()
    })
    .await?;

    let mut cursor = db
        .session()
        .query_with("MATCH (p:P) RETURN p.n")
        .cursor()
        .await?;

    let mut delivered = 0usize;
    let mut failure = None;
    while let Some(batch) = cursor.next_batch().await {
        match batch {
            Ok(rows) => delivered += rows.len(),
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }

    let failure = failure.expect("a result this size must cross the ceiling");
    let message = match &failure {
        uni_db::UniError::MemoryLimitExceeded {
            limit_bytes: CEILING,
            message,
        } => message.clone(),
        other => panic!("expected the {CEILING}-byte memory ceiling, got: {other:?}"),
    };

    // The half that fails against a collecting executor: it delivers nothing
    // before erroring, because the whole result exists before the first check.
    assert!(
        delivered > 0,
        "the ceiling fired before any row was delivered, so the result was \
         materialized whole before it was measured: {failure}"
    );
    assert!(
        delivered < usize::try_from(MULTI_BATCH_ROWS).expect("fits"),
        "the whole result was delivered, so the ceiling never applied"
    );

    // The second half: the figure reported is what had been built, not the
    // total. A collecting executor reports the whole result set here.
    let reported: usize = message
        .split_once("estimated at ")
        .and_then(|(_, rest)| rest.split_once(" bytes"))
        .and_then(|(digits, _)| digits.parse().ok())
        .unwrap_or_else(|| panic!("could not read the byte figure from: {message}"));
    assert!(
        reported < 4_000_000,
        "reported {reported} bytes, which is the size of the whole result — the \
         check ran after full materialization"
    );

    Ok(())
}

/// A consumer that pages slowly is not charged the query's own budget.
///
/// `query_timeout` bounds how long the *query* may take. While `execute_stream`
/// emitted one item that was the same as wall clock from cursor creation, since
/// all the work happened in the first poll. Once batches arrive as they are
/// produced, the two diverge: an absolute deadline would fail a cursor for the
/// crime of being read slowly, which is precisely what a cursor is for.
///
/// The consumer here sleeps well past `query_timeout` in total while the query
/// itself runs in a fraction of it.
#[tokio::test]
async fn a_slow_consumer_is_not_charged_the_query_budget() -> Result<()> {
    let db = multi_batch_store(uni_db::UniConfig {
        batch_size: 8192,
        max_query_memory: 256_000_000,
        query_timeout: std::time::Duration::from_secs(3),
        ..Default::default()
    })
    .await?;

    let mut cursor = db
        .session()
        .query_with("MATCH (p:P) RETURN p.n")
        .cursor()
        .await?;

    let mut delivered = 0usize;
    while let Some(batch) = cursor.next_batch().await {
        delivered += batch?.len();
        // Cumulative sleep exceeds the 3 s budget across the four batches.
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    }

    assert_eq!(
        delivered,
        usize::try_from(MULTI_BATCH_ROWS).expect("fits"),
        "a slow consumer lost rows to the query deadline"
    );
    Ok(())
}
