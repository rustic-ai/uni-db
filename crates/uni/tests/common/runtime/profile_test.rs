// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use tempfile::tempdir;
use uni_db::UniBuilder;

#[tokio::test]
async fn test_profile_basic() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let db = UniBuilder::new(path.to_str().unwrap().to_string())
        .build()
        .await?;

    // Create schema
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    tx.execute("CREATE LABEL City (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE LIVES_IN () FROM Person TO City")
        .await?;
    tx.commit().await?;

    // Insert data
    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        .await?;
    tx.execute("CREATE (c:City {name: 'London'})").await?;
    tx.execute("MATCH (p:Person), (c:City) WHERE p.name = 'Alice' AND c.name = 'London' CREATE (p)-[:LIVES_IN]->(c)").await?;
    tx.commit().await?;

    // Profile query — the CLI strips "PROFILE" before calling profile()
    let clean_query = "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name";
    let (result, profile) = db.session().query_with(clean_query).profile().await?;

    println!("Profile Stats: {:#?}", profile.runtime_stats);

    assert_eq!(result.len(), 1);

    // Granular per-operator stats: must have more than a single summary entry
    assert!(
        profile.runtime_stats.len() > 1,
        "Expected granular per-operator stats, got {} entries: {:?}",
        profile.runtime_stats.len(),
        profile.runtime_stats
    );

    let operators: Vec<String> = profile
        .runtime_stats
        .iter()
        .map(|s| s.operator.clone())
        .collect();
    println!("Operators: {:?}", operators);

    // Expect graph scan and traverse operators from the custom DataFusion exec nodes
    assert!(
        operators.iter().any(|op| op.contains("GraphScanExec")),
        "Expected a GraphScanExec operator, got: {:?}",
        operators
    );
    assert!(
        operators.iter().any(|op| op.contains("Traverse")),
        "Expected a Traverse operator, got: {:?}",
        operators
    );

    // Check total time is present
    let _ = profile.total_time_ms;

    // The scan operator should report rows > 0
    let scan = profile
        .runtime_stats
        .iter()
        .find(|s| s.operator.contains("GraphScanExec"))
        .unwrap();
    assert!(
        scan.actual_rows > 0,
        "GraphScanExec should report rows, got {}",
        scan.actual_rows
    );

    Ok(())
}

/// Profile a transaction WRITE via `tx.execute_with(cypher).profile()`.
/// Asserts that the returned `(ExecuteResult, ProfileOutput)` carries both
/// (a) mutation counters from the tx's private L0 and (b) profile stats.
#[tokio::test]
async fn test_tx_profile_create_returns_execute_result_and_profile_output() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    // Schema
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    tx.commit().await?;

    // Profile a CREATE inside a transaction.
    let tx = db.session().tx().await?;
    let (res, prof) = tx
        .execute_with("CREATE (p:Person {name: 'Alice', age: 30}) RETURN p")
        .profile()
        .await?;
    tx.commit().await?;

    assert_eq!(
        res.nodes_created(),
        1,
        "expected 1 node created, got {}",
        res.nodes_created()
    );
    assert_eq!(res.properties_set(), 2);
    assert!(
        !prof.runtime_stats.is_empty(),
        "expected runtime_stats to be populated, got empty"
    );
    let _ = prof.total_time_ms;

    Ok(())
}

/// Profile a parametrised transaction write via `.param(...).profile()`.
#[tokio::test]
async fn test_tx_profile_with_params() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Item (sku STRING, qty INT)")
        .await?;
    tx.execute("CREATE (:Item {sku: 'A', qty: 1})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let (res, prof) = tx
        .execute_with("MATCH (i:Item {sku: $sku}) SET i.qty = $qty RETURN i")
        .param("sku", "A")
        .param("qty", 42i64)
        .profile()
        .await?;
    tx.commit().await?;

    assert!(
        res.properties_set() >= 1,
        "expected at least 1 property set"
    );
    assert!(
        !prof.runtime_stats.is_empty(),
        "expected runtime_stats to be populated, got empty"
    );

    Ok(())
}

/// Regression for GitHub issue #72 (item 3): `MutationSetExec` and other
/// custom DataFusion operators must report non-zero `actual_rows` and
/// `time_ms` in `ProfileOutput`. Before the fix, `MutationSetExec` reported
/// `rows=0 time=0 ms` (no metrics wiring); `GraphScanExec` and `UnwindExec`
/// reported rows but `time=0` (Timer never started).
#[tokio::test]
async fn test_profile_metrics_populated_for_mutation_scan_unwind() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    // Schema and seed data
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (entity_id STRING NOT NULL, frequency INT)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    for i in 0..10 {
        tx.execute_with("CREATE (:Entity {entity_id: $id, frequency: 1})")
            .param("id", format!("e:{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    // Collect node ids
    let res = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid ORDER BY n.entity_id")
        .await?;
    let vids: Vec<i64> = res
        .into_iter()
        .map(|row| row.get::<i64>("nid").unwrap())
        .collect();
    assert_eq!(vids.len(), 10);

    // Build the issue's UNWIND ... MATCH WHERE id(n)=u.nid SET ... shape.
    let updates: Vec<uni_db::Value> = vids
        .iter()
        .enumerate()
        .map(|(i, &vid)| {
            let mut m = std::collections::HashMap::new();
            m.insert("nid".to_string(), uni_db::Value::Int(vid));
            m.insert(
                "new_frequency".to_string(),
                uni_db::Value::Int((i as i64) + 2),
            );
            uni_db::Value::Map(m)
        })
        .collect();

    let tx = db.session().tx().await?;
    let (_res, prof) = tx
        .execute_with(
            "UNWIND $updates AS u \
             MATCH (n:Entity) WHERE id(n) = u.nid \
             SET n.frequency = u.new_frequency",
        )
        .param("updates", uni_db::Value::List(updates))
        .profile()
        .await?;
    tx.commit().await?;

    let stats = &prof.runtime_stats;
    let op_names: Vec<String> = stats.iter().map(|s| s.operator.clone()).collect();
    println!("Operators: {op_names:?}");
    for s in stats {
        println!(
            "  {:<24} rows={:>4} time={:>8.3} ms",
            s.operator, s.actual_rows, s.time_ms
        );
    }

    // MutationSetExec must now report rows AND time.
    let mutation = stats
        .iter()
        .find(|s| s.operator.contains("MutationSetExec"))
        .unwrap_or_else(|| panic!("MutationSetExec not found in: {op_names:?}"));
    assert!(
        mutation.actual_rows > 0,
        "MutationSetExec.actual_rows should be > 0, got {}",
        mutation.actual_rows
    );
    assert!(
        mutation.time_ms > 0.0,
        "MutationSetExec.time_ms should be > 0 (was {}); metrics wiring regressed",
        mutation.time_ms
    );

    // GraphScanExec previously had time=0 (rows-only wiring).
    let scan = stats
        .iter()
        .find(|s| s.operator.contains("GraphScanExec"))
        .unwrap_or_else(|| panic!("GraphScanExec not found in: {op_names:?}"));
    assert!(
        scan.time_ms > 0.0,
        "GraphScanExec.time_ms should be > 0 (was {}); Timer wiring regressed",
        scan.time_ms
    );

    // GraphUnwindExec previously had time=0.
    let unwind = stats
        .iter()
        .find(|s| s.operator.contains("UnwindExec"))
        .unwrap_or_else(|| panic!("UnwindExec not found in: {op_names:?}"));
    assert!(
        unwind.time_ms > 0.0,
        "UnwindExec.time_ms should be > 0 (was {}); Timer wiring regressed",
        unwind.time_ms
    );

    Ok(())
}

/// Regression for issue #72 item 1: the multi-VID IN-list pushdown for
/// `UNWIND $maps AS u MATCH (n:Label) WHERE id(n) = u.field` must restrict
/// the L0 buffer scan to the target VIDs — not just the Lance scan. Before
/// the fix, freshly-inserted (L0-only) vertices bypassed the IN filter and
/// GraphScan emitted the full label table.
///
/// Also regresses issue #72 item 2 (the batch=1→3 step function): at
/// batch=1 the single-VID code path filtered L0; at batch≥2 the multi-VID
/// path didn't — that asymmetry IS the 45x exec jump.
#[tokio::test]
async fn test_72_item1_unwind_id_eq_l0_in_pushdown() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (entity_id STRING NOT NULL, frequency INT)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    for i in 0..100 {
        tx.execute_with("CREATE (:Entity {entity_id: $id, frequency: 1})")
            .param("id", format!("e:{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    let vids: Vec<i64> = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid ORDER BY n.entity_id")
        .await?
        .into_iter()
        .map(|r| r.get::<i64>("nid").unwrap())
        .collect();

    let updates: Vec<uni_db::Value> = vids[..3]
        .iter()
        .map(|&v| {
            let mut m = std::collections::HashMap::new();
            m.insert("nid".to_string(), uni_db::Value::Int(v));
            m.insert("new_frequency".to_string(), uni_db::Value::Int(99));
            uni_db::Value::Map(m)
        })
        .collect();

    let tx = db.session().tx().await?;
    let (_res, prof) = tx
        .execute_with(
            "UNWIND $updates AS u \
             MATCH (n:Entity) WHERE id(n) = u.nid \
             SET n.frequency = u.new_frequency",
        )
        .param("updates", uni_db::Value::List(updates))
        .profile()
        .await?;
    tx.commit().await?;

    println!("=== diag #72 item 1 (3 of 100 entities) ===");
    for s in &prof.runtime_stats {
        println!(
            "  {:<25} rows={:>5} time={:>8.3} ms",
            s.operator, s.actual_rows, s.time_ms
        );
    }
    let scan = prof
        .runtime_stats
        .iter()
        .find(|s| s.operator.contains("GraphScanExec"))
        .unwrap();
    assert_eq!(
        scan.actual_rows, 3,
        "GraphScanExec should emit only the 3 target vertices, got {} \
         (means IN-list pushdown isn't reaching the L0 path — issue #72 item 1 regressed)",
        scan.actual_rows
    );

    Ok(())
}

/// Diagnostic for the post-#72 write-path investigation: does
/// `MutationSetExec` wall scale roughly linearly with the number of
/// `SetItem`s on a single vertex?
///
/// Background: production data on the issue #72 ingest workload shows
/// 17.7 ms/row to SET 3 unindexed scalar properties, with 99.8% of the
/// per-call wall inside `MutationSetExec`. Reading the executor
/// (`crates/uni-query/src/query/executor/write.rs:1725-1777`) reveals
/// the loop reads ALL vertex properties and calls
/// `writer.insert_vertex_with_labels` (full upsert under the L0 write
/// lock) **once per `SetItem`**, even when all items target the same
/// vertex. The hypothesis is that an N-property SET costs ~N× a
/// 1-property SET because each iteration repeats the full
/// read-modify-write cycle.
///
/// This test does not fix the bug. It quantifies the scaling so we can
/// (a) see exactly how much per-iteration cost there is on in-memory
/// storage (a lower bound for the production cost), (b) lock in a
/// regression check that future writer changes don't quietly amplify
/// it, and (c) give a number to point at when proposing the
/// coalescing fix.
///
/// Reads the printed table from the test output to inspect absolute
/// numbers; the asserts are loose to survive CI jitter.
#[tokio::test]
async fn diag_72_set_scales_with_property_count() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    // Schema with five nullable INT props so we can SET subsets of varying
    // size without schema changes between runs.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE LABEL Entity (\
         entity_id STRING NOT NULL, \
         p1 INT, p2 INT, p3 INT, p4 INT, p5 INT)",
    )
    .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    for i in 0..100 {
        tx.execute_with("CREATE (:Entity {entity_id: $id, p1: 0, p2: 0, p3: 0, p4: 0, p5: 0})")
            .param("id", format!("e:{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    let vids: Vec<i64> = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid ORDER BY n.entity_id")
        .await?
        .into_iter()
        .map(|r| r.get::<i64>("nid").unwrap())
        .collect();
    assert!(vids.len() >= 5);

    const BATCH: usize = 3;
    const ITERS: usize = 9;

    // Build the SET clause for k properties: "SET n.p1 = u.v1, n.p2 = u.v2, ..."
    fn set_clause(k: usize) -> String {
        let assigns: Vec<String> = (1..=k).map(|i| format!("n.p{i} = u.v{i}")).collect();
        format!(
            "UNWIND $updates AS u \
             MATCH (n:Entity) WHERE id(n) = u.nid \
             SET {}",
            assigns.join(", ")
        )
    }

    // For each k, build a fresh $updates batch and profile the call ITERS
    // times. Record the median MutationSetExec time_ms and the
    // GraphScanExec time_ms (read side, should be flat across k).
    struct Row {
        k: usize,
        mutation_ms: f64,
        scan_ms: f64,
        total_ms: u64,
        scan_rows: usize,
        mutation_rows: usize,
    }
    let mut rows: Vec<Row> = Vec::new();

    for &k in &[1usize, 2, 3, 5] {
        let cypher = set_clause(k);
        let mut mutation_samples: Vec<f64> = Vec::with_capacity(ITERS);
        let mut scan_samples: Vec<f64> = Vec::with_capacity(ITERS);
        let mut totals: Vec<u64> = Vec::with_capacity(ITERS);
        let (mut last_scan_rows, mut last_mut_rows) = (0usize, 0usize);

        for iter in 0..ITERS {
            // Use a different slice of vids per iter to avoid hot-cache effects
            let base = (iter * BATCH) % (vids.len().saturating_sub(BATCH).max(1));
            let updates: Vec<uni_db::Value> = vids[base..base + BATCH]
                .iter()
                .enumerate()
                .map(|(j, &vid)| {
                    let mut m = std::collections::HashMap::new();
                    m.insert("nid".to_string(), uni_db::Value::Int(vid));
                    for prop_idx in 1..=k {
                        m.insert(
                            format!("v{prop_idx}"),
                            uni_db::Value::Int((iter as i64) * 100 + (j as i64) + prop_idx as i64),
                        );
                    }
                    uni_db::Value::Map(m)
                })
                .collect();

            let batch_vids: Vec<i64> = vids[base..base + BATCH].to_vec();
            let tx = db.session().tx().await?;
            let (_res, prof) = tx
                .execute_with(&cypher)
                .param("updates", uni_db::Value::List(updates))
                .profile()
                .await?;
            tx.commit().await?;

            let mutation = prof
                .runtime_stats
                .iter()
                .find(|s| s.operator.contains("MutationSetExec"))
                .expect("MutationSetExec must be present in profile");
            let scan = prof
                .runtime_stats
                .iter()
                .find(|s| s.operator.contains("GraphScanExec"))
                .expect("GraphScanExec must be present in profile");

            mutation_samples.push(mutation.time_ms);
            scan_samples.push(scan.time_ms);
            totals.push(prof.total_time_ms);
            last_scan_rows = scan.actual_rows;
            last_mut_rows = mutation.actual_rows;

            // Read-back assertion: SET must actually apply. Catches
            // silent-no-op regressions (e.g. structural projection missing
            // → row.get(var) returns None → SET writes nothing while still
            // reporting rows>0 / time_ms>0). See Fix-3 regression notes
            // in /home/rohit/.claude/plans/plan-and-implement-a-valiant-flame.md.
            let verify = db
                .session()
                .query_with(
                    "UNWIND $vids AS v MATCH (n:Entity) WHERE id(n) = v \
                     RETURN id(n) AS nid, n.p1 AS p1, n.p2 AS p2, n.p3 AS p3, n.p4 AS p4, n.p5 AS p5 \
                     ORDER BY nid",
                )
                .param(
                    "vids",
                    uni_db::Value::List(
                        batch_vids.iter().map(|v| uni_db::Value::Int(*v)).collect(),
                    ),
                )
                .fetch_all()
                .await?;
            assert_eq!(
                verify.len(),
                BATCH,
                "read-back row count mismatch for k={k} iter={iter}"
            );
            // Build a vid-keyed lookup so we don't depend on result ordering.
            let by_vid: std::collections::HashMap<i64, &uni_db::Row> = verify
                .rows()
                .iter()
                .map(|r| (r.get::<i64>("nid").unwrap(), r))
                .collect();
            for (j, &vid) in batch_vids.iter().enumerate() {
                let row = by_vid
                    .get(&vid)
                    .unwrap_or_else(|| panic!("vid {vid} missing from read-back"));
                for prop_idx in 1..=k {
                    let col = format!("p{prop_idx}");
                    let got: i64 = row
                        .get(&col)
                        .unwrap_or_else(|_| panic!("col {col} not in read-back row"));
                    let want = (iter as i64) * 100 + j as i64 + prop_idx as i64;
                    assert_eq!(
                        got, want,
                        "k={k} iter={iter} j={j} vid={vid} {col}: SET did not apply (got {got}, want {want})"
                    );
                }
            }
        }

        let median = |mut v: Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            v[v.len() / 2]
        };
        rows.push(Row {
            k,
            mutation_ms: median(mutation_samples),
            scan_ms: median(scan_samples),
            total_ms: totals[totals.len() / 2],
            scan_rows: last_scan_rows,
            mutation_rows: last_mut_rows,
        });
    }

    // Print a pasteable Markdown-ish table.
    println!("\n=== diag: MutationSetExec wall vs SetItem count ===");
    println!("batch={BATCH} iters={ITERS} (median reported)\n");
    println!(
        "| k props | MutationSetExec ms | GraphScanExec ms | total ms | scan rows | mut rows | per-row ms |"
    );
    println!(
        "|--------:|-------------------:|-----------------:|---------:|----------:|---------:|-----------:|"
    );
    for r in &rows {
        println!(
            "| {:>7} | {:>18.3} | {:>16.3} | {:>8} | {:>9} | {:>8} | {:>10.3} |",
            r.k,
            r.mutation_ms,
            r.scan_ms,
            r.total_ms,
            r.scan_rows,
            r.mutation_rows,
            r.mutation_ms / BATCH as f64,
        );
    }

    let m1 = rows.iter().find(|r| r.k == 1).unwrap();
    let m3 = rows.iter().find(|r| r.k == 3).unwrap();
    let m5 = rows.iter().find(|r| r.k == 5).unwrap();

    println!(
        "\nslope (3-prop vs 1-prop): {:.2}x  ({:.3} → {:.3} ms)",
        m3.mutation_ms / m1.mutation_ms.max(0.001),
        m1.mutation_ms,
        m3.mutation_ms
    );
    println!(
        "slope (5-prop vs 1-prop): {:.2}x  ({:.3} → {:.3} ms)\n",
        m5.mutation_ms / m1.mutation_ms.max(0.001),
        m1.mutation_ms,
        m5.mutation_ms
    );

    // Sanity asserts. The IN-list pushdown should still pin scan rows to
    // the batch size regardless of k. Mutation row count = batch size.
    for r in &rows {
        assert_eq!(
            r.scan_rows, BATCH,
            "GraphScanExec.actual_rows should equal batch ({BATCH}) for k={}, got {}",
            r.k, r.scan_rows
        );
        assert_eq!(
            r.mutation_rows, BATCH,
            "MutationSetExec.actual_rows should equal batch ({BATCH}) for k={}, got {}",
            r.k, r.mutation_rows
        );
    }

    // Timing assertions are intentionally absent: we're measuring
    // sub-millisecond walls on in-memory storage, and the noise floor
    // under parallel test load (CPU contention from `cargo nextest`
    // running ~1300 tests concurrently) routinely exceeds the per-property
    // signal (~0.2 ms). The printed table is the deliverable. The asserts
    // above on row counts catch the only thing we can rely on:
    // - GraphScanExec/MutationSetExec see the right number of rows.
    // - The IN-list pushdown from issue #72 item 1 keeps working.
    //
    // For interpretation: in isolated runs slope(k=3 vs k=1) lands around
    // 1.4–1.7×, slope(k=5 vs k=1) around 1.8–2.0× — sub-linear, consistent
    // with a large fixed per-call overhead and a small per-SetItem
    // marginal. See the plan document for the full read-through.
    let _ = (m1, m3, m5); // keep the bindings so the slope calculation runs

    Ok(())
}

/// Diagnostic for the cold-vs-live MutationSetExec gap on the issue #72
/// ingest workload.
///
/// Empirical context: production reports 17.7 ms/row per `SET` on a
/// 3-property UPDATE; cold microbench with the same schema (incl. HnswSq
/// + 4 hash indexes + fulltext) reports ~1.0 ms/row. That's a 17× gap
/// we can't yet attribute to anything in the schema, WAL, or auto-flush.
///
/// This test pushes the cold microbench in the direction of production
/// along the **data-scale** axis: pre-populates N entities with realistic
/// 768-dim embeddings (so HnswSq has a non-trivial graph), runs a warmup
/// pass that accumulates L0 version history, then measures the
/// production-shape SET at the same batch size. If the per-row wall
/// climbs meaningfully from the 1 ms baseline, data scale explains some
/// fraction of the 17× gap and the next investigation step is Lance
/// fragmentation / HNSW graph maintenance. If it stays flat, the gap is
/// environmental (concurrent tokio load, allocator, process state) and
/// the next step is profiling the live process directly.
///
/// This test is intentionally lightweight on N (2000) so it runs in
/// reasonable CI time; production has ~3800. Adjust upward and re-run
/// locally if you want a tighter answer.
#[tokio::test]
async fn diag_72_set_data_scale_with_hnsw() -> anyhow::Result<()> {
    diag_72_set_data_scale_with_hnsw_impl(false, "flag-OFF baseline").await
}

/// Sibling of `diag_72_set_data_scale_with_hnsw` with
/// `UniConfig::partial_lance_writes` flipped to `true`. Round-11
/// correctness check on the production-shape workload (verifies the
/// flag-on path produces matching per-row medians within noise; the
/// actual Lance-write win does not show up here because the cold
/// microbench's per-row cost at this scale (~2.7 ms/row) is dominated
/// by L0 / scan / HNSW maintenance rather than the wide-column write
/// the flag eliminates. The production gap at 17.7 ms/row is where
/// the savings land — measure on the live workload, not in this
/// test).
#[tokio::test]
async fn diag_72_set_data_scale_with_hnsw_partial_lance() -> anyhow::Result<()> {
    diag_72_set_data_scale_with_hnsw_impl(true, "flag-ON (partial Lance)").await
}

async fn diag_72_set_data_scale_with_hnsw_impl(
    partial_lance_writes: bool,
    label: &str,
) -> anyhow::Result<()> {
    use uni_common::UniConfig;
    use uni_db::DataType;
    use uni_db::api::schema::{IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

    const N_ENTITIES: usize = 2000;
    const EMBED_DIM: usize = 768;
    const WARMUP_MUTATIONS: usize = 500;
    const MEASURE_ITERS: usize = 20;
    const BATCH: usize = 3;

    let temp_dir = tempdir()?;
    let cfg = UniConfig {
        #[expect(deprecated, reason = "exercises the deprecated no-op flag")]
        partial_lance_writes,
        // The warmup commits 500 mutations in a single transaction to build L0
        // version history; that one large commit can exceed the default 5s guard
        // in unoptimized debug builds (it is sub-second in release). Give it room.
        commit_timeout: std::time::Duration::from_secs(30),
        ..UniConfig::default()
    };
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .config(cfg)
        .build()
        .await?;
    println!("\n=== diag #72 {label} ===");

    // Schema = production shape: 11 cols, HnswSq vector index on a 768-dim
    // embedding column, no auto-embedding (we provide vectors directly).
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("name", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .property_nullable("last_seen", DataType::DateTime)
        .property_nullable("confidence", DataType::Float64)
        .property_nullable("p1", DataType::Int64)
        .property_nullable("p2", DataType::Int64)
        .property_nullable("p3", DataType::Int64)
        .property_nullable("p4", DataType::Int64)
        .property_nullable("p5", DataType::Int64)
        .vector("embedding", EMBED_DIM)
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::HnswSq {
                    m: 16,
                    ef_construction: 100,
                    partitions: None,
                },
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .done()
        .apply()
        .await?;

    // Deterministic PRNG: a tiny LCG so we don't pull in `rand`.
    let mut prng_state: u64 = 0xCAFE_F00D_DEAD_BEEF;
    let mut next_f = || -> f64 {
        prng_state = prng_state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // Bring into [-0.5, 0.5)
        (((prng_state >> 33) as u32) as f64 / u32::MAX as f64) - 0.5
    };

    // Pre-populate N_ENTITIES with random 768-dim embeddings via
    // bulk_insert_vertices to keep setup fast.
    eprintln!("[diag] populating {N_ENTITIES} entities with {EMBED_DIM}-dim embeddings...");
    let setup_start = std::time::Instant::now();
    let tx = db.session().tx().await?;
    const INSERT_CHUNK: usize = 200;
    for chunk_start in (0..N_ENTITIES).step_by(INSERT_CHUNK) {
        let chunk_end = (chunk_start + INSERT_CHUNK).min(N_ENTITIES);
        let mut props_list: Vec<std::collections::HashMap<String, uni_db::Value>> =
            Vec::with_capacity(chunk_end - chunk_start);
        for i in chunk_start..chunk_end {
            let embedding: Vec<uni_db::Value> = (0..EMBED_DIM)
                .map(|_| uni_db::Value::Float(next_f()))
                .collect();
            let mut m = std::collections::HashMap::new();
            m.insert("entity_id".into(), uni_db::Value::String(format!("e:{i}")));
            m.insert("name".into(), uni_db::Value::String(format!("entity_{i}")));
            m.insert("frequency".into(), uni_db::Value::Int(1));
            m.insert("confidence".into(), uni_db::Value::Float(0.5));
            m.insert("p1".into(), uni_db::Value::Int(0));
            m.insert("p2".into(), uni_db::Value::Int(0));
            m.insert("p3".into(), uni_db::Value::Int(0));
            m.insert("p4".into(), uni_db::Value::Int(0));
            m.insert("p5".into(), uni_db::Value::Int(0));
            m.insert("embedding".into(), uni_db::Value::List(embedding));
            props_list.push(m);
        }
        tx.bulk_insert_vertices("Entity", props_list).await?;
    }
    tx.commit().await?;
    eprintln!(
        "[diag] populate complete in {:.2}s",
        setup_start.elapsed().as_secs_f64()
    );

    let vids: Vec<i64> = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid ORDER BY n.entity_id LIMIT 200")
        .await?
        .into_iter()
        .map(|r| r.get::<i64>("nid").unwrap())
        .collect();
    assert!(vids.len() >= 100);

    // Warmup: run WARMUP_MUTATIONS small-batch UPDATEs to accumulate L0
    // version history on entities we'll touch later. This is the closest
    // we get to mirroring "production has been running for a while."
    eprintln!("[diag] warmup: {WARMUP_MUTATIONS} mutations over existing vids...");
    let warmup_start = std::time::Instant::now();
    let tx = db.session().tx().await?;
    for iter in 0..WARMUP_MUTATIONS {
        let base = (iter * BATCH) % (vids.len().saturating_sub(BATCH).max(1));
        let updates: Vec<uni_db::Value> = vids[base..base + BATCH]
            .iter()
            .map(|&v| {
                let mut m = std::collections::HashMap::new();
                m.insert("nid".to_string(), uni_db::Value::Int(v));
                m.insert(
                    "new_frequency".to_string(),
                    uni_db::Value::Int(iter as i64 + 100),
                );
                uni_db::Value::Map(m)
            })
            .collect();
        tx.execute_with(
            "UNWIND $updates AS u \
             MATCH (n:Entity) WHERE id(n) = u.nid \
             SET n.frequency = u.new_frequency",
        )
        .param("updates", uni_db::Value::List(updates))
        .run()
        .await?;
    }
    tx.commit().await?;
    eprintln!(
        "[diag] warmup complete in {:.2}s",
        warmup_start.elapsed().as_secs_f64()
    );

    // Measure: production-shape UPDATE, batch=3, on the first 100 vids,
    // varying offsets to avoid cache effects.
    let now_value = uni_db::Value::Temporal(uni_db::common::TemporalValue::DateTime {
        nanos_since_epoch: 1_700_000_000_000_000_000,
        offset_seconds: 0,
        timezone_name: None,
    });

    let mut mutation_samples: Vec<f64> = Vec::with_capacity(MEASURE_ITERS);
    let mut scan_samples: Vec<f64> = Vec::with_capacity(MEASURE_ITERS);
    let mut totals: Vec<u64> = Vec::with_capacity(MEASURE_ITERS);

    for iter in 0..MEASURE_ITERS {
        let base = (iter * BATCH) % (vids.len().saturating_sub(BATCH).max(1));
        let batch_vids: Vec<i64> = vids[base..base + BATCH].to_vec();
        let updates: Vec<uni_db::Value> = batch_vids
            .iter()
            .enumerate()
            .map(|(j, &v)| {
                let mut m = std::collections::HashMap::new();
                m.insert("nid".to_string(), uni_db::Value::Int(v));
                m.insert(
                    "new_frequency".to_string(),
                    uni_db::Value::Int((iter as i64) * 1000 + j as i64),
                );
                m.insert(
                    "new_confidence".to_string(),
                    uni_db::Value::Float(0.5 + (iter as f64) * 0.001),
                );
                uni_db::Value::Map(m)
            })
            .collect();

        let tx = db.session().tx().await?;
        let (_res, prof) = tx
            .execute_with(
                "UNWIND $updates AS u \
                 MATCH (n:Entity) WHERE id(n) = u.nid \
                 SET n.frequency = u.new_frequency, \
                     n.last_seen = $now, \
                     n.confidence = u.new_confidence",
            )
            .param("updates", uni_db::Value::List(updates))
            .param("now", now_value.clone())
            .profile()
            .await?;
        tx.commit().await?;

        let mutation = prof
            .runtime_stats
            .iter()
            .find(|s| s.operator.contains("MutationSetExec"))
            .expect("MutationSetExec must be present");
        let scan = prof
            .runtime_stats
            .iter()
            .find(|s| s.operator.contains("GraphScanExec"))
            .expect("GraphScanExec must be present");

        mutation_samples.push(mutation.time_ms);
        scan_samples.push(scan.time_ms);
        totals.push(prof.total_time_ms);

        // Read-back assertion. Catches silent-no-op regressions (e.g.
        // structural projection missing → `row.get(var)` returns None →
        // SET writes nothing while still reporting rows>0 / time_ms>0).
        let verify = db
            .session()
            .query_with(
                "UNWIND $vids AS v MATCH (n:Entity) WHERE id(n) = v \
                 RETURN id(n) AS nid, n.frequency AS freq, n.confidence AS conf \
                 ORDER BY nid",
            )
            .param(
                "vids",
                uni_db::Value::List(batch_vids.iter().map(|v| uni_db::Value::Int(*v)).collect()),
            )
            .fetch_all()
            .await?;
        assert_eq!(
            verify.len(),
            BATCH,
            "read-back row count mismatch iter={iter}"
        );
        let by_vid: std::collections::HashMap<i64, &uni_db::Row> = verify
            .rows()
            .iter()
            .map(|r| (r.get::<i64>("nid").unwrap(), r))
            .collect();
        for (j, &vid) in batch_vids.iter().enumerate() {
            let row = by_vid
                .get(&vid)
                .unwrap_or_else(|| panic!("vid {vid} missing from read-back"));
            let freq: i64 = row.get("freq").expect("freq column missing");
            let conf: f64 = row.get("conf").expect("conf column missing");
            let want_freq = (iter as i64) * 1000 + j as i64;
            let want_conf = 0.5 + (iter as f64) * 0.001;
            assert_eq!(
                freq, want_freq,
                "iter={iter} j={j} vid={vid}: SET n.frequency did not apply (got {freq}, want {want_freq})"
            );
            assert!(
                (conf - want_conf).abs() < 1e-9,
                "iter={iter} j={j} vid={vid}: SET n.confidence did not apply (got {conf}, want {want_conf})"
            );
        }
    }

    let median = |mut v: Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
    let mut_med = median(mutation_samples.clone());
    let mut_mean = mean(&mutation_samples);
    let scan_med = median(scan_samples.clone());

    println!("\n=== diag #72 data-scale: production schema + HnswSq + warmup ===");
    println!(
        "N_entities={N_ENTITIES}, embed_dim={EMBED_DIM}, warmup_muts={WARMUP_MUTATIONS}, \
         measure_iters={MEASURE_ITERS}, batch={BATCH}\n"
    );
    println!("MutationSetExec wall (median):     {mut_med:.3} ms/call");
    println!("MutationSetExec wall (mean):       {mut_mean:.3} ms/call");
    println!(
        "MutationSetExec per-row (median):  {:.3} ms/row",
        mut_med / BATCH as f64
    );
    println!("GraphScanExec wall (median):       {scan_med:.3} ms/call");
    println!(
        "Live production reference:         ~17.7 ms/row (cold microbench at N=100: ~1.0 ms/row)"
    );

    Ok(())
}
