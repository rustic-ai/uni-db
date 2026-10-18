// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Guards for costs the profiler used to be unable to see (O1 of
//! `docs/proposals/open_issues_plan_2026_10_17.md`).
//!
//! Each test runs the same shape at two scales under `profile()` and asserts
//! on counters, not wall time:
//!
//! - **VLP property-map pre-scans.** `-[:R*1..3 {w: 1}]->` and
//!   `->(b:N {x: 2})` enumerate the whole edge type before the anchored walk
//!   (`build_edge_property_filter` / `build_accepting_vid_filter`). That scan
//!   must reach `rows_scanned`, and must grow with the edge type while the
//!   map-free control stays flat.
//! - **Locy IS NOT anti-join and fact dedup.** Both ran outside any physical
//!   plan, so the per-iteration operator tree of a rule never showed them.
//! - **Planning.** Physical planning — once per query, once per correlated row,
//!   once per clause body per fixpoint iteration — had no counter at all.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{Uni, Value};

fn locy_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 500,
        timeout: Some(Duration::from_secs(120)),
        ..Default::default()
    }
}

/// `n` nodes `:N {uid, x}` and `2n` `:R {w}` edges (`i -> i+1`, `i -> 7i+3`),
/// bulk-loaded and flushed so every edge is read from L1.
async fn flushed_graph(n: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    for ddl in [
        "CREATE LABEL N (uid STRING, x INT)",
        "CREATE EDGE TYPE R (w INT) FROM N TO N",
        "CREATE INDEX idx_n_uid FOR (n:N) ON (n.uid)",
    ] {
        tx.execute(ddl).await?;
    }
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let props: Vec<HashMap<String, Value>> = (0..n)
        .map(|i| {
            HashMap::from([
                ("uid".to_string(), Value::String(format!("u{i}"))),
                ("x".to_string(), Value::Int(i as i64)),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("N", props).await?;
    let mut edges = Vec::with_capacity(2 * n);
    for i in 0..n {
        for (dst, w) in [((i + 1) % n, i % 10), ((i * 7 + 3) % n, (i * 3) % 10)] {
            edges.push(uni_bulk::EdgeData::new(
                vids[i],
                vids[dst],
                HashMap::from([("w".to_string(), Value::Int(w as i64))]),
            ));
        }
    }
    bulk.insert_edges("R", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// `(count(*), rows_scanned, l0_reads)` of a profiled query.
async fn profiled(db: &Uni, q: &str) -> Result<(i64, usize, usize)> {
    let (result, _profile) = db.session().query_with(q).profile().await?;
    let count = result.rows()[0].get::<i64>("c")?;
    let m = result.metrics();
    Ok((count, m.rows_scanned, m.l0_reads))
}

/// A VLP inline property map pre-scans the whole edge type; `rows_scanned`
/// must report it, at both scales, while the map-free control stays flat.
#[tokio::test(flavor = "multi_thread")]
async fn vlp_property_map_prescan_counts_toward_rows_scanned() -> Result<()> {
    const EDGE_MAP: &str = "MATCH (a:N {uid: 'u0'})-[:R*1..3 {w: 1}]->(b) RETURN count(*) AS c";
    const TARGET_MAP: &str = "MATCH (a:N {uid: 'u0'})-[:R*1..3]->(b:N {x: 2}) RETURN count(*) AS c";
    const CONTROL: &str = "MATCH (a:N {uid: 'u0'})-[:R*1..3]->(b) RETURN count(*) AS c";

    let mut edge_scanned = Vec::new();
    let mut target_scanned = Vec::new();
    for n in [200usize, 800] {
        let db = flushed_graph(n).await?;
        let edges = 2 * n;

        let (_, edge_rows, _) = profiled(&db, EDGE_MAP).await?;
        let (_, target_rows, _) = profiled(&db, TARGET_MAP).await?;
        let (_, control_rows, _) = profiled(&db, CONTROL).await?;
        eprintln!(
            "N={n}: rows_scanned edge-map={edge_rows} target-map={target_rows} control={control_rows}"
        );

        assert!(
            edge_rows >= edges,
            "N={n}: the edge-map pre-scan reads all {edges} :R edges but \
             rows_scanned = {edge_rows}"
        );
        assert!(
            target_rows >= edges,
            "N={n}: the target-map pre-scan reads all {edges} :R edges but \
             rows_scanned = {target_rows}"
        );
        // The anchored walk itself is not a scan; only the anchor lookup is.
        assert!(
            control_rows < n / 4,
            "N={n}: control VLP without a map should not scan the edge type, \
             rows_scanned = {control_rows}"
        );
        edge_scanned.push(edge_rows);
        target_scanned.push(target_rows);
    }
    // 4x the edges: the pre-scan's count follows the edge type, not the answer.
    for (what, v) in [("edge map", &edge_scanned), ("target map", &target_scanned)] {
        assert!(
            v[1] >= v[0] * 3,
            "{what}: rows_scanned should grow ~4x with the edge type, got {v:?}"
        );
    }
    Ok(())
}

/// The same pre-scan over edges that are still in L0 counts as L0 rows.
#[tokio::test]
async fn vlp_property_map_prescan_counts_l0_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL N (uid STRING, x INT)").await?;
    tx.execute("CREATE EDGE TYPE R (w INT) FROM N TO N").await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "UNWIND range(0, 59) AS i \
         CREATE (a:N {uid: 'u' + toString(i), x: i})-[:R {w: i % 10}]->(:N {uid: 'v' + toString(i), x: 1000 + i})",
    )
    .await?;
    tx.commit().await?;

    // The anchor lookup scans the 120 L0 `:N` rows either way, so the
    // pre-scan is the difference between the map and the map-free control.
    let (count, scanned, l0) = profiled(
        &db,
        "MATCH (a:N {uid: 'u1'})-[:R*1..3 {w: 1}]->(b) RETURN count(*) AS c",
    )
    .await?;
    let (_, control_scanned, control_l0) = profiled(
        &db,
        "MATCH (a:N {uid: 'u1'})-[:R*1..3]->(b) RETURN count(*) AS c",
    )
    .await?;
    eprintln!(
        "L0: rows_scanned {scanned} vs control {control_scanned}; l0_reads {l0} vs {control_l0}"
    );
    assert_eq!(count, 1, "u1 -[w=1]-> v1 is the only match");
    assert!(
        scanned >= control_scanned + 60,
        "the pre-scan examined all 60 L0 :R edges: rows_scanned {scanned} vs \
         {control_scanned} without the map"
    );
    assert!(
        l0 >= control_l0 + 60,
        "the pre-scan's 60 edges came from L0: l0_reads {l0} vs {control_l0} \
         without the map"
    );
    Ok(())
}

/// Recursive and non-recursive IS NOT program over a chain of `len` nodes with
/// one banned leaf hanging off every chain node.
async fn chain_with_banned_leaves(len: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {last}) AS i \
         CREATE (:E {{uid: 'c' + toString(i), i: i, seed: i = 0}})",
        last = len - 1
    ))
    .await?;
    tx.execute(
        "MATCH (a:E), (b:E) WHERE b.i = a.i + 1 AND a.uid STARTS WITH 'c' \
         AND b.uid STARTS WITH 'c' CREATE (a)-[:T]->(b)",
    )
    .await?;
    tx.execute(
        "MATCH (a:E) WHERE a.uid STARTS WITH 'c' \
         CREATE (a)-[:T]->(:E {uid: 'x' + toString(a.i), i: -1, seed: false})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

const ISNOT_PROGRAM: &str = "\
CREATE RULE banned AS MATCH (e:E) WHERE e.uid STARTS WITH 'x' YIELD KEY e \n\
CREATE RULE ok AS MATCH (e:E) WHERE e IS NOT banned YIELD KEY e \n\
CREATE RULE d AS MATCH (a:E)-[:T]->(b:E) WHERE a.seed = true AND b IS NOT banned YIELD KEY b \n\
CREATE RULE d AS MATCH (o:E)-[:T]->(b:E) WHERE o IS d AND b IS NOT banned YIELD KEY b";

fn rule<'a>(
    profile: &'a uni_db::locy::LocyProfileOutput,
    name: &str,
) -> &'a uni_db::locy::LocyRuleProfile {
    profile
        .profile
        .strata
        .iter()
        .flat_map(|s| s.rules.iter())
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("rule {name} missing from the profile"))
}

/// Rows reported by every operator named `op` across a rule's iterations, and
/// how many entries carried that name.
fn op_rows(rule: &uni_db::locy::LocyRuleProfile, op: &str) -> (usize, usize) {
    rule.iterations
        .iter()
        .flat_map(|it| it.operators.iter())
        .filter(|o| o.operator == op)
        .fold((0, 0), |(rows, n), o| (rows + o.actual_rows, n + 1))
}

/// IS NOT and the fact dedup appear as operators in the Locy profile tree.
#[tokio::test(flavor = "multi_thread")]
async fn locy_profile_shows_isnot_antijoin_and_fact_dedup() -> Result<()> {
    for len in [6usize, 24] {
        let db = chain_with_banned_leaves(len).await?;
        let (result, profile) = db
            .session()
            .locy_with(ISNOT_PROGRAM)
            .with_config(locy_config())
            .profile()
            .await?;
        let chain_facts = result.derived.get("d").map_or(0, Vec::len);
        let ok_facts = result.derived.get("ok").map_or(0, Vec::len);
        assert_eq!(
            chain_facts,
            len - 1,
            "len={len}: d reaches c1..c{}",
            len - 1
        );
        assert_eq!(ok_facts, len, "len={len}: ok = the {len} chain nodes");

        // Non-recursive: one anti-join keeping the chain nodes, one dedup.
        let ok = rule(&profile, "ok");
        assert_eq!(
            op_rows(ok, "LocyIsNotAntiJoin"),
            (len, 1),
            "len={len}: rule ok's IS NOT anti-join should keep {len} of {} rows",
            2 * len
        );
        assert_eq!(
            op_rows(ok, "LocyFactDedup"),
            (len, 1),
            "len={len}: rule ok's dedup should keep {len} facts"
        );

        // Recursive: the anti-join runs in every iteration in which a clause
        // with IS NOT was evaluated; the dedup once per iteration, and what it
        // keeps summed over iterations is the rule's fact count.
        let d = rule(&profile, "d");
        let iterations = d.iterations.len();
        assert!(iterations >= len - 1, "len={len}: {iterations} iterations");
        let (kept, antijoins) = op_rows(d, "LocyIsNotAntiJoin");
        assert!(
            antijoins >= iterations,
            "len={len}: expected an anti-join entry in each of {iterations} \
             iterations, found {antijoins}"
        );
        assert!(
            kept >= len - 1,
            "len={len}: the anti-joins kept {kept} rows, fewer than the facts"
        );
        assert_eq!(
            op_rows(d, "LocyFactDedup"),
            (len - 1, iterations),
            "len={len}: one dedup per iteration, keeping {} facts in total",
            len - 1
        );
    }
    Ok(())
}

/// Planning time and plan counts reach the Cypher profile and the metrics; a
/// correlated subquery's per-row re-plan is counted per row.
#[tokio::test(flavor = "multi_thread")]
async fn cypher_profile_reports_planning() -> Result<()> {
    let mut per_row_plans = Vec::new();
    for n in [20usize, 80] {
        let db = Uni::in_memory().build().await?;
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            "UNWIND range(1, {n}) AS i CREATE (:N {{x: i}})-[:R]->(:M {{x: i}})"
        ))
        .await?;
        tx.commit().await?;

        let (result, profile) = db
            .session()
            .query_with("MATCH (n:N) RETURN count(n) AS c")
            .profile()
            .await?;
        assert_eq!(result.rows()[0].get::<i64>("c")?, n as i64);
        assert!(
            profile.logical_planning_ms > 0.0,
            "logical planning time missing from the profile"
        );
        assert!(
            profile.physical_planning_ms > 0.0,
            "physical planning time missing from the profile"
        );
        assert_eq!(
            profile.physical_plans, 1,
            "one physical plan for a flat query"
        );
        assert_eq!(result.metrics().physical_plans, 1);
        assert!(result.metrics().physical_plan_time > Duration::ZERO);
        assert!(
            result.metrics().plan_time > Duration::ZERO,
            "profile() left QueryMetrics::plan_time at zero"
        );

        let (result, profile) = db
            .session()
            .query_with(
                "MATCH (n:N) CALL { WITH n MATCH (n)-[:R]->(m:M) RETURN count(m) AS k } \
                 RETURN sum(k) AS c",
            )
            .profile()
            .await?;
        assert_eq!(result.rows()[0].get::<i64>("c")?, n as i64);
        assert_eq!(
            result.metrics().physical_plans,
            profile.physical_plans,
            "metrics and profile read the same counter"
        );
        eprintln!(
            "N={n}: correlated physical_plans={} physical_planning_ms={:.3} logical_planning_ms={:.3}",
            profile.physical_plans, profile.physical_planning_ms, profile.logical_planning_ms
        );
        per_row_plans.push(profile.physical_plans);
    }
    // The correlated body is planned once per outer row.
    assert!(
        per_row_plans[0] > 20 && per_row_plans[1] > 80,
        "a correlated subquery re-plans per row; counted {per_row_plans:?}"
    );
    assert!(
        per_row_plans[1] >= per_row_plans[0] * 3,
        "per-row re-plans should grow with the outer rows: {per_row_plans:?}"
    );
    Ok(())
}

/// Every fixpoint iteration re-plans its clause bodies; the Locy profile
/// reports that per iteration.
#[tokio::test(flavor = "multi_thread")]
async fn locy_profile_reports_per_iteration_planning() -> Result<()> {
    let mut plans_by_len = Vec::new();
    for len in [6usize, 24] {
        let db = chain_with_banned_leaves(len).await?;
        let (result, profile) = db
            .session()
            .locy_with(ISNOT_PROGRAM)
            .with_config(locy_config())
            .profile()
            .await?;
        let d = rule(&profile, "d");
        for it in &d.iterations {
            assert!(
                it.plans >= 1,
                "len={len}: iteration {} of d evaluated a clause body without \
                 a counted physical plan",
                it.iteration
            );
            assert!(
                it.planning_ms > 0.0,
                "len={len}: iteration {} of d has no planning time",
                it.iteration
            );
        }
        let plans: u64 = d.iterations.iter().map(|it| it.plans).sum();
        plans_by_len.push(plans);
        assert!(
            result.metrics().physical_plans >= plans,
            "len={len}: the result's metrics must include the fixpoint re-plans"
        );
        let rendered = format!("{profile}");
        eprintln!("len={len}: d plans={plans}\n{rendered}");
        assert!(
            rendered.contains("planning"),
            "Display should show planning per iteration: {rendered}"
        );
    }
    assert!(
        plans_by_len[1] >= plans_by_len[0] * 3,
        "re-plans should follow the iteration count: {plans_by_len:?}"
    );
    Ok(())
}

/// Guard for the A4 "ASSUME `rows()` read 0" reading: `LocyResult::rows()`
/// returns the first row-producing result (`Query`, `Assume` or `Cypher`), so
/// the rows an `ASSUME … THEN { QUERY … }` produced are returned by `rows()`,
/// as the Python binding's `rows` already did. Before this contract, Rust
/// returned `None` here while Python returned the rows.
#[tokio::test]
async fn assume_rows_are_returned_by_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {uid: 'a'}), (:E {uid: 'b'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE e AS MATCH (n:E) YIELD KEY n \n\
             ASSUME { CREATE (:E {uid: 'zz'}) } THEN { QUERY e RETURN n.uid AS uid }",
        )
        .with_config(locy_config())
        .run()
        .await?;

    let assume_rows = match result.command_results() {
        [uni_db::locy::CommandResult::Assume(rows)] => rows,
        other => panic!("expected exactly one Assume result, got {other:?}"),
    };
    assert_eq!(assume_rows.len(), 3, "the hypothetical sees a, b and zz");
    let rows = result
        .rows()
        .expect("rows() returns the ASSUME result's rows");
    assert!(
        std::ptr::eq(rows, assume_rows),
        "rows() is the Assume result itself"
    );
    assert_eq!(result.columns(), Some(vec!["uid".to_string()]));
    Ok(())
}
