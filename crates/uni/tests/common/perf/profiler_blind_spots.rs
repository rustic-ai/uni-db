// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Guards for costs the profiler used to be unable to see (O1 of
//! `docs/proposals/open_issues_plan_2026_10_17.md`).
//!
//! Each test runs the same shape at two scales under `profile()` and asserts
//! on counters, not wall time:
//!
//! - **VLP property maps.** `-[:R*1..3 {w: 0}]->` and `->(b:N {x: 2})` used to
//!   enumerate the whole edge type before the anchored walk; O1 made that scan
//!   reach `rows_scanned`. P7 replaced it with a read of the reachable region
//!   only, and the guard now pins that read exactly. A QPP's per-hop maps are
//!   read the same way, one slot per hop of the iteration, and pinned too.
//! - **Locy IS NOT anti-join and fact dedup.** Both ran outside any physical
//!   plan, so the per-iteration operator tree of a rule never showed them.
//! - **Planning.** Physical planning — once per query, once per correlated row,
//!   once per clause body per fixpoint iteration — had no counter at all.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{Uni, UniConfig, Value};

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

/// The `:R` edges of [`flushed_graph`]`(n)`, as `(src, dst, w)`.
fn ladder_edges(n: usize) -> Vec<(usize, usize, i64)> {
    (0..n)
        .flat_map(|i| {
            [((i + 1) % n, i % 10), ((i * 7 + 3) % n, (i * 3) % 10)].map(|(d, w)| (i, d, w as i64))
        })
        .collect()
}

/// What a map-gated walk from vertex 0 over `*1..max_hops` outgoing must read:
/// the edges leaving every vertex reached through edges that pass `keep`
/// (each read once), and the distinct vertices those passing edges reach.
///
/// This is the P7 contract for the map's cost, computed from the graph alone:
/// a breadth-first region walk that expands only through passing edges, up to
/// the hop bound. The whole-type pre-scan it replaced read all `2n` edges.
fn region_reads(n: usize, max_hops: usize, keep: impl Fn(i64) -> bool) -> (usize, usize) {
    let edges = ladder_edges(n);
    let mut depth = vec![usize::MAX; n];
    depth[0] = 0;
    let mut frontier = vec![0usize];
    let mut read = std::collections::HashSet::new();
    let mut reached = std::collections::HashSet::new();
    let mut d = 0;
    while !frontier.is_empty() && d < max_hops {
        let mut next = Vec::new();
        for &v in &frontier {
            for (k, &(src, dst, w)) in edges.iter().enumerate() {
                if src != v {
                    continue;
                }
                read.insert(k);
                if keep(w) {
                    reached.insert(dst);
                    if depth[dst] > d + 1 {
                        depth[dst] = d + 1;
                        next.push(dst);
                    }
                }
            }
        }
        frontier = next;
        d += 1;
    }
    (read.len(), reached.len())
}

/// A VLP inline property map reads properties only for the region the walk
/// can reach, and `rows_scanned` reports exactly that read, at both scales.
///
/// History. O1 made the old whole-type pre-scan visible: `{w: 1}` read all
/// `2n` edges before an anchored walk, and this guard asserted
/// `rows_scanned >= 2n`. P7 removed that pre-scan (the map is evaluated per
/// input batch over the reachable region, `ReachableMapFilter`), so the guard
/// now pins the new cost *exactly*: the anchor's one row plus one row per
/// edge (edge map) or reached vertex (target map) whose properties were read,
/// as computed from the graph by [`region_reads`] (plus, for the target map,
/// one row per matched endpoint hydrated for the `FilterExec` above). It still has teeth both
/// ways — reads hidden from `rows_scanned` again make the count fall to the
/// anchor's 1, and a return of the whole-type scan makes it grow with `n`.
#[tokio::test(flavor = "multi_thread")]
async fn vlp_property_map_reads_only_the_reachable_region() -> Result<()> {
    const EDGE_MAP: &str = "MATCH (a:N {uid: 'u0'})-[:R*1..3 {w: 0}]->(b) RETURN count(*) AS c";
    const TARGET_MAP: &str = "MATCH (a:N {uid: 'u0'})-[:R*1..3]->(b:N {x: 2}) RETURN count(*) AS c";
    const CONTROL: &str = "MATCH (a:N {uid: 'u0'})-[:R*1..3]->(b) RETURN count(*) AS c";

    for n in [200usize, 800] {
        let db = flushed_graph(n).await?;
        let (_, edge_rows, _) = profiled(&db, EDGE_MAP).await?;
        let (target_matches, target_rows, _) = profiled(&db, TARGET_MAP).await?;
        let (_, control_rows, _) = profiled(&db, CONTROL).await?;
        let (edges_read, _) = region_reads(n, 3, |w| w == 0);
        let (_, vertices_read) = region_reads(n, 3, |_| true);
        eprintln!(
            "N={n}: rows_scanned edge-map={edge_rows} (region {edges_read} edges) \
             target-map={target_rows} (region {vertices_read} vertices) control={control_rows}"
        );
        assert_eq!(
            control_rows, 1,
            "N={n}: the map-free walk reads only its anchor"
        );
        assert!(
            edges_read > 1 && vertices_read > 1,
            "fixture must read something"
        );
        assert_eq!(
            edge_rows,
            1 + edges_read,
            "N={n}: the edge map must read exactly the {edges_read} edges of the \
             reachable region (plus the anchor) and report them in rows_scanned"
        );
        // Each matched path then hydrates its endpoint's properties for the
        // `FilterExec` above the walk: one more row per match.
        assert_eq!(
            target_rows,
            1 + vertices_read + target_matches as usize,
            "N={n}: the target map must read exactly the {vertices_read} reached \
             vertices (plus the anchor and the {target_matches} matched endpoints) \
             and report them in rows_scanned"
        );
    }
    Ok(())
}

/// What a quantified walk from vertex 0 must read for
/// `((x)-[:R {w: 0}]->(y)-[:R]->(z)){1,iterations}`: the hop-0 edges leaving
/// every vertex reached at the start of an iteration through passing edges,
/// each read once. Hop 1 has no map and reads nothing.
///
/// The region pass tracks `(vertex, hop of the iteration)` pairs at their
/// shallowest depth, up to `2 * iterations` hops; this is that pass computed
/// from the graph alone.
fn qpp_region_reads(n: usize, iterations: usize, keep: impl Fn(i64) -> bool) -> usize {
    let edges = ladder_edges(n);
    let mut depth: HashMap<(usize, usize), usize> = HashMap::from([((0, 0), 0)]);
    let mut frontier = vec![(0usize, 0usize)];
    let mut read = std::collections::HashSet::new();
    let mut d = 0;
    while !frontier.is_empty() && d < 2 * iterations {
        let mut next = Vec::new();
        for &(v, slot) in &frontier {
            for (k, &(src, dst, w)) in edges.iter().enumerate() {
                if src != v {
                    continue;
                }
                if slot == 0 {
                    read.insert(k);
                    if !keep(w) {
                        continue;
                    }
                }
                let state = (dst, (slot + 1) % 2);
                if depth.get(&state).is_none_or(|&s| s > d + 1) {
                    depth.insert(state, d + 1);
                    next.push(state);
                }
            }
        }
        frontier = next;
        d += 1;
    }
    read.len()
}

/// A quantified path pattern's per-hop map reads properties only for the
/// region the walk can reach, and `rows_scanned` reports exactly that read.
///
/// History. A QPP's hop maps were evaluated by the whole-type pre-scan
/// (`build_edge_property_filter`), so `{w: 0}` on one hop read all `2n` `:R`
/// edges before an anchored walk, and this guard asserted `rows_scanned >=
/// 2n`. The region read now models each hop of the iteration as its own slot
/// (`ReachableMapFilter::for_steps`), so the guard pins the read exactly, as
/// computed from the graph by [`qpp_region_reads`]: the anchor's one row plus
/// one row per hop-0 edge whose `w` was read. The answer is checked against
/// the same walk with the map written in `WHERE`. A return of the whole-type
/// scan makes the count grow with `n`; reads hidden from `rows_scanned` make
/// it fall to 1.
#[tokio::test(flavor = "multi_thread")]
async fn qpp_property_map_reads_only_the_reachable_region() -> Result<()> {
    const QPP: &str = "MATCH (a:N {uid: 'u0'})((x)-[:R {w: 0}]->(y)-[:R]->(z)){1,2}(b) \
                       RETURN count(*) AS c";
    // One and two iterations, spelled out.
    const REFERENCE: [&str; 2] = [
        "MATCH (a:N {uid: 'u0'})-[r1:R]->()-[:R]->(b) WHERE r1.w = 0 RETURN count(*) AS c",
        "MATCH (a:N {uid: 'u0'})-[r1:R]->()-[:R]->()-[r2:R]->()-[:R]->(b) \
         WHERE r1.w = 0 AND r2.w = 0 RETURN count(*) AS c",
    ];
    const CONTROL: &str = "MATCH (a:N {uid: 'u0'})((x)-[:R]->(y)-[:R]->(z)){1,2}(b) \
                           RETURN count(*) AS c";
    for n in [200usize, 800] {
        let db = flushed_graph(n).await?;
        let (count, rows, _) = profiled(&db, QPP).await?;
        let want = profiled(&db, REFERENCE[0]).await?.0 + profiled(&db, REFERENCE[1]).await?.0;
        let (_, control_rows, _) = profiled(&db, CONTROL).await?;
        let reads = qpp_region_reads(n, 2, |w| w == 0);
        eprintln!(
            "N={n}: QPP map rows_scanned={rows} (region {reads} edges) control={control_rows}"
        );
        assert_eq!(count, want, "N={n}: the QPP map changed the answer");
        assert!(
            count > 0 && reads > 1,
            "N={n}: fixture must match and read something"
        );
        assert_eq!(
            control_rows, 1,
            "N={n}: the map-free walk reads only its anchor"
        );
        assert_eq!(
            rows,
            1 + reads,
            "N={n}: the hop map must read exactly the {reads} hop-0 edges of the reachable \
             region (plus the anchor) and report them in rows_scanned, not all {} :R edges",
            2 * n
        );
    }
    Ok(())
}

/// The map's reads over L0 edges count too.
///
/// O1 asserted that the whole-type pre-scan's 60 L0 edges appeared in
/// `rows_scanned` and `l0_reads`. With the region read (P7) the map reads
/// only what `u1` reaches: its one `:R` edge, to `v1`, which has none. That
/// read is counted in `rows_scanned`; it is a batched point read whose tier
/// the property layer does not report, so `l0_reads` is not asserted.
#[tokio::test]
async fn vlp_property_map_region_read_counts_l0_edges() -> Result<()> {
    // No timer flush: the graph must stay in L0 across both profiled queries.
    // A flush landing between them, or in flight during one (which scans the
    // pending L0 and the written Lance version both), skews the difference.
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
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

    // The anchor lookup scans the 120 L0 `:N` rows either way.
    let (count, scanned, _) = profiled(
        &db,
        "MATCH (a:N {uid: 'u1'})-[:R*1..3 {w: 1}]->(b) RETURN count(*) AS c",
    )
    .await?;
    let (_, control_scanned, _) = profiled(
        &db,
        "MATCH (a:N {uid: 'u1'})-[:R*1..3]->(b) RETURN count(*) AS c",
    )
    .await?;
    eprintln!("L0: rows_scanned {scanned} vs control {control_scanned}");
    assert_eq!(count, 1, "u1 -[w=1]-> v1 is the only match");
    assert_eq!(
        scanned,
        control_scanned + 1,
        "the map reads exactly u1's one L0 edge, and rows_scanned must show it \
         ({scanned} vs {control_scanned} without the map); 60 more is the \
         whole-type pre-scan back"
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
        let chain_facts = result
            .derived_rules()
            .await
            .unwrap()
            .get("d")
            .map_or(0, Vec::len);
        let ok_facts = result
            .derived_rules()
            .await
            .unwrap()
            .get("ok")
            .map_or(0, Vec::len);
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
/// returns the first row-producing result (`Query` or `Cypher`), descending
/// into an `Assume` depth first, so the rows an `ASSUME … THEN { QUERY … }`
/// produced are returned by `rows()`, as the Python binding's `rows` does.
/// Before this contract, Rust returned `None` here while Python returned the
/// rows.
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
        [uni_db::locy::CommandResult::Assume { results, .. }] => match results.as_slice() {
            [uni_db::locy::CommandResult::Query(rows)] => rows,
            other => panic!("expected the body's one Query result, got {other:?}"),
        },
        other => panic!("expected exactly one Assume result, got {other:?}"),
    };
    assert_eq!(assume_rows.len(), 3, "the hypothetical sees a, b and zz");
    let rows = result
        .rows()
        .expect("rows() returns the ASSUME body's QUERY rows");
    assert!(
        std::ptr::eq(rows, assume_rows),
        "rows() is the ASSUME body's Query result itself"
    );
    assert_eq!(result.columns(), Some(vec!["uid".to_string()]));
    Ok(())
}

/// Timing probe for P7: an anchored map-gated walk at two scales. Prints only.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing probe, run manually"]
async fn vlp_property_map_timing_probe() -> Result<()> {
    for n in [20_000usize, 80_000] {
        let db = flushed_graph(n).await?;
        for q in [
            "MATCH (a:N {uid: 'u0'})-[:R*1..3 {w: 0}]->(b) RETURN count(*) AS c",
            "MATCH (a:N {uid: 'u0'})-[:R*1..3]->(b:N {x: 2}) RETURN count(*) AS c",
            "MATCH (a:N {uid: 'u0'})-[:R*1..3]->(b) RETURN count(*) AS c",
        ] {
            let mut best = f64::MAX;
            let mut rows = 0;
            for _ in 0..3 {
                let start = std::time::Instant::now();
                let (_, r, _) = profiled(&db, q).await?;
                best = best.min(start.elapsed().as_secs_f64() * 1000.0);
                rows = r;
            }
            eprintln!("P7 probe n={n}: best of 3 {best:.1} ms, rows_scanned {rows}: {q}");
        }
    }
    Ok(())
}

/// Timing probe for the region read's fall-back: a deep unbounded walk on a
/// chain where every edge passes the map, so the region grows by one vertex
/// per property-read round. Prints only.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing probe, run manually"]
async fn vlp_property_map_deep_walk_timing_probe() -> Result<()> {
    for n in [20_000usize, 80_000] {
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
        let edges = (0..n - 1)
            .map(|i| {
                uni_bulk::EdgeData::new(
                    vids[i],
                    vids[i + 1],
                    HashMap::from([("w".to_string(), Value::Int(1))]),
                )
            })
            .collect();
        bulk.insert_edges("R", edges).await?;
        bulk.commit().await?;
        tx.commit().await?;
        db.flush().await?;
        for q in [
            "MATCH (a:N {uid: 'u0'})-[:R*1..12 {w: 1}]->(b) RETURN count(*) AS c",
            "MATCH (a:N {uid: 'u0'})-[:R*1..40 {w: 1}]->(b) RETURN count(*) AS c",
            "MATCH (a:N {uid: 'u0'})-[:R* {w: 1}]->(b) RETURN count(*) AS c",
        ] {
            let mut best = f64::MAX;
            let mut rows = 0;
            for _ in 0..3 {
                let start = std::time::Instant::now();
                let (_, r, _) = profiled(&db, q).await?;
                best = best.min(start.elapsed().as_secs_f64() * 1000.0);
                rows = r;
            }
            eprintln!("P7 deep n={n}: best of 3 {best:.1} ms, rows_scanned {rows}: {q}");
        }
        db.shutdown().await?;
    }
    Ok(())
}
