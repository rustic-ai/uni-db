// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Entity hydration after a Locy evaluation (perf-class audit A3).
//!
//! A rule's facts carry a node as its VID and a relationship as its EID. Before
//! a result is returned those IDs are replaced by `Value::Node` / `Value::Edge`
//! ("hydration"), for every rule: `LocyResult::derived` is a public map holding
//! every rule's facts, and the Python binding converts it eagerly, so a rule the
//! program never `QUERY`s is still part of the result a caller can read.
//!
//! The audit measured that hydration dominating small programs: one unreturned
//! rule of 8k facts added 1.94 s to an 18 ms fixpoint, growing 7.2x for 4x N.
//! Two mechanisms, both measured by `locy_entity_hydration_probe`:
//!
//! - every rule was hydrated twice, once for the commands and once more for the
//!   returned map, from the same rows;
//! - each hydration ran `MATCH (entity) WHERE id(entity) IN [<N literals>]`.
//!   The scan below it already restricts itself to those VIDs, but the residual
//!   `FilterExec` evaluates the `IN` per row against the whole list, so it costs
//!   O(N²): 38.6 ms → 585.8 ms (15.2x) for 2k → 8k IDs while the scan went
//!   5.7 ms → 19.6 ms. Parsing the N-literal statement cost another 130 ms at 8k.
//!
//! The guards here pin both, and the equality tests pin that the hydrated values
//! did not change shape: every node and edge a result carries must equal what a
//! Cypher `RETURN` of that entity reads from the same session.
//!
//! Run with:
//!   cargo nextest run -p uni-db -E 'test(locy_entity_hydration)'

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Result, bail, ensure};
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{Session, Uni, Value};

/// `n` `E` vertices `c0..c{n-1}` chained by `T` edges, bulk-loaded and
/// optionally flushed so both the L0 and the L1 read paths are exercised.
async fn chain(n: usize, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING, seed BOOL)").await?;
    tx.execute("CREATE EDGE TYPE T (w INT) FROM E TO E").await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..n)
        .map(|i| {
            HashMap::from([
                ("uid".to_string(), Value::String(format!("c{i}"))),
                ("seed".to_string(), Value::Bool(i == 0)),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("E", vertices).await?;
    let edges: Vec<uni_bulk::EdgeData> = (1..n)
        .map(|i| {
            uni_bulk::EdgeData::new(
                vids[i - 1],
                vids[i],
                HashMap::from([("w".to_string(), Value::Int(i as i64))]),
            )
        })
        .collect();
    bulk.insert_edges("T", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

fn config() -> LocyConfig {
    LocyConfig {
        timeout: Some(Duration::from_secs(300)),
        ..Default::default()
    }
}

/// One fact, returned by the program's only `QUERY`.
const SMALL: &str = "CREATE RULE sd AS MATCH (e:E) WHERE e.uid = 'c0' YIELD KEY e \n\
                     QUERY sd RETURN e.uid AS uid";

/// [`SMALL`] plus a rule of one fact per vertex that no command reads.
fn small_plus_unreturned() -> String {
    format!("CREATE RULE allv AS MATCH (e:E) YIELD KEY e \n{SMALL}")
}

async fn run(db: &Uni, program: &str) -> Result<(uni_db::locy::LocyResult, Duration)> {
    let started = Instant::now();
    let result = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?;
    Ok((result, started.elapsed()))
}

/// The unreturned rule's facts are hydrated exactly once.
///
/// `rows_scanned` is deterministic, so this is the counter half of the A3
/// guard. Adding `allv` to the program costs its own fixpoint scan (N rows) and
/// one hydration of its N facts (N rows), so the difference between the two
/// programs is 2N. Hydrating it once for the commands and again for the
/// returned map read 3N, which is what this rejects. A future change that
/// anchors the fixpoint scan only lowers the difference.
#[tokio::test]
async fn locy_entity_hydration_unreturned_rule_is_hydrated_once() -> Result<()> {
    for n in [400usize, 1_600] {
        let db = chain(n, true).await?;
        let (small, _) = run(&db, SMALL).await?;
        let (big, _) = run(&db, &small_plus_unreturned()).await?;

        // The contract the optimisation must keep: the unreturned rule is in
        // the result, every fact hydrated to a node of the right vertex.
        let facts = big
            .derived_facts("allv")
            .ok_or_else(|| anyhow::anyhow!("allv missing from derived"))?;
        ensure!(
            facts.len() == n,
            "allv derived {} facts, want {n}",
            facts.len()
        );
        for row in facts {
            match row.get("e") {
                Some(Value::Node(node)) => ensure!(
                    matches!(node.properties.get("uid"), Some(Value::String(s)) if s.starts_with('c')),
                    "hydrated node lacks its uid: {node:?}"
                ),
                other => bail!("allv KEY e came back as {other:?}, not a hydrated node"),
            }
        }
        ensure!(
            big.warnings().is_empty(),
            "unexpected warnings: {:?}",
            big.warnings()
        );

        let small_rows = small.metrics().rows_scanned;
        let big_rows = big.metrics().rows_scanned;
        let added = big_rows.saturating_sub(small_rows);
        eprintln!("N={n}: rows_scanned small={small_rows} small+allv={big_rows} added={added}");
        ensure!(
            added <= 2 * n + 2,
            "adding an unreturned rule of {n} facts scanned {added} more rows; its \
             fixpoint plus one hydration is {} — a second hydration pass of the \
             same rows is back (A3)",
            2 * n
        );
    }
    Ok(())
}

/// Hydration cost grows with the facts, not with their square.
///
/// The timing half of the A3 guard. Both arms run in one process on one graph,
/// so machine speed divides out of the ratio: the control is a Cypher read that
/// materialises the same N nodes. Measured in a debug build at N = 12000:
///
/// | hydration                                        | ratio |
/// |--------------------------------------------------|-------|
/// | scan of the listed VIDs, once (current)          | 0.7x  |
/// | `id(n) IN [<N literals>]` statement, once        | 5.8x  |
/// | that statement, twice (before the fix)           | ~10x (9.6x at N = 8000) |
///
/// The quadratic rows grow with N and the linear one does not, so the margin
/// only widens on a bigger graph. Each arm is measured three times and the
/// fastest kept, as `locy_issue_267_node_key_scaling` does, so a one-off stall
/// does not decide the outcome.
///
/// **Parallelism:** wall-clock. Load moves both arms together, but re-run with
/// `-j1` before reading a failure near the threshold as a defect.
#[tokio::test(flavor = "multi_thread")]
async fn locy_entity_hydration_cost_is_linear() -> Result<()> {
    const N: usize = 12_000;
    let db = chain(N, true).await?;
    let program = small_plus_unreturned();

    let control = |db: &Uni| {
        let session = db.session();
        async move {
            let started = Instant::now();
            let rows = session.query("MATCH (e:E) RETURN e").await?.rows().len();
            ensure!(rows == N, "control read {rows} rows");
            Ok::<_, anyhow::Error>(started.elapsed())
        }
    };
    let mut ctl = Duration::MAX;
    let mut locy = Duration::MAX;
    for _ in 0..3 {
        ctl = ctl.min(control(&db).await?);
        locy = locy.min(run(&db, &program).await?.1);
    }
    let ratio = locy.as_secs_f64() / ctl.as_secs_f64().max(1e-9);
    eprintln!(
        "N={N}: locy small+allv {:.1} ms, cypher control {:.1} ms, ratio {ratio:.1}x",
        locy.as_secs_f64() * 1e3,
        ctl.as_secs_f64() * 1e3
    );
    ensure!(
        ratio < 3.0,
        "a Locy program with one unreturned rule of {N} facts cost {ratio:.1}x a \
         Cypher read of the same {N} nodes ({:.1} ms vs {:.1} ms), past the 3x guard. \
         Hydration by an `id(n) IN [<N literals>]` statement measured 5.8x here and \
         grows with N; a scan of the listed VIDs measured 0.7x (A3).",
        locy.as_secs_f64() * 1e3,
        ctl.as_secs_f64() * 1e3
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Value equality: what hydration produces is what Cypher reads.
// ---------------------------------------------------------------------------

/// Labels in a stable order, so two reads of one node compare equal whatever
/// order each path happened to list them in.
fn normalized(v: &Value) -> Value {
    match v {
        Value::Node(n) => {
            let mut n = n.clone();
            n.labels.sort();
            Value::Node(n)
        }
        other => other.clone(),
    }
}

/// Every node and edge reachable inside `v`.
fn collect_entities(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Node(_) | Value::Edge(_) => out.push(v.clone()),
        Value::List(items) => items.iter().for_each(|i| collect_entities(i, out)),
        Value::Map(m) => m.values().for_each(|i| collect_entities(i, out)),
        _ => {}
    }
}

fn rows_entities(rows: &[HashMap<String, Value>], out: &mut Vec<Value>) {
    for row in rows {
        row.values().for_each(|v| collect_entities(v, out));
    }
}

fn explain_entities(node: &uni_locy::DerivationNode, out: &mut Vec<Value>) {
    node.bindings
        .values()
        .for_each(|v| collect_entities(v, out));
    node.children.iter().for_each(|c| explain_entities(c, out));
}

/// Read `entity` back through Cypher on `session` and require equality.
async fn assert_matches_cypher(session: &Session, entity: &Value, context: &str) -> Result<()> {
    let (query, id) = match entity {
        Value::Node(n) => (
            "MATCH (x) WHERE id(x) = $id RETURN x",
            n.vid.as_u64() as i64,
        ),
        Value::Edge(e) => (
            "MATCH ()-[x]->() WHERE id(x) = $id RETURN x",
            e.eid.as_u64() as i64,
        ),
        other => bail!("{context}: not an entity: {other:?}"),
    };
    let rows = session
        .query_with(query)
        .param("id", Value::Int(id))
        .fetch_all()
        .await?;
    let rows = rows.rows();
    ensure!(
        rows.len() == 1,
        "{context}: Cypher found {} entities for id {id}",
        rows.len()
    );
    let cypher = rows[0]
        .value("x")
        .ok_or_else(|| anyhow::anyhow!("{context}: no x column"))?;
    ensure!(
        normalized(cypher) == normalized(entity),
        "{context}: hydrated entity differs from Cypher's read of it\n  hydrated: {entity:?}\n  cypher:   {cypher:?}"
    );
    Ok(())
}

/// Every KEY column of every rule holds a hydrated entity, never a raw ID.
fn assert_keys_hydrated(
    result: &uni_db::locy::LocyResult,
    rule_keys: &[(&str, &[&str])],
) -> Result<()> {
    for (rule, keys) in rule_keys {
        let facts = result
            .derived_facts(rule)
            .ok_or_else(|| anyhow::anyhow!("rule {rule} missing from derived"))?;
        ensure!(
            !facts.is_empty(),
            "rule {rule} derived nothing; the check would be vacuous"
        );
        for row in facts {
            for key in *keys {
                match row.get(*key) {
                    Some(Value::Node(_) | Value::Edge(_)) => {}
                    other => bail!("{rule}.{key} came back as {other:?}, not an entity"),
                }
            }
        }
    }
    Ok(())
}

/// Typed properties on a declared label, an undeclared label, a multi-label
/// vertex, relationships with properties, some rows flushed and some in L0, and
/// an L0 overwrite of a flushed vertex.
async fn rich_graph(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE LABEL P (name STRING, age INT, score FLOAT, ok BOOL, tags LIST<STRING>, \
         attrs MAP<STRING, FLOAT>, emb VECTOR(2))",
    )
    .await?;
    tx.execute("CREATE EDGE TYPE R (since INT, note STRING) FROM P TO P")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:P {name: 'a', age: 30, score: 1.5, ok: true, tags: ['x', 'y'], \
                 attrs: {k: 2.5}, emb: [0.25, 0.5]}), \
                (b:P {name: 'b', age: 41, ok: false}), \
                (c:P:Q {name: 'c', extra: 'undeclared', when: datetime('2024-05-06T07:08:09Z')}), \
                (a)-[:R {since: 2001, note: 'ab'}]->(b), \
                (b)-[:R {since: 2002}]->(c)",
    )
    .await?;
    tx.execute(
        "CREATE (:Q {name: 'q1', n: 7, l: [1, 2]}), (:Q {name: 'q2', d: date('2020-01-02')})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // Unflushed rows: a new vertex and edge, and an L0 overwrite of flushed `a`.
    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (c:P {name: 'c'}) CREATE (d:P {name: 'd', age: 9, tags: []}), (c)-[:R {since: 2003}]->(d)",
    )
    .await?;
    tx.execute("MATCH (a:P {name: 'a'}) SET a.age = 31, a.score = null")
        .await?;
    tx.execute("CREATE (:Q {name: 'q3'})").await?;
    tx.commit().await?;
    Ok(())
}

const RICH_RULES: &str = "\
CREATE RULE pn AS MATCH (p:P) YIELD KEY p \n\
CREATE RULE qn AS MATCH (q:Q) YIELD KEY q \n\
CREATE RULE rel AS MATCH (a:P)-[r:R]->(b:P) YIELD KEY a, KEY r, KEY b \n\
CREATE RULE reach AS MATCH (a:P)-[:R]->(b:P) YIELD KEY a, KEY b \n\
CREATE RULE reach AS MATCH (a:P)-[:R]->(m:P) WHERE m IS reach TO b YIELD KEY a, KEY b \n";

const RICH_KEYS: &[(&str, &[&str])] = &[
    ("pn", &["p"]),
    ("qn", &["q"]),
    ("rel", &["a", "r", "b"]),
    ("reach", &["a", "b"]),
];

/// Every entity in `result` — derived facts and command results alike — equals
/// Cypher's read of it on `session`. Returns how many were checked.
async fn assert_result_matches_cypher(
    session: &Session,
    result: &uni_db::locy::LocyResult,
    context: &str,
) -> Result<usize> {
    let mut entities = Vec::new();
    for rows in result.derived.values() {
        rows_entities(rows, &mut entities);
    }
    for cr in &result.command_results {
        match cr {
            CommandResult::Query(rows)
            | CommandResult::Assume(rows)
            | CommandResult::Cypher(rows) => rows_entities(rows, &mut entities),
            CommandResult::Explain(node) => explain_entities(node, &mut entities),
            _ => {}
        }
    }
    for e in &entities {
        assert_matches_cypher(session, e, context).await?;
    }
    Ok(entities.len())
}

/// Hydrated nodes and edges equal Cypher's reads across QUERY, EXPLAIN and the
/// derived-facts map, on a session, inside a transaction (pinned snapshot plus
/// the transaction's own unflushed writes), and on a fork.
#[tokio::test]
async fn locy_entity_hydration_values_match_cypher() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    rich_graph(&db).await?;
    let program = format!(
        "{RICH_RULES}\
         QUERY pn RETURN p \n\
         QUERY rel RETURN a, r, b \n\
         EXPLAIN RULE reach WHERE a.name = 'a'"
    );

    // Session.
    let session = db.session();
    let result = session
        .locy_with(&program)
        .with_config(config())
        .run()
        .await?;
    ensure!(
        result.warnings().is_empty(),
        "warnings: {:?}",
        result.warnings()
    );
    assert_keys_hydrated(&result, RICH_KEYS)?;
    let checked = assert_result_matches_cypher(&session, &result, "session").await?;
    ensure!(checked > 20, "only {checked} entities checked");

    // Transaction: its own unflushed write and delete must be honoured.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {name: 'tx-only', age: 1})").await?;
    tx.execute("MATCH (q:Q {name: 'q1'}) DETACH DELETE q")
        .await?;
    let result = tx.locy_with(&program).with_config(config()).run().await?;
    ensure!(
        result.warnings().is_empty(),
        "warnings: {:?}",
        result.warnings()
    );
    assert_keys_hydrated(&result, RICH_KEYS)?;
    let names = |rule: &str, key: &str| -> Vec<String> {
        result
            .derived_facts(rule)
            .into_iter()
            .flatten()
            .filter_map(|row| match row.get(key) {
                Some(Value::Node(n)) => match n.properties.get("name") {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    };
    ensure!(
        names("pn", "p").contains(&"tx-only".to_string()),
        "tx write not hydrated"
    );
    ensure!(
        !names("qn", "q").contains(&"q1".to_string()),
        "deleted vertex resurrected"
    );
    let mut entities = Vec::new();
    for rows in result.derived.values() {
        rows_entities(rows, &mut entities);
    }
    for e in &entities {
        // Read back inside the same transaction, which sees the same state.
        let (query, id) = match e {
            Value::Node(n) => (
                "MATCH (x) WHERE id(x) = $id RETURN x",
                n.vid.as_u64() as i64,
            ),
            Value::Edge(ed) => (
                "MATCH ()-[x]->() WHERE id(x) = $id RETURN x",
                ed.eid.as_u64() as i64,
            ),
            _ => unreachable!(),
        };
        let rows = tx
            .query_with(query)
            .param("id", Value::Int(id))
            .fetch_all()
            .await?;
        ensure!(
            rows.rows().len() == 1,
            "tx: {} rows for {id}",
            rows.rows().len()
        );
        let cypher = rows.rows()[0].value("x").cloned().unwrap_or(Value::Null);
        ensure!(
            normalized(&cypher) == normalized(e),
            "tx: hydrated entity differs\n  hydrated: {e:?}\n  cypher:   {cypher:?}"
        );
    }
    tx.rollback();

    // Fork: fork-local writes are read through the fork's branch.
    let forked = db.session().fork("hydration").await?;
    let ftx = forked.tx().await?;
    ftx.execute("CREATE (:P {name: 'fork-only', age: 2})")
        .await?;
    ftx.execute("MATCH (b:P {name: 'b'}) SET b.age = 99")
        .await?;
    ftx.commit().await?;
    let result = forked
        .locy_with(&program)
        .with_config(config())
        .run()
        .await?;
    ensure!(
        result.warnings().is_empty(),
        "warnings: {:?}",
        result.warnings()
    );
    assert_keys_hydrated(&result, RICH_KEYS)?;
    let checked = assert_result_matches_cypher(&forked, &result, "fork").await?;
    ensure!(checked > 20, "only {checked} entities checked on the fork");
    let fork_b = result
        .derived_facts("pn")
        .into_iter()
        .flatten()
        .find_map(|row| match row.get("p") {
            Some(Value::Node(n))
                if n.properties.get("name") == Some(&Value::String("b".into())) =>
            {
                n.properties.get("age").cloned()
            }
            _ => None,
        });
    ensure!(
        fork_b == Some(Value::Int(99)),
        "fork overwrite not hydrated: {fork_b:?}"
    );
    Ok(())
}

/// The commands that re-derive or mutate: ASSUME (hypothetical vertex visible
/// to its body), ABDUCE, and DERIVE (whose presence means the returned map is
/// hydrated after the commands ran). The derived map stays hydrated and equal
/// to Cypher's reads in each.
#[tokio::test]
async fn locy_entity_hydration_commands_keep_hydrated_values() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    rich_graph(&db).await?;
    let session = db.session();

    // ASSUME: the hypothetical node exists only in the forked overlay.
    let program = format!(
        "{RICH_RULES}\
         ASSUME {{ MATCH (d:P {{name: 'd'}}) CREATE (d)-[:R {{since: 2004}}]->(:P {{name: 'hyp', age: 5}}) }} \
         THEN {{ QUERY reach WHERE a.name = 'a' RETURN a, b }}"
    );
    let result = session
        .locy_with(&program)
        .with_config(config())
        .run()
        .await?;
    assert_keys_hydrated(&result, RICH_KEYS)?;
    let rows = result
        .command_results
        .iter()
        .find_map(|c| match c {
            CommandResult::Assume(rows) => Some(rows.clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("no ASSUME result"))?;
    let mut hyp = None;
    for row in &rows {
        match (row.get("a"), row.get("b")) {
            (Some(Value::Node(_)), Some(Value::Node(b))) => {
                if b.properties.get("name") == Some(&Value::String("hyp".into())) {
                    hyp = Some(b.clone());
                }
            }
            other => bail!("ASSUME row not hydrated: {other:?}"),
        }
    }
    let hyp = hyp.ok_or_else(|| anyhow::anyhow!("hypothetical node not reached: {rows:?}"))?;
    ensure!(
        hyp.labels == vec!["P".to_string()],
        "hyp labels {:?}",
        hyp.labels
    );
    ensure!(
        hyp.properties.get("age") == Some(&Value::Int(5)),
        "hyp props {:?}",
        hyp.properties
    );
    // Everything except the hypothetical node is a real entity Cypher can read.
    let mut entities = Vec::new();
    for rows in result.derived.values() {
        rows_entities(rows, &mut entities);
    }
    rows_entities(
        &rows
            .iter()
            .filter(|r| !matches!(r.get("b"), Some(Value::Node(n)) if n.vid == hyp.vid))
            .cloned()
            .collect::<Vec<_>>(),
        &mut entities,
    );
    for e in &entities {
        assert_matches_cypher(&session, e, "assume").await?;
    }

    // ABDUCE.
    let program = format!("{RICH_RULES}ABDUCE NOT reach WHERE a.name = 'a'");
    let result = session
        .locy_with(&program)
        .with_config(config())
        .run()
        .await?;
    assert_keys_hydrated(&result, RICH_KEYS)?;
    ensure!(
        result
            .command_results
            .iter()
            .any(|c| matches!(c, CommandResult::Abduce(a) if !a.modifications.is_empty())),
        "ABDUCE proposed nothing: {:?}",
        result.command_results
    );
    assert_result_matches_cypher(&session, &result, "abduce").await?;

    // DERIVE inside a transaction, which writes: the returned map is hydrated
    // after the command, and still equals what the transaction reads.
    let program = format!(
        "{RICH_RULES}\
         CREATE RULE link AS MATCH (a:P)-[:R]->(b:P) DERIVE (a)-[:LINK]->(b) \n\
         DERIVE link"
    );
    let tx = db.session().tx().await?;
    let result = tx.locy_with(&program).with_config(config()).run().await?;
    ensure!(
        result.warnings().is_empty(),
        "warnings: {:?}",
        result.warnings()
    );
    assert_keys_hydrated(&result, RICH_KEYS)?;
    ensure!(
        result
            .command_results
            .iter()
            .any(|c| matches!(c, CommandResult::Derive { affected } if *affected > 0)),
        "DERIVE affected nothing: {:?}",
        result.command_results
    );
    let links = tx
        .query("MATCH (:P)-[l:LINK]->(:P) RETURN count(l) AS c")
        .await?
        .rows()[0]
        .value("c")
        .cloned();
    ensure!(
        links == Some(Value::Int(3)),
        "DERIVE wrote {links:?} LINK edges, want 3"
    );
    tx.rollback();
    Ok(())
}

// ---------------------------------------------------------------------------
// Measurement probe (not a guard).
// ---------------------------------------------------------------------------

/// Attributes hydration cost to operators at two scales. `#[ignore]`d: it
/// asserts nothing. Run with
/// `cargo nextest run -p uni-db --run-ignored all -E 'test(locy_entity_hydration_probe)' --no-capture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement probe; run with --run-ignored all --no-capture"]
async fn locy_entity_hydration_probe() -> Result<()> {
    for flush in [true, false] {
        for n in [2_000usize, 8_000] {
            let db = chain(n, flush).await?;
            let ids: Vec<String> = db
                .session()
                .query("MATCH (e:E) RETURN id(e) AS id")
                .await?
                .rows()
                .iter()
                .map(|r| match r.value("id") {
                    Some(Value::Int(i)) => i.to_string(),
                    other => panic!("{other:?}"),
                })
                .collect();
            let q = format!(
                "MATCH (entity) WHERE id(entity) IN [{}] RETURN id(entity) AS _id, entity",
                ids.join(", ")
            );
            let t = Instant::now();
            drop(uni_cypher::parse(&q)?);
            let parse_ms = t.elapsed().as_secs_f64() * 1e3;
            let t = Instant::now();
            let (res, prof) = db.session().query_with(&q).profile().await?;
            let wall = t.elapsed().as_secs_f64() * 1e3;
            let ops: Vec<String> = prof
                .runtime_stats
                .iter()
                .map(|o| {
                    format!(
                        "{}={}r/{:.1}ms",
                        o.operator.split(['(', ':', ' ']).next().unwrap_or(""),
                        o.actual_rows,
                        o.time_ms
                    )
                })
                .collect();
            eprintln!(
                "PROBE|flush={flush}|N={n}|IN-list statement: parse={parse_ms:.1}ms wall={wall:.1}ms \
                 rows={} rows_scanned={} ops={ops:?}",
                res.rows().len(),
                res.metrics().rows_scanned
            );

            let t = Instant::now();
            let rows = db
                .session()
                .query("MATCH (e:E) RETURN e")
                .await?
                .rows()
                .len();
            eprintln!(
                "PROBE|flush={flush}|N={n}|cypher control MATCH (e:E) RETURN e: {:.1}ms rows={rows}",
                t.elapsed().as_secs_f64() * 1e3
            );

            for (tag, prog) in [
                ("small", SMALL.to_string()),
                ("small+allv", small_plus_unreturned()),
            ] {
                let (r, wall) = run(&db, &prog).await?;
                eprintln!(
                    "PROBE|flush={flush}|N={n}|locy {tag} wall={:.1}ms rows_scanned={}",
                    wall.as_secs_f64() * 1e3,
                    r.metrics().rows_scanned
                );
            }
        }
    }
    Ok(())
}
