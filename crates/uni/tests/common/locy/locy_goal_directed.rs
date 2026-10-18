// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! P8 / A1: goal-directed `QUERY` evaluation (the magic-sets rewrite,
//! `uni_locy::magic`).
//!
//! * Answers: every `QUERY` returns the same rows with the rewrite off, on and
//!   in shadow mode, over a graph with branches, a cycle and a negated
//!   property, for each construct the rewrite adorns and each one it refuses.
//!   Each case also states whether the rewrite applies, so a case that
//!   silently stops being rewritten fails instead of passing for free.
//! * `derived`: in `On` mode a rule evaluated only for a rewritten goal is
//!   absent, never partial; in `Shadow` mode `derived` equals the full run's.
//! * Work: on `k` disjoint chains of 20, a QUERY from one chain head derives
//!   a constant number of facts with the rewrite on, and `k`-proportional
//!   facts with it off, measured at two scales through `profile()`.

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use uni_db::locy::{CommandResult, GoalDirected, LocyConfig, rewrite_goal_directed};
use uni_db::{Uni, Value};

/// `(name, bad)` nodes and `(src, dst, w)` edges. `a -> b -> c -> d` with a
/// branch `b -> e`, a cycle `d -> a`, a separate component `x -> y -> z`,
/// and parallel cheap/expensive `a -> c` edges for BEST BY / FOLD.
const NODES: &[(&str, bool)] = &[
    ("a", false),
    ("b", false),
    ("c", true),
    ("d", false),
    ("e", false),
    ("x", false),
    ("y", true),
    ("z", false),
];
const EDGES: &[(&str, &str, i64)] = &[
    ("a", "b", 1),
    ("b", "c", 2),
    ("c", "d", 3),
    ("d", "a", 4),
    ("b", "e", 5),
    ("a", "c", 7),
    ("a", "c", 2),
    ("x", "y", 1),
    ("y", "z", 1),
];

async fn graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL N (name STRING, bad BOOL)").await?;
    tx.execute("CREATE EDGE TYPE E (w INT) FROM N TO N").await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    for (name, bad) in NODES {
        tx.execute(&format!("CREATE (:N {{name: '{name}', bad: {bad}}})"))
            .await?;
    }
    for (s, d, w) in EDGES {
        tx.execute(&format!(
            "MATCH (s:N {{name: '{s}'}}), (d:N {{name: '{d}'}}) CREATE (s)-[:E {{w: {w}}}]->(d)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

const TC_RIGHT: &str = "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
    CREATE RULE reach AS MATCH (a:N)-[:E]->(m:N) WHERE m IS reach TO b YIELD KEY a, KEY b\n";
const TC_LEFT: &str = "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
    CREATE RULE reach AS MATCH (m:N)-[:E]->(b:N) WHERE a IS reach TO m YIELD KEY a, KEY b\n";

/// `(label, program, params, rewritten)`.
fn cases() -> Vec<(&'static str, String, Vec<(&'static str, Value)>, bool)> {
    let s = |v: &str| Value::String(v.to_string());
    vec![
        (
            "right-linear TC",
            format!("{TC_RIGHT}QUERY reach WHERE a.name = 'a' RETURN b.name AS n"),
            vec![],
            true,
        ),
        (
            "left-linear TC",
            format!("{TC_LEFT}QUERY reach WHERE a.name = 'b' RETURN b.name AS n"),
            vec![],
            true,
        ),
        (
            "target bound",
            format!("{TC_RIGHT}QUERY reach WHERE b.name = 'c' RETURN a.name AS n"),
            vec![],
            true,
        ),
        (
            "both bound, IN list",
            format!(
                "{TC_RIGHT}QUERY reach WHERE a.name = 'a' AND b.name IN ['d', 'z', 'e'] \
                 RETURN b.name AS n"
            ),
            vec![],
            true,
        ),
        (
            "parameter",
            format!("{TC_RIGHT}QUERY reach WHERE a.name = $who RETURN b.name AS n"),
            vec![("who", s("x"))],
            true,
        ),
        (
            "parameter IN list",
            format!("{TC_RIGHT}QUERY reach WHERE a.name IN $who RETURN b.name AS n"),
            vec![("who", Value::List(vec![s("x"), s("d")]))],
            true,
        ),
        (
            "extra non-seed conjunct",
            format!(
                "{TC_RIGHT}QUERY reach WHERE a.name = 'a' AND b.bad = false RETURN b.name AS n"
            ),
            vec![],
            true,
        ),
        (
            "no match",
            format!("{TC_RIGHT}QUERY reach WHERE a.name = 'nobody' RETURN b.name AS n"),
            vec![],
            true,
        ),
        (
            "non-linear recursion",
            "CREATE RULE tc AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
             CREATE RULE tc AS MATCH (a:N) WHERE a IS tc TO m, m IS tc TO b YIELD KEY a, KEY b\n\
             QUERY tc WHERE a.name = 'x' RETURN b.name AS n"
                .to_string(),
            vec![],
            true,
        ),
        (
            "negation of a lower stratum",
            format!(
                "{TC_RIGHT}CREATE RULE flagged AS MATCH (v:N) WHERE v.bad = true YIELD KEY v\n\
                 CREATE RULE clean AS MATCH (a:N)-[:E]->(b:N) WHERE a IS reach TO b, \
                   b IS NOT flagged YIELD KEY a, KEY b\n\
                 QUERY clean WHERE a.name = 'a' RETURN b.name AS n"
            ),
            vec![],
            true,
        ),
        (
            "non-recursive FOLD over a bound callee",
            format!(
                "{TC_RIGHT}CREATE RULE fan AS MATCH (a:N) WHERE a IS reach TO b \
                   FOLD k = COUNT(b) YIELD KEY a, k\n\
                 QUERY fan WHERE a.name = 'b' RETURN k AS n"
            ),
            vec![],
            true,
        ),
        (
            "non-recursive BEST BY",
            "CREATE RULE cheap AS MATCH (a:N)-[e:E]->(b:N) BEST BY e.w ASC YIELD KEY a, KEY b, e.w AS w\n\
             QUERY cheap WHERE a.name = 'a' RETURN b.name AS n, w"
                .to_string(),
            vec![],
            true,
        ),
        (
            "mutual recursion",
            "CREATE RULE odd AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
             CREATE RULE odd AS MATCH (a:N)-[:E]->(m:N) WHERE m IS even TO b YIELD KEY a, KEY b\n\
             CREATE RULE even AS MATCH (a:N)-[:E]->(m:N) WHERE m IS odd TO b YIELD KEY a, KEY b\n\
             QUERY even WHERE a.name = 'a' RETURN b.name AS n"
                .to_string(),
            vec![],
            true,
        ),
        (
            "helper rule bound through the MATCH",
            "CREATE RULE hop AS MATCH (a:N)-[:E]->(b:N) WHERE b.bad = false YIELD KEY a, KEY b\n\
             CREATE RULE two AS MATCH (a:N)-[:E]->(m:N) WHERE m IS hop TO b YIELD KEY a, KEY b\n\
             QUERY two WHERE a.name = 'a' RETURN b.name AS n"
                .to_string(),
            vec![],
            true,
        ),
        (
            "two goals and an unfiltered QUERY",
            format!(
                "{TC_RIGHT}QUERY reach WHERE a.name = 'a' RETURN b.name AS n\n\
                 QUERY reach WHERE a.name = 'x' RETURN b.name AS n\n\
                 QUERY reach RETURN a.name AS n, b.name AS m"
            ),
            vec![],
            true,
        ),
        (
            "module-qualified rule",
            format!("MODULE m\n{TC_RIGHT}QUERY reach WHERE a.name = 'a' RETURN b.name AS n"),
            vec![],
            true,
        ),
        (
            "fallback: recursive ALONG",
            "CREATE RULE path AS MATCH (a:N)-[e:E]->(b:N) ALONG c = e.w BEST BY c ASC YIELD KEY a, KEY b, c\n\
             CREATE RULE path AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS path TO b \
               ALONG c = prev.c + e.w BEST BY c ASC YIELD KEY a, KEY b, c\n\
             QUERY path WHERE a.name = 'a' RETURN b.name AS n, c"
                .to_string(),
            vec![],
            false,
        ),
        (
            "fallback: goal on a value column",
            "CREATE RULE out AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b\n\
             QUERY out WHERE b.name = 'c' RETURN a.name AS n"
                .to_string(),
            vec![],
            false,
        ),
    ]
}

fn config(mode: GoalDirected, params: &[(&str, Value)]) -> LocyConfig {
    LocyConfig {
        goal_directed: mode,
        params: params
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
        ..Default::default()
    }
}

/// Each QUERY's rows, cells sorted by column and rows sorted, in command
/// order.
fn answers(result: &uni_db::locy::LocyResult) -> Vec<Vec<String>> {
    result
        .command_results
        .iter()
        .filter_map(|c| match c {
            CommandResult::Query(rows) => {
                let mut v: Vec<String> = rows
                    .iter()
                    .map(|r| {
                        let mut cells: Vec<String> =
                            r.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
                        cells.sort();
                        cells.join(",")
                    })
                    .collect();
                v.sort();
                Some(v)
            }
            _ => None,
        })
        .collect()
}

/// Every QUERY answers the same rows off, on and in shadow mode.
#[tokio::test]
async fn goal_directed_answers_equal_full_evaluation() -> Result<()> {
    let db = graph().await?;
    let mut failures = Vec::new();
    for (label, program, params, rewritten) in cases() {
        let params_map: HashMap<String, Value> = params
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        let compiled = db.session().compile_locy(&program)?;
        let (_, report) = rewrite_goal_directed(compiled, GoalDirected::On, &params_map);
        if (report.rewritten() > 0) != rewritten {
            failures.push(format!(
                "{label}: expected rewritten={rewritten}, got {report:?}"
            ));
        }
        let off = db
            .session()
            .locy_with(&program)
            .with_config(config(GoalDirected::Off, &params))
            .run()
            .await?;
        let want = answers(&off);
        assert!(!want.is_empty(), "{label}: no QUERY result");
        for mode in [GoalDirected::On, GoalDirected::Shadow] {
            let got = db
                .session()
                .locy_with(&program)
                .with_config(config(mode, &params))
                .run()
                .await?;
            if answers(&got) != want {
                failures.push(format!(
                    "{label} {mode:?}: {:?} != off {want:?}",
                    answers(&got)
                ));
            }
            if mode == GoalDirected::Shadow {
                let names = |r: &uni_db::locy::LocyResult| {
                    let mut m: BTreeMap<String, usize> = BTreeMap::new();
                    for (k, v) in &r.derived {
                        m.insert(k.clone(), v.len());
                    }
                    m
                };
                if names(&got) != names(&off) {
                    failures.push(format!(
                        "{label}: shadow derived {:?} != off {:?}",
                        names(&got),
                        names(&off)
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// In `On` mode a rule only rewritten goals needed is absent from `derived`
/// rather than partial, and no generated rule leaks into it.
#[tokio::test]
async fn on_mode_omits_rules_only_goals_needed() -> Result<()> {
    let db = graph().await?;
    let program = format!(
        "{TC_RIGHT}CREATE RULE hub AS MATCH (v:N)-[:E]->(:N) YIELD KEY v\n\
         QUERY reach WHERE a.name = 'a' RETURN b.name AS n"
    );
    let on = db
        .session()
        .locy_with(&program)
        .with_config(config(GoalDirected::On, &[]))
        .run()
        .await?;
    assert!(!on.derived.contains_key("reach"), "{:?}", on.derived.keys());
    assert!(
        on.derived.contains_key("hub"),
        "an unrelated rule is still published"
    );
    assert!(
        on.derived.keys().all(|k| !k.starts_with("__locy_gd")),
        "{:?}",
        on.derived.keys()
    );
    let off = db.session().locy(&program).await?;
    assert_eq!(off.derived["hub"].len(), on.derived["hub"].len());
    Ok(())
}

/// `k` disjoint chains `c{i}_0 -> … -> c{i}_19` of `:E {uid}` linked by `:T`.
async fn chains(k: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE T () FROM E TO E").await?;
    tx.execute("CREATE INDEX idx_e_uid FOR (e:E) ON (e.uid)")
        .await?;
    tx.commit().await?;
    let nodes: Vec<Value> = (0..k)
        .flat_map(|c| (0..20).map(move |p| Value::String(format!("c{c}_{p}"))))
        .collect();
    let edges: Vec<Value> = (0..k)
        .flat_map(|c| {
            (1..20).map(move |p| {
                Value::Map(HashMap::from([
                    ("a".to_string(), Value::String(format!("c{c}_{}", p - 1))),
                    ("b".to_string(), Value::String(format!("c{c}_{p}"))),
                ]))
            })
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.execute_with("UNWIND $rows AS u CREATE (:E {uid: u})")
        .param("rows", Value::List(nodes))
        .run()
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute_with(
        "UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) CREATE (a)-[:T]->(b)",
    )
    .param("rows", Value::List(edges))
    .run()
    .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

const CHAIN_TC: &str = "CREATE RULE reach AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b\n\
    CREATE RULE reach AS MATCH (a:E)-[:T]->(m:E) WHERE m IS reach TO b YIELD KEY a, KEY b\n\
    QUERY reach WHERE a.uid = 'c0_0' RETURN b.uid AS uid";

/// `(facts derived over every stratum, rows scanned, answer rows)`.
async fn work(db: &Uni, mode: GoalDirected) -> Result<(usize, u64, usize)> {
    let (result, profile) = db
        .session()
        .locy_with(CHAIN_TC)
        .with_config(config(mode, &[]))
        .profile()
        .await?;
    let facts = profile.profile.strata.iter().map(|s| s.facts_derived).sum();
    let rows = result.rows().map_or(0, |r| r.len());
    Ok((facts, result.metrics().rows_scanned as u64, rows))
}

/// The rewrite's work follows the goal's reachable subgraph, not the graph.
///
/// One chain of 20 is reachable from `c0_0`: 20 magic facts plus the
/// 190 `reach` facts among those 20 nodes, at any `k`. With the rewrite off
/// the fixpoint derives all `190 k`. Measured at `k` = 25 and 100.
#[tokio::test]
async fn goal_directed_work_is_proportional_to_the_goal() -> Result<()> {
    let mut seen = Vec::new();
    for k in [25usize, 100] {
        let db = chains(k).await?;
        let off = work(&db, GoalDirected::Off).await?;
        let on = work(&db, GoalDirected::On).await?;
        eprintln!("k={k}: off (facts, rows_scanned, rows) = {off:?}; on = {on:?}");
        assert_eq!(on.2, 19, "k={k}: on-mode answer");
        assert_eq!(off.2, 19, "k={k}: off-mode answer");
        seen.push((k, off, on));
    }
    let (_, off_small, on_small) = seen[0];
    let (_, off_large, on_large) = seen[1];
    // Off: every chain's closure, so 4x the chains is 4x the facts.
    assert_eq!(off_small.0, 190 * 25, "full closure at k=25");
    assert_eq!(off_large.0, 190 * 100, "full closure at k=100");
    // On: the same facts at both scales — the goal's chain only.
    assert_eq!(on_small.0, on_large.0, "on-mode facts must not grow with k");
    // 20 magic facts (the seed and the 19 nodes it reaches) plus the 190
    // `reach` facts among those 20 nodes.
    assert_eq!(on_small.0, 20 + 190, "on-mode facts");
    // Rows scanned: on-mode must not grow like the graph does.
    assert!(
        on_large.1 * 2 < off_large.1,
        "on-mode scanned {} vs off {}",
        on_large.1,
        off_large.1
    );
    Ok(())
}
