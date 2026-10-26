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
//! * `derived`: in `Auto` mode (the default) a rule evaluated only for a
//!   rewritten goal is derived on first access, against the evaluation's
//!   snapshot, and every rule's facts equal the full run's — for programs
//!   mixing QUERY with other commands, recursion, constructs the rewrite
//!   refuses, writes between commands, and a transaction. In `On` mode such a
//!   rule is absent, never partial; in `Shadow` mode the facts equal the full
//!   run's.
//! * Work: on `k` disjoint chains of 20, a QUERY from one chain head derives
//!   a constant number of facts with the rewrite on, and `k`-proportional
//!   facts with it off, measured at two scales through `profile()`.

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use uni_db::locy::{CommandResult, GoalDirected, LocyConfig, RuleFacts, rewrite_goal_directed};
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

/// A cell independent of map order: a node's properties are a hash map,
/// whose Debug order varies between two reads of the same node.
fn cell(v: &Value) -> String {
    match v {
        Value::Node(n) => {
            let props: BTreeMap<&String, String> = n
                .properties
                .iter()
                .map(|(k, v)| (k, format!("{v:?}")))
                .collect();
            format!("node#{}{:?}{props:?}", n.vid.as_u64(), n.labels)
        }
        other => format!("{other:?}"),
    }
}

/// Every rule's facts, cells sorted by column and rows sorted, so two
/// evaluations compare regardless of row order.
fn canonical(facts: &RuleFacts) -> BTreeMap<String, Vec<String>> {
    facts
        .iter()
        .map(|(name, rows)| {
            let mut v: Vec<String> = rows
                .iter()
                .map(|r| {
                    let mut cells: Vec<String> =
                        r.iter().map(|(k, v)| format!("{k}={}", cell(v))).collect();
                    cells.sort();
                    cells.join(",")
                })
                .collect();
            v.sort();
            (name.clone(), v)
        })
        .collect()
}

/// Every QUERY answers the same rows off, auto, on and in shadow mode, and
/// auto and shadow publish the derived facts off does.
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
        let want_derived = canonical(off.derived_rules().await?);
        for mode in [GoalDirected::Auto, GoalDirected::On, GoalDirected::Shadow] {
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
            // Auto and Shadow publish every rule's full relation; Auto derives
            // the rules it left out on this first access.
            if mode != GoalDirected::On {
                let got_derived = canonical(got.derived_rules().await?);
                if got_derived != want_derived {
                    failures.push(format!(
                        "{label}: {mode:?} derived {got_derived:?} != off {want_derived:?}"
                    ));
                }
            }
            // A goal not rewritten leaves nothing out. (A rewritten one may
            // not either: another reader can keep its rule.)
            if mode == GoalDirected::Auto && !rewritten && !got.lazily_derived_rules().is_empty() {
                failures.push(format!(
                    "{label}: Auto left out {:?} for a goal it did not rewrite",
                    got.lazily_derived_rules()
                ));
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
    let derived = on.derived_rules().await?;
    assert!(!derived.contains_key("reach"), "{:?}", derived.keys());
    assert!(
        derived.contains_key("hub"),
        "an unrelated rule is still published"
    );
    assert!(
        derived.keys().all(|k| !k.starts_with("__locy_gd")),
        "{:?}",
        derived.keys()
    );
    assert!(
        on.lazily_derived_rules().is_empty(),
        "On derives nothing later"
    );
    let off = db
        .session()
        .locy_with(&program)
        .with_config(config(GoalDirected::Off, &[]))
        .run()
        .await?;
    assert_eq!(
        off.derived_rule("hub").await?.map(Vec::len),
        on.derived_rule("hub").await?.map(Vec::len)
    );
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
        let auto = work(&db, GoalDirected::Auto).await?;
        eprintln!("k={k}: off (facts, rows_scanned, rows) = {off:?}; on = {on:?}; auto = {auto:?}");
        assert_eq!(auto.0, on.0, "k={k}: auto derives what on derives");
        assert_eq!(auto.2, 19, "k={k}: auto-mode answer");
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

/// Evaluates `program` with `mode`, every result's facts read in full.
async fn derived_in(
    db: &Uni,
    program: &str,
    mode: GoalDirected,
) -> Result<(uni_db::locy::LocyResult, BTreeMap<String, Vec<String>>)> {
    let result = db
        .session()
        .locy_with(program)
        .with_config(config(mode, &[]))
        .run()
        .await?;
    let facts = canonical(result.derived_rules().await?);
    Ok((result, facts))
}

/// Under the default mode every rule's facts equal full evaluation's, for
/// programs that mix a rewritten QUERY with other commands, recursion, and
/// constructs the rewrite refuses; and the rules left out are the ones only
/// the rewritten goals needed.
#[tokio::test]
async fn auto_derived_equals_full_evaluation() -> Result<()> {
    let db = graph().await?;
    // `(label, program, rules Auto must leave out)`.
    let cases: Vec<(&str, String, Vec<&str>)> = vec![
        (
            "QUERY beside an unrelated rule and a Cypher read",
            format!(
                "{TC_RIGHT}CREATE RULE hub AS MATCH (v:N)-[:E]->(:N) YIELD KEY v\n\
                 QUERY reach WHERE a.name = 'a' RETURN b.name AS n\n\
                 QUERY hub RETURN v.name AS n\n\
                 MATCH (n:N) RETURN count(n) AS c"
            ),
            vec!["reach"],
        ),
        (
            "non-linear recursion",
            "CREATE RULE tc AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
             CREATE RULE tc AS MATCH (a:N) WHERE a IS tc TO m, m IS tc TO b YIELD KEY a, KEY b\n\
             QUERY tc WHERE a.name = 'x' RETURN b.name AS n"
                .to_string(),
            // The copy's inner call reads `tc` in full, which keeps it.
            vec![],
        ),
        (
            "two rewritten goals over one rule",
            format!(
                "{TC_RIGHT}QUERY reach WHERE a.name = 'a' RETURN b.name AS n\n\
                 QUERY reach WHERE a.name = 'x' RETURN b.name AS n"
            ),
            vec!["reach"],
        ),
        (
            "a rewritten goal over a rule another rule reads in full",
            format!(
                "{TC_RIGHT}CREATE RULE fan AS MATCH (a:N) WHERE a IS reach TO b \
                   FOLD k = COUNT(b) YIELD KEY a, k\n\
                 QUERY reach WHERE a.name = 'a' RETURN b.name AS n"
            ),
            vec![],
        ),
        (
            "non-recursive FOLD adorned",
            format!(
                "{TC_RIGHT}CREATE RULE fan AS MATCH (a:N) WHERE a IS reach TO b \
                   FOLD k = COUNT(b) YIELD KEY a, k\n\
                 QUERY fan WHERE a.name = 'b' RETURN k AS n"
            ),
            vec!["fan", "reach"],
        ),
        (
            "PROB goal falls back, a second goal is rewritten",
            format!(
                "{TC_RIGHT}CREATE RULE risky AS MATCH (a:N)-[e:E]->(b:N) \
                   YIELD KEY a, KEY b, 0.5 AS p PROB\n\
                 QUERY risky WHERE a.name = 'a' RETURN b.name AS n, p\n\
                 QUERY reach WHERE a.name = 'x' RETURN b.name AS n"
            ),
            vec!["reach"],
        ),
        (
            "recursive FOLD falls back",
            "CREATE RULE far AS MATCH (a:N)-[e:E]->(b:N) FOLD w = MAX(e.w) YIELD KEY a, KEY b, w\n\
             CREATE RULE far AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS far TO b \
               FOLD w = MAX(e.w) YIELD KEY a, KEY b, w\n\
             QUERY far WHERE a.name = 'a' RETURN b.name AS n, w"
                .to_string(),
            vec![],
        ),
        (
            "ASSUME reading the rewritten goal's rule",
            format!(
                "{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b.name AS n\n\
                 ASSUME {{ MATCH (z:N {{name: 'z'}}), (a:N {{name: 'a'}}) \
                   CREATE (z)-[:E {{w: 1}}]->(a) }} \
                 THEN {{ QUERY reach WHERE a.name = 'x' RETURN b.name AS n }}"
            ),
            // The body's QUERY is not rewritten and reads `reach` in full.
            vec![],
        ),
    ];
    let mut failures = Vec::new();
    for (label, program, left_out) in cases {
        let (off, want) = derived_in(&db, &program, GoalDirected::Off).await?;
        let auto = db
            .session()
            .locy_with(&program)
            .with_config(config(GoalDirected::Auto, &[]))
            .run()
            .await?;
        let lazy: Vec<&str> = auto
            .lazily_derived_rules()
            .iter()
            .map(String::as_str)
            .collect();
        if lazy != left_out {
            failures.push(format!("{label}: left out {lazy:?}, expected {left_out:?}"));
        }
        // One rule first, then the whole map: both reach the same facts.
        for rule in &left_out {
            let one = auto.derived_rule(rule).await?.map(Vec::len);
            let full = off.derived_rule(rule).await?.map(Vec::len);
            if one != full {
                failures.push(format!(
                    "{label}: derived_rule({rule}) {one:?} != off {full:?}"
                ));
            }
        }
        let got = canonical(auto.derived_rules().await?);
        if got != want {
            failures.push(format!("{label}: derived {got:?} != off {want:?}"));
        }
        if answers(&auto) != answers(&off) {
            failures.push(format!(
                "{label}: answers {:?} != off {:?}",
                answers(&auto),
                answers(&off)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// A rule left out is derived in the state the evaluation read, not the
/// state at the time of access: a commit between the evaluation and the
/// first access does not reach it.
#[tokio::test]
async fn auto_derives_left_out_rules_in_the_evaluated_snapshot() -> Result<()> {
    let db = graph().await?;
    let program = format!("{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b.name AS n");
    let (_, want) = derived_in(&db, &program, GoalDirected::Off).await?;
    let auto = db.session().locy(&program).await?;
    assert_eq!(auto.lazily_derived_rules(), ["reach".to_string()]);

    // `z -> a` joins the two components: full evaluation now derives more.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (z:N {name: 'z'}), (a:N {name: 'a'}) CREATE (z)-[:E {w: 1}]->(a)")
        .await?;
    tx.commit().await?;
    let (_, after) = derived_in(&db, &program, GoalDirected::Off).await?;
    assert_ne!(after, want, "the commit changes the full relation");

    assert_eq!(canonical(auto.derived_rules().await?), want);
    Ok(())
}

/// A program that writes is evaluated in full under the default mode: its
/// derived facts are read in the state its commands leave, which no snapshot
/// taken before them holds.
#[tokio::test]
async fn auto_evaluates_writing_programs_in_full() -> Result<()> {
    let db = graph().await?;
    let program = format!(
        "{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b.name AS n\n\
         MATCH (y:N {{name: 'y'}}) SET y.bad = false\n\
         QUERY reach WHERE a.name = 'x' RETURN b.name AS n"
    );
    let (off, want) = derived_in(&db, &program, GoalDirected::Off).await?;
    let (auto, got) = derived_in(&db, &program, GoalDirected::Auto).await?;
    assert!(auto.lazily_derived_rules().is_empty());
    assert_eq!(got, want);
    assert_eq!(answers(&auto), answers(&off));
    Ok(())
}

/// Inside a transaction the default mode evaluates in full: the
/// transaction's own writes keep changing after the call and its reads must
/// reach its conflict check, so nothing is left for later.
#[tokio::test]
async fn auto_in_a_transaction_evaluates_in_full() -> Result<()> {
    let db = graph().await?;
    let program = format!("{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b.name AS n");
    let (off, want) = derived_in(&db, &program, GoalDirected::Off).await?;
    let tx = db.session().tx().await?;
    let in_tx = tx.locy(&program).await?;
    assert!(in_tx.lazily_derived_rules().is_empty());
    assert_eq!(canonical(in_tx.derived_rules().await?), want);
    assert_eq!(answers(&in_tx), answers(&off));
    tx.rollback();
    Ok(())
}

/// A rule left out of an evaluation cut short is reported as skipped and not
/// derived later.
#[tokio::test]
async fn auto_cut_short_reports_left_out_rules_as_skipped() -> Result<()> {
    let db = chains(2).await?;
    let config = LocyConfig {
        max_iterations: 3,
        allow_partial: true,
        ..Default::default()
    };
    let result = db
        .session()
        .locy_with(CHAIN_TC)
        .with_config(config)
        .run()
        .await?;
    let incomplete = result.incomplete.as_ref().expect("cut short");
    // The goal-directed copy was the rule cut short; the diagnostics name
    // the program's rule, never a generated one.
    assert!(
        incomplete.incomplete_rules.iter().any(|r| r == "reach")
            || incomplete.skipped_rules.iter().any(|r| r == "reach"),
        "{incomplete:?}"
    );
    assert!(
        incomplete
            .incomplete_rules
            .iter()
            .chain(&incomplete.skipped_rules)
            .chain(&incomplete.complement_rules_affected)
            .all(|r| !r.starts_with("__locy_gd")),
        "{incomplete:?}"
    );
    assert!(result.lazily_derived_rules().is_empty());
    assert!(result.derived_rule("reach").await?.is_none());
    Ok(())
}

/// Once the database is gone the left-out rules cannot be derived, and
/// reading them says so instead of returning nothing.
#[tokio::test]
async fn auto_left_out_rules_after_the_database_closed_is_an_error() -> Result<()> {
    let db = graph().await?;
    let program = format!("{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b.name AS n");
    let result = db.session().locy(&program).await?;
    assert_eq!(result.lazily_derived_rules(), ["reach".to_string()]);
    drop(db);
    let err = result
        .derived_rules()
        .await
        .expect_err("the database is closed");
    assert!(err.to_string().contains("closed"), "{err}");
    // The rules the evaluation itself derived are still there.
    assert!(result.evaluated_rules().keys().all(|k| k != "reach"));
    Ok(())
}

/// `(code, rule)` of every warning, sorted.
fn warning_keys(result: &uni_db::locy::LocyResult) -> Vec<String> {
    let mut keys: Vec<String> = result
        .warnings()
        .iter()
        .map(|w| format!("{:?}@{}", w.code, w.rule_name))
        .collect();
    keys.sort();
    keys
}

/// After the derived facts are read, a result that left rules out reports
/// the runtime warnings full evaluation does, each once.
///
/// `reach` is an MNOR rule whose two proofs of `A -> D` share the edge
/// `C -> D`, so it raises `SharedProbabilisticDependency`. `hop` is an
/// unrelated rule and `link` a plain rule over `reach`, both pruned by their
/// goals; deriving `link` later evaluates `reach` again, and that second
/// warning must not be reported twice.
#[tokio::test]
async fn auto_reports_full_evaluation_warnings_once_derived() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE Q (p FLOAT) FROM P TO P")
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    for n in ["A", "B", "C", "D"] {
        tx.execute(&format!("CREATE (:P {{name: '{n}'}})")).await?;
    }
    for (a, b, p) in [
        ("A", "B", 0.5),
        ("A", "C", 0.3),
        ("B", "C", 0.4),
        ("C", "D", 0.6),
    ] {
        tx.execute(&format!(
            "MATCH (x:P {{name: '{a}'}}), (y:P {{name: '{b}'}}) CREATE (x)-[:Q {{p: {p}}}]->(y)"
        ))
        .await?;
    }
    tx.commit().await?;
    let program = "CREATE RULE reach AS MATCH (a:P)-[e:Q]->(b:P) \
                     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob\n\
                   CREATE RULE reach AS MATCH (a:P)-[e:Q]->(mid:P) WHERE mid IS reach TO b \
                     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob\n\
                   CREATE RULE hop AS MATCH (a:P)-[:Q]->(b:P) YIELD KEY a, KEY b\n\
                   CREATE RULE hop AS MATCH (a:P)-[:Q]->(m:P) WHERE m IS hop TO b \
                     YIELD KEY a, KEY b\n\
                   CREATE RULE link AS MATCH (a:P)-[:Q]->(b:P) WHERE b IS reach TO c \
                     YIELD KEY a, KEY c\n\
                   QUERY reach RETURN a.name AS src, b.name AS dst, prob\n\
                   QUERY hop WHERE a.name = 'A' RETURN b.name AS n\n\
                   QUERY link WHERE a.name = 'A' RETURN c.name AS n";
    let (off, want) = derived_in(&db, program, GoalDirected::Off).await?;
    let auto = db
        .session()
        .locy_with(program)
        .with_config(config(GoalDirected::Auto, &[]))
        .run()
        .await?;
    assert_eq!(
        auto.lazily_derived_rules(),
        ["hop".to_string(), "link".to_string()]
    );
    let off_warnings = warning_keys(&off);
    assert!(
        off_warnings
            .iter()
            .any(|k| k.starts_with("SharedProbabilisticDependency")),
        "the fixture must warn: {off_warnings:?}"
    );
    assert_eq!(canonical(auto.derived_rules().await?), want);
    assert_eq!(warning_keys(&auto), off_warnings);
    Ok(())
}

/// `stats.derived_nodes` counts the facts the evaluation derived: under
/// `Auto` the left-out rules are not counted, before or after they are read.
#[tokio::test]
async fn auto_derived_nodes_counts_what_the_evaluation_derived() -> Result<()> {
    let db = graph().await?;
    let program = format!(
        "{TC_RIGHT}CREATE RULE hub AS MATCH (v:N)-[:E]->(:N) YIELD KEY v\n\
         QUERY reach WHERE a.name = 'x' RETURN b.name AS n"
    );
    let (off, _) = derived_in(&db, &program, GoalDirected::Off).await?;
    let auto = db.session().locy(&program).await?;
    assert_eq!(auto.lazily_derived_rules(), ["reach".to_string()]);
    let evaluated: usize = auto.evaluated_rules().values().map(Vec::len).sum();
    assert_eq!(auto.stats.derived_nodes, evaluated);
    let all: usize = auto.derived_rules().await?.values().map(Vec::len).sum();
    assert_eq!(auto.stats.derived_nodes, evaluated, "unchanged by the read");
    assert_eq!(off.stats.derived_nodes, all);
    assert!(evaluated < all);
    Ok(())
}
