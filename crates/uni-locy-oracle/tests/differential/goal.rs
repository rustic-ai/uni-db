// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! P8: goal-directed `QUERY` families against the oracle.
//!
//! Each case is a [`random_program_strategy`] program (recursion, stratified
//! negation, FOLD, ALONG, BEST BY, PROB and the #300 binding families). For
//! every relation with a node KEY column, a `QUERY rel WHERE <key>.id = k`
//! — `k` a key some fact has, or one no fact has; a literal or a `$param`;
//! sometimes binding a second node KEY too — is answered with the rewrite
//! off and on, and both answers must equal the oracle's bag filtered to the
//! bound keys. The goals the rewrite actually rewrote are counted (through
//! `rewrite_goal_directed` on the compiled program), and a run must rewrite
//! enough of them, including recursive ones, to not pass by falling back.
//! The default mode (`Auto`) must answer as Off does too, and publish the
//! derived facts Off publishes, deriving the rules it left out on access.

// Rust guideline compliant

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use anyhow::Result;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use uni_common::Value;
use uni_db::Uni;
use uni_db::locy::magic::{Fallback, GoalOutcome};
use uni_db::locy::{CommandResult, GoalDirected, LocyConfig, rewrite_goal_directed};
use uni_locy_oracle::eval::{check_program, evaluate_bags};
use uni_locy_oracle::generator::random_program_strategy;
use uni_locy_oracle::ir::{Generated, Tuple, WireKind};

use super::random::{Check, Divergence, judge, splitmix};

/// One goal: `QUERY rel WHERE ...` and the oracle filter it means.
#[derive(Debug, Clone)]
struct Goal {
    rel: String,
    /// `(key position, seeded id)` pairs the WHERE binds.
    bound: Vec<(usize, i64)>,
    /// Bind through `$param` instead of a literal.
    param: bool,
}

/// What one case found.
#[derive(Debug, Default)]
struct GoalReport {
    divergences: Vec<Divergence>,
    goals: usize,
    rewritten: usize,
    /// Rewritten goals over a recursive relation.
    rewritten_recursive: usize,
    /// Rewritten goals whose oracle answer is non-empty.
    rewritten_non_empty: usize,
    fallbacks: BTreeMap<String, usize>,
    /// Goals whose rewritten answer differs from the full one.
    rewrite_mismatches: usize,
    /// Cases where Auto left a rule out, to be derived on access.
    auto_left_out: usize,
}

fn key_count(g: &Generated, rel: &str) -> usize {
    g.oracle_rules
        .strata
        .iter()
        .flatten()
        .find(|r| r.name == rel)
        .map_or(0, |r| r.key_count)
}

fn is_recursive(g: &Generated, rel: &str) -> bool {
    g.dependency_closure(rel)
        .iter()
        .filter(|r| *r != rel)
        .any(|r| g.dependency_closure(r).contains(rel))
        || g.oracle_rules
            .strata
            .iter()
            .flatten()
            .filter(|r| r.name == rel)
            .flat_map(|r| &r.clauses)
            .any(|c| c.pos_refs.iter().any(|p| p.rule == rel))
}

/// Picks goals for every relation with a node KEY.
fn goals(g: &Generated, oracle: &HashMap<String, Vec<Tuple>>, rng: &mut u64) -> Vec<Goal> {
    let mut out = Vec::new();
    let mut rels: Vec<&String> = g.key_schema.keys().collect();
    rels.sort();
    let all_ids: BTreeSet<i64> = oracle
        .iter()
        .flat_map(|(rel, bag)| {
            let kinds = &g.col_kinds[rel];
            bag.iter().flat_map(move |t| {
                t.iter()
                    .zip(kinds)
                    .filter(|(_, k)| **k == WireKind::Node)
                    .map(|(v, _)| *v)
            })
        })
        .collect();
    for rel in rels {
        let kinds = &g.col_kinds[rel];
        let keys = key_count(g, rel);
        let node_keys: Vec<usize> = (0..keys).filter(|&p| kinds[p] == WireKind::Node).collect();
        let Some(&first) = node_keys.first() else {
            continue;
        };
        let bag = oracle.get(rel).cloned().unwrap_or_default();
        let r = splitmix(rng);
        let pick_fact = !bag.is_empty() && !r.is_multiple_of(4);
        let fact = pick_fact.then(|| bag[(r as usize / 4) % bag.len()].clone());
        let id = |pos: usize, salt: u64| -> i64 {
            fact.as_ref().map_or_else(
                || {
                    let ids: Vec<i64> = all_ids.iter().copied().collect();
                    if ids.is_empty() || salt.is_multiple_of(3) {
                        -7 // no node has this id
                    } else {
                        ids[(salt as usize) % ids.len()]
                    }
                },
                |t| t[pos],
            )
        };
        let mut bound = vec![(first, id(first, splitmix(rng)))];
        if let Some(&second) = node_keys.get(1)
            && splitmix(rng).is_multiple_of(3)
        {
            bound.push((second, id(second, splitmix(rng))));
        }
        out.push(Goal {
            rel: rel.clone(),
            bound,
            param: splitmix(rng).is_multiple_of(3),
        });
    }
    out
}

/// The goals' QUERY lines and parameters.
fn render(g: &Generated, goals: &[Goal]) -> (Vec<String>, HashMap<String, Value>) {
    let mut lines = Vec::new();
    let mut params = HashMap::new();
    for (i, goal) in goals.iter().enumerate() {
        let cols = &g.key_schema[&goal.rel];
        let conds: Vec<String> = goal
            .bound
            .iter()
            .enumerate()
            .map(|(j, (pos, id))| {
                if goal.param {
                    let name = format!("gd_{i}_{j}");
                    params.insert(name.clone(), Value::Int(*id));
                    format!("{}.id = ${name}", cols[*pos])
                } else {
                    format!("{}.id = {id}", cols[*pos])
                }
            })
            .collect();
        lines.push(format!("QUERY {} WHERE {}", goal.rel, conds.join(" AND ")));
    }
    (lines, params)
}

/// Each QUERY's rows, and every rule's facts canonicalized, with the number
/// of rules the evaluation left out to derive on access.
type ModeRun = (
    Vec<Vec<HashMap<String, Value>>>,
    BTreeMap<String, Vec<String>>,
    usize,
);

async fn query_rows(
    db: &Uni,
    text: &str,
    params: &HashMap<String, Value>,
    mode: GoalDirected,
) -> std::result::Result<ModeRun, String> {
    let config = LocyConfig {
        goal_directed: mode,
        params: params.clone(),
        ..Default::default()
    };
    let result = db
        .session()
        .locy_with(text)
        .with_config(config)
        .run()
        .await
        .map_err(|e| format!("{e:#}"))?;
    let answers = result
        .command_results
        .iter()
        .filter_map(|c| match c {
            CommandResult::Query(rows) => Some(rows.clone()),
            _ => None,
        })
        .collect();
    let left_out = result.lazily_derived_rules().len();
    let derived = result
        .derived_rules()
        .await
        .map_err(|e| format!("derived: {e:#}"))?
        .iter()
        .map(|(name, rows)| {
            let mut v: Vec<String> = rows.iter().map(canonical_row).collect();
            v.sort();
            (name.clone(), v)
        })
        .collect();
    Ok((answers, derived, left_out))
}

async fn run_goal_case(g: &Generated, seed: u64) -> Result<GoalReport> {
    let mut report = GoalReport::default();
    // Only programs the oracle accepts; a language error is the identity
    // harness's business.
    if check_program(&g.oracle_rules).is_err() {
        return Ok(report);
    }
    let db = super::seeded_db(g).await?;
    // Relations the engine evaluates without an error, closed downward.
    if db.session().locy(&g.program_text).await.is_err() {
        return Ok(report);
    }
    let oracle = evaluate_bags(&g.oracle_rules);
    let mut rng = seed;
    let goals = goals(g, &oracle, &mut rng);
    if goals.is_empty() {
        return Ok(report);
    }
    let (lines, params) = render(g, &goals);
    let text = format!("{}\n{}", g.program_text, lines.join("\n"));

    let compiled = db.session().compile_locy(&text)?;
    let (_, rewrite) = rewrite_goal_directed(compiled, GoalDirected::On, &params);
    report.goals = goals.len();
    for (goal, outcome) in goals.iter().zip(&rewrite.goals) {
        match &outcome.outcome {
            GoalOutcome::Rewritten { .. } => {
                report.rewritten += 1;
                if is_recursive(g, &goal.rel) {
                    report.rewritten_recursive += 1;
                }
            }
            GoalOutcome::Fallback(f) => {
                let key = match f {
                    Fallback::Unsafe { reason, .. } => format!("Unsafe:{reason:?}"),
                    other => format!("{other:?}")
                        .split([' ', '(', '{'])
                        .next()
                        .unwrap_or("")
                        .to_string(),
                };
                *report.fallbacks.entry(key).or_default() += 1;
            }
        }
    }

    let mut by_mode: Vec<Option<Vec<Vec<String>>>> = Vec::new();
    let mut derived_by_mode: Vec<Option<BTreeMap<String, Vec<String>>>> = Vec::new();
    for mode in [GoalDirected::Off, GoalDirected::On, GoalDirected::Auto] {
        let got = query_rows(&db, &text, &params, mode).await;
        by_mode.push(got.as_ref().ok().map(|(answers, _, _)| {
            answers
                .iter()
                .map(|rows| {
                    let mut v: Vec<String> = rows.iter().map(canonical_row).collect();
                    v.sort();
                    v
                })
                .collect()
        }));
        derived_by_mode.push(got.as_ref().ok().map(|(_, d, _)| d.clone()));
        if mode == GoalDirected::Auto
            && let Ok((_, _, left_out)) = &got
            && *left_out > 0
        {
            report.auto_left_out += 1;
        }
        let got = got.map(|(answers, _, _)| answers);
        match got {
            Err(e) => report.divergences.push(Divergence::new(
                Check::GoalDirected,
                "*",
                format!("{mode:?}: ERR {e}"),
            )),
            Ok(answers) => {
                assert_eq!(answers.len(), goals.len(), "one QUERY result per goal");
                for (i, (goal, rows)) in goals.iter().zip(&answers).enumerate() {
                    let want: Vec<Tuple> = oracle
                        .get(&goal.rel)
                        .into_iter()
                        .flatten()
                        .filter(|t| goal.bound.iter().all(|(p, id)| t[*p] == *id))
                        .cloned()
                        .collect();
                    if mode == GoalDirected::On
                        && !want.is_empty()
                        && matches!(rewrite.goals[i].outcome, GoalOutcome::Rewritten { .. })
                    {
                        report.rewritten_non_empty += 1;
                    }
                    let filtered = HashMap::from([(goal.rel.clone(), want)]);
                    if let Some(mut d) = judge(
                        g,
                        &filtered,
                        &goal.rel,
                        &g.key_schema[&goal.rel],
                        rows,
                        Check::GoalDirected,
                    ) {
                        d.detail = format!("{mode:?} {}: {}", lines[i], d.detail);
                        report.divergences.push(d);
                    }
                }
            }
        }
    }
    // The rewrite's own contract: on and auto equal off, row for row,
    // whatever the oracle says about either.
    if let [Some(off), rest @ ..] = by_mode.as_slice() {
        for (name, other) in ["On", "Auto"].iter().zip(rest) {
            let Some(other) = other else { continue };
            for (i, (a, b)) in off.iter().zip(other).enumerate() {
                if a != b {
                    report.rewrite_mismatches += 1;
                    report.divergences.push(Divergence::new(
                        Check::GoalDirected,
                        &goals[i].rel,
                        format!("{name} != Off for {}: {b:?} off {a:?}", lines[i]),
                    ));
                }
            }
        }
    }
    // Auto publishes exactly Off's derived facts.
    if let [Some(off), _, Some(auto)] = derived_by_mode.as_slice()
        && off != auto
    {
        report.rewrite_mismatches += 1;
        report.divergences.push(Divergence::new(
            Check::GoalDirected,
            "*",
            format!("Auto derived != Off derived: auto {auto:?} off {off:?}"),
        ));
    }
    Ok(report)
}

/// A row as a string independent of map order.
fn canonical_row(row: &HashMap<String, Value>) -> String {
    // A node's properties are a hash map, whose Debug order varies between
    // two reads of the same node; its identity is its vid.
    let cell = |v: &Value| match v {
        Value::Node(n) => format!("node#{}", n.vid.as_u64()),
        other => format!("{other:?}"),
    };
    let mut cells: Vec<String> = row
        .iter()
        .map(|(k, v)| format!("{k}={}", cell(v)))
        .collect();
    cells.sort();
    cells.join(",")
}

fn drive_goal_programs(cases: u32) {
    let seed = std::env::var("ORACLE_RANDOM_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as u64)
        });
    let started = Instant::now();
    eprintln!("[oracle:goal] seed={seed} (ORACLE_RANDOM_SEED replays) cases={cases}");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut state = seed;
    let bytes: Vec<u8> = (0..4)
        .flat_map(|_| splitmix(&mut state).to_le_bytes())
        .collect();
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &bytes),
    );
    let strategy = random_program_strategy();
    let mut totals = GoalReport::default();
    let mut failing = Vec::new();
    for _ in 0..cases {
        let g = strategy
            .new_tree(&mut runner)
            .expect("the strategy draws a program")
            .current();
        let report = rt
            .block_on(run_goal_case(&g, splitmix(&mut state)))
            .unwrap_or_else(|e| panic!("infrastructure failure: {e}\n{}", g.program_text));
        totals.goals += report.goals;
        totals.rewritten += report.rewritten;
        totals.rewritten_recursive += report.rewritten_recursive;
        totals.rewritten_non_empty += report.rewritten_non_empty;
        totals.rewrite_mismatches += report.rewrite_mismatches;
        totals.auto_left_out += report.auto_left_out;
        for (k, v) in &report.fallbacks {
            *totals.fallbacks.entry(k.clone()).or_default() += v;
        }
        if !report.divergences.is_empty() {
            failing.push((g, report.divergences));
        }
    }
    eprintln!(
        "[oracle:goal] {cases} cases in {:.1?}: goals {}, rewritten {} ({} recursive, {} non-empty), \
         fallbacks {:?}, on/auto != off {}, cases where auto left rules out {}, \
         cases diverging from the oracle {}",
        started.elapsed(),
        totals.goals,
        totals.rewritten,
        totals.rewritten_recursive,
        totals.rewritten_non_empty,
        totals.fallbacks,
        totals.rewrite_mismatches,
        totals.auto_left_out,
        failing.len()
    );
    let mut by_rel: BTreeMap<String, usize> = BTreeMap::new();
    for (_, ds) in &failing {
        for d in ds {
            let class = d.detail.split_whitespace().next().unwrap_or("").to_string();
            *by_rel.entry(format!("{} {class}", d.rel)).or_default() += 1;
        }
    }
    eprintln!("[oracle:goal] divergences by (relation, mode): {by_rel:?}");
    let mut detail = String::new();
    for (g, ds) in failing.iter().take(3) {
        for d in ds.iter().take(4) {
            detail.push_str(&format!("\n[{:?} {}] {}", d.check, d.rel, d.detail));
        }
        detail.push_str(&format!(
            "\n  program:\n{}\n  graph: {}\n",
            g.program_text, g.base_graph_cypher
        ));
    }
    assert!(
        failing.is_empty(),
        "seed={seed}: {} of {cases} cases diverge{detail}",
        failing.len()
    );
    // Floors: the rewrite must engage, on recursive relations too, and on
    // goals with answers, or agreement proves nothing about it.
    let n = totals.goals.max(1);
    assert!(
        totals.rewritten * 100 >= 25 * n,
        "seed={seed}: only {} of {n} goals rewritten",
        totals.rewritten
    );
    assert!(
        totals.rewritten_recursive * 100 >= 5 * n,
        "seed={seed}: only {} of {n} goals rewritten over a recursive relation",
        totals.rewritten_recursive
    );
    assert!(
        totals.rewritten_non_empty * 100 >= 10 * n,
        "seed={seed}: only {} of {n} rewritten goals have a non-empty answer",
        totals.rewritten_non_empty
    );
    // Auto's derived-facts check is vacuous unless it left rules out.
    assert!(
        totals.auto_left_out > 0,
        "seed={seed}: Auto left no rule out in {cases} cases"
    );
}

fn goal_cases(default: u32) -> u32 {
    std::env::var("ORACLE_GOAL_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Goal-directed QUERYs over random programs: rewrite on and off both equal
/// the oracle.
#[test]
fn goal_directed_queries_match_the_oracle() {
    drive_goal_programs(goal_cases(48));
}

/// The same at soak volume (`ORACLE_GOAL_CASES`, default 500).
#[test]
#[ignore = "soak: goal-directed oracle programs"]
fn goal_directed_queries_soak() {
    drive_goal_programs(goal_cases(500));
}
