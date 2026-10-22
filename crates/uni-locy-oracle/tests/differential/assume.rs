// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A4: incremental `ASSUME` against a fresh evaluation of the mutated graph.
//!
//! The oracle's IR fixes each clause's base tuples at generation time, so it
//! cannot evaluate a hypothetical graph. The reference here is the engine
//! itself, without any hypothetical machinery: the same program evaluated on a
//! second database whose graph had the mutation committed. `ASSUME` reuses the
//! strata the mutation cannot affect (`uni_locy::incremental`); the fresh run
//! derives everything. Any difference in a `QUERY` answer is a reuse that was
//! not sound, or a recomputation that missed part of the change.
//!
//! Each case is a [`random_program_strategy`] program (recursion, negation,
//! FOLD, ALONG, BEST BY, PROB, binding families) and one random mutation from
//! a family chosen to hit a subset of the program's inputs: an edge of one
//! type created or deleted, a node created or deleted, an edge weight or a
//! node property set, a label removed, or something no rule reads. Floors
//! require both reused and recomputed strata, and answers the mutation
//! actually changed, so the run cannot pass vacuously.

// Rust guideline compliant

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::Result;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use uni_common::Value;
use uni_db::Uni;
use uni_db::locy::CommandResult;

type FactRow = std::collections::HashMap<String, Value>;
use uni_locy_oracle::eval::check_program;
use uni_locy_oracle::generator::random_program_strategy;
use uni_locy_oracle::ir::Generated;

use super::random::splitmix;

/// A cell independent of VIDs and map order: a node by its seeded `id`.
fn cell(v: &Value) -> String {
    match v {
        Value::Node(n) => format!("node:{:?}", n.properties.get("id")),
        Value::Edge(e) => {
            let mut props: Vec<String> = e
                .properties
                .iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect();
            props.sort();
            format!("edge:{}:{}", e.edge_type, props.join(","))
        }
        // MNOR / MPROD / MSUM fold in row order, and row order follows the
        // physical layout, which differs between the two databases; the last
        // bits of a float can differ with it. Compared to 12 significant
        // digits (the oracle's own PROB fixed point is coarser still).
        Value::Float(f) => format!("Float({f:.11e})"),
        Value::List(items) => format!("[{}]", items.iter().map(cell).collect::<Vec<_>>().join(",")),
        other => format!("{other:?}"),
    }
}

fn canonical(rows: &[FactRow]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            let mut cells: Vec<String> = row
                .iter()
                .map(|(k, v)| format!("{k}={}", cell(v)))
                .collect();
            cells.sort();
            cells.join(",")
        })
        .collect();
    out.sort();
    out
}

/// One random mutation over the generated graph and the family it is from.
fn mutation(node_count: i64, rng: &mut u64) -> (&'static str, String) {
    let n = node_count.max(2);
    let mut pick = || (splitmix(rng) % n as u64) as i64;
    let (x, y) = {
        let (a, b) = (pick(), pick());
        // New edges go to a greater id, as the generator's do, so a DAG stays
        // one (ALONG facts are per path and need acyclic input).
        if a < b {
            (a, b)
        } else if b < a {
            (b, a)
        } else {
            (0, n - 1)
        }
    };
    let w = 1 + (splitmix(rng) % 5) as i64;
    match splitmix(rng) % 10 {
        0 => (
            "create EDGE",
            format!(
                "MATCH (a:Node {{id: {x}}}), (b:Node {{id: {y}}}) CREATE (a)-[:EDGE {{w: {w}}}]->(b)"
            ),
        ),
        1 => (
            "create EDGE2",
            format!(
                "MATCH (a:Node {{id: {x}}}), (b:Node {{id: {y}}}) CREATE (a)-[:EDGE2 {{w: {w}}}]->(b)"
            ),
        ),
        2 => (
            "delete EDGE",
            format!("MATCH (a:Node {{id: {x}}})-[r:EDGE]->(:Node) DELETE r"),
        ),
        3 => (
            "delete EDGE2",
            format!("MATCH (a:Node {{id: {x}}})-[r:EDGE2]->(:Node) DELETE r"),
        ),
        4 => (
            "create node",
            format!(
                "MATCH (a:Node {{id: {x}}}) CREATE (a)-[:EDGE {{w: {w}}}]->(:Node {{id: {}, name: 's{}'}})",
                n + 100,
                n + 100
            ),
        ),
        5 => (
            "delete node",
            format!("MATCH (a:Node {{id: {x}}}) DETACH DELETE a"),
        ),
        6 => (
            "set weight",
            format!("MATCH (:Node {{id: {x}}})-[r]->(:Node) SET r.w = {w}"),
        ),
        7 => (
            "set name",
            format!("MATCH (a:Node {{id: {x}}}) SET a.name = 's{y}'"),
        ),
        8 => (
            "remove label",
            format!("MATCH (a:Node {{id: {x}}}) REMOVE a:Node"),
        ),
        _ => (
            "unrelated",
            format!(
                "MATCH (a:Node {{id: {x}}}) SET a.unused = {w} CREATE (:Other {{id: 999}})-[:OTHER]->(:Other)"
            ),
        ),
    }
}

/// Reuse counters over one subset of the compared cases.
#[derive(Debug, Default)]
struct Subset {
    cases: usize,
    reused: usize,
    evaluated: usize,
}

impl Subset {
    fn add(&mut self, (reused, evaluated): (usize, usize)) {
        self.cases += 1;
        self.reused += reused;
        self.evaluated += evaluated;
    }

    /// Reused strata as a percentage of all strata, for messages.
    fn reuse_pct(&self) -> f64 {
        let all = self.reused + self.evaluated;
        if all == 0 {
            0.0
        } else {
            100.0 * self.reused as f64 / all as f64
        }
    }
}

#[derive(Debug, Default)]
struct Totals {
    cases: usize,
    compared: usize,
    changed: usize,
    reused: usize,
    evaluated: usize,
    /// Compared cases whose program can reuse strata (no provenance tracker).
    eligible: Subset,
    /// Compared cases whose program runs with a provenance tracker, so the
    /// runtime never offers `ASSUME` a baseline to reuse.
    tracked: Subset,
    skipped_errors: usize,
    families: BTreeMap<&'static str, usize>,
    divergences: Vec<String>,
}

/// Every relation's `QUERY` answer, in relation order, or the error.
async fn answers(
    db: &Uni,
    text: &str,
) -> std::result::Result<(Vec<Vec<String>>, (usize, usize)), String> {
    let result = db
        .session()
        .locy(text)
        .await
        .map_err(|e| format!("{e:#}"))?;
    let rows = result
        .command_results
        .iter()
        .filter_map(|c| match c {
            CommandResult::Query(rows) => Some(canonical(rows)),
            // Each ASSUME's body is one QUERY (`THEN { QUERY r }`).
            CommandResult::Assume { results, .. } => match results.as_slice() {
                [CommandResult::Query(rows)] => Some(canonical(rows)),
                other => panic!("expected the ASSUME body's one QUERY, got {other:?}"),
            },
            _ => None,
        })
        .collect();
    let stats = result.stats();
    Ok((
        rows,
        (
            stats.hypothetical_strata_reused,
            stats.hypothetical_strata_evaluated,
        ),
    ))
}

async fn seeded(g: &Generated, mutation: Option<&str>) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(&g.base_graph_cypher).await?;
    if let Some(m) = mutation {
        tx.execute(m).await?;
    }
    tx.commit().await?;
    Ok(db)
}

async fn run_case(g: &Generated, seed: u64, totals: &mut Totals) -> Result<()> {
    if check_program(&g.oracle_rules).is_err() {
        return Ok(());
    }
    let mut rels: Vec<&String> = g.key_schema.keys().collect();
    rels.sort();
    if rels.is_empty() {
        return Ok(());
    }
    let node_count = g.base_graph_cypher.matches(":Node {id:").count() as i64;
    let mut rng = seed;
    let (family, m) = mutation(node_count, &mut rng);

    let queries: Vec<String> = rels.iter().map(|r| format!("QUERY {r}")).collect();
    let base_db = seeded(g, None).await?;
    let Ok((base, _)) = answers(
        &base_db,
        &format!("{}\n{}", g.program_text, queries.join("\n")),
    )
    .await
    else {
        totals.skipped_errors += 1;
        return Ok(());
    };
    let assume_text = format!(
        "{}\n{}",
        g.program_text,
        rels.iter()
            .map(|r| format!("ASSUME {{ {m} }} THEN {{ QUERY {r} }}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    // Whether the runtime will offer this program a baseline to reuse: the
    // engine's own predicate over the same text it is about to run.
    let tracked = base_db
        .session()
        .compile_locy(&assume_text)
        .map(|p| p.needs_provenance_tracker());
    let hypothetical = answers(&base_db, &assume_text).await;
    let fresh_db = seeded(g, Some(&m)).await?;
    let fresh = answers(
        &fresh_db,
        &format!("{}\n{}", g.program_text, queries.join("\n")),
    )
    .await;
    totals.cases += 1;
    *totals.families.entry(family).or_default() += 1;
    match (hypothetical, fresh) {
        (Ok((got, stats)), Ok((want, _))) => {
            totals.compared += 1;
            totals.reused += stats.0;
            totals.evaluated += stats.1;
            // It ran, so it compiled.
            match tracked {
                Ok(true) => totals.tracked.add(stats),
                Ok(false) => totals.eligible.add(stats),
                Err(e) => anyhow::bail!("ASSUME ran but did not compile: {e}"),
            }
            if want != base {
                totals.changed += 1;
            }
            for ((rel, got), want) in rels.iter().zip(&got).zip(&want) {
                if got != want {
                    totals.divergences.push(format!(
                        "[{family}] {rel}: assume {got:?}\n   fresh {want:?}\n   mutation: {m}\n   program:\n{}\n   graph: {}",
                        g.program_text, g.base_graph_cypher
                    ));
                }
            }
        }
        // The mutated graph is beyond the program (an ALONG cycle, a limit):
        // both must say so.
        (Err(_), Err(_)) => totals.skipped_errors += 1,
        (Ok(_), Err(e)) => totals.divergences.push(format!(
            "[{family}] ASSUME answered, fresh failed: {e}\n   mutation: {m}\n{}",
            g.program_text
        )),
        (Err(e), Ok(_)) => totals.divergences.push(format!(
            "[{family}] ASSUME failed, fresh answered: {e}\n   mutation: {m}\n{}",
            g.program_text
        )),
    }
    Ok(())
}

fn drive(cases: u32) {
    let seed = std::env::var("ORACLE_RANDOM_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as u64)
        });
    let started = Instant::now();
    eprintln!("[oracle:assume] seed={seed} (ORACLE_RANDOM_SEED replays) cases={cases}");
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
    let mut totals = Totals::default();
    for _ in 0..cases {
        let g = strategy
            .new_tree(&mut runner)
            .expect("the strategy draws a program")
            .current();
        rt.block_on(run_case(&g, splitmix(&mut state), &mut totals))
            .unwrap_or_else(|e| panic!("infrastructure failure: {e}\n{}", g.program_text));
    }
    eprintln!(
        "[oracle:assume] {} cases in {:.1?}: compared {}, answers changed by the mutation {}, \
         strata reused {} / evaluated {}, reuse-eligible cases {} (reused {} / evaluated {}, \
         {:.1}%), tracked cases {} (reused {} / evaluated {}), both-error {}, families {:?}, \
         divergences {}",
        totals.cases,
        started.elapsed(),
        totals.compared,
        totals.changed,
        totals.reused,
        totals.evaluated,
        totals.eligible.cases,
        totals.eligible.reused,
        totals.eligible.evaluated,
        totals.eligible.reuse_pct(),
        totals.tracked.cases,
        totals.tracked.reused,
        totals.tracked.evaluated,
        totals.skipped_errors,
        totals.families,
        totals.divergences.len()
    );
    // Every check runs and every failure is reported together: a divergence
    // (which an unsound reuse usually also causes) must not hide that the
    // tracked-reuse pin fired, or the reverse.
    let mut failures: Vec<String> = Vec::new();
    if !totals.divergences.is_empty() {
        failures.push(format!(
            "{} divergences; first:\n{}",
            totals.divergences.len(),
            totals
                .divergences
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n\n")
        ));
    }
    // Floors: reuse must engage, recomputation must happen, and the mutation
    // must change answers, or agreement proves nothing.
    let n = totals.compared.max(1);
    if totals.compared * 2 < totals.cases {
        failures.push(format!("too few compared: {totals:?}"));
    }
    // Reuse is only possible where the runtime offers a baseline: a program
    // with a provenance tracker (an MNOR / MPROD fold, or EXPLAIN) never
    // gets one, so the reuse floor is taken over the other programs only,
    // and needs enough of them that it cannot hold vacuously.
    let min_eligible = (totals.compared / 6).max(3);
    let e = &totals.eligible;
    if e.cases < min_eligible {
        failures.push(format!(
            "only {} of {} compared cases can reuse strata (no provenance tracker); the \
             reuse floor needs at least {min_eligible}. eligible {:?}, tracked {:?}",
            e.cases, totals.compared, e, totals.tracked
        ));
    }
    if e.reused * 100 < 10 * (e.reused + e.evaluated) {
        failures.push(format!(
            "reuse-eligible cases reused {} of {} strata ({:.1}%), below the 10% floor. \
             eligible {:?}, tracked {:?}",
            e.reused,
            e.reused + e.evaluated,
            e.reuse_pct(),
            e,
            totals.tracked
        ));
    }
    // Pins today's behavior: with a provenance tracker the runtime skips the
    // top-level baseline, and a hypothetical state that records provenance
    // reuses nothing either, so nothing is reused. Expected to change if
    // baseline reuse is ever made sound under provenance; update this then.
    // It needs tracked cases to mean anything.
    if totals.tracked.cases == 0 {
        failures.push(format!(
            "no compared case runs with a provenance tracker, so the tracked-reuse pin is \
             vacuous. tracked {:?}, eligible {:?}",
            totals.tracked, totals.eligible
        ));
    }
    if totals.tracked.reused != 0 {
        failures.push(format!(
            "programs with a provenance tracker reused {} strata, but the runtime gives them \
             no baseline (impl_locy.rs, `enable_top_level_baseline`) and reuses nothing in a \
             state that records provenance (`re_evaluate_strata_reusing`). If baseline reuse \
             was made sound under provenance, this pin is expected to change. tracked {:?}, \
             eligible {:?}",
            totals.tracked.reused, totals.tracked, totals.eligible
        ));
    }
    if totals.evaluated * 100 < 10 * (totals.reused + totals.evaluated) {
        failures.push(format!("too little recomputation: {totals:?}"));
    }
    if totals.changed * 100 < 20 * n {
        failures.push(format!(
            "too few answers changed by the mutation: {totals:?}"
        ));
    }
    assert!(
        failures.is_empty(),
        "seed={seed}: {} check(s) failed:\n- {}",
        failures.len(),
        failures.join("\n- ")
    );
}

fn assume_cases(default: u32) -> u32 {
    std::env::var("ORACLE_ASSUME_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `ASSUME` answers equal a fresh evaluation of the mutated graph.
#[test]
fn assume_matches_a_fresh_evaluation() {
    drive(assume_cases(48));
}

/// The same at soak volume (`ORACLE_ASSUME_CASES`, default 500).
#[test]
#[ignore = "soak: incremental ASSUME against fresh evaluation"]
fn assume_soak() {
    drive(assume_cases(500));
}
