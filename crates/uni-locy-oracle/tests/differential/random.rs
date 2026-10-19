// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! W5 + issue #300: random programs over a multigraph against the oracle.
//!
//! Each case is a [`random_program_strategy`] program: the legacy rules
//! (recursion, stratified negation, FOLD over bags, ALONG, BEST BY, PROB) plus
//! the binding families of a `BindingShape`. A case is judged in four ways:
//!
//! 1. a program the oracle rejects with a language error must fail to
//!    compile with a message naming it (`ambiguous`); the case then goes on
//!    without the offending rule;
//! 2. every derived relation must equal the oracle's, decoded **strictly**
//!    by each column's [`WireKind`] — an integer column that comes back as a
//!    node is a divergence, which is how the "a KEY is a VID" confusion
//!    (#300 Q1-A) shows;
//! 3. the text-only relations of [`super::relations`] (conjunct permutation,
//!    alpha renaming, renaming into a collision, interleaved `YIELD`) must not
//!    change any `QUERY` answer;
//! 4. `EXPLAIN RULE` must derive exactly the oracle's facts for every rule
//!    with two or more positive references.
//!
//! When the whole program fails, each relation is re-run alone (with the
//! rules it depends on) so the error is attributed to the relations that
//! raise it, and the other relations are still judged.
//!
//! The driver runs every case without stopping or shrinking, then reports
//! the divergences grouped by check and relation, with each relation's
//! features, and enforces coverage floors so a run cannot pass by never
//! reaching the shapes it targets.

// Rust guideline compliant

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use uni_common::Value;
use uni_db::Uni;
use uni_locy_oracle::eval::{OracleError, check_program, evaluate_bags};
use uni_locy_oracle::generator::{
    P1_READER, PREV_RULE, SCALAR_INT_READER, random_program_strategy,
};
use uni_locy_oracle::ir::{Feature, Generated, PROB_SCALE, Tuple, WireKind};

use super::relations;

/// Why an engine value does not decode as its column's kind.
#[derive(Debug, Clone)]
pub(crate) struct DecodeError {
    /// A node came back where a scalar belongs.
    pub leak: bool,
    /// The value and the kind expected.
    pub what: String,
}

/// Decodes an engine value to the oracle's `i64` by the column's wire kind,
/// and by nothing else. See [`WireKind`].
///
/// # Errors
/// A value of any other shape — including a node in a scalar column, even
/// one whose `id` equals the expected scalar.
pub(crate) fn decode(
    kind: WireKind,
    value: &Value,
    strings: &HashMap<String, i64>,
) -> std::result::Result<i64, DecodeError> {
    let bad = |leak: bool| DecodeError {
        leak,
        what: format!("{value:?} in a {kind:?} column"),
    };
    let is_node = matches!(value, Value::Node(_));
    match (kind, value) {
        (WireKind::Node, Value::Node(node)) => node
            .properties
            .get("id")
            .and_then(Value::as_i64)
            .ok_or_else(|| bad(false)),
        (WireKind::Int | WireKind::IntegralFloat, Value::Int(i)) => Ok(*i),
        (WireKind::IntegralFloat, Value::Float(f)) if f.fract() == 0.0 => Ok(*f as i64),
        (WireKind::Str, Value::String(s)) => strings.get(s).copied().ok_or_else(|| bad(false)),
        (WireKind::Prob, Value::Float(f)) => Ok((f * PROB_SCALE as f64).round() as i64),
        _ => Err(bad(is_node && kind != WireKind::Node)),
    }
}

/// Which judgment a divergence failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Check {
    /// A derived relation differs from the oracle's.
    Derived,
    /// The relation's program fails although the oracle accepts it.
    EngineError,
    /// The oracle rejects the program; the engine did not refuse it.
    CompileError,
    /// A text-only relation changed a `QUERY` answer.
    Transformed,
    /// `EXPLAIN RULE` derives other facts than the oracle.
    Explain,
    /// A goal-directed `QUERY` answers other rows than the filtered oracle.
    GoalDirected,
}

/// One disagreement between the engine and the oracle.
#[derive(Debug, Clone)]
pub(crate) struct Divergence {
    pub check: Check,
    /// The relation, under its original name; `*` for the whole program.
    pub rel: String,
    pub detail: String,
    /// A node came back in a scalar column.
    pub kind_leak: bool,
    /// For [`Check::Transformed`]: the transforms that reproduce it alone.
    pub attributed: Vec<&'static str>,
}

impl Divergence {
    pub(crate) fn new(check: Check, rel: &str, detail: String) -> Self {
        Self {
            check,
            rel: rel.to_string(),
            detail,
            kind_leak: false,
            attributed: Vec::new(),
        }
    }
}

/// Everything one case found and reached.
#[derive(Debug, Clone, Default)]
pub(crate) struct CaseReport {
    pub divergences: Vec<Divergence>,
    /// Relations the oracle derived facts for.
    pub non_empty: BTreeSet<String>,
    /// The permutation changed some clause's conjunct order.
    pub permuted: bool,
    /// `rename_into_collision` found a variable to rename.
    pub collided: bool,
    /// ... in a relation the transformed program evaluated without an error.
    pub collided_judged: bool,
}

/// An engine relation's rows decoded by `kinds`, sorted, with the decode
/// failures. `cols` are the engine-side column names.
pub(crate) fn decode_rows(
    rows: &[HashMap<String, Value>],
    cols: &[String],
    kinds: &[WireKind],
    strings: &HashMap<String, i64>,
) -> (Vec<Tuple>, Vec<DecodeError>) {
    let mut errors = Vec::new();
    let mut out: Vec<Tuple> = rows
        .iter()
        .map(|row| {
            cols.iter()
                .zip(kinds)
                .map(|(c, k)| match row.get(c) {
                    Some(v) => decode(*k, v, strings).unwrap_or_else(|e| {
                        errors.push(e);
                        i64::MIN
                    }),
                    None => {
                        errors.push(DecodeError {
                            leak: false,
                            what: format!("missing column {c}"),
                        });
                        i64::MIN
                    }
                })
                .collect()
        })
        .collect();
    out.sort();
    (out, errors)
}

/// Whether two sorted bags agree, a probability within two fixed-point units
/// (floating-point evaluation order differs between the two sides).
pub(crate) fn bags_agree(got: &[Tuple], want: &[Tuple], kinds: &[WireKind]) -> bool {
    got.len() == want.len()
        && got.iter().zip(want).all(|(g, w)| {
            g.iter().zip(w).zip(kinds).all(|((a, b), k)| {
                if *k == WireKind::Prob {
                    (a - b).abs() <= 2
                } else {
                    a == b
                }
            })
        })
}

/// Judges one relation's engine rows against the oracle's bag.
pub(crate) fn judge(
    g: &Generated,
    oracle: &HashMap<String, Vec<Tuple>>,
    rel: &str,
    engine_cols: &[String],
    rows: &[HashMap<String, Value>],
    check: Check,
) -> Option<Divergence> {
    let kinds = &g.col_kinds[rel];
    let (got, errors) = decode_rows(rows, engine_cols, kinds, &g.strings);
    let want = oracle.get(rel).cloned().unwrap_or_default();
    if errors.is_empty() && bags_agree(&got, &want, kinds) {
        return None;
    }
    let mut d = Divergence::new(
        check,
        rel,
        format!(
            "engine {got:?}\n      oracle {want:?}{}",
            errors
                .first()
                .map(|e| format!("\n      decode: {} ({} cells)", e.what, errors.len()))
                .unwrap_or_default()
        ),
    );
    d.kind_leak = errors.iter().any(|e| e.leak);
    Some(d)
}

/// The engine error, or `"Ok"`, for a report.
fn outcome<T>(r: &std::result::Result<T, uni_db::UniError>) -> String {
    match r {
        Ok(_) => "Ok (rows)".to_string(),
        Err(e) => format!("ERR {e:#}"),
    }
}

/// The relations whose dependency closure meets `bad`.
fn dependents(g: &Generated, bad: &BTreeSet<String>) -> BTreeSet<String> {
    g.key_schema
        .keys()
        .filter(|rel| !g.dependency_closure(rel).is_disjoint(bad))
        .cloned()
        .collect()
}

/// Step 1: a program the oracle rejects must fail to compile. Returns the
/// case without the offending rule (and its dependents).
async fn expect_compile_error(db: &Uni, g: &Generated, report: &mut CaseReport) -> Generated {
    match check_program(&g.oracle_rules) {
        Ok(()) => g.clone(),
        Err(e) if e.is_language_error() => {
            let rule = e.rule().to_string();
            let got = db.session().locy(&g.program_text).await;
            let refused =
                matches!(&got, Err(err) if format!("{err:#}").to_lowercase().contains("ambiguous"));
            if !refused || !matches!(e, OracleError::AmbiguousRuleColumn { .. }) {
                report.divergences.push(Divergence::new(
                    Check::CompileError,
                    &rule,
                    format!("expected a compile error ({e}), got {}", outcome(&got)),
                ));
            }
            let bad = dependents(g, &BTreeSet::from([rule]));
            g.retain_rules(&|r| !bad.contains(r))
        }
        Err(e) => panic!(
            "malformed oracle IR (a generator bug): {e}\n{}",
            g.program_text
        ),
    }
}

/// Step 2: the derived relations, strictly decoded. On a program-level
/// failure, re-runs each relation with its dependencies to attribute it.
/// Returns the relations that fail alone.
async fn identity_divergences(
    db: &Uni,
    g: &Generated,
    oracle: &HashMap<String, Vec<Tuple>>,
    report: &mut CaseReport,
) -> BTreeSet<String> {
    let mut rels: Vec<&String> = g.key_schema.keys().collect();
    rels.sort();
    let mut failing = BTreeSet::new();
    match db.session().locy(&g.program_text).await {
        Ok(result) => {
            for rel in rels {
                let rows = result
                    .derived
                    .get(rel.as_str())
                    .cloned()
                    .unwrap_or_default();
                report.divergences.extend(judge(
                    g,
                    oracle,
                    rel,
                    &g.key_schema[rel],
                    &rows,
                    Check::Derived,
                ));
            }
        }
        Err(whole) => {
            for rel in rels {
                let keep = g.dependency_closure(rel);
                let sub = g.retain_rules(&|r| keep.contains(r));
                match db.session().locy(&sub.program_text).await {
                    Ok(result) => {
                        let rows = result
                            .derived
                            .get(rel.as_str())
                            .cloned()
                            .unwrap_or_default();
                        report.divergences.extend(judge(
                            g,
                            oracle,
                            rel,
                            &g.key_schema[rel],
                            &rows,
                            Check::Derived,
                        ));
                    }
                    Err(e) => {
                        report.divergences.push(Divergence::new(
                            Check::EngineError,
                            rel,
                            format!("ERR {e:#}"),
                        ));
                        failing.insert(rel.clone());
                    }
                }
            }
            if failing.is_empty() {
                report.divergences.push(Divergence::new(
                    Check::EngineError,
                    "*",
                    format!("the program fails, no relation alone does: {whole:#}"),
                ));
            }
        }
    }
    failing
}

/// Runs one case through every judgment.
///
/// # Errors
/// Only infrastructure failures (building the database); every engine
/// outcome is a judgment.
pub(crate) async fn run_case(g: &Generated, seed: u64) -> Result<CaseReport> {
    let db = super::seeded_db(g).await?;
    let mut report = CaseReport::default();
    let g = expect_compile_error(&db, g, &mut report).await;
    let oracle = evaluate_bags(&g.oracle_rules);
    report.non_empty = oracle
        .iter()
        .filter(|(_, bag)| !bag.is_empty())
        .map(|(rel, _)| rel.clone())
        .collect();
    let failing = identity_divergences(&db, &g, &oracle, &mut report).await;
    relations::transformed_divergences(&db, &g, &oracle, &failing, seed, &mut report).await;
    let bad = dependents(&g, &failing);
    let healthy = g.retain_rules(&|r| !bad.contains(r));
    relations::explain_agrees(&db, &healthy, &oracle, &mut report).await;
    Ok(report)
}

/// A fresh run seed: `ORACLE_RANDOM_SEED` when set, else the clock.
fn run_seed() -> u64 {
    std::env::var("ORACLE_RANDOM_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as u64);
            nanos ^ (u64::from(std::process::id()) << 32)
        })
}

/// splitmix64: per-case seeds from the run seed.
pub(crate) fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// One case's program and report.
pub(crate) struct Case {
    pub g: Generated,
    pub report: CaseReport,
}

/// One case in this many is drawn with an ambiguous rule column, the rest
/// without: 10 of the 96 PR cases, inside the 3%..20% band of
/// [`floor_failures`].
///
/// The band is two-sided, and no independent per-case rate keeps a binomial
/// count inside it at 96 cases: the generator's own rate (7.5%) fell below 3%
/// in about one run in 45, and the best rate (10.5%) still leaves one in 250.
/// A quota makes the count exact, so a missed floor means the generator can no
/// longer produce the shape — the regression the floor exists to catch.
const AMBIGUOUS_EVERY: u32 = 10;

/// Draws before [`draw_case`] gives up on the quota and takes the last
/// program. At the generator's 7.5% rate the chance of 2,000 misses in a row
/// is below 1e-60, so reaching it means the shape is gone; the floor then
/// reports it.
const QUOTA_DRAWS: usize = 2_000;

/// Draws a program that has (`ambiguous`) or lacks an expected
/// `AmbiguousRuleColumn` error, redrawing up to [`QUOTA_DRAWS`] times.
fn draw_case(
    strategy: &impl Strategy<Value = Generated>,
    runner: &mut TestRunner,
    ambiguous: bool,
) -> Generated {
    let mut draw = || {
        strategy
            .new_tree(runner)
            .expect("the strategy draws a program")
            .current()
    };
    let mut g = draw();
    for _ in 1..QUOTA_DRAWS {
        if g.features.contains(&Feature::AmbiguousExpected) == ambiguous {
            break;
        }
        g = draw();
    }
    g
}

/// Draws and runs `cases` programs from `seed`, every one, with no early
/// stop and no shrinking.
pub(crate) fn run_cases(cases: u32, seed: u64) -> Vec<Case> {
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
    (0..cases)
        .map(|i| {
            let g = draw_case(&strategy, &mut runner, i % AMBIGUOUS_EVERY == 0);
            let report = rt
                .block_on(run_case(&g, splitmix(&mut state)))
                .unwrap_or_else(|e| panic!("infrastructure failure: {e}\n{}", g.program_text));
            Case { g, report }
        })
        .collect()
}

/// The features a relation's divergence is attributed to.
fn rel_features(g: &Generated, rel: &str) -> BTreeSet<Feature> {
    g.rel_features.get(rel).cloned().unwrap_or_default()
}

/// The divergences grouped by `(check, relation)`: how many cases, the
/// relation's features, the transforms that reproduce it alone, and the
/// first case's program as a repro.
fn grouped(cases: &[Case]) -> String {
    #[derive(Default)]
    struct Group {
        cases: usize,
        features: BTreeSet<Feature>,
        attributed: BTreeMap<&'static str, usize>,
        leaks: usize,
        first: Option<(String, String, String)>,
    }
    let mut groups: BTreeMap<(Check, String), Group> = BTreeMap::new();
    for case in cases {
        let mut seen = BTreeSet::new();
        for d in &case.report.divergences {
            let key = (d.check, d.rel.clone());
            let group = groups.entry(key.clone()).or_default();
            if seen.insert(key) {
                group.cases += 1;
            }
            group.features.extend(rel_features(&case.g, &d.rel));
            group.leaks += usize::from(d.kind_leak);
            for t in &d.attributed {
                *group.attributed.entry(t).or_default() += 1;
            }
            if group.first.is_none() {
                group.first = Some((
                    d.detail.clone(),
                    case.g.program_text.clone(),
                    case.g.base_graph_cypher.clone(),
                ));
            }
        }
    }
    let mut out = String::new();
    for ((check, rel), g) in &groups {
        out.push_str(&format!(
            "\n[{check:?} {rel}] {} case(s); features {:?}; kind leaks {}; alone {:?}",
            g.cases, g.features, g.leaks, g.attributed
        ));
        if let Some((detail, program, graph)) = &g.first {
            out.push_str(&format!(
                "\n    e.g. {detail}\n    program:\n{program}\n    graph: {graph}\n"
            ));
        }
    }
    out
}

/// Coverage floors: every targeted shape in enough cases, else the run is
/// too often vacuous. Returns one line per floor missed.
fn floor_failures(cases: &[Case]) -> Vec<String> {
    let n = cases.len();
    let mut failures = Vec::new();
    let count = |pred: &dyn Fn(&Case) -> bool| cases.iter().filter(|c| pred(c)).count();
    let at_least = |what: &str, k: usize, pct: usize, failures: &mut Vec<String>| {
        if k * 100 < pct * n {
            failures.push(format!("{what} in only {k} of {n} cases (floor {pct}%)"));
        }
    };
    for f in [
        Feature::MultiRef,
        Feature::RefBoundKey,
        Feature::RefBoundValue,
        Feature::RefBoundTuple,
        Feature::RefBoundTo,
        Feature::NameCollision,
        Feature::SharedValueName,
        Feature::PrevBesideValue,
        Feature::ScalarKeyInt,
        Feature::ScalarKeyStr,
        Feature::NegBeforeBinder,
    ] {
        at_least(
            &format!("{f:?}"),
            count(&|c| c.g.features.contains(&f)),
            15,
            &mut failures,
        );
    }
    let ambiguous = count(&|c| c.g.features.contains(&Feature::AmbiguousExpected));
    if ambiguous * 100 < 3 * n || ambiguous * 100 > 20 * n {
        failures.push(format!(
            "AmbiguousExpected in {ambiguous} of {n} cases (bounds 3%..20%)"
        ));
    }
    at_least(
        "a non-identity permutation",
        count(&|c| c.report.permuted),
        15,
        &mut failures,
    );
    at_least(
        "a collision rename",
        count(&|c| c.report.collided),
        15,
        &mut failures,
    );
    // A collision rename only tests capture when the renamed program reaches a
    // relation the engine evaluates; one that dies on an engine error compares
    // nothing. Before the #300 fix 19 of 53 renames got that far.
    let collided = count(&|c| c.report.collided);
    let judged = count(&|c| c.report.collided_judged);
    if judged * 10 < collided * 9 {
        failures.push(format!(
            "only {judged} of {collided} collision renames reached a judged relation (floor 90%)"
        ));
    }
    // A comparison over empty relations proves nothing.
    for rel in [
        "link", "reach", "un", "deg", "span", "flow", "cost", "tot", "roll", "opt", "pr", "nor",
        "prd", "safe",
    ] {
        at_least(
            &format!("relation {rel} non-empty"),
            count(&|c| c.report.non_empty.contains(rel)),
            20,
            &mut failures,
        );
    }
    for rel in [
        P1_READER,
        "ch",
        "pm",
        "p4",
        "sh",
        PREV_RULE,
        SCALAR_INT_READER,
        "sks",
        "ng",
        // P5 recursion families: non-linear and mutual strata.
        "nlr",
        "nlb",
        "mua",
        "mub",
        "mba",
        "mbb",
        "mfa",
        "mfb",
        "mpa",
        "mpb",
    ] {
        at_least(
            &format!("relation {rel} non-empty"),
            count(&|c| c.report.non_empty.contains(rel)),
            8,
            &mut failures,
        );
    }
    failures
}

/// The per-feature and per-relation tallies, for the run log.
fn tallies(cases: &[Case]) -> String {
    let mut features: BTreeMap<Feature, usize> = BTreeMap::new();
    let mut non_empty: BTreeMap<String, usize> = BTreeMap::new();
    for c in cases {
        for f in &c.g.features {
            *features.entry(*f).or_default() += 1;
        }
        for rel in &c.report.non_empty {
            *non_empty.entry(rel.clone()).or_default() += 1;
        }
    }
    let permuted = cases.iter().filter(|c| c.report.permuted).count();
    let collided = cases.iter().filter(|c| c.report.collided).count();
    let judged = cases.iter().filter(|c| c.report.collided_judged).count();
    format!(
        "features per case: {features:?}\n  permuted: {permuted}, collision-renamed: {collided} \
         ({judged} in a relation judged without an engine error)\n  \
         non-empty per relation: {non_empty:?}"
    )
}

fn drive_random_programs(cases: u32) {
    let seed = run_seed();
    let started = Instant::now();
    eprintln!("[oracle:random] seed={seed} (set ORACLE_RANDOM_SEED to replay) cases={cases}");
    let ran = run_cases(cases, seed);
    eprintln!(
        "[oracle:random] {} in {:.1?}\n  {}",
        cases,
        started.elapsed(),
        tallies(&ran)
    );
    let floors = floor_failures(&ran);
    let diverging = ran
        .iter()
        .filter(|c| !c.report.divergences.is_empty())
        .count();
    assert!(
        floors.is_empty() && diverging == 0,
        "seed={seed}: {diverging} of {cases} cases diverge; floors missed: {floors:#?}\n{}",
        grouped(&ran)
    );
}

fn random_cases(default: u32) -> u32 {
    std::env::var("ORACLE_RANDOM_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Random programs over a multigraph — parallel and identical edges, self-loops,
/// two relationship types — with recursion, stratified negation, FOLD over
/// bags and the #300 binding families, against the oracle.
#[test]
fn random_programs_match_the_oracle() {
    drive_random_programs(random_cases(96));
}

/// The same at nightly volume (`ORACLE_RANDOM_CASES`, default 5 000).
#[test]
#[ignore = "soak: random oracle programs at nightly volume"]
fn random_programs_soak() {
    drive_random_programs(random_cases(5_000));
}

/// Teeth harness for the #300 binding families, not a pass/fail test.
///
/// It asserts that, unseeded and within the PR case budget, the generator
/// re-finds P1 (multi-reference KEY capture), Q1-A (an integer KEY equal to
/// a VID comes back as a node) and Q2 (`prev.q` beside another reference
/// exposing `q`). It therefore **fails on a fixed engine** by design: it only
/// proves something when run against a build that still has those bugs, and
/// that result is what makes a green [`random_programs_match_the_oracle`]
/// evidence that the bugs are gone rather than out of the generator's reach.
///
/// Recorded against the pre-fix engine (52e7a4d70 plus this harness), 10 runs
/// of 96 cases: P1 hit in 34–51 cases, Q1-A in 9–16, Q2 in 9–19, every run.
/// On the fixed engine: 0/0/0. Re-run after changing the generator with
///
/// ```text
/// cargo nextest run -p uni-locy-oracle --run-ignored all \
///     -E 'test(teeth_refind_issue_300)' --no-capture
/// ```
///
/// in a checkout of the pre-fix engine carrying the current oracle crate.
///
/// Runs 96 cases with a fresh seed (printed; `ORACLE_RANDOM_SEED` replays)
/// in a manual loop: no early stop, no shrinking. Each divergence is
/// attributed to its relation and that relation's features.
#[test]
#[ignore = "teeth: passes only against a pre-#300-fix engine"]
fn teeth_refind_issue_300() {
    let seed = run_seed();
    let started = Instant::now();
    let ran = run_cases(96, seed);
    let mut p1 = 0;
    let mut q1a = 0;
    let mut q2 = 0;
    let mut by: BTreeMap<(Check, String), usize> = BTreeMap::new();
    for case in &ran {
        let ds = &case.report.divergences;
        p1 += usize::from(ds.iter().any(|d| d.rel == P1_READER));
        q1a += usize::from(ds.iter().any(|d| d.rel == SCALAR_INT_READER && d.kind_leak));
        q2 += usize::from(ds.iter().any(|d| d.rel == PREV_RULE));
        for d in ds {
            *by.entry((d.check, d.rel.clone())).or_default() += 1;
        }
    }
    eprintln!(
        "[oracle:refind] seed={seed} cases=96 in {:.1?}: P1 hit in {p1} cases, Q1-A in {q1a}, Q2 in {q2}\n  \
         divergences by (check, relation): {by:?}\n  {}",
        started.elapsed(),
        tallies(&ran)
    );
    assert!(
        p1 >= 1 && q1a >= 1 && q2 >= 1,
        "seed={seed}: the generator is still blind: P1 {p1}, Q1-A {q1a}, Q2 {q2}\n{}",
        grouped(&ran)
    );
}

/// The oracle reads a node's seeded `id` where the engine holds its VID; the
/// two must stay equal, and every `k` must be another node's VID, or the
/// "integer equals a VID" shapes of family 6 stop colliding (T6).
#[tokio::test]
async fn seeded_ids_equal_vids() -> Result<()> {
    let mut runner = TestRunner::deterministic();
    let strategy = random_program_strategy();
    let mut checked = 0;
    for _ in 0..8 {
        let g = strategy
            .new_tree(&mut runner)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .current();
        let db = super::seeded_db(&g).await?;
        let result = db
            .session()
            .query("MATCH (n) RETURN n.id AS id, id(n) AS vid, n.k AS k")
            .await?;
        let mut vids = BTreeSet::new();
        let mut ks = Vec::new();
        for row in result.rows() {
            let (Some(Value::Int(id)), Some(Value::Int(vid)), Some(Value::Int(k))) =
                (row.value("id"), row.value("vid"), row.value("k"))
            else {
                anyhow::bail!("unexpected row {:?}", row.value("id"));
            };
            anyhow::ensure!(
                id == vid,
                "node id {id} has VID {vid}:\n{}",
                g.base_graph_cypher
            );
            vids.insert(*vid);
            ks.push((*vid, *k));
        }
        for (vid, k) in ks {
            anyhow::ensure!(
                vids.contains(&k) && (k != vid || vids.len() == 1),
                "k = {k} of node {vid} is not another node's VID"
            );
        }
        checked += 1;
    }
    anyhow::ensure!(checked == 8);
    Ok(())
}
