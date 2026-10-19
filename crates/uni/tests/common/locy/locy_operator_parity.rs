// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy `QUERY ... RETURN <expr>` answers every operator as Cypher's
//! `RETURN <expr>` does, over every pairing of operand types.
//!
//! Locy's in-memory evaluator (`locy_eval`) kept its own copy of the binary
//! operators: `+` was numeric only, so `'a' + 'b'` failed with "numeric op
//! requires number" and list concatenation, `string + number` and
//! `temporal + duration` were unreachable; `=~` was unsupported; `=` on a
//! list holding NULL answered true; durations ordered; `AND` over a
//! non-boolean answered NULL; and integer overflow panicked in a debug build
//! (wrapped in a release one).
//!
//! Plain Cypher over the same bound row is the oracle, including whether an
//! expression is an error or a value (NULL included). Each binary pairing is
//! asked in three forms — both operands literal, the left one a property of
//! the bound node, the right one a property — because Cypher answers them on
//! different paths (static types and Arrow kernels for literals, the per-row
//! value evaluator for properties). The test also holds those paths to each
//! other: every form of a pairing must give the pairing's answer, and no
//! form may panic.
//!
//! The answer to a pairing is what its Cypher forms agree on, except where
//! the openCypher rule decides it (see [`spec`]): NULL propagation, Kleene
//! logic with a dominating operand, the compile-time rejection of a
//! non-boolean literal under `AND`/`OR`/`XOR` (TCK Boolean1-3 \[8\]), and
//! `string + scalar` concatenation. Edge cases the type matrix does not reach
//! carry their openCypher answer where one form alone cannot show it.
//!
//! Cypher defects this used to tolerate, now fixed and pinned: a DataFusion
//! panic on `n.missing AND null`; planning errors on a NULL operand
//! (`'ab' + null`, `n.i =~ null`) and run-time ones (`'ab' ^ n.missing`);
//! `n.s =~ 'ab'` failing to plan; `true OR n.i` answering by batch
//! short-circuit; `date - date` an integer day count on the literal path;
//! `datetime + duration` and `duration * 2` failing to plan; and `n.s + 1`
//! an error on the row path.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_operator_parity)'

// Rust guideline compliant

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Result;
use uni_db::Uni;
use uni_db::Value;
use uni_db::locy::{CommandResult, LocyConfig};

/// One node carrying a property of each operand type, each equal to the
/// operand's literal form, so the three forms of a pairing ask the same
/// question.
const GRAPH: &str = "CREATE (:One {i: 1, f: 2.5, s: 'ab', b: true, l: [1, 2], \
     d: date('2020-01-02'), dt: datetime('2020-01-02T03:04:05Z'), dur: duration('P1D'), \
     ldt: localdatetime('2020-01-02T03:04:05'), lt: localtime('03:04:05'), \
     t: time('03:04:05+01:00')})";

/// The type of an operand, for the openCypher rules in [`spec`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Int,
    Float,
    Str,
    Bool,
    Null,
    List,
    Map,
    Temporal,
}

/// `(literal form, property form, kind)` of each operand type.
const OPERANDS: &[(&str, &str, Kind)] = &[
    ("1", "n.i", Kind::Int),
    ("2.5", "n.f", Kind::Float),
    ("'ab'", "n.s", Kind::Str),
    ("true", "n.b", Kind::Bool),
    ("null", "n.missing", Kind::Null),
    ("[1, 2]", "n.l", Kind::List),
    ("{k: 1}", "{k: n.i}", Kind::Map),
    ("date('2020-01-02')", "n.d", Kind::Temporal),
    ("datetime('2020-01-02T03:04:05Z')", "n.dt", Kind::Temporal),
    ("duration('P1D')", "n.dur", Kind::Temporal),
    (
        "localdatetime('2020-01-02T03:04:05')",
        "n.ldt",
        Kind::Temporal,
    ),
    ("localtime('03:04:05')", "n.lt", Kind::Temporal),
    ("time('03:04:05+01:00')", "n.t", Kind::Temporal),
];

const BINARY_OPS: &[&str] = &[
    "+",
    "-",
    "*",
    "/",
    "%",
    "^",
    "=",
    "<>",
    "<",
    "<=",
    ">",
    ">=",
    "AND",
    "OR",
    "XOR",
    "STARTS WITH",
    "ENDS WITH",
    "CONTAINS",
    "=~",
    "IN",
];

/// Edge cases the type matrix does not reach: overflow, zero divisors, NaN,
/// lists and maps holding NULL, regexes, and temporal arithmetic. The second
/// element, when present, is the openCypher answer, which both Cypher and
/// Locy must give; without one, Cypher's answer is the oracle.
const EDGE_CASES: &[(&str, Option<Expect>)] = &[
    ("9223372036854775807 + 1", None),
    ("-9223372036854775807 - 2", None),
    ("9223372036854775807 * 2", None),
    ("-(-9223372036854775807 - 1)", None),
    ("1 / 0", None),
    ("1 % 0", None),
    ("1 / 0.0", None),
    ("0.0 / 0.0 = 0.0 / 0.0", None),
    ("0.0 / 0.0 < 1", None),
    // Integers against floats compare by exact value: widening the integer to
    // `f64` rounds above 2^53, and `i64::MAX` widens to 2^63.
    (
        "9007199254740993 = 9007199254740992.0",
        Some(Expect::Canon("Bool(false)")),
    ),
    (
        "9007199254740993 > 9007199254740992.0",
        Some(Expect::Canon("Bool(true)")),
    ),
    (
        "9007199254740992.0 < 9007199254740993",
        Some(Expect::Canon("Bool(true)")),
    ),
    (
        "9007199254740992 = 9007199254740992.0",
        Some(Expect::Canon("Bool(true)")),
    ),
    (
        "9223372036854775807 < 9223372036854775808.0",
        Some(Expect::Canon("Bool(true)")),
    ),
    (
        "9223372036854775807 <> 9223372036854775808.0",
        Some(Expect::Canon("Bool(true)")),
    ),
    (
        "-9223372036854775808 = -9223372036854775808.0",
        Some(Expect::Canon("Bool(true)")),
    ),
    (
        "[9007199254740993] = [9007199254740992.0]",
        Some(Expect::Canon("Bool(false)")),
    ),
    (
        "9007199254740993 IN [9007199254740992.0]",
        Some(Expect::Canon("Bool(false)")),
    ),
    ("7 / 2", None),
    ("-7 % 3", None),
    ("7.5 % 2", None),
    ("2 ^ 3", Some(Expect::Canon("Float(8.0)"))),
    ("n.i ^ 3", Some(Expect::Canon("Float(1.0)"))),
    ("2 ^ -1", None),
    ("[1, null] = [1, null]", None),
    ("[1, 2] = [1, 2]", None),
    ("[1, 2] = [1, 2.0]", None),
    ("{a: null} = {a: null}", None),
    ("{a: 1} = {a: 1}", None),
    ("[1, 2] < [1, 3]", None),
    ("[1, null] < [2, 1]", None),
    ("'b' < 'ab'", None),
    ("'ab' =~ 'a.*'", None),
    ("'ab' =~ 'b'", None),
    ("'ab' =~ '('", None),
    ("1 IN [1, null]", None),
    ("3 IN [1, null]", None),
    ("[1] IN [[1], 2]", None),
    ("null IN []", None),
    ("1 IN null", None),
    ("'a' + 1", None),
    ("1 + 'a'", None),
    ("'a' + 2.5", None),
    ("'a' + true", None),
    ("'a' + null", Some(Expect::Canon("Null"))),
    ("'a' + 1.0", Some(Expect::Canon("String(\"a1.0\")"))),
    ("n.s + 1.0", Some(Expect::Canon("String(\"ab1.0\")"))),
    ("[1] + 2", None),
    ("2 + [1]", None),
    ("[1] + [2]", None),
    ("'x' + [1]", None),
    ("[1] + null", None),
    ("date('2020-01-31') + duration('P1M')", None),
    (
        "datetime('2020-01-02T03:04:05Z') + duration('PT1H')",
        Some(Expect::Cypher("n.dt + duration('PT1H')")),
    ),
    // A number is not a duration.
    ("datetime('2020-01-02T03:04:05Z') + 1", Some(Expect::Error)),
    ("n.dt + 1", Some(Expect::Error)),
    ("date('2020-01-02') - 1", Some(Expect::Error)),
    ("n.d - 1", Some(Expect::Error)),
    ("duration('P1D') + date('2020-01-02')", None),
    ("date('2020-01-02') - duration('P1D')", None),
    ("duration('P1D') + duration('PT1H')", None),
    ("duration('P1D') * 2", Some(Expect::Cypher("n.dur * 2"))),
    (
        "duration('P2D') / 2",
        Some(Expect::Cypher("(n.dur * 2) / 2")),
    ),
    // A duration, not the Arrow kernel's day count.
    (
        "date('2020-01-02') - date('2020-01-02')",
        Some(Expect::Cypher("n.d - date('2020-01-02')")),
    ),
    ("date('2020-01-02') < date('2020-01-03')", None),
    (
        "date('2020-01-02') < datetime('2020-01-02T03:04:05Z')",
        None,
    ),
    ("duration('P1D') < duration('PT25H')", None),
    ("localtime('10:00') < localtime('11:00')", None),
    ("true < false", None),
    ("true AND 1", Some(Expect::Error)),
    // Kleene logic: a dominating operand decides, whatever the other one is
    // and in either position; otherwise a non-boolean operand is an error.
    ("false AND n.i", Some(Expect::Canon("Bool(false)"))),
    ("n.i AND false", Some(Expect::Canon("Bool(false)"))),
    ("n.i OR true", Some(Expect::Canon("Bool(true)"))),
    ("null AND n.i", Some(Expect::Error)),
    ("n.missing AND n.i", Some(Expect::Error)),
    ("n.missing XOR n.i", Some(Expect::Error)),
    ("n.missing AND null", Some(Expect::Canon("Null"))),
    ("n.missing OR false", Some(Expect::Canon("Null"))),
    ("1 STARTS WITH 'a'", Some(Expect::Canon("Null"))),
    ("n.i ENDS WITH 'a'", Some(Expect::Canon("Null"))),
    ("n.s =~ 'a.*'", Some(Expect::Canon("Bool(true)"))),
    ("n.s =~ null", Some(Expect::Canon("Null"))),
    ("NOT 1", None),
    ("-'a'", None),
    ("-duration('P1D')", None),
    ("1 = 'a'", None),
    ("1 <> 'a'", None),
    ("1 < 'a'", None),
    ("n.missing IS NULL", None),
    ("n.s STARTS WITH 'a'", None),
    ("n.s CONTAINS null", None),
];

/// An answer: Ok(canonical value) or Err(message). Comparison asks only
/// which, and the value.
type Answer = std::result::Result<String, String>;

/// The answer openCypher requires for an edge case.
#[derive(Clone, Copy)]
enum Expect {
    /// This canonical value.
    Canon(&'static str),
    /// An error.
    Error,
    /// What Cypher answers for this equivalent expression (its row path).
    Cypher(&'static str),
}

/// A value rendered with map keys sorted and NaN spelled out, so two
/// producers of one answer compare equal.
fn canon(value: &Value) -> String {
    match value {
        Value::Map(map) => {
            let mut entries: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{k}: {}", canon(v)))
                .collect();
            entries.sort();
            format!("{{{}}}", entries.join(", "))
        }
        Value::List(items) => format!(
            "[{}]",
            items.iter().map(canon).collect::<Vec<_>>().join(", ")
        ),
        Value::Float(f) if f.is_nan() => "Float(NaN)".to_string(),
        other => format!("{other:?}"),
    }
}

/// The answer, or `None` when evaluating it panicked.
async fn guarded(fut: impl std::future::Future<Output = Answer>) -> Option<Answer> {
    use futures::FutureExt;
    std::panic::AssertUnwindSafe(fut).catch_unwind().await.ok()
}

async fn locy(db: &Uni, expr: &str) -> Answer {
    let program =
        format!("CREATE RULE one AS MATCH (n:One) YIELD KEY n\nQUERY one RETURN {expr} AS v");
    let result = db
        .session()
        .locy_with(&program)
        .with_config(LocyConfig {
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .run()
        .await
        .map_err(|e| e.to_string())?;
    match result.command_results.last() {
        Some(CommandResult::Query(rows)) if rows.len() == 1 => {
            Ok(canon(rows[0].get("v").unwrap_or(&Value::Null)))
        }
        other => Err(format!("expected one QUERY row, got {other:?}")),
    }
}

async fn cypher(db: &Uni, expr: &str) -> Answer {
    let result = db
        .session()
        .query(&format!("MATCH (n:One) RETURN {expr} AS v"))
        .await
        .map_err(|e| e.to_string())?;
    match result.rows() {
        [row] => Ok(canon(&row.values()[0])),
        rows => Err(format!("expected one row, got {}", rows.len())),
    }
}

/// Whether the planner rejects `text` as an `AND`/`OR`/`XOR`/`NOT` operand at
/// compile time: a number, string, list or map literal (TCK Boolean1-4).
/// `{k: n.i}` is a map literal too.
fn non_boolean_literal(text: &str) -> bool {
    matches!(text, "1" | "2.5" | "'ab'" | "[1, 2]") || text.starts_with('{')
}

/// The openCypher answer to `l op r` (`l`, `r` as written, with their kinds)
/// where it decides the pairing regardless of what Cypher's forms answer:
///
/// * `AND`/`OR`/`XOR` with a non-boolean literal operand is a compile-time
///   type error (TCK Boolean1-3 \[8\], `RETURN 123.4 AND null`).
/// * Otherwise Kleene logic: an operand that dominates (`false` for `AND`,
///   `true` for `OR`) decides, whatever the other one is, so `true OR n.i` is
///   true in either order and in every batch. Without one, a non-boolean,
///   non-NULL operand is a type error (`n.missing AND n.i`); `XOR` has no
///   dominating operand.
/// * NULL propagation: an arithmetic, comparison, string or regex operator
///   over NULL is NULL (`'ab' + null`, `1 =~ null`, `'ab' ^ n.missing`).
///   `IN` is excluded: a non-list right side is an error, NULL or not.
/// * `string + scalar` (number, boolean, temporal) concatenates the scalar's
///   `toString`, on both paths, and renders a duration as `P1D`.
fn spec(l: (&'static str, Kind), op: &str, r: (&'static str, Kind)) -> Option<SpecAnswer> {
    if matches!(op, "AND" | "OR" | "XOR") {
        if non_boolean_literal(l.0) || non_boolean_literal(r.0) {
            return Some(SpecAnswer::Error);
        }
        // Every non-NULL boolean operand in the matrix is `true`.
        let is_true = |k: Kind| k == Kind::Bool;
        let non_boolean = |k: Kind| !matches!(k, Kind::Bool | Kind::Null);
        if op == "OR" && (is_true(l.1) || is_true(r.1)) {
            return Some(SpecAnswer::Canon("Bool(true)"));
        }
        if non_boolean(l.1) || non_boolean(r.1) {
            return Some(SpecAnswer::Error);
        }
        return Some(SpecAnswer::Canon(match (op, is_true(l.1), is_true(r.1)) {
            ("AND", true, true) => "Bool(true)",
            ("XOR", true, true) => "Bool(false)",
            _ => "Null",
        }));
    }
    if op == "IN" {
        return None;
    }
    if l.1 == Kind::Null || r.1 == Kind::Null {
        return Some(SpecAnswer::Canon("Null"));
    }
    let scalar = |k: Kind| matches!(k, Kind::Int | Kind::Float | Kind::Bool | Kind::Temporal);
    if op == "+" {
        if l.1 == Kind::Str && scalar(r.1) {
            return Some(SpecAnswer::Cypher(format!("{} + toString({})", l.0, r.0)));
        }
        if r.1 == Kind::Str && scalar(l.1) {
            return Some(SpecAnswer::Cypher(format!("toString({}) + {}", l.0, r.0)));
        }
    }
    None
}

/// A resolved [`spec`] answer.
enum SpecAnswer {
    Canon(&'static str),
    Error,
    Cypher(String),
}

/// Resolves a spec answer to an [`Answer`], asking Cypher where it names an
/// equivalent expression.
async fn resolve(db: &Uni, spec: SpecAnswer) -> Answer {
    match spec {
        SpecAnswer::Canon(c) => Ok(c.to_string()),
        SpecAnswer::Error => Err("openCypher type error".to_string()),
        SpecAnswer::Cypher(expr) => cypher(db, &expr).await,
    }
}

fn same(a: &Answer, b: &Answer) -> bool {
    match (a, b) {
        (Ok(x), Ok(y)) => x == y,
        (Err(_), Err(_)) => true,
        _ => false,
    }
}

/// Running tally of the comparison.
#[derive(Default)]
struct Tally {
    cases: usize,
    values: usize,
    failures: Vec<String>,
    oracle_defects: Vec<String>,
}

impl Tally {
    /// Holds Locy's answer to `expr` to `want`.
    async fn check(&mut self, db: &Uni, expr: &str, want: &Answer) {
        self.cases += 1;
        if want.is_ok() {
            self.values += 1;
        }
        match guarded(locy(db, expr)).await {
            None => self.failures.push(format!("{expr}\n    locy panicked")),
            Some(got) if !same(want, &got) => self
                .failures
                .push(format!("{expr}\n    want: {want:?}\n    locy: {got:?}")),
            Some(_) => {}
        }
    }

    /// Records each Cypher answer that differs from the oracle.
    fn note_oracle(&mut self, forms: &[(String, Option<Answer>)], want: &Answer) {
        for (expr, answer) in forms {
            match answer {
                Some(a) if same(a, want) => {}
                other => self
                    .oracle_defects
                    .push(format!("{expr}: cypher {other:?}, want {want:?}")),
            }
        }
    }
}

/// One binary pairing in its three forms.
async fn check_pairing(
    db: &Uni,
    tally: &mut Tally,
    (l_lit, l_prop, l_kind): (&'static str, &'static str, Kind),
    op: &str,
    (r_lit, r_prop, r_kind): (&'static str, &'static str, Kind),
) {
    let shapes = [
        ((l_lit, l_kind), (r_lit, r_kind)),
        ((l_prop, l_kind), (r_lit, r_kind)),
        ((l_lit, l_kind), (r_prop, r_kind)),
    ];
    let mut forms = Vec::new();
    for (l, r) in shapes {
        let expr = format!("({}) {op} ({})", l.0, r.0);
        let answer = guarded(cypher(db, &expr)).await;
        forms.push((expr, answer, spec(l, op, r)));
    }
    if forms.iter().any(|f| f.2.is_some()) {
        // A rule decides every form of the pairing (the rules depend on
        // kinds only, except for the operand text a concatenation renders).
        for (expr, answer, rule) in forms {
            let Some(rule) = rule else {
                tally
                    .failures
                    .push(format!("{expr}\n    rule applies to a sibling form only"));
                continue;
            };
            let want = resolve(db, rule).await;
            tally.note_oracle(&[(expr.clone(), answer)], &want);
            tally.check(db, &expr, &want).await;
        }
        return;
    }
    // Cypher's own answer: every form must give the same one.
    let mut answers = BTreeSet::new();
    for (_, answer, _) in &forms {
        match answer {
            Some(Err(_)) => {
                answers.insert(Err(String::new()));
            }
            Some(Ok(v)) => {
                answers.insert(Ok(v.clone()));
            }
            None => {}
        }
    }
    let observed: Vec<(String, Option<Answer>)> =
        forms.iter().map(|f| (f.0.clone(), f.1.clone())).collect();
    let want: Answer = match answers.len() {
        1 => answers.into_iter().next().unwrap_or(Err(String::new())),
        _ => {
            tally.oracle_defects.push(format!(
                "({l_lit}) {op} ({r_lit}): Cypher forms disagree: {:?}",
                forms.iter().map(|f| &f.1).collect::<Vec<_>>()
            ));
            return;
        }
    };
    tally.note_oracle(&observed, &want);
    for (expr, _, _) in &forms {
        tally.check(db, expr, &want).await;
    }
}

#[tokio::test]
async fn locy_operator_parity_matches_cypher() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(GRAPH).await?;
    tx.commit().await?;

    let mut tally = Tally::default();
    for &left in OPERANDS {
        for &right in OPERANDS {
            for op in BINARY_OPS {
                check_pairing(&db, &mut tally, left, op, right).await;
            }
        }
    }
    for &(lit, prop, _) in OPERANDS {
        for unary in ["NOT", "-"] {
            // The literal and property forms must agree, as binary forms do.
            let forms: Vec<(String, Option<Answer>)> = {
                let mut forms = Vec::new();
                for operand in [lit, prop] {
                    let expr = format!("{unary} ({operand})");
                    let answer = guarded(cypher(&db, &expr)).await;
                    forms.push((expr, answer));
                }
                forms
            };
            match (&forms[0].1, &forms[1].1) {
                (Some(a), Some(b)) if same(a, b) => {
                    for (expr, _) in &forms {
                        tally.check(&db, expr, a).await;
                    }
                }
                _ => tally
                    .oracle_defects
                    .push(format!("{unary} ({lit}): Cypher forms disagree: {forms:?}")),
            }
        }
    }
    for (expr, known) in EDGE_CASES {
        let observed = guarded(cypher(&db, expr)).await;
        let want = match (known, &observed) {
            (Some(Expect::Canon(c)), _) => Ok((*c).to_string()),
            (Some(Expect::Error), _) => Err("openCypher error".to_string()),
            (Some(Expect::Cypher(alt)), _) => cypher(&db, alt).await,
            (None, Some(answer)) => answer.clone(),
            (None, None) => {
                tally
                    .oracle_defects
                    .push(format!("{expr}: cypher panicked"));
                continue;
            }
        };
        tally.note_oracle(&[((*expr).to_string(), observed)], &want);
        tally.check(&db, expr, &want).await;
    }

    eprintln!(
        "locy_operator_parity: {} expressions, {} with a value, {} disagreements; \
         {} Cypher answers differ from the oracle:\n  {}",
        tally.cases,
        tally.values,
        tally.failures.len(),
        tally.oracle_defects.len(),
        tally.oracle_defects.join("\n  ")
    );
    // Guard against a vacuous run: the matrix must contain real answers,
    // not only agreeing errors.
    assert!(
        tally.values > tally.cases / 4,
        "only {} values in {} cases",
        tally.values,
        tally.cases
    );
    assert!(
        tally.oracle_defects.is_empty() && tally.failures.is_empty(),
        "{} Cypher answers differ from the oracle or between paths:\n{}\n\
         {} of {} Locy answers disagree:\n{}",
        tally.oracle_defects.len(),
        tally.oracle_defects.join("\n"),
        tally.failures.len(),
        tally.cases,
        tally.failures.join("\n")
    );
    Ok(())
}
