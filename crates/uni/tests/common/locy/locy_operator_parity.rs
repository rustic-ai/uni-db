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
//! value evaluator for properties), and those paths do not always agree.
//!
//! The oracle for a pairing is the Cypher answer its forms agree on, with
//! two kinds of exception, each listed in the test output as a Cypher defect
//! rather than silently absorbed:
//!
//! * A *static* error (planning, parse, Arrow kernel) on one form is not an
//!   answer when another form of the same values answers: `datetime(...) + 1`
//!   fails to plan with literals and answers with a property.
//! * Where the literal form answers a different value than the two property
//!   forms, which agree, the property forms decide: `date - date` is an
//!   `Int` day count from the Arrow kernel and a duration on the row path.
//! * Where every form fails or the forms disagree on a value, the openCypher
//!   rule decides (see [`spec`]): NULL propagation, Kleene logic, `AND`/`OR`
//!   type errors, and `string + scalar` concatenation.
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
     d: date('2020-01-02'), dt: datetime('2020-01-02T03:04:05Z'), dur: duration('P1D')})";

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
/// element, when present, is the answer to hold Locy to where Cypher's
/// literal path is known to fail the expression, with the reason.
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
    ("7 / 2", None),
    ("-7 % 3", None),
    ("7.5 % 2", None),
    ("2 ^ 3", None),
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
    // Planning: "Cannot coerce arithmetic expression Utf8 + Utf8".
    ("'a' + null", Some(Expect::Canon("Null"))),
    ("[1] + 2", None),
    ("2 + [1]", None),
    ("[1] + [2]", None),
    ("'x' + [1]", None),
    ("[1] + null", None),
    ("date('2020-01-31') + duration('P1M')", None),
    // Planning: "Cannot coerce arithmetic expression Struct(..) + Interval".
    (
        "datetime('2020-01-02T03:04:05Z') + duration('PT1H')",
        Some(Expect::Cypher("n.dt + duration('PT1H')")),
    ),
    ("duration('P1D') + date('2020-01-02')", None),
    ("date('2020-01-02') - duration('P1D')", None),
    ("duration('P1D') + duration('PT1H')", None),
    // Planning: "Cannot get result type for temporal operation Interval * Int64".
    ("duration('P1D') * 2", Some(Expect::Cypher("n.dur * 2"))),
    (
        "duration('P2D') / 2",
        Some(Expect::Cypher("(n.dur * 2) / 2")),
    ),
    // The Arrow kernel answers `Date32 - Date32` as a day count, Int(0); the
    // row path answers a duration.
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
    ("true AND 1", None),
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

/// The answer openCypher requires where Cypher's executor is known to fail.
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

/// Whether a Cypher error comes from static typing or an Arrow kernel rather
/// than from evaluating the values.
fn is_static_error(message: &str) -> bool {
    ["Error during planning", "SyntaxError", "Arrow error"]
        .iter()
        .any(|m| message.contains(m))
}

/// The openCypher answer to `l op r` (`l`, `r` as written, with their kinds)
/// where it decides the pairing regardless of what Cypher's forms answer:
///
/// * Kleene logic: `AND`/`OR`/`XOR` over booleans and NULL. Cypher errors on
///   `n.b AND null` (an Arrow schema mismatch) and on `null AND n.b`
///   ("Cannot evaluate binary expression And with types Null and Boolean"),
///   and panics on `n.missing AND null`.
/// * `XOR` with a NULL operand is NULL, as Cypher's row path answers
///   `n.missing XOR n.l`; its planner rejects `null XOR {k: 1}` statically.
/// * `AND`/`OR` with a non-boolean, non-NULL operand is a type error. Cypher
///   answers `true OR n.i` true — DataFusion skips the right side when the
///   left is true for the whole batch — and `n.i OR true` an error.
/// * NULL propagation: an arithmetic, comparison, string or regex operator
///   over NULL is NULL. Cypher's planner rejects many (`'ab' + null`: "Cannot
///   coerce arithmetic expression"; `1 =~ null`: "Cannot infer common
///   argument type"), and `'ab' ^ n.missing` errors at run time. `IN` is
///   excluded: a non-list right side is an error, NULL or not.
/// * `string + scalar` (number, boolean, temporal) concatenates the scalar's
///   `toString`. Cypher's literal path does (`'ab' + 1` is `'ab1'`), its row
///   path errors (`n.s + 1`), and its literal path renders a duration as
///   Arrow's `1 days` rather than `P1D`.
fn spec(l: (&'static str, Kind), op: &str, r: (&'static str, Kind)) -> Option<SpecAnswer> {
    let logical = matches!(op, "AND" | "OR" | "XOR");
    let boolish = |k: Kind| matches!(k, Kind::Bool | Kind::Null);
    if logical && boolish(l.1) && boolish(r.1) {
        let (lv, rv) = (l.1 == Kind::Bool, r.1 == Kind::Bool);
        // Every non-NULL boolean operand in the matrix is `true`.
        return Some(SpecAnswer::Canon(match (op, lv, rv) {
            ("AND", true, true) | ("OR", true, _) | ("OR", _, true) => "Bool(true)",
            ("XOR", true, true) => "Bool(false)",
            _ => "Null",
        }));
    }
    if matches!(op, "AND" | "OR") {
        return Some(SpecAnswer::Error);
    }
    if op == "XOR" && (l.1 == Kind::Null || r.1 == Kind::Null) {
        return Some(SpecAnswer::Canon("Null"));
    }
    if logical || op == "IN" {
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
    // Cypher's own answer: static errors are not answers when another form
    // has a value; the forms left must agree.
    let has_value = forms.iter().any(|f| matches!(f.1, Some(Ok(_))));
    let mut answers = BTreeSet::new();
    for (_, answer, _) in &forms {
        match answer {
            Some(Err(e)) if has_value && is_static_error(e) => {}
            Some(Err(_)) => {
                answers.insert(Err(String::new()));
            }
            Some(Ok(v)) => {
                answers.insert(Ok(v.clone()));
            }
            None => {}
        }
    }
    let want: Answer = match (answers.len(), &forms[1].1, &forms[2].1) {
        (1, _, _) => answers.into_iter().next().unwrap_or(Err(String::new())),
        // The literal form alone disagrees: the two row-path forms decide
        // (`date - date` is a day count from the Arrow kernel, a duration on
        // the row path).
        (_, Some(l), Some(r)) if same(l, r) => l.clone(),
        _ => {
            tally.failures.push(format!(
                "({l_lit}) {op} ({r_lit}): Cypher forms have no common answer: {:?}",
                forms.iter().map(|f| &f.1).collect::<Vec<_>>()
            ));
            return;
        }
    };
    let observed: Vec<(String, Option<Answer>)> =
        forms.iter().map(|f| (f.0.clone(), f.1.clone())).collect();
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
            for operand in [lit, prop] {
                let expr = format!("{unary} ({operand})");
                match guarded(cypher(&db, &expr)).await {
                    Some(want) => tally.check(&db, &expr, &want).await,
                    None => tally.failures.push(format!("{expr}\n    cypher panicked")),
                }
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
                tally.failures.push(format!("{expr}\n    cypher panicked"));
                continue;
            }
        };
        if known.is_some() {
            tally.note_oracle(&[((*expr).to_string(), observed)], &want);
        }
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
        tally.failures.is_empty(),
        "{} of {} expressions disagree:\n{}",
        tally.failures.len(),
        tally.cases,
        tally.failures.join("\n")
    );
    Ok(())
}
