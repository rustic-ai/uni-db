// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `s =~ p` matches the whole string, as Java's `Pattern.matches` does in
//! Neo4j: `'abc' =~ 'b'` is false, not a substring hit.
//!
//! The operator used to be a partial ("find") match on every evaluator, so
//! `'abc' =~ 'b'` was true. Full-match semantics are not a `^…$` wrap: with
//! leftmost-first alternation `'ab' =~ 'a|ab'` must still be true (the engine
//! has to look for a match that spans the input, not check the end of the
//! first one), and the match must consume the whole input, so
//! `'abc\n' =~ 'abc$'` is false even though `$` may match before the newline.
//!
//! Patterns use Java's dialect and defaults: ASCII `\w`/`\b`, ASCII-only
//! `(?i)` unless `(?u)`, `.` excluding Java's line terminators, `\<` as a
//! literal; possessive quantifiers are rejected; and input the translation
//! cannot answer exactly is refused ("Unsupported regex input").
//!
//! `=~` has several evaluators and each one is driven here: the DataFusion
//! expression path over literals, parameters, typed and schemaless columns
//! (in L0 and after a flush, with and without a scalar index on the column),
//! the fallback executor (a standalone `CALL` runs the statement outside
//! DataFusion), and the Locy evaluators (a rule-body `WHERE` and a
//! `QUERY … RETURN`).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(regex_full_match)'

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};
use uni_query::{ProcedureOutput, ProcedureValueType, RegisteredProcedure};

/// `(subject, pattern, expected)` triples with Java `matches()` answers.
const CASES: &[(&str, &str, bool)] = &[
    ("abc", "b", false),
    ("abc", "a", false),
    ("abc", "c", false),
    ("abc", "a.*", true),
    ("abc", ".*b.*", true),
    ("abc", "abc", true),
    ("ab", "a|ab", true),
    ("ab", "ab|a", true),
    ("xab", "a|ab", false),
    ("abc", "(?i)ABC", true),
    ("abc", "(?i)AB", false),
    ("abc\n", "abc", false),
    ("abc\n", "abc$", false),
    ("abc\n", "(?m)abc$", false),
    ("abc\n", "abc\n", true),
    ("abc\n", "abc.", false),
    ("abc\n", "(?s)abc.", true),
    ("a\nb", "(?m)^b$", false),
    ("", "", true),
    ("", "a*", true),
    ("a", "", false),
    // Java's defaults: ASCII `\w`/`\b`, ASCII-only `(?i)` unless `(?u)`,
    // and `.` excludes every Java line terminator.
    ("\u{e9}", r"\w", false),
    ("\u{e9}", r"(?U)\w", true),
    ("\u{c9}", "(?i)\u{e9}", false),
    ("\u{c9}", "(?iu)\u{e9}", true),
    ("a\u{2028}", "a.", false),
    ("a\u{85}", "(?s)a.", true),
    ("ab", r"a\b", false),
    ("<a>", r"\<a\>", true),
];

/// Renders `s` as a Cypher string literal.
fn lit(s: &str) -> String {
    format!(
        "'{}'",
        s.replace('\\', "\\\\")
            .replace('\'', "\\'")
            .replace('\n', "\\n")
    )
}

/// A typed label `T` (scalar-indexed `s` when `indexed`) and a schemaless
/// label `U`, each holding one node per case subject.
async fn setup(flush: bool, indexed: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let label = db
        .schema()
        .label("T")
        .property("s", DataType::String)
        .property("k", DataType::Int64);
    let label = if indexed {
        label.index("s", IndexType::Scalar(ScalarType::BTree))
    } else {
        label
    };
    label.apply().await?;
    let tx = db.session().tx().await?;
    for (i, (subject, _, _)) in CASES.iter().enumerate() {
        tx.execute_with("CREATE (:T {k: $k, s: $s}), (:U {k: $k, s: $s})")
            .param("k", Value::Int(i as i64))
            .param("s", Value::String((*subject).to_string()))
            .run()
            .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

async fn single(db: &Uni, query: &str) -> Result<Value> {
    let rows = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
    assert_eq!(rows.len(), 1, "{query}: expected one row");
    Ok(rows.rows()[0].value("r").cloned().unwrap_or(Value::Int(-1)))
}

/// DataFusion path over two literals, a parameter pattern and parameter
/// subject, and a subject bound through `WITH`.
#[tokio::test]
async fn regex_full_match_literals_and_params() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    for &(subject, pattern, expected) in CASES {
        let q = format!("RETURN {} =~ {} AS r", lit(subject), lit(pattern));
        assert_eq!(single(&db, &q).await?, Value::Bool(expected), "{q}");

        let q = format!(
            "WITH {} AS s RETURN s =~ {} AS r",
            lit(subject),
            lit(pattern)
        );
        assert_eq!(single(&db, &q).await?, Value::Bool(expected), "{q}");

        let q = format!(
            "UNWIND [{}] AS s RETURN s =~ {} AS r",
            lit(subject),
            lit(pattern)
        );
        assert_eq!(single(&db, &q).await?, Value::Bool(expected), "{q}");

        let rows = db
            .session()
            .query_with("RETURN $s =~ $p AS r")
            .param("s", Value::String(subject.to_string()))
            .param("p", Value::String(pattern.to_string()))
            .fetch_all()
            .await?;
        assert_eq!(
            rows.rows()[0].value("r").cloned(),
            Some(Value::Bool(expected)),
            "$s={subject:?} =~ $p={pattern:?}"
        );
    }
    Ok(())
}

/// DataFusion path over stored columns — a typed `String` column (with and
/// without a scalar index) and a schemaless one — in L0 and after a flush,
/// both projected and as a `WHERE` filter.
#[tokio::test]
async fn regex_full_match_columns_l0_and_flushed() -> Result<()> {
    for flush in [false, true] {
        for indexed in [false, true] {
            let db = setup(flush, indexed).await?;
            for label in ["T", "U"] {
                for (i, &(subject, pattern, expected)) in CASES.iter().enumerate() {
                    let ctx = format!("{label} flush={flush} indexed={indexed}");
                    let q = format!(
                        "MATCH (n:{label}) WHERE n.k = {i} RETURN n.s =~ {} AS r",
                        lit(pattern)
                    );
                    assert_eq!(
                        single(&db, &q).await?,
                        Value::Bool(expected),
                        "{q} ({ctx}, subject {subject:?})"
                    );

                    let q = format!(
                        "MATCH (n:{label}) WHERE n.s =~ {} RETURN collect(n.k) AS r",
                        lit(pattern)
                    );
                    let want: Vec<Value> = CASES
                        .iter()
                        .enumerate()
                        .filter(|(_, (s, p, e))| *p == pattern && *s == subject && *e)
                        .map(|(j, _)| Value::Int(j as i64))
                        .collect();
                    let got = single(&db, &q).await?;
                    let Value::List(mut got) = got else {
                        panic!("{q} ({ctx}): not a list: {got:?}");
                    };
                    // Other cases may share a subject that this pattern also
                    // matches; keep only the case under test.
                    got.retain(|v| *v == Value::Int(i as i64));
                    assert_eq!(got, want, "{q} ({ctx}, subject {subject:?})");
                }
            }
        }
    }
    Ok(())
}

/// Registers `test.regex_rows()`, yielding one `(s, p)` row per case.
fn register_case_procedure(db: &Uni) {
    db.procedure_registry().register(RegisteredProcedure {
        name: "test.regex_rows".to_string(),
        params: Vec::new(),
        outputs: vec![
            ProcedureOutput {
                name: "s".to_string(),
                output_type: ProcedureValueType::String,
            },
            ProcedureOutput {
                name: "p".to_string(),
                output_type: ProcedureValueType::String,
            },
            ProcedureOutput {
                name: "k".to_string(),
                output_type: ProcedureValueType::Number,
            },
        ],
        data: CASES
            .iter()
            .enumerate()
            .map(|(i, (s, p, _))| {
                HashMap::from([
                    ("s".to_string(), Value::String((*s).to_string())),
                    ("p".to_string(), Value::String((*p).to_string())),
                    ("k".to_string(), Value::Int(i as i64)),
                ])
            })
            .collect(),
    });
}

/// Fallback executor: a standalone `CALL` runs the statement outside
/// DataFusion, with the subject and the pattern both per-row values.
#[tokio::test]
async fn regex_full_match_fallback_executor() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    register_case_procedure(&db);
    let rows = db
        .session()
        .query("CALL test.regex_rows() YIELD s, p, k RETURN k, s =~ p AS r")
        .await?;
    assert_eq!(rows.len(), CASES.len());
    for row in rows.rows() {
        let Some(Value::Int(k)) = row.value("k").cloned() else {
            panic!("no k in {row:?}");
        };
        let (subject, pattern, expected) = CASES[k as usize];
        assert_eq!(
            row.value("r").cloned(),
            Some(Value::Bool(expected)),
            "fallback: {subject:?} =~ {pattern:?}"
        );
    }
    Ok(())
}

/// Locy: a rule-body `WHERE n.s =~ …` and a `QUERY … RETURN s =~ …` both
/// full-match.
#[tokio::test]
async fn regex_full_match_locy() -> Result<()> {
    for flush in [false, true] {
        let db = setup(flush, false).await?;
        let session = db.session();
        for &(subject, pattern, expected) in CASES {
            let program = format!(
                r#"
                CREATE RULE hit AS
                    MATCH (n:T)
                    WHERE n.s =~ {p}
                    YIELD KEY n.k AS k, n.s AS s

                CREATE RULE all AS
                    MATCH (n:T)
                    YIELD KEY n.k AS k, n.s AS s

                QUERY all RETURN k, s, s =~ {p} AS r
                "#,
                p = lit(pattern)
            );
            let result = session.locy(&program).await?;
            let hit = result.derived_facts("hit").cloned().unwrap_or_default();
            let hit_subjects: Vec<Value> = hit.iter().filter_map(|r| r.get("s").cloned()).collect();
            let subject_value = Value::String(subject.to_string());
            assert_eq!(
                hit_subjects.contains(&subject_value),
                expected,
                "Locy rule WHERE {subject:?} =~ {pattern:?} (flush={flush}): {hit_subjects:?}"
            );

            let rows = result.rows().expect("QUERY rows");
            let row = rows
                .iter()
                .find(|r| r.get("s") == Some(&subject_value))
                .expect("subject row");
            assert_eq!(
                row.get("r").cloned(),
                Some(Value::Bool(expected)),
                "Locy QUERY RETURN {subject:?} =~ {pattern:?} (flush={flush})"
            );
        }
    }
    Ok(())
}

/// Unchanged: NULL in either position is NULL on every path, and an invalid
/// pattern is an error.
#[tokio::test]
async fn regex_full_match_null_and_invalid_pattern() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    for q in [
        "RETURN null =~ 'a' AS r",
        "RETURN 'a' =~ null AS r",
        "WITH null AS s RETURN s =~ 'a' AS r",
        "WITH null AS p RETURN 'a' =~ p AS r",
    ] {
        assert_eq!(single(&db, q).await?, Value::Null, "{q}");
    }
    for q in [
        "RETURN 'a' =~ '[a' AS r",
        "RETURN 'a' =~ 'a)|(b' AS r",
        "WITH '[a' AS p RETURN 'a' =~ p AS r",
        // Possessive quantifiers are rejected, never read as `(a*)+`.
        "RETURN 'aa' =~ 'a*+a' AS r",
        "WITH 'a++' AS p RETURN 'aa' =~ p AS r",
    ] {
        let err = db
            .session()
            .query(q)
            .await
            .err()
            .unwrap_or_else(|| panic!("{q}: expected an error"));
        assert!(
            err.to_string().to_lowercase().contains("regex"),
            "{q}: {err}"
        );
    }
    Ok(())
}

/// A pattern whose Java answer depends on input the translation cannot
/// evaluate exactly is refused for that input on every path: here `$` before
/// more pattern over an input with an inner newline.
#[tokio::test]
async fn regex_full_match_refuses_inexact_input() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    for q in [
        r"RETURN 'a\nb' =~ 'a$\\nb' AS r",
        r"WITH 'a\nb' AS s RETURN s =~ 'a$\\nb' AS r",
        r"UNWIND ['a\nb'] AS s RETURN s =~ 'a$\\nb' AS r",
    ] {
        let err = session
            .query(q)
            .await
            .err()
            .unwrap_or_else(|| panic!("{q}: expected an error"));
        assert!(
            err.to_string().contains("Unsupported regex input"),
            "{q}: {err}"
        );
    }
    // Fallback executor: the per-row values include "a\nb".
    register_case_procedure(&db);
    let err = session
        .query(r"CALL test.regex_rows() YIELD s RETURN s =~ 'a$\\nb' AS r")
        .await
        .err()
        .expect("fallback: expected an error");
    assert!(
        err.to_string().contains("Unsupported regex input"),
        "fallback: {err}"
    );
    // Locy: a stored subject "a\nb".
    let db2 = setup(false, false).await?;
    let err = db2
        .session()
        .locy(
            r"CREATE RULE all AS MATCH (n:T) YIELD KEY n.k AS k, n.s AS s
              QUERY all RETURN k, s =~ 'a$\\nb' AS r",
        )
        .await
        .err()
        .expect("Locy: expected an error");
    assert!(
        err.to_string().contains("Unsupported regex input"),
        "Locy: {err}"
    );
    // The same pattern over a single-line input with a final newline answers.
    assert_eq!(
        single(&db, r"RETURN 'a\n' =~ 'a$\\n' AS r").await?,
        Value::Bool(true)
    );
    Ok(())
}
