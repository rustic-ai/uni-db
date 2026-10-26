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
//! literal. Look-around, backreferences (case-insensitive ones too), atomic
//! groups, possessive quantifiers, `\X`, `\b{g}`, `(?c)`, `\b` and anchors
//! away from the pattern's ends or under `(?m)` run on a backtracking matcher that follows
//! Java's semantics, the rest on a linear-time automaton; no valid pattern
//! is refused. A match that
//! backtracks past `UniConfig::regex_backtrack_limit` fails with
//! `UniError::RegexBacktrackLimit`, a match on which Java's own matcher
//! throws fails with that exception named, and a long scan of regex rows
//! honours the query timeout.
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
    // Inputs the automaton cannot answer exactly, answered by the
    // backtracking matcher instead of refused.
    ("a\nb", "a$\nb", false),
    ("a\n", "a$\n", true),
    ("a\u{85}", "a$\u{85}", true),
    ("e\u{301}", r"e\x{301}\b", true),
    // Constructs only the backtracking matcher evaluates.
    ("aa", "a*+a", false),
    ("aa", "a++", true),
    ("ab", "a(?<=a)b", true),
    ("b", "(?<!a)b", true),
    ("aa", r"(a)\1", true),
    ("abc", "(?>a|ab)c", false),
    ("\r\n", r"\R?\n", false),
    // Java-only syntax.
    ("a.b", r"\Qa.b\E", true),
    ("xx", "x{1}{2}", false),
    ("\r\n", r"\R", true),
    // Once rejected as unsupported, now answered as Java 21 answers.
    ("aA", r"(?i)(a)\1", true),
    ("k\u{212a}", r"(?iu)(k)\1", true),
    ("k\u{212a}", r"(?i)(k)\1", false),
    ("\u{3b1}", r"\p{InGreek}", true),
    ("A", r"\p{javaLowerCase}", false),
    ("A", r"(?i)\p{javaLowerCase}", true),
    ("a", r"\N{LATIN SMALL LETTER A}", true),
    ("e\u{301}", r"\X", true),
    ("\r\n", r"\X\X", false),
    ("e\u{301}", r"e\b{g}\x{301}", false),
    ("e\u{301}", r"(?c)[\x{e9}]", true),
    ("a", r"(?:a|\G){2}", false),
    ("xa", r"(?:(?=(.))x|\1a+)", true),
    ("\u{10428}a", r".*(?<=\b\p{L})a", false),
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
            let hit = result
                .derived_rule("hit")
                .await
                .unwrap()
                .cloned()
                .unwrap_or_default();
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
        // Java rejects these; so does uni-db.
        "RETURN 'aa' =~ 'a**' AS r",
        "WITH 'a{,2}' AS p RETURN 'aa' =~ p AS r",
        "RETURN 'a' =~ '\\N{NO SUCH CHARACTER}' AS r",
        "RETURN 'a' =~ '\\p{InNoSuchBlock}' AS r",
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

/// A pattern that backtracks catastrophically on the backtracking matcher.
const CATASTROPHIC: &str = "(?:(a+)+)+b(?=)";

fn is_backtrack_limit(err: &uni_db::UniError, limit: usize) -> bool {
    matches!(
        err,
        uni_db::UniError::RegexBacktrackLimit { pattern, limit: l }
            if pattern == CATASTROPHIC && *l == limit
    )
}

/// A match past the backtrack limit fails with the typed error on every
/// path: literals, a stored column, the fallback executor, a transaction and
/// Locy.
#[tokio::test]
async fn regex_backtrack_limit_is_a_typed_error() -> Result<()> {
    let subject = "a".repeat(40);
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let default_limit = uni_db::UniConfig::default().regex_backtrack_limit;
    for q in [
        format!("RETURN '{subject}' =~ '{CATASTROPHIC}' AS r"),
        format!("WITH '{subject}' AS s RETURN s =~ '{CATASTROPHIC}' AS r"),
        format!("UNWIND ['{subject}'] AS s RETURN s =~ $p AS r"),
    ] {
        let err = session
            .query_with(&q)
            .param("p", Value::String(CATASTROPHIC.to_string()))
            .fetch_all()
            .await
            .err()
            .unwrap_or_else(|| panic!("{q}: expected an error"));
        assert!(is_backtrack_limit(&err, default_limit), "{q}: {err:?}");
    }

    // Stored column, read in a transaction. The failed statement leaves the
    // transaction rollback-only, so the node is committed beforehand.
    let tx = session.tx().await?;
    tx.execute_with("CREATE (:R {s: $s})")
        .param("s", Value::String(subject.clone()))
        .run()
        .await?;
    tx.commit().await?;
    let tx = session.tx().await?;
    let err = tx
        .query(&format!("MATCH (n:R) RETURN n.s =~ '{CATASTROPHIC}' AS r"))
        .await
        .err()
        .expect("tx: expected an error");
    assert!(is_backtrack_limit(&err, default_limit), "tx: {err:?}");
    tx.rollback();

    // Fallback executor.
    db.procedure_registry().register(RegisteredProcedure {
        name: "test.long_a".to_string(),
        params: Vec::new(),
        outputs: vec![ProcedureOutput {
            name: "s".to_string(),
            output_type: ProcedureValueType::String,
        }],
        data: vec![HashMap::from([(
            "s".to_string(),
            Value::String(subject.clone()),
        )])],
    });
    let err = session
        .query(&format!(
            "CALL test.long_a() YIELD s RETURN s =~ '{CATASTROPHIC}' AS r"
        ))
        .await
        .err()
        .expect("fallback: expected an error");
    assert!(is_backtrack_limit(&err, default_limit), "fallback: {err:?}");

    // Locy, over the stored node.
    let err = session
        .locy(&format!(
            "CREATE RULE hit AS MATCH (n:R) WHERE n.s =~ '{CATASTROPHIC}' YIELD KEY n.s AS s"
        ))
        .await
        .err()
        .expect("Locy: expected an error");
    assert!(is_backtrack_limit(&err, default_limit), "Locy: {err:?}");
    Ok(())
}

/// `UniConfig::regex_backtrack_limit` sets the limit: a small one refuses a
/// match the default answers, and the error reports it.
#[tokio::test]
async fn regex_backtrack_limit_follows_the_config() -> Result<()> {
    let subject = "a".repeat(12);
    let q = format!("RETURN '{subject}' =~ '{CATASTROPHIC}' AS r");
    let db = Uni::in_memory().build().await?;
    assert_eq!(single(&db, &q).await?, Value::Bool(false));

    let config = uni_db::UniConfig {
        regex_backtrack_limit: 50,
        ..Default::default()
    };
    let small = Uni::in_memory().config(config).build().await?;
    let err = small
        .session()
        .query(&q)
        .await
        .err()
        .expect("small limit: expected an error");
    assert!(is_backtrack_limit(&err, 50), "{err:?}");
    // A pattern on the automaton is not subject to the limit.
    assert_eq!(
        single(&small, &format!("RETURN '{subject}' =~ '((a+)+)+b' AS r")).await?,
        Value::Bool(false)
    );
    Ok(())
}

/// A long run of regex rows honours the query timeout: the deadline is
/// checked between rows, not only between batches. Each row here takes
/// milliseconds on the backtracking matcher, and all of them sit in one
/// batch. The backreference matters: without one, Java's matcher (and its
/// port) remembers the positions where a loop iteration failed, and
/// `(?:a|aa)*c` is no longer exponential.
#[tokio::test]
async fn regex_scan_honours_the_query_timeout() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let q = "UNWIND range(1, 6000) AS i \
             WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 0, 24 + i % 2) AS s \
             WHERE s =~ '(a|aa)*c\\\\1' RETURN count(*) AS c";
    let budget = std::time::Duration::from_millis(300);
    let start = std::time::Instant::now();
    let err = db
        .session()
        .query_with(q)
        .timeout(budget)
        .fetch_all()
        .await
        .err()
        .expect("expected a timeout");
    let elapsed = start.elapsed();
    assert!(matches!(err, uni_db::UniError::Timeout { .. }), "{err:?}");
    assert!(
        elapsed < std::time::Duration::from_millis(1500),
        "timed out only after {elapsed:?}"
    );
    Ok(())
}

/// Where Java's own matcher throws on an input, the match fails with an
/// error naming the exception, as the query does in Neo4j: `\b{g}` reads
/// the character at a hidden position `last`, which the look-ahead left at
/// the end of the input (`StringIndexOutOfBoundsException` on Java 21 and 25). An input
/// whose backtrack stack outgrows the matcher's memory budget (about 64 MB;
/// Java overflows its default stack on far shorter ones) fails with a
/// recursion-limit error.
#[tokio::test]
async fn regex_java_exceptions_and_recursion_limit_are_errors() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db
        .session()
        .query(r"RETURN 'ab' =~ '(?=ab)a\\b{g}b' AS r")
        .await
        .err()
        .expect("Java throws: expected an error");
    assert!(
        err.to_string()
            .contains("java.lang.StringIndexOutOfBoundsException"),
        "{err}"
    );
    let err = db
        .session()
        .query_with("RETURN $s =~ '(?:a|b)*(?=)' AS r")
        .param("s", Value::String("ab".repeat(1_200_000)))
        .fetch_all()
        .await
        .err()
        .expect("deep recursion: expected an error");
    assert!(
        err.to_string().contains("Regex recursion limit exceeded"),
        "{err}"
    );
    // A shorter one is answered, though Java's default stack overflows on
    // it.
    let rows = db
        .session()
        .query_with("RETURN $s =~ '(?:a|b)*(?=)' AS r")
        .param("s", Value::String("ab".repeat(100_000)))
        .fetch_all()
        .await?;
    assert_eq!(rows.rows()[0].value("r"), Some(&Value::Bool(true)));
    Ok(())
}
