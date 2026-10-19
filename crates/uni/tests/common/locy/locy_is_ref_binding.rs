// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! One rule decides which column of a referenced rule an IS reference binds.
//!
//! A referenced rule's columns form one *binding sequence*: its KEY columns in
//! YIELD order, then its other columns in YIELD order. An IS reference's
//! subjects take columns from the front of that sequence, and a `TO` target
//! takes the next one. For a rule whose KEY columns come first this is the
//! positional binding the Locy reference describes; for one or two subjects it
//! is what the planner's positive IS has always done.
//!
//! Before it, each layer had its own rule. The planner bound subjects to KEY
//! columns only and rejected `(o, e, pct) IS stake` (issue #298), although the
//! compiler and the documentation accept it. `IS NOT` bound by YIELD position,
//! so against a rule yielding `b, KEY a` it tested the wrong column and
//! returned exactly the wrong nodes. With KEY columns interleaved with value
//! columns, `IS NOT ... TO t` compared `t` with a value column and failed.
//!
//! `IS NOT` cannot compare a bound variable with a value column: a fact is
//! identified by its KEY, and negation is about whether a fact exists. An
//! unbound variable there is existentially quantified and ignored.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(is_ref_binding)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni};
use uni_locy::result::Modification;

/// `a -> b` (5) and `a -> c` (7).
const FAN: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
     (a)-[:E {w: 5}]->(b), (a)-[:E {w: 7}]->(c)";

fn config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

/// Runs `program` and returns its last `QUERY`'s rows as sorted
/// `column=value` cells, the rows sorted.
async fn query(db: &Uni, program: &str) -> Result<Vec<String>> {
    let result = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?;
    let Some(rows) = result.command_results.iter().rev().find_map(|c| match c {
        CommandResult::Query(rows) => Some(rows),
        _ => None,
    }) else {
        bail!("program has no QUERY: {program}");
    };
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v}")).collect();
            cells.sort();
            cells.join(" ")
        })
        .collect();
    out.sort();
    Ok(out)
}

async fn graph(setup: &str) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(setup).await?;
    tx.commit().await?;
    Ok(db)
}

/// The data of issue #298: `x -> a` twice at 25 (one stake reported by two
/// datasets), `y -> a` at 20, `a -> b` at 60.
async fn stakes() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("uid", DataType::String)
        .done()
        .edge_type("OWNS", &["E"], &["E"])
        .property_nullable("pct", DataType::Float64)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {uid: 'x'}), (:E {uid: 'y'}), (:E {uid: 'a'}), (:E {uid: 'b'})")
        .await?;
    for (o, e, pct) in [
        ("x", "a", 25.0),
        ("x", "a", 25.0),
        ("y", "a", 20.0),
        ("a", "b", 60.0),
    ] {
        tx.execute_with("MATCH (o:E {uid: $o}), (e:E {uid: $e}) CREATE (o)-[:OWNS {pct: $p}]->(e)")
            .param("o", o)
            .param("e", e)
            .param("p", pct)
            .run()
            .await?;
    }
    tx.commit().await?;
    Ok(db)
}

const STAKE: &str = "CREATE RULE stake AS \
    MATCH (o:E)-[r:OWNS]->(e:E) WHERE r.pct IS NOT NULL \
    FOLD pct = MAX(r.pct) YIELD KEY o, KEY e, pct\n";

const BLOCKED_BASE: &str = "CREATE RULE blocked AS \
    MATCH (e:E) WHERE e.uid IN ['x', 'y'] YIELD KEY e, 100.0 AS agg\n";

/// Issue #298: a third subject binds the rule's value column, so the second
/// stage sums one contribution per owner: `a` = max(25, 25) + 20 = 45, below
/// the threshold, and nothing beyond the designated entities is blocked.
///
/// Both conjunct orders: binding is order-independent (issue #300), and the
/// first order is #300's own shape, which returned no rows before.
#[tokio::test]
async fn is_ref_binding_tuple_reads_a_value_column() -> Result<()> {
    let db = stakes().await?;
    for conditions in [
        "o IS blocked AND (o, e, pct) IS stake",
        "(o, e, pct) IS stake AND o IS blocked",
    ] {
        let program = format!(
            "{STAKE}{BLOCKED_BASE}CREATE RULE blocked AS MATCH (o:E), (e:E) \
             WHERE {conditions} \
             FOLD agg = MSUM(pct) REQUIRE agg >= 50.0 YIELD KEY e, agg\n\
             QUERY blocked RETURN e.uid AS uid"
        );
        assert_eq!(
            query(&db, &program).await?,
            ["uid=x", "uid=y"],
            "{conditions}"
        );
    }
    Ok(())
}

/// The tuple form, `TO`, and the bare column name are three spellings of one
/// binding and derive the same facts.
#[tokio::test]
async fn is_ref_binding_tuple_to_and_name_agree() -> Result<()> {
    let db = stakes().await?;
    let spellings = [
        "MATCH (o:E), (e:E) WHERE (o, e, v) IS stake YIELD KEY o, KEY e, v",
        "MATCH (o:E), (e:E) WHERE (o, e) IS stake TO v YIELD KEY o, KEY e, v",
        "MATCH (o:E), (e:E) WHERE (o, e) IS stake YIELD KEY o, KEY e, pct AS v",
    ];
    let expected = ["e=a o=x v=25.0", "e=a o=y v=20.0", "e=b o=a v=60.0"];
    for body in spellings {
        let program = format!(
            "{STAKE}CREATE RULE t AS {body}\n\
             QUERY t RETURN o.uid AS o, e.uid AS e, v"
        );
        assert_eq!(query(&db, &program).await?, expected, "{body}");
    }
    Ok(())
}

/// A subject that is already bound is tested against the column, not rebound.
#[tokio::test]
async fn is_ref_binding_bound_value_subject_is_a_test() -> Result<()> {
    let db = stakes().await?;
    let program = format!(
        "{STAKE}CREATE RULE big AS MATCH (o:E), (e:E) \
         WHERE (o, e) IS stake TO v, (o, e, v) IS stake, v > 21.0 YIELD KEY o, KEY e, v\n\
         QUERY big RETURN o.uid AS o, e.uid AS e, v"
    );
    assert_eq!(
        query(&db, &program).await?,
        ["e=a o=x v=25.0", "e=b o=a v=60.0"]
    );
    Ok(())
}

/// `IS` and `IS NOT` bind the same column when a value column precedes the
/// KEY: the nodes with an out-edge, and the rest.
#[tokio::test]
async fn is_ref_binding_negation_follows_the_key_not_the_position() -> Result<()> {
    let db = graph(FAN).await?;
    let edge = "CREATE RULE edge AS MATCH (a:N)-[:E]->(b:N) YIELD b, KEY a\n";
    let src = format!(
        "{edge}CREATE RULE src AS MATCH (x:N) WHERE x IS edge YIELD KEY x\n\
         QUERY src RETURN x.name AS x"
    );
    let nonsrc = format!(
        "{edge}CREATE RULE nonsrc AS MATCH (x:N) WHERE x IS NOT edge YIELD KEY x\n\
         QUERY nonsrc RETURN x.name AS x"
    );
    assert_eq!(query(&db, &src).await?, ["x=a"]);
    assert_eq!(query(&db, &nonsrc).await?, ["x=b", "x=c"]);
    Ok(())
}

/// With KEY columns interleaved with a value column, `IS NOT ... TO t` binds
/// `t` to the second KEY, as the positive form does.
#[tokio::test]
async fn is_ref_binding_negation_with_interleaved_keys() -> Result<()> {
    let db = graph(FAN).await?;
    let link = "CREATE RULE link AS MATCH (x:N)-[e:E]->(y:N) YIELD KEY x, e.w AS w, KEY y\n";
    let via = format!(
        "{link}CREATE RULE via AS MATCH (x:N), (t:N) WHERE x IS link TO t YIELD KEY x, KEY t\n\
         QUERY via RETURN x.name AS x, t.name AS t"
    );
    let not_via = format!(
        "{link}CREATE RULE nv AS MATCH (x:N {{name: 'a'}}), (t:N) \
         WHERE x IS NOT link TO t YIELD KEY x, KEY t\n\
         QUERY nv RETURN x.name AS x, t.name AS t"
    );
    assert_eq!(query(&db, &via).await?, ["t=b x=a", "t=c x=a"]);
    assert_eq!(query(&db, &not_via).await?, ["t=a x=a"]);
    Ok(())
}

/// `IS NOT` that compares a bound variable with a value column is refused at
/// compile time, naming both. (An unbound one is existentially quantified and
/// legal: `locy_is_not_scalar_to_target`.)
///
/// Refused in both conjunct orders: `s` is bound by the positive reference
/// wherever it appears (issue #300). The compiler used to walk the WHERE left
/// to right and let the reversed order through, treating `s` as an unbound,
/// existential value; now the binding plan refuses both at compile time.
#[tokio::test]
async fn is_ref_binding_negation_of_a_bound_value_is_refused() -> Result<()> {
    let db = graph(FAN).await?;
    for conditions in [
        "x IS tot TO s, x IS NOT tot TO s",
        "x IS NOT tot TO s, x IS tot TO s",
    ] {
        let program = format!(
            "CREATE RULE tot AS MATCH (x:N)-[e:E]->(y:N) FOLD s = SUM(e.w) YIELD KEY x, s\n\
             CREATE RULE r AS MATCH (x:N) WHERE {conditions} YIELD KEY x\n\
             QUERY r RETURN x.name AS x"
        );
        let err = db
            .session()
            .locy_with(&program)
            .with_config(config())
            .run()
            .await
            .expect_err("IS NOT comparing the bound value `s` must be refused");
        let message = format!("{err:#}");
        assert!(
            message.contains("IS NOT") && message.contains("`s`"),
            "{conditions}: unexpected error: {message}"
        );
    }
    Ok(())
}

/// More bindings than the referenced rule has columns is a compile error.
#[tokio::test]
async fn is_ref_binding_too_many_subjects_is_refused() -> Result<()> {
    let db = stakes().await?;
    let program = format!(
        "{STAKE}CREATE RULE t AS MATCH (o:E), (e:E) WHERE (o, e, v, w) IS stake \
         YIELD KEY o, KEY e, v\nQUERY t RETURN v"
    );
    let err = db
        .session()
        .locy_with(&program)
        .with_config(config())
        .run()
        .await
        .expect_err("four bindings into a three-column rule must be refused");
    assert!(
        format!("{err:#}").contains("arity"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

/// ABDUCE traces an IS reference with interleaved KEYs to the facts it read.
///
/// ABDUCE builds its derivation tree with the in-memory evaluator, which bound
/// `TO t` by the stored relation's column order and put the weight `w` in `t`.
/// No clause reproduced the `via` facts, the tree ended at them, and ABDUCE
/// reported no valid edit at all, though removing the `E` edge from `a` to `b`
/// removes `via(a, b)`.
///
/// The goal names one fact. `x.name = 'a'` alone matches `via(a, b)` and
/// `via(a, c)`, which no single edge removal ends; this test used to accept
/// that goal only because a removal deleted every `E` edge.
#[tokio::test]
async fn is_ref_binding_abduce_follows_the_binding() -> Result<()> {
    let db = graph(FAN).await?;
    let program = "CREATE RULE link AS MATCH (x:N)-[e:E]->(y:N) YIELD KEY x, e.w AS w, KEY y\n\
         CREATE RULE via AS MATCH (x:N) WHERE x IS link TO t YIELD KEY x, KEY t\n\
         ABDUCE NOT via WHERE x.name = 'a' AND t.name = 'b'";
    let result = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?;
    let Some(CommandResult::Abduce(abduction)) = result.command_results.last() else {
        bail!("expected an ABDUCE result");
    };
    let edge_removals = abduction
        .modifications
        .iter()
        .filter(|m| {
            m.validated
                && matches!(&m.modification, Modification::RemoveEdge { edge_type, .. } if edge_type == "E")
        })
        .count();
    assert!(
        edge_removals > 0,
        "no validated edge removal for `via`: {:#?}",
        abduction.modifications
    );
    Ok(())
}

/// YIELD column order changes no answer. Each program is written twice, its
/// KEYs yielded first and last, and must derive the same facts.
///
/// The FOLD pipeline assumed KEY-first: the FOLD operator emits KEYs before
/// aggregates, a seeding clause emitted YIELD order, and derived scans were
/// declared in YIELD order. Yielding the value first failed with
/// "concatenate arrays of different data types" (a seed beside a FOLD), or
/// "column types must match schema types" (a recursive seeded FOLD). Rules are
/// now stored in binding order, KEYs first, whatever the YIELD says.
#[tokio::test]
async fn is_ref_binding_yield_order_changes_no_answer() -> Result<()> {
    let db = graph(FAN).await?;
    let cases = [
        (
            "recursive seeded fold",
            "CREATE RULE roll AS MATCH (a:N) YIELD KEY a, 1 AS s\n\
             CREATE RULE roll AS MATCH (a:N)-[e:E]->(c:N) WHERE c IS roll \
             FOLD s = MSUM(s + e.w) YIELD KEY a, s\n\
             QUERY roll RETURN a.name AS a, s",
            "CREATE RULE roll AS MATCH (a:N) YIELD 1 AS s, KEY a\n\
             CREATE RULE roll AS MATCH (a:N)-[e:E]->(c:N) WHERE c IS roll \
             FOLD s = MSUM(s + e.w) YIELD s, KEY a\n\
             QUERY roll RETURN a.name AS a, s",
        ),
        (
            "seed beside a non-recursive fold",
            "CREATE RULE g AS MATCH (a:N) WHERE a.name = 'b' YIELD KEY a, 0 AS s\n\
             CREATE RULE g AS MATCH (a:N)-[e:E]->(c:N) FOLD s = MSUM(e.w) YIELD KEY a, s\n\
             QUERY g RETURN a.name AS a, s",
            "CREATE RULE g AS MATCH (a:N) WHERE a.name = 'b' YIELD 0 AS s, KEY a\n\
             CREATE RULE g AS MATCH (a:N)-[e:E]->(c:N) FOLD s = MSUM(e.w) YIELD s, KEY a\n\
             QUERY g RETURN a.name AS a, s",
        ),
        (
            "two folds yielded out of FOLD order, with a seed",
            "CREATE RULE h AS MATCH (a:N) WHERE a.name = 'b' YIELD KEY a, 0 AS t, 0 AS n\n\
             CREATE RULE h AS MATCH (a:N)-[e:E]->(c:N) FOLD n = MCOUNT(e), t = MSUM(e.w) \
             YIELD KEY a, t, n\n\
             QUERY h RETURN a.name AS a, t, n",
            "CREATE RULE h AS MATCH (a:N) WHERE a.name = 'b' YIELD 0 AS t, 0 AS n, KEY a\n\
             CREATE RULE h AS MATCH (a:N)-[e:E]->(c:N) FOLD n = MCOUNT(e), t = MSUM(e.w) \
             YIELD t, n, KEY a\n\
             QUERY h RETURN a.name AS a, t, n",
        ),
    ];
    for (label, key_first, key_last) in cases {
        let want = query(&db, key_first).await?;
        assert!(
            !want.is_empty(),
            "{label}: the key-first form derived nothing"
        );
        assert_eq!(query(&db, key_last).await?, want, "{label}");
    }
    Ok(())
}
