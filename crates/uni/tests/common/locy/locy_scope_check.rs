// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Every expression in a Locy program is checked, at compile time, against the
//! variables visible where it is written.
//!
//! Locy reparses every expression slot except the inside of ALONG as plain
//! Cypher, and nothing checked those expressions' variables. A Locy construct
//! written there, or a misspelt variable, failed late or not at all:
//!
//! - `prev.x` outside ALONG became a property of an unbound variable `prev`
//!   and failed in DataFusion with "No field named prev";
//! - `coalesce(prev.c, 0)` in a base clause slipped past `PrevInBaseCase`;
//! - `BEST BY zz.w` ranked by a column named `w`, whatever `zz` was;
//! - an unbound variable in an ABDUCE, EXPLAIN or DERIVE `WHERE` evaluated
//!   to NULL, so the command silently matched nothing;
//! - `REQUIRE t * 1.0 <= 50` and `REQUIRE t >= 1 AND t <= 50` bypassed the
//!   check that keeps a recursive REQUIRE one-way;
//! - `FOLD t = SUM(e.w) * 1.0` bypassed the recursion monotonicity check.
//!
//! The second half of the matrix is the programs a scope check must keep
//! accepting: a referenced rule's value columns by bare name, `$params`,
//! ALONG names, `prev` directly in a recursive ALONG, relationship variables,
//! KEY and FOLD aliases after the FOLD, local variables of subqueries and
//! comprehensions, and a pattern variable that happens to be named `prev`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(scope_check)'

// Rust guideline compliant

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Result, bail};
use uni_common::Value;
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyConfig};

/// `a -> b` (5) and `a -> c` (7).
const FAN: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
     (a)-[:E {w: 5}]->(b), (a)-[:E {w: 7}]->(c)";

/// `a -> b -> c -> d`, every weight 1.
const CHAIN: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
     (d:N {name: 'd'}), (a)-[:E {w: 1}]->(b), (b)-[:E {w: 1}]->(c), (c)-[:E {w: 1}]->(d)";

const TOT: &str = "CREATE RULE tot AS MATCH (x:N)-[e:E]->(y:N) FOLD s = SUM(e.w) YIELD KEY x, s\n";

/// Recursive path rule over CHAIN, with an MSUM per (x, y); `{body}` replaces
/// the recursive clause's FOLD/REQUIRE tail. The base clause seeds the fold
/// rather than stating a FOLD of its own, so the recursive clause's tail is the
/// rule's: clauses that fold must state the same tail.
const RECURSIVE: &str = "CREATE RULE r AS MATCH (x:N)-[e:E]->(y:N) YIELD KEY x, KEY y, e.w AS t\n\
     CREATE RULE r AS MATCH (x:N)-[e:E]->(m:N) WHERE m IS r TO y {body} YIELD KEY x, KEY y, t\n";

fn config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        params: HashMap::from([("min".to_string(), Value::Int(6))]),
        ..Default::default()
    }
}

/// What a program must do.
enum Expect {
    /// Fail to compile, with every fragment in the message.
    Rejected(&'static [&'static str]),
    /// Run, and its last QUERY return these rows (sorted `column=value` cells).
    Rows(&'static [&'static str]),
    /// Run without error (a program whose last command is not a QUERY).
    Runs,
}

async fn run(setup: &str, program: &str) -> Result<Option<Vec<String>>> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(setup).await?;
    tx.commit().await?;
    let result = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?;
    let Some(CommandResult::Query(rows)) = result.command_results.last() else {
        return Ok(None);
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
    Ok(Some(out))
}

async fn check(label: &str, setup: &str, program: &str, expect: &Expect) -> Result<()> {
    let outcome = run(setup, program).await;
    match (expect, outcome) {
        (Expect::Rejected(fragments), Err(e)) => {
            let message = format!("{e:#}");
            if !message.contains("LocyCompileError") {
                bail!("{label}: expected a compile error, got: {message}");
            }
            for fragment in *fragments {
                if !message.contains(fragment) {
                    bail!("{label}: compile error lacks {fragment:?}: {message}");
                }
            }
        }
        (Expect::Rejected(_), Ok(rows)) => bail!("{label}: compiled and ran: {rows:?}"),
        (Expect::Rows(want), Ok(Some(got))) => {
            if got != *want {
                bail!("{label}: expected {want:?}, got {got:?}");
            }
        }
        (Expect::Rows(_), Ok(None)) => bail!("{label}: no QUERY result"),
        (Expect::Runs, Ok(_)) => {}
        (Expect::Rows(_) | Expect::Runs, Err(e)) => bail!("{label}: failed: {e:#}"),
    }
    Ok(())
}

async fn check_all(cases: Vec<(&str, &str, String, Expect)>) -> Result<()> {
    let mut failures = Vec::new();
    for (label, setup, program, expect) in &cases {
        if let Err(e) = check(label, setup, program, expect).await {
            failures.push(e.to_string());
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} of {} cases failed:\n{}",
            failures.len(),
            cases.len(),
            failures.join("\n")
        );
    }
    Ok(())
}

#[tokio::test]
async fn scope_check_rejects_what_cannot_be_evaluated() -> Result<()> {
    use Expect::Rejected;
    let rec = |body: &str| format!("{}QUERY r RETURN t", RECURSIVE.replace("{body}", body));
    check_all(vec![
        (
            "prev in a FOLD argument",
            CHAIN,
            rec("FOLD t = MSUM(prev.t)"),
            Rejected(&["`prev`", "ALONG"]),
        ),
        (
            "prev in a rule WHERE",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) WHERE prev.w > 0 YIELD KEY x, KEY y\n\
             QUERY p RETURN x"
                .into(),
            Rejected(&["`prev`", "ALONG"]),
        ),
        (
            "prev in a YIELD item",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) YIELD KEY x, KEY y, prev.w AS v\n\
             QUERY p RETURN v"
                .into(),
            Rejected(&["`prev`", "ALONG"]),
        ),
        (
            "prev in a post-FOLD WHERE",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) FOLD t = SUM(e.w) WHERE prev.t > 0 \
             YIELD KEY x, t\nQUERY p RETURN t"
                .into(),
            Rejected(&["`prev`", "ALONG"]),
        ),
        (
            "prev in REQUIRE",
            CHAIN,
            rec("FOLD t = MSUM(e.w) REQUIRE prev.t >= 1"),
            Rejected(&["`prev`", "ALONG"]),
        ),
        (
            "prev under a function call in a base ALONG",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) ALONG c = coalesce(prev.c, 0) + e.w \
             YIELD KEY x, KEY y, c\nQUERY p RETURN c"
                .into(),
            Rejected(&["prev"]),
        ),
        (
            "prev in parentheses in a recursive ALONG",
            CHAIN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) ALONG c = e.w YIELD KEY x, KEY y, c\n\
             CREATE RULE p AS MATCH (x:N)-[e:E]->(m:N) WHERE m IS p TO y \
             ALONG c = coalesce(prev.c, 0) + e.w YIELD KEY x, KEY y, c\nQUERY p RETURN c"
                .into(),
            Rejected(&["`prev`"]),
        ),
        (
            "BEST BY a misspelt variable",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) BEST BY zz.w ASC YIELD KEY x, e.w AS w\n\
             QUERY p RETURN w"
                .into(),
            Rejected(&["`zz`", "BEST BY"]),
        ),
        (
            "an unbound variable in a rule WHERE",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) WHERE q.name = 'a' YIELD KEY x\n\
             QUERY p RETURN x"
                .into(),
            Rejected(&["`q`", "WHERE"]),
        ),
        (
            "an unbound variable in a FOLD argument",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) FOLD s = SUM(q.w) YIELD KEY x, s\n\
             QUERY p RETURN s"
                .into(),
            Rejected(&["`q`", "FOLD"]),
        ),
        (
            "an unbound variable in a YIELD item",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) YIELD KEY x, q.name AS n\n\
             QUERY p RETURN n"
                .into(),
            Rejected(&["`q`", "YIELD"]),
        ),
        (
            "an unbound variable in ABDUCE WHERE",
            FAN,
            format!("{TOT}ABDUCE NOT tot WHERE zz.name = 'a'"),
            Rejected(&["`zz`", "ABDUCE"]),
        ),
        (
            "an unbound variable in EXPLAIN WHERE",
            FAN,
            format!("{TOT}EXPLAIN RULE tot WHERE zz.name = 'a'"),
            Rejected(&["`zz`", "EXPLAIN"]),
        ),
        (
            "an unbound variable in DERIVE WHERE",
            FAN,
            "CREATE RULE d AS MATCH (x:N)-[:E]->(y:N) DERIVE (x)-[:L]->(y)\n\
             DERIVE d WHERE zz.name = 'a'"
                .into(),
            Rejected(&["`zz`", "DERIVE"]),
        ),
        (
            "REQUIRE over an expression of the fold output",
            CHAIN,
            rec("FOLD t = MSUM(e.w) REQUIRE t * 1.0 <= 50"),
            Rejected(&["REQUIRE", "monotone"]),
        ),
        (
            "REQUIRE with an upper bound behind AND",
            CHAIN,
            rec("FOLD t = MSUM(e.w) REQUIRE t >= 1 AND t <= 50"),
            Rejected(&["REQUIRE", "monotone"]),
        ),
        (
            "a recursive FOLD that is not an aggregate call",
            CHAIN,
            rec("FOLD t = SUM(e.w) * 1.0"),
            Rejected(&["`t`", "aggregate"]),
        ),
        (
            "a FOLD that is not an aggregate call",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) FOLD s = SUM(e.w) + 1 YIELD KEY x, s\n\
             QUERY p RETURN s"
                .into(),
            Rejected(&["`s`", "aggregate"]),
        ),
    ])
    .await
}

#[tokio::test]
async fn scope_check_accepts_what_is_visible() -> Result<()> {
    use Expect::{Rows, Runs};
    check_all(vec![
        (
            "a referenced rule's value column by name in WHERE",
            FAN,
            format!(
                "{TOT}CREATE RULE big AS MATCH (x:N) WHERE x IS tot, s > 10 YIELD KEY x\n\
                 QUERY big RETURN x.name AS x"
            ),
            Rows(&["x=a"]),
        ),
        (
            "a referenced rule's value column by name in FOLD",
            FAN,
            format!(
                "{TOT}CREATE RULE all AS MATCH (x:N) WHERE x IS tot FOLD z = MSUM(s) YIELD KEY x, z\n\
                 QUERY all RETURN z"
            ),
            Rows(&["z=12"]),
        ),
        (
            "a $param in WHERE and in a post-FOLD WHERE",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) WHERE e.w >= $min \
             FOLD s = SUM(e.w) WHERE s >= $min YIELD KEY x, s\nQUERY p RETURN s"
                .into(),
            Rows(&["s=7"]),
        ),
        (
            "prev directly in a recursive ALONG, its name in YIELD and BEST BY",
            CHAIN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) ALONG c = e.w BEST BY c ASC \
             YIELD KEY x, KEY y, c\n\
             CREATE RULE p AS MATCH (x:N)-[e:E]->(m:N) WHERE m IS p TO y \
             ALONG c = prev.c + e.w BEST BY c ASC YIELD KEY x, KEY y, c\n\
             QUERY p WHERE x.name = 'a' AND y.name = 'd' RETURN c"
                .into(),
            Rows(&["c=3"]),
        ),
        (
            "BEST BY a relationship variable's property",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) BEST BY e.w DESC YIELD KEY x, e.w AS w\n\
             QUERY p RETURN w"
                .into(),
            Rows(&["w=7"]),
        ),
        (
            "a KEY alias and a renamed FOLD output after the FOLD",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) FOLD total = SUM(e.w) WHERE total > 1 \
             YIELD KEY x.name AS nm, total AS sum_out\nQUERY p RETURN nm, sum_out"
                .into(),
            Rows(&["nm=a sum_out=12"]),
        ),
        (
            "local variables of an EXISTS subquery and a comprehension",
            FAN,
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) WHERE EXISTS { MATCH (x)-[:E]->(z:N) } \
             AND size([v IN [1, 2] WHERE v < e.w]) = 2 YIELD KEY x, KEY y\n\
             QUERY p RETURN y.name AS y"
                .into(),
            Rows(&["y=b", "y=c"]),
        ),
        (
            "a pattern variable named prev",
            FAN,
            "CREATE RULE p AS MATCH (prev:N)-[e:E]->(y:N) WHERE prev.name = 'a' \
             YIELD KEY prev, KEY y\nQUERY p RETURN y.name AS y"
                .into(),
            Rows(&["y=b", "y=c"]),
        ),
        (
            "ABDUCE and EXPLAIN WHERE on the rule's columns",
            FAN,
            format!(
                "{TOT}EXPLAIN RULE tot WHERE x.name = 'a' AND s > 0\n\
                 ABDUCE NOT tot WHERE x.name = 'a'"
            ),
            Runs,
        ),
        (
            "DERIVE WHERE on the DERIVE rule's MATCH variables",
            FAN,
            "CREATE RULE d AS MATCH (x:N)-[:E]->(y:N) DERIVE (x)-[:L]->(y)\n\
             DERIVE d WHERE x.name = 'a'"
                .into(),
            Runs,
        ),
    ])
    .await
}

/// Every (slot, expression) pair is either refused by the compiler or runs:
/// no scope failure reaches the planner.
///
/// Each mutant fills one slot of a rule template with one candidate and the
/// other slots with neutral defaults. The candidates are a MATCH variable's
/// property, a referenced rule's value column, an ALONG name, a FOLD output,
/// `prev`, an unbound variable and a parameter, so each slot meets both names
/// it can see and names it cannot. A planner "No field named" is a scope error
/// the compiler should have reported; any other outcome (a result, a compile
/// error, a type error at run time) is allowed.
#[tokio::test]
async fn scope_check_no_scope_failure_reaches_the_planner() -> Result<()> {
    const CANDIDATES: [&str; 8] = ["e.w", "s", "c", "t", "prev.w", "zz.w", "$min", "y.name"];
    // (slot, template with `{}` for the slot, the other slots neutral)
    let fold_rule = |w: &str, f: &str, h: &str, r: &str, y: &str| {
        format!(
            "{TOT}CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) WHERE x IS tot, ({w}) IS NOT NULL \
             FOLD t = MSUM({f}) REQUIRE ({r}) IS NOT NULL WHERE ({h}) IS NOT NULL \
             YIELD KEY x, t, t + ({y}) AS u\nQUERY p RETURN t"
        )
    };
    let plain_rule = |w: &str, a: &str, b: &str, y: &str| {
        format!(
            "{TOT}CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) WHERE x IS tot, ({w}) IS NOT NULL \
             ALONG c = {a} BEST BY {b} ASC YIELD KEY x, KEY y, {y} AS v\nQUERY p RETURN v"
        )
    };
    let mut programs = Vec::new();
    for cand in CANDIDATES {
        programs.push((
            format!("fold WHERE {cand}"),
            fold_rule(cand, "e.w", "t", "t", "1"),
        ));
        programs.push((
            format!("fold argument {cand}"),
            fold_rule("e.w", cand, "t", "t", "1"),
        ));
        programs.push((
            format!("post-FOLD WHERE {cand}"),
            fold_rule("e.w", "e.w", cand, "t", "1"),
        ));
        programs.push((
            format!("REQUIRE {cand}"),
            fold_rule("e.w", "e.w", "t", cand, "1"),
        ));
        programs.push((
            format!("post-fold YIELD {cand}"),
            fold_rule("e.w", "e.w", "t", "t", cand),
        ));
        programs.push((format!("WHERE {cand}"), plain_rule(cand, "e.w", "c", "c")));
        programs.push((format!("ALONG {cand}"), plain_rule("e.w", cand, "c", "c")));
        programs.push((
            format!("BEST BY {cand}"),
            plain_rule("e.w", "e.w", cand, "c"),
        ));
        programs.push((format!("YIELD {cand}"), plain_rule("e.w", "e.w", "c", cand)));
    }
    let mut late = Vec::new();
    for (label, program) in &programs {
        if let Err(e) = run(FAN, program).await {
            let message = format!("{e:#}");
            if message.contains("No field named") {
                late.push(format!(
                    "{label}: {}",
                    message.chars().take(160).collect::<String>()
                ));
            }
        }
    }
    assert!(
        late.is_empty(),
        "{} of {} programs failed scope in the planner:\n{}",
        late.len(),
        programs.len(),
        late.join("\n")
    );
    Ok(())
}
