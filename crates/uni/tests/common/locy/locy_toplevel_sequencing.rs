// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The top-level commands of a Locy program run in sequence: a command reads
//! the state every earlier command in the same program left, exactly as the
//! commands of an `ASSUME` body do (`locy_assume_body_sequencing`).
//!
//! Before, every command read the facts the fixpoint derived before the
//! first command ran, so a `QUERY` (or a second `DERIVE`) after a `DERIVE`
//! or a Cypher write answered from the state before that write, while a
//! Cypher read after it saw the new edges. A Cypher write as a top-level
//! command failed outright with "Mutation context not set".
//!
//! The oracle for every case is the same commands run one by one, each as
//! its own `tx.locy` program (a Cypher command as `tx.execute` /
//! `tx.query`), in one transaction that is rolled back. That holds by the
//! transaction's read-your-writes, independently of how a single program
//! dispatches its commands. Every oracle is also checked against a
//! hard-coded answer, so no case passes by both sides being wrong the same
//! way.
//!
//! Every case runs through each Rust entry point (`session.locy`,
//! `session.locy_with`, the session profile, a session-prepared program,
//! `tx.locy`, `tx.locy_with`, the transaction profile) over the seed data in
//! L0 and after a flush.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_toplevel_sequencing)'

// Rust guideline compliant

use anyhow::{Result, bail};
use uni_common::Value;
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyResult};
use uni_locy::{DerivationNode, LocyStats, Modification};

type FactRow = std::collections::HashMap<String, Value>;

/// `A -> B -> C -> D` over `KNOWS`, optionally flushed to L1.
async fn seeded(flushed: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})\
                -[:KNOWS]->(c:Person {name: 'C'})-[:KNOWS]->(d:Person {name: 'D'})",
    )
    .await?;
    tx.commit().await?;
    if flushed {
        db.flush().await?;
    }
    Ok(db)
}

/// `link` derives `LINK` edges from `KNOWS`; `linked` (non-recursive) and
/// `reach` (recursive) read `LINK`; `link2` derives `LINK2` from `LINK`, and
/// `linked2` reads `LINK2`; `knows` reads only `KNOWS`.
const RULES: &str = "\
CREATE RULE link AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (a)-[:LINK]->(b)\n\
CREATE RULE linked AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b\n\
CREATE RULE reach AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b\n\
CREATE RULE reach AS MATCH (a:Person)-[:LINK]->(m:Person) WHERE m IS reach TO b YIELD KEY a, KEY b\n\
CREATE RULE link2 AS MATCH (a:Person)-[:LINK]->(b:Person) DERIVE (a)-[:LINK2]->(b)\n\
CREATE RULE linked2 AS MATCH (a:Person)-[:LINK2]->(b:Person) YIELD KEY a, KEY b\n\
CREATE RULE knows AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b\n";

const Q_LINKED: &str = "QUERY linked RETURN a.name AS a, b.name AS b";
const Q_REACH: &str = "QUERY reach RETURN a.name AS a, b.name AS b";
const Q_LINKED2: &str = "QUERY linked2 RETURN a.name AS a, b.name AS b";
const Q_KNOWS: &str = "QUERY knows RETURN a.name AS a, b.name AS b";
const W_AC: &str = "MATCH (a:Person {name: 'A'}), (c:Person {name: 'C'}) CREATE (a)-[:LINK]->(c)";
const R_LINKS: &str = "MATCH (:Person)-[r:LINK]->(:Person) RETURN count(r) AS n";

/// A value rendered without identities: a node by its `name`.
fn value(v: &Value) -> String {
    match v {
        Value::Node(n) => match n.properties.get("name") {
            Some(name) => format!("node({name:?})"),
            None => "node(?)".to_string(),
        },
        other => format!("{other:?}"),
    }
}

/// A row rendered as sorted `column=value` cells.
fn render(row: &FactRow) -> String {
    let mut cells: Vec<String> = row
        .iter()
        .map(|(k, v)| format!("{k}={}", value(v)))
        .collect();
    cells.sort();
    cells.join(" ")
}

/// An EXPLAIN tree rendered without identities, children sorted.
fn render_tree(node: &DerivationNode) -> String {
    let mut bindings: Vec<String> = node
        .bindings
        .iter()
        .map(|(k, v)| format!("{k}={}", value(v)))
        .collect();
    bindings.sort();
    let mut children: Vec<String> = node.children.iter().map(render_tree).collect();
    children.sort();
    format!(
        "{}[{}]{{{}}}",
        node.rule,
        bindings.join(","),
        children.join(";")
    )
}

/// An ABDUCE modification rendered by kind, edge type and validation.
fn render_modification(m: &Modification, validated: bool) -> String {
    match m {
        Modification::RemoveEdge { edge_type, .. } => format!("remove {edge_type} {validated}"),
        Modification::AddEdge { edge_type, .. } => format!("add {edge_type} {validated}"),
        Modification::ChangeProperty { property, .. } => format!("change {property} {validated}"),
    }
}

/// One command's result, rendered: its rows sorted. An ASSUME's rows are
/// its body results', each prefixed with the body command's index (`0:`).
fn command_rows(result: &CommandResult) -> Result<Vec<String>> {
    let mut rows = match result {
        CommandResult::Query(rows) | CommandResult::Cypher(rows) => {
            rows.iter().map(render).collect()
        }
        CommandResult::Assume { results, .. } => {
            let mut rows = Vec::new();
            for (i, r) in results.iter().enumerate() {
                rows.extend(command_rows(r)?.into_iter().map(|row| format!("{i}:{row}")));
            }
            rows
        }
        CommandResult::Derive { affected } => vec![format!("affected={affected}")],
        CommandResult::Explain(node) => vec![render_tree(node)],
        CommandResult::Abduce(a) => a
            .modifications
            .iter()
            .map(|m| render_modification(&m.modification, m.validated))
            .collect(),
        other => bail!("unexpected command result: {other:?}"),
    };
    rows.sort();
    Ok(rows)
}

fn rendered(result: &LocyResult) -> Result<Vec<Vec<String>>> {
    result.command_results.iter().map(command_rows).collect()
}

/// Whether a command is plain Cypher rather than a Locy command.
fn is_cypher(command: &str) -> bool {
    command.starts_with("MATCH") || command.starts_with("CREATE (")
}

/// Every Rust entry point that runs a Locy program.
#[derive(Clone, Copy, Debug)]
enum Entry {
    Session,
    SessionWith,
    SessionProfile,
    SessionPrepared,
    Tx,
    TxWith,
    TxProfile,
}

const ENTRIES: [Entry; 7] = [
    Entry::Session,
    Entry::SessionWith,
    Entry::SessionProfile,
    Entry::SessionPrepared,
    Entry::Tx,
    Entry::TxWith,
    Entry::TxProfile,
];

impl Entry {
    fn is_tx(self) -> bool {
        matches!(self, Entry::Tx | Entry::TxWith | Entry::TxProfile)
    }
}

/// What one program run left behind.
struct Run {
    commands: Vec<Vec<String>>,
    stats: LocyStats,
    /// `LINK` edges in the stored graph afterwards: committed for a
    /// transaction entry, applied from the `DerivedFactSet` for a session one.
    links_after: i64,
    /// `LINK` edges a session entry stored before anything was applied
    /// (always 0 for a transaction entry).
    session_wrote: i64,
}

async fn count_links(db: &Uni) -> Result<i64> {
    Ok(db.session().query(R_LINKS).await?.rows()[0].get::<i64>("n")?)
}

/// Runs `program` through `entry` on a fresh seeded database.
async fn run(entry: Entry, flushed: bool, program: &str) -> Result<Run> {
    let db = seeded(flushed).await?;
    let session = db.session();
    let mut session_wrote = 0;
    let result = if entry.is_tx() {
        let tx = session.tx().await?;
        let result = match entry {
            Entry::Tx => tx.locy(program).await?,
            Entry::TxWith => tx.locy_with(program).run().await?,
            Entry::TxProfile => tx.locy_with(program).profile().await?.0,
            _ => unreachable!(),
        };
        tx.commit().await?;
        result
    } else {
        let result = match entry {
            Entry::Session => session.locy(program).await?,
            Entry::SessionWith => session.locy_with(program).run().await?,
            Entry::SessionProfile => session.locy_with(program).profile().await?.0,
            Entry::SessionPrepared => session.prepare_locy(program).await?.execute(&[]).await?,
            _ => unreachable!(),
        };
        // A session program writes nothing itself.
        session_wrote = count_links(&db).await?;
        if let Some(derived) = result.derived_fact_set.clone() {
            let tx = session.tx().await?;
            tx.apply(derived).await?;
            tx.commit().await?;
        }
        result
    };
    Ok(Run {
        commands: rendered(&result)?,
        stats: result.stats.clone(),
        links_after: count_links(&db).await?,
        session_wrote,
    })
}

/// The oracle: each command as its own program in one transaction, rolled
/// back.
async fn oracle(flushed: bool, commands: &[&str]) -> Result<Vec<Vec<String>>> {
    let db = seeded(flushed).await?;
    let tx = db.session().tx().await?;
    let mut out = Vec::new();
    for command in commands {
        if is_cypher(command) {
            let rows = if command.contains("RETURN") {
                let result = tx.query(command).await?;
                let mut rows: Vec<String> = result
                    .rows()
                    .iter()
                    .map(|r| {
                        let row: FactRow = r
                            .columns()
                            .iter()
                            .cloned()
                            .zip(r.values().iter().cloned())
                            .collect();
                        render(&row)
                    })
                    .collect();
                rows.sort();
                rows
            } else {
                tx.execute(command).await?;
                Vec::new()
            };
            out.push(rows);
            continue;
        }
        let result = tx.locy(&format!("{RULES}{command}")).await?;
        out.extend(rendered(&result)?);
    }
    tx.rollback();
    Ok(out)
}

fn program(commands: &[&str]) -> String {
    format!("{RULES}{}", commands.join("\n"))
}

/// Requires the oracle to equal `expected`, then every entry point, on L0
/// and on flushed data, to equal the oracle. `links` is the number of `LINK`
/// edges stored afterwards (committed or applied). Returns each run's stats.
async fn check(commands: &[&str], expected: &[&[&str]], links: i64) -> Result<Vec<LocyStats>> {
    let want: Vec<Vec<String>> = expected
        .iter()
        .map(|rows| {
            let mut rows: Vec<String> = rows.iter().map(|s| s.to_string()).collect();
            rows.sort();
            rows
        })
        .collect();
    let mut stats = Vec::new();
    for flushed in [false, true] {
        let oracle = oracle(flushed, commands).await?;
        assert_eq!(oracle, want, "oracle for {commands:?} (flushed: {flushed})");
        for entry in ENTRIES {
            let got = run(entry, flushed, &program(commands)).await?;
            assert_eq!(
                got.commands, oracle,
                "{entry:?} (flushed: {flushed}) for {commands:?}"
            );
            assert_eq!(got.session_wrote, 0, "{entry:?} wrote to the graph");
            assert_eq!(
                got.links_after, links,
                "{entry:?} (flushed: {flushed}): LINK edges stored after {commands:?}"
            );
            stats.push(got.stats);
        }
    }
    Ok(stats)
}

const AB: &str = "a=String(\"A\") b=String(\"B\")";
const BC: &str = "a=String(\"B\") b=String(\"C\")";
const CD: &str = "a=String(\"C\") b=String(\"D\")";
const AC: &str = "a=String(\"A\") b=String(\"C\")";
const AD: &str = "a=String(\"A\") b=String(\"D\")";
const BD: &str = "a=String(\"B\") b=String(\"D\")";

fn affected(n: usize) -> String {
    format!("affected={n}")
}

#[tokio::test]
async fn derive_then_query_reads_the_derived_edges() -> Result<()> {
    check(
        &["DERIVE link", Q_LINKED],
        &[&[&affected(3)], &[AB, BC, CD]],
        3,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn query_derive_query_reads_before_then_after() -> Result<()> {
    check(
        &[Q_LINKED, "DERIVE link", Q_LINKED],
        &[&[], &[&affected(3)], &[AB, BC, CD]],
        3,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn second_derive_reads_the_first_derives_edges() -> Result<()> {
    check(
        &["DERIVE link", "DERIVE link2", Q_LINKED2],
        &[&[&affected(3)], &[&affected(3)], &[AB, BC, CD]],
        3,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn derive_with_nothing_to_derive_changes_nothing() -> Result<()> {
    // `link2` reads LINK, which does not exist yet.
    let stats = check(&["DERIVE link2", Q_LINKED2], &[&[&affected(0)], &[]], 0).await?;
    for s in stats {
        assert_eq!(s.sequenced_strata_evaluated, 0, "{s:?}");
    }
    Ok(())
}

#[tokio::test]
async fn cypher_write_then_query() -> Result<()> {
    // Failed with "Mutation context not set" before: a Cypher command ahead
    // of the program's first DERIVE ran inline, where nothing can write.
    check(&[W_AC, Q_LINKED], &[&[], &[AC]], 1).await?;
    Ok(())
}

#[tokio::test]
async fn derive_cypher_write_query() -> Result<()> {
    check(
        &["DERIVE link", W_AC, Q_LINKED],
        &[&[&affected(3)], &[], &[AB, AC, BC, CD]],
        4,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn derive_then_cypher_read() -> Result<()> {
    check(
        &["DERIVE link", R_LINKS],
        &[&[&affected(3)], &["n=Int(3)"]],
        3,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn derive_then_assume_body_reads_the_derived_edges() -> Result<()> {
    check(
        &[
            "DERIVE link",
            "ASSUME { CREATE (:Marker {name: 'm'}) } THEN { QUERY linked RETURN a.name AS a, b.name AS b }",
        ],
        &[
            &[&affected(3)],
            &[&format!("0:{AB}"), &format!("0:{BC}"), &format!("0:{CD}")],
        ],
        3,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn derive_then_abduce_sees_the_derived_edges() -> Result<()> {
    check(
        &["DERIVE link", "ABDUCE NOT linked WHERE a.name = 'A'"],
        &[&[&affected(3)], &["remove LINK true"]],
        3,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn derive_then_explain_rule_explains_the_new_facts() -> Result<()> {
    let tree = "linked[]{linked[a=node(String(\"A\")),b=node(String(\"B\"))]{}}";
    check(
        &["DERIVE link", "EXPLAIN RULE linked WHERE a.name = 'A'"],
        &[&[&affected(3)], &[tree]],
        3,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn derive_then_recursive_rule() -> Result<()> {
    check(
        &["DERIVE link", Q_REACH],
        &[&[&affected(3)], &[AB, AC, AD, BC, BD, CD]],
        3,
    )
    .await?;
    Ok(())
}

/// The six strata of [`RULES`], by what a `LINK` write does to them: `knows`
/// reads `KNOWS` and `linked2` reads `LINK2`, so those two are reused;
/// `linked`, `reach` and `link2` read `LINK` and are derived again, and so is
/// `link`, because a DERIVE rule's stratum is never skipped
/// (`uni_locy::incremental` treats its body as opaque).
#[tokio::test]
async fn a_stratum_the_write_cannot_affect_is_reused() -> Result<()> {
    let stats = check(
        &["DERIVE link", Q_KNOWS],
        &[&[&affected(3)], &[AB, BC, CD]],
        3,
    )
    .await?;
    for s in stats {
        assert_eq!(s.sequenced_strata_reused, 2, "{s:?}");
        assert_eq!(s.sequenced_strata_evaluated, 4, "{s:?}");
    }
    Ok(())
}

#[tokio::test]
async fn consecutive_writes_fold_into_one_rederivation() -> Result<()> {
    let stats = check(
        &["DERIVE link", W_AC, Q_LINKED],
        &[&[&affected(3)], &[], &[AB, AC, BC, CD]],
        4,
    )
    .await?;
    for s in stats {
        assert_eq!(
            s.sequenced_strata_reused + s.sequenced_strata_evaluated,
            6,
            "one re-derivation of six strata: {s:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_program_without_writes_derives_nothing_again() -> Result<()> {
    let stats = check(
        &[
            Q_LINKED,
            Q_KNOWS,
            "EXPLAIN RULE knows WHERE a.name = 'A'",
            R_LINKS,
        ],
        &[
            &[],
            &[AB, BC, CD],
            &["knows[]{knows[a=node(String(\"A\")),b=node(String(\"B\"))]{}}"],
            &["n=Int(0)"],
        ],
        0,
    )
    .await?;
    for s in stats {
        assert_eq!(
            s.sequenced_strata_evaluated + s.sequenced_strata_reused,
            0,
            "{s:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_trailing_write_derives_nothing_again() -> Result<()> {
    let stats = check(&[Q_LINKED, "DERIVE link"], &[&[], &[&affected(3)]], 3).await?;
    for s in stats {
        assert_eq!(
            s.sequenced_strata_evaluated + s.sequenced_strata_reused,
            0,
            "{s:?}"
        );
    }
    Ok(())
}

/// `LocyResult::derived` holds the facts the program derived before its
/// first command, the state its first command reads.
#[tokio::test]
async fn derived_holds_the_facts_before_the_first_command() -> Result<()> {
    let db = seeded(false).await?;
    let tx = db.session().tx().await?;
    let result = tx.locy(&program(&["DERIVE link", Q_LINKED])).await?;
    assert_eq!(rendered(&result)?[1], vec![AB, BC, CD]);
    assert_eq!(result.derived.get("linked").map(Vec::len), Some(0));
    assert_eq!(result.derived.get("knows").map(Vec::len), Some(3));
    Ok(())
}

/// A session program's writes come back as a `DerivedFactSet` that
/// `tx.apply` replays, and a DERIVE finds the nodes it connects by id. A node
/// an earlier command of the same program created gets a new id in the
/// replay, so a DERIVE connecting it is refused rather than silently lost.
/// In a transaction the node is real and the program runs.
#[tokio::test]
async fn session_derive_over_a_node_the_program_created_is_refused() -> Result<()> {
    const PROGRAM: &str = "\
CREATE RULE tag AS MATCH (p:Person) WHERE p.name = 'A' DERIVE (NEW t:Tag {name: 'x'})<-[:TAGGED]-(p)\n\
CREATE RULE mark AS MATCH (t:Tag), (d:Person) WHERE d.name = 'D' DERIVE (d)-[:MARKS]->(t)\n\
DERIVE tag\n\
DERIVE mark\n\
MATCH (:Person)-[r:MARKS]->(:Tag) RETURN count(r) AS n";
    let db = seeded(false).await?;
    let tx = db.session().tx().await?;
    let result = tx.locy(PROGRAM).await?;
    assert_eq!(
        rendered(&result)?,
        vec![
            vec![affected(1)],
            vec![affected(1)],
            vec!["n=Int(1)".to_string()]
        ]
    );
    tx.rollback();
    let err = match db.session().locy(PROGRAM).await {
        Ok(r) => bail!("expected a refusal, got {:?}", r.command_results),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("an earlier command of this session-level program created"),
        "{err}"
    );
    Ok(())
}

/// VALIDATE after a write measures the state the write left.
#[tokio::test]
async fn validate_after_a_write_measures_the_written_state() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {label: true, prob: 0.9}), (:Item {label: false, prob: 0.1})")
        .await?;
    tx.commit().await?;
    let program = "CREATE RULE risk AS MATCH (s:Item) YIELD KEY s, s.prob AS risk PROB\n\
        CREATE (:Item {label: true, prob: 0.8})\n\
        VALIDATE risk ON MATCH (s:Item) TARGET s.label METRICS accuracy";
    for tx_entry in [false, true] {
        let result = if tx_entry {
            let tx = db.session().tx().await?;
            let r = tx.locy(program).await?;
            tx.rollback();
            r
        } else {
            db.session().locy(program).await?
        };
        let Some(CommandResult::Validate(v)) = result.command_results.last() else {
            bail!("no VALIDATE result: {:?}", result.command_results);
        };
        assert_eq!(v.n_samples, 3, "tx: {tx_entry}");
    }
    Ok(())
}
