//! W4: Locy programs through the DQP levers.
//!
//! The Cypher levers compare one query down two execution paths. These do the
//! same for Locy: each program runs on both sides of a lever — primary and a
//! pristine fork, live and a pinned snapshot, before and after a flush — and
//! every derived relation must be the same bag on both. The programs cover each
//! rule family: a plain rule, FOLD, recursion with stratified negation, a
//! variable-length body, per-path ALONG under a FOLD, ALONG with BEST BY over
//! cycles, and a recursive FOLD reading its children's folded values.
//!
//! **Activation.** Each side's `LocyResult::metrics()` carries the evaluation's
//! scan counters, so a lever is held to the same witness its Cypher form uses,
//! for *every* program: the fork side scanned a branch, the pinned side read a
//! snapshot, the unflushed side read L0 and the flushed side storage alone.
//!
//! Not applicable: the plan-cache lever (Locy evaluation has no plan cache, so
//! the witness could never move) and the delete lever (its law, "a deletion
//! only removes rows", fails for a program with `IS NOT`).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(/metamorphic::dqp::locy_levers::/)'

// Rust guideline compliant

use std::collections::BTreeMap;

use uni_db::{Session, Uni, Value};

use super::lever::Witness;
use super::topo::{Layout, build_topo};

/// The programs, each a set of rules over the topology fixture.
const PROGRAMS: &[(&str, &str)] = &[
    (
        "rule",
        "CREATE RULE r AS MATCH (a:Person)-[:KNOWS]->(b) YIELD KEY a, KEY b",
    ),
    (
        "fold",
        "CREATE RULE f AS MATCH (a:Person)-[e:KNOWS]->(b) \
         FOLD n = COUNT(*), s = SUM(e.w), lo = MIN(e.w), hi = MAX(e.w) YIELD KEY a, n, s, lo, hi",
    ),
    (
        "reach_negation",
        "CREATE RULE reach AS MATCH (a)-[:KNOWS]->(b) YIELD KEY a, KEY b \
         CREATE RULE reach AS MATCH (a)-[:KNOWS]->(m) WHERE m IS reach TO b YIELD KEY a, KEY b \
         CREATE RULE un AS MATCH (a:Person), (b:Robot) WHERE a IS NOT reach TO b YIELD KEY a, KEY b",
    ),
    (
        "variable_length",
        "CREATE RULE v AS MATCH (a:Person)-[:KNOWS*1..2]->(b:Robot) YIELD KEY a, KEY b",
    ),
    (
        "along_fold",
        "CREATE RULE cost AS MATCH (a)-[e:KNOWS]->(b) WHERE a.id < b.id, e.w > 0 \
         ALONG q = e.w YIELD KEY a, KEY b, q \
         CREATE RULE cost AS MATCH (a)-[e:KNOWS]->(m) WHERE a.id < m.id, e.w > 0, m IS cost TO b \
         ALONG q = prev.q + e.w YIELD KEY a, KEY b, q \
         CREATE RULE tot AS MATCH (a) WHERE a IS cost TO b FOLD n = COUNT(*), s = SUM(q) \
         YIELD KEY a, n, s",
    ),
    (
        "best_by",
        "CREATE RULE short AS MATCH (a)-[e:KNOWS]->(b) WHERE e.w > 0 \
         ALONG d = e.w BEST BY d ASC YIELD KEY a, KEY b, d \
         CREATE RULE short AS MATCH (a)-[e:KNOWS]->(m) WHERE e.w > 0, m IS short TO b \
         ALONG d = prev.d + e.w BEST BY d ASC YIELD KEY a, KEY b, d",
    ),
    (
        "recursive_fold",
        "CREATE RULE roll AS MATCH (a:Person) YIELD KEY a, 1 AS s \
         CREATE RULE roll AS MATCH (a:Person)-[e:KNOWS]->(c:Person) \
         WHERE a.id < c.id, e.w > 0, c IS roll FOLD s = MSUM(s + e.w) YIELD KEY a, s",
    ),
];

/// Every derived relation of one evaluation, as a bag of rendered rows (a node
/// by its `id`), and the evaluation's counters.
struct LocyObserved {
    relations: BTreeMap<String, Vec<String>>,
    witness: Witness,
}

fn render(value: &Value) -> String {
    match value {
        Value::Node(node) => format!("node {:?}", node.properties.get("id")),
        other => format!("{other:?}"),
    }
}

async fn observe_locy(session: &Session, program: &str) -> anyhow::Result<LocyObserved> {
    let result = session
        .locy(program)
        .await
        .map_err(|e| anyhow::anyhow!("{e}\n  program: {program}"))?;
    let mut relations = BTreeMap::new();
    for (name, facts) in result.derived_rules().await.unwrap() {
        let mut rows: Vec<String> = facts
            .iter()
            .map(|fact| {
                let mut columns: Vec<_> = fact.iter().collect();
                columns.sort_by(|a, b| a.0.cmp(b.0));
                columns
                    .into_iter()
                    .map(|(k, v)| format!("{k}={}", render(v)))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        rows.sort();
        relations.insert(name.clone(), rows);
    }
    let m = result.metrics();
    Ok(LocyObserved {
        relations,
        witness: Witness {
            l0_reads: m.l0_reads,
            storage_reads: m.storage_reads,
            rows_scanned: m.rows_scanned,
            branch_scans: m.branch_scans,
            snapshot_reads: m.snapshot_reads,
            plan_cache_hits: 0,
            index_scans: m.index_scans,
            lance_iops: m.lance_iops,
        },
    })
}

/// Compares the two sides of a lever for one program: same bags, a non-empty
/// result, and the lever's witness.
fn compare(
    lever: &str,
    name: &str,
    a: &LocyObserved,
    b: &LocyObserved,
    activated: impl Fn(&Witness, &Witness) -> bool,
) {
    assert!(
        a.relations.values().any(|rows| !rows.is_empty()),
        "[{lever}] {name}: derived nothing; the comparison would be vacuous"
    );
    assert_eq!(
        a.relations, b.relations,
        "[{lever}] {name}: the two sides differ"
    );
    assert!(
        activated(&a.witness, &b.witness),
        "[{lever}] {name}: not activated (a {:?}, b {:?})",
        a.witness,
        b.witness
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn locy_through_the_fork_lever() -> anyhow::Result<()> {
    let db = build_topo(Layout::Flushed, None).await?;
    let primary = db.session();
    let fork = db.session().fork("locy_lever_fork").await?;
    for (name, program) in PROGRAMS {
        let a = observe_locy(&primary, program).await?;
        let b = observe_locy(&fork, program).await?;
        compare("fork", name, &a, &b, |a, b| {
            b.branch_scans > 0 && a.branch_scans == 0
        });
    }
    Ok(())
}

/// Live against a pinned snapshot. Writes after the snapshot make the check
/// behavioural as well as counted: the pinned side must still give what the
/// live side gave before them, while the live side moves.
#[tokio::test(flavor = "multi_thread")]
async fn locy_through_the_pinned_lever() -> anyhow::Result<()> {
    let db = build_topo(Layout::HalfUnflushed, None).await?;
    let live = db.session();
    let mut before = Vec::new();
    for (_, program) in PROGRAMS {
        before.push(observe_locy(&live, program).await?);
    }
    let snapshot = db.create_snapshot("locy_lever_pin").await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Person {id: 0}), (b:Robot {id: 100}) CREATE (a)-[:KNOWS {w: 1}]->(b)")
        .await?;
    tx.execute("MATCH (a:Person {id: 1}), (b:Person {id: 47}) CREATE (a)-[:KNOWS {w: 1}]->(b)")
        .await?;
    tx.commit().await?;
    let mut pinned = db.session();
    pinned.pin_to_version(&snapshot).await?;
    let mut live_moved = 0;
    for ((name, program), a) in PROGRAMS.iter().zip(&before) {
        let b = observe_locy(&pinned, program).await?;
        compare("pinned", name, a, &b, |a, b| {
            b.snapshot_reads > 0 && a.snapshot_reads == 0
        });
        if observe_locy(&live, program).await?.relations != a.relations {
            live_moved += 1;
        }
    }
    assert!(
        live_moved * 2 >= PROGRAMS.len(),
        "the writes after the snapshot moved only {live_moved} live results"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn locy_through_the_flush_lever() -> anyhow::Result<()> {
    for (name, program) in PROGRAMS {
        let db: Uni = build_topo(Layout::HalfUnflushed, None).await?;
        let a = observe_locy(&db.session(), program).await?;
        db.flush().await?;
        let b = observe_locy(&db.session(), program).await?;
        compare("flush", name, &a, &b, |a, b| {
            a.l0_reads > 0 && b.l0_reads == 0 && b.storage_reads > a.storage_reads
        });
    }
    Ok(())
}
