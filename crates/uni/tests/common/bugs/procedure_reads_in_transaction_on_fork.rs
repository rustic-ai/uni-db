// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A procedure called inside a transaction on a fork reads the fork's view.
//!
//! The fork's transaction sees, as plain Cypher does: the primary data the
//! fork was created from, what the fork committed since, and its own
//! uncommitted writes — never what the primary committed after the fork was
//! created. Every procedure kind that reads the graph is checked against the
//! transaction's own `MATCH` count, with the primary and fork data unflushed
//! and flushed. `uni.graph.project` follows its own rule (it refuses a
//! transaction with writes, and otherwise projects that transaction's
//! snapshot), and `uni.schema.labels` lists the fork's merged schema.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(procedure_reads_in_transaction_on_fork)'

// Rust guideline compliant

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use uni_db::{DataType, IndexType, QueryResult, Session, Uni};
use uni_plugin::traits::connector::Principal;
use uni_plugin::{Capability, CapabilitySet};

/// The procedure kinds that read the graph, each counting `:Node` rows.
#[derive(Clone, Copy, Debug)]
enum Probe {
    /// A declared Cypher-bodied procedure in `READ` mode.
    DeclaredRead,
    /// A declared Cypher-bodied procedure in `WRITE` mode whose body reads.
    DeclaredWrite,
    /// A built-in algorithm over a native projection.
    Wcc,
    /// A built-in algorithm that walks a `GraphView` from `AlgorithmHost::project`.
    Reachability,
    /// Vector search.
    Vector,
    /// Full-text search.
    Fts,
}

const PROBES: [Probe; 6] = [
    Probe::DeclaredRead,
    Probe::DeclaredWrite,
    Probe::Wcc,
    Probe::Reachability,
    Probe::Vector,
    Probe::Fts,
];

impl Probe {
    /// The query whose row count (or `c` for a declared procedure) is the
    /// number of `:Node` rows the procedure saw. `root` is `n0`'s vid.
    fn cypher(self, root: i64) -> String {
        match self {
            Self::DeclaredRead => "CALL t.count_read() YIELD row_json RETURN row_json".to_owned(),
            Self::DeclaredWrite => "CALL t.count_write() YIELD row_json RETURN row_json".to_owned(),
            Self::Wcc => "CALL uni.algo.wcc({nodeLabels: ['Node'], edgeTypes: ['LINK']}, {}) \
                          YIELD nodeId RETURN nodeId"
                .to_owned(),
            Self::Reachability => format!(
                "CALL uni.algo.reachability({root}, {{nodeLabels: ['Node'], edgeTypes: ['LINK']}}) \
                 YIELD nodeId RETURN nodeId"
            ),
            Self::Vector => "CALL uni.vector.query('Node', 'embedding', [1.0, 0.0, 0.0], 100) \
                             YIELD node RETURN node.name AS name"
                .to_owned(),
            Self::Fts => "CALL uni.fts.query('Node', 'body', 'gamma', 100, null, null, {}) \
                          YIELD node RETURN node.name AS name"
                .to_owned(),
        }
    }

    /// The `:Node` count a result of [`Self::cypher`] reports.
    fn count(self, res: &QueryResult) -> Result<i64> {
        match self {
            Self::DeclaredRead | Self::DeclaredWrite => {
                let json: String = res.rows().first().context("no row")?.get("row_json")?;
                let v: serde_json::Value = serde_json::from_str(&json)?;
                v["c"]
                    .as_i64()
                    .with_context(|| format!("no count in {json}"))
            }
            _ => Ok(i64::try_from(res.len())?),
        }
    }
}

/// The plain-Cypher oracle: `:Node` rows the reader sees.
const ORACLE: &str = "MATCH (n:Node) RETURN count(n) AS c";

/// A `:Node` pattern every probe matches.
fn node(name: &str) -> String {
    format!("n_{name}:Node {{name: '{name}', body: 'alpha gamma', embedding: [1.0, 0.0, 0.0]}}")
}

/// A principal allowed to call the declared `WRITE` procedure.
fn admin() -> Arc<Principal> {
    let mut caps = CapabilitySet::new();
    caps.insert(Capability::ProcedureWrites);
    Arc::new(Principal {
        id: "admin".to_owned(),
        groups: vec!["admin".to_owned()],
        capabilities: caps,
    })
}

/// The state every case starts from.
///
/// * Primary, before the fork: `n0 -> n1`, the two declared counting
///   procedures.
/// * Fork `f`, committed: `n1 -> n2`, and a fork-local label `ForkLocal`.
/// * Primary, after the fork: `n1 -> n3a, n3b, n3c` and a label
///   `PrimaryLater` — none of which the fork may see.
///
/// So the fork has 3 committed `:Node`s and the primary 5.
struct Fixture {
    _db: Uni,
    primary: Session,
    fork: Session,
    root: i64,
}

async fn fixture(flush_base: bool, flush_fork: bool) -> Result<Fixture> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .property("body", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 3 })
        .done()
        .edge_type("LINK", &["Node"], &["Node"])
        .property_nullable("w", DataType::Float)
        .done()
        .apply()
        .await?;
    db.schema()
        .label("Node")
        .index("body", IndexType::FullText)
        .apply()
        .await?;
    let primary = db.session().with_principal(admin());
    let tx = primary.tx().await?;
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.count_read', \
         'MATCH (n:Node) RETURN count(n) AS c', 'READ', '[]', '[]')",
    )
    .await?;
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.count_write', \
         'MATCH (n:Node) RETURN count(n) AS c', 'WRITE', '[]', '[]')",
    )
    .await?;
    tx.execute(&format!(
        "CREATE ({}) -[:LINK {{w: 1.0}}]-> ({})",
        node("n0"),
        node("n1")
    ))
    .await?;
    tx.commit().await?;
    if flush_base {
        db.flush().await?;
    }

    let fork = primary.fork("f").await?.with_principal(admin());
    fork.fork_schema().label("ForkLocal").apply().await?;
    let ftx = fork.tx().await?;
    ftx.execute(&format!(
        "MATCH (n1:Node {{name: 'n1'}}) CREATE (n1)-[:LINK {{w: 1.0}}]->({})",
        node("n2")
    ))
    .await?;
    ftx.commit().await?;
    if flush_fork {
        fork.flush().await?;
    }

    db.schema().label("PrimaryLater").apply().await?;
    let ptx = primary.tx().await?;
    ptx.execute(&format!(
        "MATCH (n1:Node {{name: 'n1'}}) \
         CREATE (n1)-[:LINK {{w: 1.0}}]->({}), (n1)-[:LINK {{w: 1.0}}]->({}), \
         (n1)-[:LINK {{w: 1.0}}]->({})",
        node("n3a"),
        node("n3b"),
        node("n3c")
    ))
    .await?;
    ptx.commit().await?;
    if flush_base {
        db.flush().await?;
    }

    let root = fork
        .query("MATCH (n:Node {name: 'n0'}) RETURN id(n) AS v")
        .await?
        .rows()[0]
        .get::<i64>("v")?;
    Ok(Fixture {
        _db: db,
        primary,
        fork,
        root,
    })
}

/// The flush combinations every case runs under: (primary data, fork data).
const FLUSHES: [(bool, bool); 4] = [(false, false), (true, false), (false, true), (true, true)];

/// Requires the multi-thread runtime: a declared procedure bridges
/// sync→async with `block_in_place`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_on_fork_see_the_forks_data() -> Result<()> {
    let mut failures = Vec::new();
    for (flush_base, flush_fork) in FLUSHES {
        for probe in PROBES {
            let fx = fixture(flush_base, flush_fork).await?;
            let what = format!("{probe:?} flush_base={flush_base} flush_fork={flush_fork}");

            // Committed fork data, no writes of the transaction's own.
            let tx = fx.fork.tx().await?;
            let oracle = tx.query(ORACLE).await?.rows()[0].get::<i64>("c")?;
            assert_eq!(oracle, 3, "fork oracle, {what}");
            match tx.query(&probe.cypher(fx.root)).await {
                Ok(res) => {
                    let seen = probe.count(&res)?;
                    if seen != oracle {
                        failures.push(format!("{what} committed: saw {seen}, tx sees {oracle}"));
                    }
                }
                Err(e) => failures.push(format!("{what} committed: error {e}")),
            }

            // Plus an uncommitted node of the fork transaction, reachable from n0.
            tx.execute(&format!(
                "MATCH (n2:Node {{name: 'n2'}}) CREATE (n2)-[:LINK {{w: 1.0}}]->({})",
                node("n4")
            ))
            .await?;
            let oracle = tx.query(ORACLE).await?.rows()[0].get::<i64>("c")?;
            assert_eq!(oracle, 4, "fork oracle with own write, {what}");
            match tx.query(&probe.cypher(fx.root)).await {
                Ok(res) => {
                    let seen = probe.count(&res)?;
                    if seen != oracle {
                        failures.push(format!("{what} own write: saw {seen}, tx sees {oracle}"));
                    }
                }
                Err(e) => failures.push(format!("{what} own write: error {e}")),
            }
            tx.rollback();

            // The primary is untouched by the fork, inside a transaction too.
            let ptx = fx.primary.tx().await?;
            let primary_root = ptx
                .query("MATCH (n:Node {name: 'n0'}) RETURN id(n) AS v")
                .await?
                .rows()[0]
                .get::<i64>("v")?;
            let oracle = ptx.query(ORACLE).await?.rows()[0].get::<i64>("c")?;
            assert_eq!(oracle, 5, "primary oracle, {what}");
            match ptx.query(&probe.cypher(primary_root)).await {
                Ok(res) => {
                    let seen = probe.count(&res)?;
                    if seen != oracle {
                        failures.push(format!("{what} primary: saw {seen}, tx sees {oracle}"));
                    }
                }
                Err(e) => failures.push(format!("{what} primary: error {e}")),
            }
            ptx.rollback();
        }
    }
    assert!(failures.is_empty(), "fork procedure reads: {failures:#?}");
    Ok(())
}

/// `uni.graph.project` inside a fork transaction: refused once the
/// transaction has written (the projection catalog is not transactional),
/// and otherwise a projection of the fork's committed data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_on_fork_project_the_forks_graph() -> Result<()> {
    let mut failures = Vec::new();
    for (flush_base, flush_fork) in FLUSHES {
        let fx = fixture(flush_base, flush_fork).await?;
        let what = format!("flush_base={flush_base} flush_fork={flush_fork}");
        let project = |name: &str| {
            format!(
                "CALL uni.graph.project('{name}', {{nodeLabels: ['Node'], edgeTypes: ['LINK']}}, {{}}) \
                 YIELD node_count, edge_count RETURN node_count, edge_count"
            )
        };

        let tx = fx.fork.tx().await?;
        match tx.query(&project("pf")).await {
            Ok(res) => {
                let row = &res.rows()[0];
                let counts = (row.get::<i64>("node_count")?, row.get::<i64>("edge_count")?);
                if counts != (3, 2) {
                    failures.push(format!("{what} fork projection: {counts:?}, want (3, 2)"));
                }
            }
            Err(e) => failures.push(format!("{what} fork projection: error {e}")),
        }
        tx.execute(&format!(
            "MATCH (n2:Node {{name: 'n2'}}) CREATE (n2)-[:LINK]->({})",
            node("n4")
        ))
        .await?;
        if tx.query(&project("pf_written")).await.is_ok() {
            failures.push(format!("{what}: projection after a write was not refused"));
        }
        tx.rollback();

        let ptx = fx.primary.tx().await?;
        match ptx.query(&project("pp")).await {
            Ok(res) => {
                let row = &res.rows()[0];
                let counts = (row.get::<i64>("node_count")?, row.get::<i64>("edge_count")?);
                if counts != (5, 4) {
                    failures.push(format!(
                        "{what} primary projection: {counts:?}, want (5, 4)"
                    ));
                }
            }
            Err(e) => failures.push(format!("{what} primary projection: error {e}")),
        }
        ptx.rollback();
    }
    assert!(failures.is_empty(), "fork graph.project: {failures:#?}");
    Ok(())
}

/// `uni.schema.labels` inside a fork transaction lists the fork's merged
/// schema — the fork-local label, not a label the primary added after the
/// fork — and counts the `:Node`s the transaction sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_on_fork_list_the_forks_schema() -> Result<()> {
    async fn labels(tx: &uni_db::Transaction) -> Result<(BTreeSet<String>, i64)> {
        let res = tx
            .query("CALL uni.schema.labels() YIELD label, nodeCount RETURN label, nodeCount")
            .await?;
        let mut names = BTreeSet::new();
        let mut nodes = -1;
        for row in res.rows() {
            let label: String = row.get("label")?;
            if label == "Node" {
                nodes = row.get::<i64>("nodeCount")?;
            }
            names.insert(label);
        }
        Ok((names, nodes))
    }

    let mut failures = Vec::new();
    for (flush_base, flush_fork) in FLUSHES {
        let fx = fixture(flush_base, flush_fork).await?;
        let what = format!("flush_base={flush_base} flush_fork={flush_fork}");

        let tx = fx.fork.tx().await?;
        tx.execute(&format!(
            "MATCH (n2:Node {{name: 'n2'}}) CREATE (n2)-[:LINK]->({})",
            node("n4")
        ))
        .await?;
        let oracle = tx.query(ORACLE).await?.rows()[0].get::<i64>("c")?;
        let (names, nodes) = labels(&tx).await?;
        tx.rollback();
        if !names.contains("ForkLocal") || names.contains("PrimaryLater") || !names.contains("Node")
        {
            failures.push(format!("{what} fork labels: {names:?}"));
        }
        if nodes != oracle {
            failures.push(format!("{what} fork nodeCount: {nodes}, tx sees {oracle}"));
        }

        let ptx = fx.primary.tx().await?;
        let oracle = ptx.query(ORACLE).await?.rows()[0].get::<i64>("c")?;
        let (names, nodes) = labels(&ptx).await?;
        ptx.rollback();
        if names.contains("ForkLocal") || !names.contains("PrimaryLater") {
            failures.push(format!("{what} primary labels: {names:?}"));
        }
        if nodes != oracle {
            failures.push(format!(
                "{what} primary nodeCount: {nodes}, tx sees {oracle}"
            ));
        }
    }
    assert!(failures.is_empty(), "fork schema listing: {failures:#?}");
    Ok(())
}
