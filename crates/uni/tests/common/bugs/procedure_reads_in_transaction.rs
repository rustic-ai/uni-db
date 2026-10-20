// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A procedure called inside a transaction reads the transaction's view.
//!
//! Every procedure kind that reads the graph is checked against the
//! transaction's own Cypher reads, which are the oracle:
//!
//! * read-your-writes: rows the transaction wrote are visible to it;
//! * isolation: a commit by another session after the transaction began is
//!   not (reads are snapshot-isolated, `UniConfig::ssi_enabled` default-on);
//! * rollback: nothing the transaction wrote is visible afterwards.
//!
//! Each case runs with the base data unflushed and flushed, and with the
//! concurrent commit flushed mid-transaction.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(procedure_reads_in_transaction)'

// Rust guideline compliant

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

/// A database with two committed, linked `:Node`s (`n0 -> n1`), the two
/// declared counting procedures, and a session allowed to call them.
/// Returns `n0`'s vid.
async fn fixture(flush: bool) -> Result<(Uni, Session, i64)> {
    let db = Uni::in_memory().build().await?;
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
    let mut caps = CapabilitySet::new();
    caps.insert(Capability::ProcedureWrites);
    let admin = Arc::new(Principal {
        id: "admin".to_owned(),
        groups: vec!["admin".to_owned()],
        capabilities: caps,
    });
    let session = db.session().with_principal(admin);
    let tx = session.tx().await?;
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
    if flush {
        db.flush().await?;
    }
    let root = session
        .query("MATCH (n:Node {name: 'n0'}) RETURN id(n) AS v")
        .await?
        .rows()[0]
        .get::<i64>("v")?;
    Ok((db, session, root))
}

/// A `:Node` pattern every probe matches.
fn node(name: &str) -> String {
    format!("n_{name}:Node {{name: '{name}', body: 'alpha gamma', embedding: [1.0, 0.0, 0.0]}}")
}

/// What one probe saw inside the transaction, beside the Cypher oracle.
async fn in_tx(tx: &uni_db::Transaction, probe: Probe, root: i64) -> Result<(i64, i64)> {
    let seen = probe.count(&tx.query(&probe.cypher(root)).await?)?;
    let oracle = tx.query(ORACLE).await?.rows()[0].get::<i64>("c")?;
    Ok((seen, oracle))
}

/// Requires the multi-thread runtime: a declared procedure bridges
/// sync→async with `block_in_place`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_see_the_transactions_writes() -> Result<()> {
    let mut failures = Vec::new();
    for flush in [false, true] {
        for probe in PROBES {
            let (_db, session, root) = fixture(flush).await?;
            let tx = session.tx().await?;
            // A new node linked from n1, so the walk from n0 reaches it.
            tx.execute(&format!(
                "MATCH (n1:Node {{name: 'n1'}}) CREATE (n1)-[:LINK]->({})",
                node("n2")
            ))
            .await?;
            let (seen, oracle) = in_tx(&tx, probe, root).await?;
            assert_eq!(oracle, 3, "oracle, flush={flush}");
            if seen != oracle {
                failures.push(format!(
                    "{probe:?} flush={flush}: saw {seen}, tx sees {oracle}"
                ));
            }
            tx.rollback();
        }
    }
    assert!(failures.is_empty(), "read-your-writes: {failures:#?}");
    Ok(())
}

/// A committed node the transaction deletes is gone for its procedures too,
/// and so is one an earlier transaction deleted (`committed`), whether the
/// node itself had been flushed or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_see_the_transactions_deletes() -> Result<()> {
    let mut failures = Vec::new();
    for committed in [false, true] {
        for flush in [false, true] {
            for probe in PROBES {
                let (_db, session, root) = fixture(flush).await?;
                let mut tx = session.tx().await?;
                tx.execute("MATCH (n:Node {name: 'n1'}) DETACH DELETE n")
                    .await?;
                if committed {
                    tx.commit().await?;
                    tx = session.tx().await?;
                }
                let (seen, oracle) = in_tx(&tx, probe, root).await?;
                assert_eq!(oracle, 1, "oracle, flush={flush} committed={committed}");
                if seen != oracle {
                    failures.push(format!(
                        "{probe:?} flush={flush} committed={committed}: \
                         saw {seen}, tx sees {oracle}"
                    ));
                }
                tx.rollback();
            }
        }
    }
    assert!(failures.is_empty(), "read-your-deletes: {failures:#?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_are_isolated_from_later_commits() -> Result<()> {
    let mut failures = Vec::new();
    for (flush_base, flush_other) in [(false, false), (true, false), (true, true)] {
        for probe in PROBES {
            let (db, session, root) = fixture(flush_base).await?;
            let tx = session.tx().await?;
            // Another session commits a node reachable from n0 after `tx` began.
            let other = db.session().tx().await?;
            other
                .execute(&format!(
                    "MATCH (n1:Node {{name: 'n1'}}) CREATE (n1)-[:LINK]->({})",
                    node("n3")
                ))
                .await?;
            other.commit().await?;
            if flush_other {
                db.flush().await?;
            }
            let (seen, oracle) = in_tx(&tx, probe, root).await?;
            assert_eq!(
                oracle, 2,
                "oracle, flush_base={flush_base} flush_other={flush_other}"
            );
            if seen != oracle {
                failures.push(format!(
                    "{probe:?} flush_base={flush_base} flush_other={flush_other}: \
                     saw {seen}, tx sees {oracle}"
                ));
            }
            tx.rollback();
        }
    }
    assert!(failures.is_empty(), "isolation: {failures:#?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_see_nothing_after_rollback() -> Result<()> {
    let mut failures = Vec::new();
    for flush in [false, true] {
        for probe in PROBES {
            let (db, session, root) = fixture(flush).await?;
            let tx = session.tx().await?;
            tx.execute(&format!(
                "MATCH (n1:Node {{name: 'n1'}}) CREATE (n1)-[:LINK]->({})",
                node("n2")
            ))
            .await?;
            // Call the procedure inside, so any state it caches is exercised.
            tx.query(&probe.cypher(root)).await?;
            tx.rollback();
            if flush {
                db.flush().await?;
            }
            // A WRITE procedure needs a transaction even to read; the next
            // transaction below covers it.
            let seen = match probe {
                Probe::DeclaredWrite => 2,
                _ => probe.count(&session.query(&probe.cypher(root)).await?)?,
            };
            let next = session.tx().await?;
            let (in_next, oracle) = in_tx(&next, probe, root).await?;
            next.rollback();
            if (seen, in_next) != (2, 2) || oracle != 2 {
                failures.push(format!(
                    "{probe:?} flush={flush}: session saw {seen}, next tx {in_next} (oracle {oracle})"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "rollback: {failures:#?}");
    Ok(())
}

/// An algorithm projection reads the transaction's own edge-property values,
/// not the committed ones: the transaction's buffer is the newest layer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_see_the_transactions_updates() -> Result<()> {
    let mut failures = Vec::new();
    for flush in [false, true] {
        let (_db, session, _root) = fixture(flush).await?;
        let tx = session.tx().await?;
        tx.execute("MATCH ()-[r:LINK]->() SET r.w = 5.0").await?;
        let oracle = tx
            .query("MATCH ()-[r:LINK]->() RETURN sum(r.w) AS w")
            .await?
            .rows()[0]
            .get::<f64>("w")?;
        let seen = tx
            .query(
                "CALL uni.algo.mst(['Node'], ['LINK'], 'w') YIELD weight \
                 RETURN sum(weight) AS w",
            )
            .await?
            .rows()[0]
            .get::<f64>("w")?;
        assert_eq!(oracle, 5.0, "oracle, flush={flush}");
        if seen != oracle {
            failures.push(format!(
                "mst flush={flush}: weight {seen}, tx sees {oracle}"
            ));
        }
        tx.rollback();
    }
    assert!(failures.is_empty(), "read-your-updates: {failures:#?}");
    Ok(())
}

/// What a procedure read joins the transaction's SSI read set exactly as the
/// same read in Cypher does: a concurrent commit changing `n0` after the read
/// fails the reading transaction's commit in both cases, or in neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_reads_in_transaction_join_the_read_set() -> Result<()> {
    /// Commits a reader that ran `probe` (the Cypher oracle for `None`),
    /// after another session changed `n0`.
    async fn reader_commits(probe: Option<Probe>) -> Result<bool> {
        let (db, session, root) = fixture(false).await?;
        let tx = session.tx().await?;
        let read = probe.map_or_else(|| ORACLE.to_owned(), |p| p.cypher(root));
        tx.query(&read).await?;
        let other = db.session().tx().await?;
        other
            .execute("MATCH (n:Node {name: 'n0'}) SET n.body = 'changed'")
            .await?;
        other.commit().await?;
        tx.execute("CREATE (:Other)").await?;
        Ok(tx.commit().await.is_ok())
    }

    let oracle = reader_commits(None).await?;
    assert!(!oracle, "the Cypher read of n0 must conflict");
    let mut failures = Vec::new();
    for probe in PROBES {
        let committed = reader_commits(Some(probe)).await?;
        if committed != oracle {
            failures.push(format!(
                "{probe:?}: commit ok = {committed}, Cypher {oracle}"
            ));
        }
    }
    assert!(failures.is_empty(), "read set: {failures:#?}");
    Ok(())
}
