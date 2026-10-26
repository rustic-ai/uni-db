// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression for <https://github.com/rustic-ai/uni-db/issues/110>
//!
//! On a FORK, under a TYPED edge schema, once any relationship `SET` has
//! happened on the fork, a recursive Locy rule that traverses edges in the
//! REVERSE direction `(a)<-[l:LINE]-(b)` returns only the seed — every inherited
//! reverse edge vanishes. Forward `(b)-[l:LINE]->(a)` (the same edges) is
//! correct, and every non-fork / no-SET / untyped case is correct. The SET can
//! be on an UNRELATED edge and still break reverse reads of OTHER inherited
//! edges.
//!
//! Mirrors the issue's Python repro. The forward case is asserted as a live
//! control (passes today); the reverse case is the regression target.

// Rust guideline compliant

use std::collections::HashSet;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Build the issue's graph. With `typed`, declare the `LINE` edge type and a
/// `availability` property; otherwise leave it schemaless. Edges are stored
/// forward `b -> a` (so a reverse walk `(a)<-[l]-(b)` reaches from seed 1):
/// `2 -> 1`, `3 -> 2`, plus an UNRELATED edge `1 -> 9`.
async fn build_db(typed: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if typed {
        db.schema()
            .label("Bus")
            .property("id", DataType::Int64)
            .property("seed", DataType::Int64)
            .apply()
            .await?;
        db.schema()
            .label("Super")
            .property("k", DataType::Int64)
            .apply()
            .await?;
        db.schema()
            .edge_type("LINE", &["Bus"], &["Bus"])
            .property("availability", DataType::Float64)
            .apply()
            .await?;
    }
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute(
        "CREATE (:Bus {id:1, seed:1}) CREATE (:Bus {id:2, seed:0}) \
         CREATE (:Bus {id:3, seed:0}) CREATE (:Bus {id:9, seed:0})",
    )
    .await?;
    // Reverse chain: edges stored 2->1 and 3->2.
    tx.execute("MATCH (a:Bus {id:2}),(b:Bus {id:1}) CREATE (a)-[:LINE {availability:1.0}]->(b)")
        .await?;
    tx.execute("MATCH (a:Bus {id:3}),(b:Bus {id:2}) CREATE (a)-[:LINE {availability:1.0}]->(b)")
        .await?;
    // Unrelated edge that we SET on the fork.
    tx.execute("MATCH (a:Bus {id:1}),(b:Bus {id:9}) CREATE (a)-[:LINE {availability:1.0}]->(b)")
        .await?;
    tx.execute("CREATE (:Super {k:0})").await?;
    tx.commit().await?;
    Ok(db)
}

const REVERSE_REACH: &str = "\
CREATE RULE reach AS \
  MATCH (sup:Super),(b:Bus) WHERE b.seed = 1 \
  YIELD KEY sup, KEY b \
CREATE RULE reach AS \
  MATCH (sup:Super),(a:Bus)<-[l:LINE]-(b:Bus) \
  WHERE sup IS reach TO a AND l.availability > 0.5 \
  YIELD KEY sup, KEY b \
QUERY reach RETURN b";

const FORWARD_REACH: &str = "\
CREATE RULE reach AS \
  MATCH (sup:Super),(b:Bus) WHERE b.seed = 1 \
  YIELD KEY sup, KEY b \
CREATE RULE reach AS \
  MATCH (sup:Super),(b:Bus)-[l:LINE]->(a:Bus) \
  WHERE sup IS reach TO a AND l.availability > 0.5 \
  YIELD KEY sup, KEY b \
QUERY reach RETURN b";

/// Collect the `id` of the `b` KEY node from each derived `reach` fact.
fn reach_ids(facts: &[std::collections::HashMap<String, Value>]) -> HashSet<i64> {
    facts
        .iter()
        .filter_map(|f| match f.get("b") {
            Some(Value::Node(n)) => match n.properties.get("id") {
                Some(Value::Int(i)) => Some(*i),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Fork the primary, SET `availability` on the UNRELATED `1 -> 9` edge, then run
/// `program` on the fork and collect the reachable `b.id` set.
async fn reach_on_fork_after_set(db: &Uni, program: &str) -> Result<HashSet<i64>> {
    let forked = db.session().fork("f").await?;
    {
        let tx = forked.tx().await?;
        tx.execute("MATCH (a:Bus {id:1})-[l:LINE]->(b:Bus {id:9}) SET l.availability = 0.0")
            .await?;
        tx.commit().await?;
    }
    let result = forked.locy(program).await?;
    let empty = vec![];
    let facts = result
        .derived_rules()
        .await
        .unwrap()
        .get("reach")
        .unwrap_or(&empty);
    Ok(reach_ids(facts))
}

#[tokio::test]
async fn typed_fork_set_reverse_reach_is_complete() -> Result<()> {
    // The bug: reverse reach returns only the seed {1} after a rel-SET on a
    // typed fork. Correct reach is {1, 2, 3}.
    let db = build_db(true).await?;
    let ids = reach_on_fork_after_set(&db, REVERSE_REACH).await?;
    assert_eq!(
        ids,
        HashSet::from([1, 2, 3]),
        "issue #110: reverse reach on a typed fork after rel-SET must be {{1,2,3}}; got {ids:?}"
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn typed_fork_set_forward_reach_is_complete() -> Result<()> {
    // Live control: the forward form (semantically identical) is correct today.
    let db = build_db(true).await?;
    let ids = reach_on_fork_after_set(&db, FORWARD_REACH).await?;
    assert_eq!(
        ids,
        HashSet::from([1, 2, 3]),
        "forward reach on a typed fork after rel-SET must be {{1,2,3}}; got {ids:?}"
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn untyped_fork_set_reverse_reach_is_complete() -> Result<()> {
    // Control: the untyped case is correct today (the bug needs a typed schema).
    let db = build_db(false).await?;
    let ids = reach_on_fork_after_set(&db, REVERSE_REACH).await?;
    assert_eq!(
        ids,
        HashSet::from([1, 2, 3]),
        "untyped reverse reach on a fork after rel-SET must be {{1,2,3}}; got {ids:?}"
    );
    db.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Behavior matrix — the standing guard for the whole class.
// ---------------------------------------------------------------------------

/// How the rule is evaluated relative to the fork lifecycle.
#[derive(Debug, Clone, Copy)]
enum Mode {
    /// On the primary session (no fork).
    Base,
    /// On a fork, with no mutation.
    ForkNoSet,
    /// On a fork, after a `SET` on the unrelated `1 -> 9` edge.
    ForkSet,
}

/// Evaluate `program` under `mode` and collect the reachable `b.id` set.
async fn reach(db: &Uni, mode: Mode, program: &str) -> Result<HashSet<i64>> {
    let session = match mode {
        Mode::Base => db.session(),
        Mode::ForkNoSet => db.session().fork("f").await?,
        Mode::ForkSet => {
            let forked = db.session().fork("f").await?;
            let tx = forked.tx().await?;
            tx.execute("MATCH (a:Bus {id:1})-[l:LINE]->(b:Bus {id:9}) SET l.availability = 0.0")
                .await?;
            tx.commit().await?;
            forked
        }
    };
    let result = session.locy(program).await?;
    let empty = vec![];
    Ok(reach_ids(
        result
            .derived_rules()
            .await
            .unwrap()
            .get("reach")
            .unwrap_or(&empty),
    ))
}

/// The full {untyped, typed} × {base, fork-no-SET, fork+SET} × {forward, reverse}
/// matrix from the issue. Every cell must reach `{1, 2, 3}`; before the fix only
/// typed × fork+SET × reverse failed (returned `{1}`).
#[tokio::test]
async fn reach_matrix_all_cells_complete() -> Result<()> {
    let expected = HashSet::from([1, 2, 3]);
    for typed in [false, true] {
        for mode in [Mode::Base, Mode::ForkNoSet, Mode::ForkSet] {
            for (dir, program) in [("forward", FORWARD_REACH), ("reverse", REVERSE_REACH)] {
                let db = build_db(typed).await?;
                let ids = reach(&db, mode, program).await?;
                assert_eq!(
                    ids, expected,
                    "cell typed={typed} mode={mode:?} dir={dir}: reach must be {{1,2,3}}; got {ids:?}"
                );
                db.shutdown().await?;
            }
        }
    }
    Ok(())
}

/// On the same typed fork+SET state, plain-Cypher reverse matching and a
/// single-hop Locy reverse rule must agree on the reverse-adjacent set
/// (adjacency + property + filter parity). Both see edges with availability
/// > 0.5: `2 -> 1` and `3 -> 2`, i.e. `b ∈ {2, 3}` (the unrelated `1 -> 9` was
/// set to 0.0). Before the fix the rule (and plain Cypher) saw only the SET edge.
#[tokio::test]
async fn cypher_vs_rule_reverse_parity_on_typed_fork_set() -> Result<()> {
    let db = build_db(true).await?;
    let forked = db.session().fork("f").await?;
    {
        let tx = forked.tx().await?;
        tx.execute("MATCH (a:Bus {id:1})-[l:LINE]->(b:Bus {id:9}) SET l.availability = 0.0")
            .await?;
        tx.commit().await?;
    }

    let cypher = forked
        .query("MATCH (a:Bus)<-[l:LINE]-(b:Bus) WHERE l.availability > 0.5 RETURN b.id AS bid")
        .await?;
    let cypher_bs: HashSet<i64> = cypher
        .rows()
        .iter()
        .filter_map(|r| match r.value("bid") {
            Some(Value::Int(i)) => Some(*i),
            _ => None,
        })
        .collect();

    let rule = forked
        .locy(
            "CREATE RULE radj AS MATCH (a:Bus)<-[l:LINE]-(b:Bus) WHERE l.availability > 0.5 \
             YIELD KEY a, KEY b \
             QUERY radj RETURN b",
        )
        .await?;
    let empty = vec![];
    let rule_bs = reach_ids(
        rule.derived_rules()
            .await
            .unwrap()
            .get("radj")
            .unwrap_or(&empty),
    );

    assert_eq!(
        cypher_bs,
        HashSet::from([2, 3]),
        "plain-Cypher reverse on typed fork+SET must see b∈{{2,3}}; got {cypher_bs:?}"
    );
    assert_eq!(
        rule_bs, cypher_bs,
        "Locy reverse rule must agree with plain Cypher on the fork; rule={rule_bs:?} cypher={cypher_bs:?}"
    );
    db.shutdown().await?;
    Ok(())
}

/// Generality: the trigger is "a fork-local overlay edge exists for the type",
/// not specifically a `SET`. A fork-local edge CREATE (also dual-writes to the
/// overlay) must likewise not suppress inherited reverse reads.
#[tokio::test]
async fn typed_fork_create_reverse_reach_is_complete() -> Result<()> {
    let db = build_db(true).await?;
    let forked = db.session().fork("f").await?;
    {
        let tx = forked.tx().await?;
        // New unrelated reverse edge 9 -> 1 (availability below the 0.5 filter),
        // so it must not change the reachable set but does populate the overlay.
        tx.execute(
            "MATCH (a:Bus {id:9}),(b:Bus {id:1}) CREATE (a)-[:LINE {availability:0.1}]->(b)",
        )
        .await?;
        tx.commit().await?;
    }
    let result = forked.locy(REVERSE_REACH).await?;
    let empty = vec![];
    let ids = reach_ids(
        result
            .derived_rules()
            .await
            .unwrap()
            .get("reach")
            .unwrap_or(&empty),
    );
    assert_eq!(
        ids,
        HashSet::from([1, 2, 3]),
        "reverse reach after a fork-local edge CREATE must be {{1,2,3}}; got {ids:?}"
    );
    db.shutdown().await?;
    Ok(())
}
