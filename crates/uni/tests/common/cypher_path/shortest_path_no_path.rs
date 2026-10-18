// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A `shortestPath` / `allShortestPaths` pair with no path, in mandatory and
//! OPTIONAL MATCH.
//!
//! openCypher (and Neo4j): a mandatory `MATCH p = shortestPath((a)-[*]->(b))`
//! drops a pair that has no path; `OPTIONAL MATCH` null-extends an entering row
//! only when the optional pattern as a whole matched nothing for it. uni used
//! to keep every no-path pair as a row with a NULL path, in both. The mandatory
//! half of that is an intended semantics change (user decision, 2026-10-17).
//!
//! The answers below are worked out by hand on a five-node graph, at both
//! storage states (unflushed and flushed), including the corners: the
//! zero-length self path under `*0..`, a source equal to its target under
//! `*1..`, a `WHERE` on the path, the unnamed form, a NULL endpoint, and an
//! OPTIONAL target that no node qualifies for.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(shortest_path_no_path)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig};

/// `:R` out-edges `1→2`, `2→3`, `3→1` (a cycle); `4` and `5` have none, and
/// no node is an `:M`.
const GRAPH: &str = "CREATE (n1:N {id: 1}), (n2:N {id: 2}), (n3:N {id: 3}), \
     (:N {id: 4}), (:N {id: 5}), \
     (n1)-[:R]->(n2), (n2)-[:R]->(n3), (n3)-[:R]->(n1)";

/// Mandatory MATCH: a pair with no path is dropped.
const MANDATORY: &[(&str, &[&str])] = &[
    // Bound pair. `1` reaches `2` and `3`; `1 → 1` has no path under `*1..`
    // (a vertex reaches itself only through the zero-length path); `4` and `5`
    // are unreachable.
    (
        "MATCH (a:N {id: 1}), (b:N) MATCH p = shortestPath((a)-[:R*]->(b)) \
         RETURN b.id, length(p)",
        &["[Int(2), Int(1)]", "[Int(3), Int(2)]"],
    ),
    (
        "MATCH (a:N {id: 1}), (b:N) MATCH p = allShortestPaths((a)-[:R*]->(b)) \
         RETURN b.id, length(p)",
        &["[Int(2), Int(1)]", "[Int(3), Int(2)]"],
    ),
    // Unbound target, from a source that reaches nothing.
    (
        "MATCH p = shortestPath((a:N {id: 4})-[:R*]->(b:N)) RETURN count(*)",
        &["[Int(0)]"],
    ),
    (
        "MATCH p = allShortestPaths((a:N {id: 4})-[:R*]->(b:N)) RETURN count(*)",
        &["[Int(0)]"],
    ),
    // Unbound target, several sources: only the reachable pairs.
    (
        "MATCH p = shortestPath((a:N)-[:R*]->(b:N)) RETURN a.id, b.id, length(p)",
        &[
            "[Int(1), Int(2), Int(1)]",
            "[Int(1), Int(3), Int(2)]",
            "[Int(2), Int(1), Int(2)]",
            "[Int(2), Int(3), Int(1)]",
            "[Int(3), Int(1), Int(1)]",
            "[Int(3), Int(2), Int(2)]",
        ],
    ),
    // `*0..` admits the zero-length self path, even for an isolated node.
    (
        "MATCH (a:N {id: 4}) MATCH p = shortestPath((a)-[:R*0..]->(a)) RETURN length(p)",
        &["[Int(0)]"],
    ),
    (
        "MATCH (a:N {id: 4}), (b:N {id: 4}) \
         MATCH p = allShortestPaths((a)-[:R*0..]->(b)) RETURN length(p)",
        &["[Int(0)]"],
    ),
    (
        "MATCH p = shortestPath((a:N {id: 4})-[:R*0..]->(b:N)) RETURN b.id, length(p)",
        &["[Int(4), Int(0)]"],
    ),
    // A source equal to its target under `*1..` has no path: dropped.
    (
        "MATCH (a:N {id: 4}) MATCH p = shortestPath((a)-[:R*]->(a)) RETURN count(*)",
        &["[Int(0)]"],
    ),
    // A WHERE on the path filters what the search found.
    (
        "MATCH (a:N {id: 1}), (b:N) MATCH p = shortestPath((a)-[:R*]->(b)) \
         WHERE length(p) > 1 RETURN b.id",
        &["[Int(3)]"],
    ),
    // The unnamed form drops the same pairs.
    (
        "MATCH (a:N {id: 1}), (b:N) MATCH shortestPath((a)-[:R*]->(b)) RETURN b.id",
        &["[Int(2)]", "[Int(3)]"],
    ),
    (
        "MATCH (a:N {id: 1}), (b:N) MATCH allShortestPaths((a)-[:R*]->(b)) RETURN b.id",
        &["[Int(2)]", "[Int(3)]"],
    ),
    // A NULL endpoint (from an earlier OPTIONAL MATCH) matches nothing.
    (
        "MATCH (a:N {id: 4}) OPTIONAL MATCH (a)-[:R]->(b:N) \
         MATCH p = shortestPath((a)-[:R*]->(b)) RETURN count(*)",
        &["[Int(0)]"],
    ),
];

/// OPTIONAL MATCH: an entering row is null-extended only when the optional
/// pattern matched nothing for it.
const OPTIONAL: &[(&str, &[&str])] = &[
    // Bound pair: the pattern is that one pair, so a pair with no path keeps
    // its row with a NULL path.
    (
        "MATCH (a:N {id: 1}), (b:N) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b)) \
         RETURN b.id, length(p)",
        &[
            "[Int(1), Null]",
            "[Int(2), Int(1)]",
            "[Int(3), Int(2)]",
            "[Int(4), Null]",
            "[Int(5), Null]",
        ],
    ),
    // Unbound target: a source's matches are the targets it reaches. `4` and
    // `5` reach none and get one NULL row each; `1`, `2`, `3` get no NULL rows
    // for the targets they do not reach.
    (
        "MATCH (a:N) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:N)) \
         RETURN a.id, b.id, length(p)",
        &[
            "[Int(1), Int(2), Int(1)]",
            "[Int(1), Int(3), Int(2)]",
            "[Int(2), Int(1), Int(2)]",
            "[Int(2), Int(3), Int(1)]",
            "[Int(3), Int(1), Int(1)]",
            "[Int(3), Int(2), Int(2)]",
            "[Int(4), Null, Null]",
            "[Int(5), Null, Null]",
        ],
    ),
    (
        "MATCH (a:N) OPTIONAL MATCH p = allShortestPaths((a)-[:R*]->(b:N)) \
         RETURN a.id, b.id, length(p)",
        &[
            "[Int(1), Int(2), Int(1)]",
            "[Int(1), Int(3), Int(2)]",
            "[Int(2), Int(1), Int(2)]",
            "[Int(2), Int(3), Int(1)]",
            "[Int(3), Int(1), Int(1)]",
            "[Int(3), Int(2), Int(2)]",
            "[Int(4), Null, Null]",
            "[Int(5), Null, Null]",
        ],
    ),
    // A WHERE on the path, bound pair: a pair whose path fails it is
    // null-extended, not dropped.
    (
        "MATCH (a:N {id: 1}), (b:N) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b)) \
         WHERE length(p) > 1 RETURN b.id, length(p)",
        &[
            "[Int(1), Null]",
            "[Int(2), Null]",
            "[Int(3), Int(2)]",
            "[Int(4), Null]",
            "[Int(5), Null]",
        ],
    ),
    // A WHERE on the path, unbound target: each source keeps the targets whose
    // path passes, or one NULL row when none does.
    (
        "MATCH (a:N) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:N)) \
         WHERE length(p) > 1 RETURN a.id, b.id, length(p)",
        &[
            "[Int(1), Int(3), Int(2)]",
            "[Int(2), Int(1), Int(2)]",
            "[Int(3), Int(2), Int(2)]",
            "[Int(4), Null, Null]",
            "[Int(5), Null, Null]",
        ],
    ),
    // No node qualifies as the target: the entering row survives, NULL-extended.
    (
        "MATCH (a:N {id: 1}) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:M)) \
         RETURN a.id, b.id, p IS NULL",
        &["[Int(1), Null, Bool(true)]"],
    ),
    (
        "MATCH (a:N {id: 1}) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:N {id: 99})) \
         RETURN a.id, b.id, p IS NULL",
        &["[Int(1), Null, Bool(true)]"],
    ),
    // `*0..` from an isolated node matches its zero-length self path.
    (
        "MATCH (a:N {id: 4}) OPTIONAL MATCH p = shortestPath((a)-[:R*0..]->(b:N)) \
         RETURN b.id, length(p)",
        &["[Int(4), Int(0)]"],
    ),
    // The unnamed form. `3 → 3` has no path under `*1..`.
    (
        "MATCH (a:N) OPTIONAL MATCH shortestPath((a)-[:R*]->(b:N {id: 3})) \
         RETURN a.id, b.id",
        &[
            "[Int(1), Int(3)]",
            "[Int(2), Int(3)]",
            "[Int(3), Null]",
            "[Int(4), Null]",
            "[Int(5), Null]",
        ],
    ),
    // Two identical entering rows are two rows to null-extend, not one: the
    // clause is closed per entering row, not per bound node.
    (
        "UNWIND [1, 1] AS x MATCH (a:N {id: 4}) \
         OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:N)) RETURN x, b.id",
        &["[Int(1), Null]", "[Int(1), Null]"],
    ),
    (
        "UNWIND [1, 1] AS x MATCH (a:N {id: 1}) \
         OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:N)) WHERE length(p) > 1 \
         RETURN x, b.id",
        &["[Int(1), Int(3)]", "[Int(1), Int(3)]"],
    ),
    // A leading OPTIONAL MATCH whose pattern matches nothing yields one row.
    (
        "OPTIONAL MATCH p = shortestPath((a:N {id: 4})-[:R*]->(b:N)) RETURN a.id, b.id",
        &["[Null, Null]"],
    ),
];

async fn open(schema: bool, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    if schema {
        db.schema()
            .label("N")
            .property("id", DataType::Int64)
            .done()
            .label("M")
            .done()
            .edge_type("R", &["N"], &["N"])
            .done()
            .apply()
            .await?;
    }
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(GRAPH).await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// Runs `query`, returning each row's values in debug form, sorted.
async fn bag(db: &Uni, query: &str) -> Result<Vec<String>> {
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut rows: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    rows.sort();
    Ok(rows)
}

async fn check(schema: bool, flush: bool) -> Result<()> {
    let db = open(schema, flush).await?;
    let mut failures = Vec::new();
    for (query, rows) in MANDATORY.iter().chain(OPTIONAL) {
        let want: Vec<String> = rows.iter().map(|r| (*r).to_string()).collect();
        match bag(&db, query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}\n  got  {got:?}\n  want {want:?}")),
            Err(e) => failures.push(format!("{query}\n  error {e}")),
        }
    }
    db.shutdown().await?;
    assert!(
        failures.is_empty(),
        "schema {schema}, flushed {flush}: {} of {} queries failing:\n{}",
        failures.len(),
        MANDATORY.len() + OPTIONAL.len(),
        failures.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn shortest_path_no_path_schema_unflushed() -> Result<()> {
    check(true, false).await
}

#[tokio::test]
async fn shortest_path_no_path_schema_flushed() -> Result<()> {
    check(true, true).await
}

#[tokio::test]
async fn shortest_path_no_path_schemaless_unflushed() -> Result<()> {
    check(false, false).await
}

#[tokio::test]
async fn shortest_path_no_path_schemaless_flushed() -> Result<()> {
    check(false, true).await
}
