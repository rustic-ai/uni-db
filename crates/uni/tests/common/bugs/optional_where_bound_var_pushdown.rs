// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An `OPTIONAL MATCH`'s `WHERE` belongs to the optional pattern: a row it
//! rejects is null-extended, never dropped.
//!
//! The planner pushed a conjunct of that `WHERE` that named only variables
//! bound by an *earlier* clause into the earlier clause's scan, so
//! `MATCH (a:N) WHERE a.x < 4 OPTIONAL MATCH (a)-[:R]->(b) WHERE a.x < 3`
//! lost the `a.x = 3` row instead of returning it with `b` NULL. Every other
//! way a conjunct can leave the optional boundary — label disjunctions, the
//! vector KNN rewrite, a correlated subquery's input filter, inline property
//! maps, VLP and shortestPath patterns, chained OPTIONALs, `WITH` and `CALL`
//! boundaries — is covered here.
//!
//! The oracle needs no reference implementation: by definition,
//! `<prefix> OPTIONAL MATCH p WHERE w` is the bag union of
//! `<prefix> MATCH p WHERE w` and `<prefix> WITH * WHERE NOT EXISTS { MATCH p
//! WHERE w }` padded with NULL. Neither arm contains an OPTIONAL MATCH, so the
//! defect under test cannot reach the oracle.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(optional_where_bound_var_pushdown)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{
    DataType, IndexType, ScalarType, Uni, UniConfig, VectorAlgo, VectorIndexCfg, VectorMetric,
};

/// `n6` has no relationship; `n2` is the only `:L`, `n4` the only `:M`; `n3`
/// has two relationships to `n4`, so a clause between two bound nodes that
/// matches yields two rows and one that is rejected yields one.
const GRAPH: &str = "CREATE (n1:N {id: 1, x: 1, emb: [1.0, 0.0]}), \
     (n2:N:L {id: 2, x: 2, emb: [0.0, 1.0]}), \
     (n3:N {id: 3, x: 3, emb: [1.0, 0.0]}), \
     (n4:N:M {id: 4, x: 4, emb: [0.0, 1.0]}), \
     (n5:N {id: 5, x: 5, emb: [1.0, 0.0]}), \
     (:N {id: 6, x: 6, emb: [0.0, 1.0]}), \
     (n1)-[:R {w: 1}]->(n2), (n2)-[:R {w: 2}]->(n3), (n3)-[:R {w: 3}]->(n4), \
     (n3)-[:R {w: 4}]->(n1), (n4)-[:R {w: 5}]->(n5), (n5)-[:R {w: 6}]->(n3), \
     (n3)-[:R {w: 7}]->(n4)";

/// The common mandatory prefix.
const PREFIX: &str = "MATCH (a:N) WHERE a.x < 6";

/// One differential case: `{prefix} OPTIONAL MATCH {optional} RETURN {ret}`.
struct Case {
    prefix: &'static str,
    /// Pattern and `WHERE` of the optional clause, as one string.
    optional: &'static str,
    /// The NULL-padded arm's condition, when `NOT EXISTS { MATCH … }` cannot
    /// carry it: `NOT EXISTS { MATCH p WHERE c(a) }` is
    /// `NOT coalesce(c(a), false) OR NOT EXISTS { MATCH p }`.
    ///
    /// No case needs it now. Label tests and `vector_similarity` on an outer
    /// variable inside `EXISTS { }` used to fail or evaluate wrongly, and
    /// those cases carried this form; both work inside the subquery since
    /// `bugs::outer_entity_in_subquery`, so they use the direct one.
    negative: Option<&'static str>,
    /// Clauses between the optional clause and `RETURN`, if any.
    tail: &'static str,
    /// `tail` for the NULL-padded arm, where optional variables are unbound.
    null_tail: &'static str,
    ret: &'static str,
    /// `ret` with every expression over an optional variable replaced by NULL.
    null_ret: &'static str,
}

const fn case(optional: &'static str, ret: &'static str, null_ret: &'static str) -> Case {
    Case {
        prefix: PREFIX,
        optional,
        negative: None,
        tail: "",
        null_tail: "",
        ret,
        null_ret,
    }
}

const CASES: &[Case] = &[
    // Conjunct on an earlier-bound variable only (the reported shape).
    case("(a)-[:R]->(b:N) WHERE a.x < 3", "a.id, b.id", "a.id, null"),
    case("(a)-[:R]->(b) WHERE a.x < 3", "a.id, b.id", "a.id, null"),
    case(
        "(a)-[:R]->(b:N) WHERE a.id IN [1, 3]",
        "a.id, b.id",
        "a.id, null",
    ),
    case("(a)-[:R]->(b:N) WHERE a.x = 2", "a.id, b.id", "a.id, null"),
    case(
        "(a)-[:R]->(b:N) WHERE NOT a.x > 2",
        "a.id, b.id",
        "a.id, null",
    ),
    case(
        "(a)-[:R]->(b:N) WHERE a.x < 3 OR a.x > 4",
        "a.id, b.id",
        "a.id, null",
    ),
    // Mixed: one conjunct on the bound variable, one on the optional one.
    case(
        "(a)-[:R]->(b:N) WHERE a.x < 3 AND b.x > 1",
        "a.id, b.id",
        "a.id, null",
    ),
    case(
        "(a)-[:R]->(b:N) WHERE a.x + b.x > 4",
        "a.id, b.id",
        "a.id, null",
    ),
    // Optional variables only.
    case("(a)-[:R]->(b:N) WHERE b.x > 2", "a.id, b.id", "a.id, null"),
    case("(a)-[r:R]->(b:N) WHERE r.w > 2", "a.id, b.id", "a.id, null"),
    // Label predicates and label disjunctions on the bound variable.
    case("(a)-[:R]->(b:N) WHERE a:L", "a.id, b.id", "a.id, null"),
    case(
        "(a)-[:R]->(b:N) WHERE a:L OR a:M",
        "a.id, b.id",
        "a.id, null",
    ),
    case("(a)-[:R]->(b:N) WHERE NOT a:L", "a.id, b.id", "a.id, null"),
    // Inline property map / label on the bound node.
    case("(a {x: 1})-[:R]->(b)", "a.id, b.id", "a.id, null"),
    case("(a:L)-[:R]->(b)", "a.id, b.id", "a.id, null"),
    case("(a)-[:R]->(b {x: 3})", "a.id, b.id", "a.id, null"),
    // Pattern predicates.
    case(
        "(a)-[:R]->(b:N) WHERE EXISTS { (a)-[:R]->(:L) }",
        "a.id, b.id",
        "a.id, null",
    ),
    case(
        "(a)-[:R]->(b:N) WHERE NOT EXISTS { (a)<-[:R]-(:M) }",
        "a.id, b.id",
        "a.id, null",
    ),
    // Vector predicate on the bound variable.
    case(
        "(a)-[:R]->(b:N) WHERE vector_similarity(a.emb, [1.0, 0.0]) > 0.5",
        "a.id, b.id",
        "a.id, null",
    ),
    // Predicates on a node the optional clause scans for itself.
    Case {
        prefix: "MATCH (a:N) WHERE a.id < 3",
        ..case("(d:N) WHERE d.x = 99", "a.id, d.id", "a.id, null")
    },
    Case {
        prefix: "MATCH (a:N) WHERE a.id < 3",
        ..case(
            "(d:N) WHERE d.x > 3 AND a.x < 2",
            "a.id, d.id",
            "a.id, null",
        )
    },
    Case {
        prefix: "MATCH (a:N) WHERE a.id < 3",
        ..case(
            "(d:N)-[:R]->(e:N) WHERE d.x > 3",
            "a.id, d.id, e.id",
            "a.id, null, null",
        )
    },
    Case {
        prefix: "MATCH (a:N) WHERE a.id < 3",
        ..case("(d:N)-[:R]->(a) WHERE d.x > 2", "a.id, d.id", "a.id, null")
    },
    Case {
        prefix: "MATCH (a:N) WHERE a.id < 3",
        ..case("(d) WHERE d:L OR d:Q", "a.id, d.id", "a.id, null")
    },
    Case {
        prefix: "MATCH (a:N) WHERE a.id < 3",
        ..case("(d:N) WHERE d.x = 2", "a.id, d.id", "a.id, null")
    },
    // A relationship an earlier clause bound.
    Case {
        prefix: "MATCH (a:N)-[r:R]->(z:N) WHERE a.x < 6",
        ..case(
            "(a)-[r]->(b) WHERE r.w < 3",
            "a.id, z.id, b.id",
            "a.id, z.id, null",
        )
    },
    Case {
        prefix: "MATCH (a:N)-[r:R]->(z:N) WHERE a.x < 6",
        ..case(
            "(a)-[r {w: 1}]->(b)",
            "a.id, z.id, b.id",
            "a.id, z.id, null",
        )
    },
    Case {
        prefix: "MATCH (a:N)-[r:R]->(z:N) WHERE a.x < 6",
        ..case("(a)-[r]->(z) WHERE a.x < 3", "a.id, z.id", "a.id, z.id")
    },
    // Variable-length.
    case(
        "(a)-[:R*1..2]->(b:N) WHERE a.x < 3",
        "a.id, b.id",
        "a.id, null",
    ),
    case(
        "(a)-[:R*2..2]->(b:N) WHERE b.x > 3",
        "a.id, b.id",
        "a.id, null",
    ),
    // Two hops.
    case(
        "(a)-[:R]->(b)-[:R]->(c) WHERE a.x < 3",
        "a.id, c.id",
        "a.id, null",
    ),
    // A clause that binds nothing new.
    case("(a) WHERE a.x < 3", "a.id", "a.id"),
    case("(a)-[:R]->(a2) WHERE a.x < 3 AND a2.x > 0", "a.id", "a.id"),
    // An earlier clause that bound both endpoints: the clause adds nothing to
    // scope, only the traversal's hidden `__rebound_c`.
    Case {
        prefix: "MATCH (a:N), (c:N) WHERE a.x < 6 AND c.id = 4",
        ..case("(a)-[:R]->(c) WHERE a.x > 2", "a.id, c.id", "a.id, c.id")
    },
    Case {
        prefix: "MATCH (a:N), (c:N) WHERE a.x < 6 AND c.id = 4",
        ..case(
            "(a)-[:R*1..3]->(c) WHERE a.x > 1",
            "a.id, c.id",
            "a.id, c.id",
        )
    },
    Case {
        prefix: "MATCH (a:N), (m:N), (c:N) WHERE a.x < 6 AND m.id = 3 AND c.id = 4",
        ..case(
            "(a)-[:R]->(m)-[:R]->(c) WHERE a.x > 1",
            "a.id, c.id",
            "a.id, c.id",
        )
    },
    Case {
        prefix: "MATCH (a:N), (c:N) WHERE a.x < 6 AND c.id = 4",
        optional: "(a)-[:R]->(c) WHERE a.x < 3 OR c.x < 3",
        negative: None,
        tail: "",
        null_tail: "",
        ret: "a.id, c.id",
        null_ret: "a.id, c.id",
    },
    // `WITH` boundary before the optional clause.
    Case {
        prefix: "MATCH (a:N) WITH a WHERE a.x < 6",
        optional: "(a)-[:R]->(b:N) WHERE a.x < 3",
        negative: None,
        tail: "",
        null_tail: "",
        ret: "a.id, b.id",
        null_ret: "a.id, null",
    },
    // UNWIND-bound earlier variable.
    Case {
        prefix: "UNWIND [1, 2, 3, 6] AS k MATCH (a:N) WHERE a.id = k",
        optional: "(a)-[:R]->(b:N) WHERE k < 3",
        negative: None,
        tail: "",
        null_tail: "",
        ret: "a.id, b.id",
        null_ret: "a.id, null",
    },
    // An equality against an UNWIND variable, which the physical planner
    // turns into an IN-list pushed into one side of the cross join.
    Case {
        prefix: "UNWIND [1, 2, 99] AS k",
        ..case("(d:N) WHERE d.id = k", "k, d.id", "k, null")
    },
    // A correlated subquery in the earlier part (`Apply` input filter).
    Case {
        prefix: "MATCH (a:N) WHERE a.x < 6 CALL { WITH a RETURN a.x * 2 AS y }",
        optional: "(a)-[:R]->(b:N) WHERE a.x < 3",
        negative: None,
        tail: "",
        null_tail: "",
        ret: "a.id, y, b.id",
        null_ret: "a.id, y, null",
    },
    Case {
        prefix: "MATCH (a:N) WHERE a.x < 6 CALL { WITH a RETURN a.x * 2 AS y }",
        optional: "(a)-[:R]->(b:N) WHERE y < 6",
        negative: None,
        tail: "",
        null_tail: "",
        ret: "a.id, y, b.id",
        null_ret: "a.id, y, null",
    },
    // Chained: the second clause's WHERE names a variable the first OPTIONAL
    // bound. Rejecting it must not null out `b`.
    Case {
        prefix: "MATCH (a:N) WHERE a.x < 6 OPTIONAL MATCH (a)-[:R]->(b:N)",
        optional: "(b)-[:R]->(c:N) WHERE b.x < 3",
        negative: None,
        tail: "",
        null_tail: "",
        ret: "a.id, b.id, c.id",
        null_ret: "a.id, b.id, null",
    },
    Case {
        prefix: "MATCH (a:N) WHERE a.x < 6 OPTIONAL MATCH (a)-[:R]->(b:N)",
        optional: "(b)-[:R]->(c:N) WHERE a.x < 3",
        negative: None,
        tail: "",
        null_tail: "",
        ret: "a.id, b.id, c.id",
        null_ret: "a.id, b.id, null",
    },
    // A later WITH ... WHERE is an ordinary filter over the optional result.
    Case {
        prefix: PREFIX,
        optional: "(a)-[:R]->(b:N) WHERE a.x < 3",
        negative: None,
        tail: " WITH a, b WHERE a.x > 1",
        null_tail: " WITH a WHERE a.x > 1",
        ret: "a.id, b.id",
        null_ret: "a.id, null",
    },
];

/// Cases with hand-computed rows, independent of the oracle.
///
/// They were written when the oracle's arms could not be trusted for them:
/// `EXISTS { shortestPath(...) }` over outer-bound endpoints failed with
/// `Source column 'a._vid' not found`, and a mandatory `MATCH (d:N) WHERE
/// vector_similarity(d.emb, q) > t` was rewritten to a KNN scan that ignored
/// `t`. Both are fixed; the rows stay as an oracle-free check. Paths to `n4`:
/// `n1` 3 hops, `n2` 2, `n3` 1. `emb` is `[0, 1]` on `n2`, `n4`, `n6` and
/// `[1, 0]` elsewhere. The vector cases also cover the KNN rewrite dropping
/// the optional scan's `optional` flag, which lost the NULL row when nothing
/// passed.
const EXACT: &[(&str, &[&str])] = &[
    (
        "MATCH (a:N) WHERE a.id < 3 \
         OPTIONAL MATCH (d:N) WHERE vector_similarity(d.emb, [0.0, 1.0]) > 0.5 \
         RETURN a.id, d.id",
        &[
            "[Int(1), Int(2)]",
            "[Int(1), Int(4)]",
            "[Int(1), Int(6)]",
            "[Int(2), Int(2)]",
            "[Int(2), Int(4)]",
            "[Int(2), Int(6)]",
        ],
    ),
    (
        "MATCH (a:N) WHERE a.id < 3 \
         OPTIONAL MATCH (d:N) WHERE vector_similarity(d.emb, [1.0, 1.0]) > 0.99 \
         RETURN a.id, d.id",
        &["[Int(1), Null]", "[Int(2), Null]"],
    ),
    (
        "MATCH (a:N), (c:N) WHERE a.x < 4 AND c.id = 4 \
         OPTIONAL MATCH p = shortestPath((a)-[:R*]->(c)) WHERE a.x < 3 \
         RETURN a.id, length(p)",
        &["[Int(1), Int(3)]", "[Int(2), Int(2)]", "[Int(3), Null]"],
    ),
    (
        "MATCH (a:N), (c:N) WHERE a.x < 4 AND c.id = 4 \
         OPTIONAL MATCH p = shortestPath((a {x: 1})-[:R*]->(c)) \
         RETURN a.id, length(p)",
        &["[Int(1), Int(3)]", "[Int(2), Null]", "[Int(3), Null]"],
    ),
    (
        "MATCH (a:N), (c:N) WHERE a.id IN [3, 6] AND c.id = 4 \
         OPTIONAL MATCH p = shortestPath((a)-[:R*]->(c)) \
         RETURN a.id, length(p)",
        &["[Int(3), Int(1)]", "[Int(6), Null]"],
    ),
];

/// Hand-computed cases that run in both modes.
///
/// The oracle's NULL arm for an OPTIONAL MATCH inside `CALL { WITH a … }`
/// would be `CALL { WITH a WITH * WHERE NOT EXISTS { … } RETURN … }`, which
/// returned no rows at all when schemaless (fixed; see
/// `bugs::outer_entity_in_subquery`).
const EXACT_ALL_MODES: &[(&str, &[&str])] = &[(
    "MATCH (a:N) WHERE a.x < 6 \
     CALL { WITH a OPTIONAL MATCH (a)-[:R]->(b:N) WHERE a.x < 3 RETURN b } \
     RETURN a.id, b.id",
    &[
        "[Int(1), Int(2)]",
        "[Int(2), Int(3)]",
        "[Int(3), Null]",
        "[Int(4), Null]",
        "[Int(5), Null]",
    ],
)];

/// Whether to build a schema with scalar and vector indexes (so index
/// pushdown and the KNN rewrite are reachable) or run schemaless.
#[derive(Clone, Copy, Debug)]
enum Mode {
    Schemaless,
    Indexed,
}

async fn open(mode: Mode, flush: bool, execution_batch_size: Option<usize>) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            execution_batch_size,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    if let Mode::Indexed = mode {
        db.schema()
            .label("N")
            .property("id", DataType::Int64)
            .property("x", DataType::Int64)
            .vector("emb", 2)
            .index("x", IndexType::Scalar(ScalarType::BTree))
            .index("id", IndexType::Scalar(ScalarType::BTree))
            .index(
                "emb",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: None,
                }),
            )
            .done()
            .label("L")
            .done()
            .label("M")
            .done()
            .edge_type("R", &["N"], &["N"])
            .property("w", DataType::Int64)
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

async fn bag(db: &Uni, query: &str) -> Result<Vec<String>> {
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
    let mut rows: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    rows.sort();
    Ok(rows)
}

async fn check(mode: Mode, flush: bool, execution_batch_size: Option<usize>) -> Result<()> {
    let db = open(mode, flush, execution_batch_size).await?;
    let mut failures = Vec::new();

    // Hand-computed rows for the reported shape, independent of the oracle.
    let reported = "MATCH (a:N) WHERE a.x < 4 \
                    OPTIONAL MATCH (a)-[:R]->(b:N) WHERE a.x < 3 RETURN a.id, b.id";
    let got = bag(&db, reported).await?;
    let want = vec![
        "[Int(1), Int(2)]".to_string(),
        "[Int(2), Int(3)]".to_string(),
        "[Int(3), Null]".to_string(),
    ];
    if got != want {
        failures.push(format!("{reported}\n  got  {got:?}\n  want {want:?}"));
    }

    // `EXACT` ran against the schema only while shortestPath could not plan
    // a relationship type the schema does not declare (`Edge type R not
    // found`).
    let exact: &[&[(&str, &[&str])]] = &[EXACT, EXACT_ALL_MODES];
    for (query, rows) in exact.iter().flat_map(|cases| cases.iter()) {
        let want: Vec<String> = rows.iter().map(|r| (*r).to_string()).collect();
        match bag(&db, query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}\n  got  {got:?}\n  want {want:?}")),
            Err(e) => failures.push(format!("{query}\n  error {e}")),
        }
    }

    for c in CASES {
        let (body, filter) = match c.optional.split_once(" WHERE ") {
            Some((body, filter)) => (body, format!(" WHERE {filter}")),
            None => (c.optional, String::new()),
        };
        let query = format!(
            "{} OPTIONAL MATCH {}{} RETURN {}",
            c.prefix, c.optional, c.tail, c.ret
        );
        let positive = format!(
            "{} MATCH {body}{filter}{} RETURN {}",
            c.prefix, c.tail, c.ret
        );
        let condition = c.negative.map_or_else(
            || format!("NOT EXISTS {{ MATCH {body}{filter} }}"),
            str::to_string,
        );
        let negative = format!(
            "{} WITH * WHERE {condition}{} RETURN {}",
            c.prefix, c.null_tail, c.null_ret
        );
        let mut want = match (bag(&db, &positive).await, bag(&db, &negative).await) {
            (Ok(mut p), Ok(n)) => {
                p.extend(n);
                p
            }
            (p, n) => {
                failures.push(format!(
                    "{query}\n  oracle error: {:?} / {:?}",
                    p.err(),
                    n.err()
                ));
                continue;
            }
        };
        want.sort();
        match bag(&db, &query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}\n  got  {got:?}\n  want {want:?}")),
            Err(e) => failures.push(format!("{query}\n  error {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{mode:?}, flushed {flush}, batch {execution_batch_size:?}: {} failing:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn optional_where_bound_var_pushdown_schemaless_unflushed() -> Result<()> {
    check(Mode::Schemaless, false, None).await
}

#[tokio::test]
async fn optional_where_bound_var_pushdown_schemaless_flushed() -> Result<()> {
    check(Mode::Schemaless, true, None).await
}

#[tokio::test]
async fn optional_where_bound_var_pushdown_indexed_unflushed() -> Result<()> {
    check(Mode::Indexed, false, None).await
}

#[tokio::test]
async fn optional_where_bound_var_pushdown_indexed_flushed() -> Result<()> {
    check(Mode::Indexed, true, None).await
}

/// A batch of one row puts every match of an entering row in its own batch,
/// so the clause's null-preserving filter must decide across batches.
#[tokio::test]
async fn optional_where_bound_var_pushdown_indexed_one_row_batches() -> Result<()> {
    check(Mode::Indexed, true, Some(1)).await
}

/// Opens a flushed store with `n` indexed `:N {uid}` nodes, three `:A {name,
/// x}` nodes and the edges `a0→u42`, `a0→u7`, `a1→u100`.
async fn open_scaled(n: i64) -> Result<Uni> {
    let db = Uni::temporary()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for ddl in [
        "CREATE LABEL N (id INT, uid STRING)",
        "CREATE LABEL A (name STRING, x INT)",
        "CREATE EDGE TYPE R FROM A TO N",
        "CREATE INDEX idx_guard_n_uid FOR (n:N) ON (n.uid)",
    ] {
        tx.execute(ddl).await?;
    }
    tx.commit().await?;
    let tx = session.tx().await?;
    tx.query_with("UNWIND range(0, $last) AS i CREATE (:N {id: i, uid: 'u' + toString(i)})")
        .param("last", uni_db::Value::Int(n - 1))
        .fetch_all()
        .await?;
    tx.execute("CREATE (:A {name: 'a0', x: 1}), (:A {name: 'a1', x: 2}), (:A {name: 'a2', x: 5})")
        .await?;
    tx.commit().await?;
    let tx = session.tx().await?;
    tx.execute(
        "MATCH (a0:A {name: 'a0'}), (a1:A {name: 'a1'}), \
         (n42:N {uid: 'u42'}), (n7:N {uid: 'u7'}), (n100:N {uid: 'u100'}) \
         CREATE (a0)-[:R]->(n42), (a0)-[:R]->(n7), (a1)-[:R]->(n100)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Rows read for a predicate the clause can answer from the `uid` index
/// must not grow with the label: a conjunct over a variable the OPTIONAL
/// clause creates may go into that variable's own (optional) scan.
async fn check_counters(n: i64) -> Result<()> {
    let db = open_scaled(n).await?;
    let cases: &[(&str, &[&str])] = &[
        (
            "MATCH (a:A) OPTIONAL MATCH (b:N) WHERE b.uid = 'u42' RETURN a.name, b.uid",
            &[
                "[String(\"a0\"), String(\"u42\")]",
                "[String(\"a1\"), String(\"u42\")]",
                "[String(\"a2\"), String(\"u42\")]",
            ],
        ),
        (
            "MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b:N) WHERE b.uid = 'u42' \
             RETURN a.name, b.uid",
            &[
                "[String(\"a0\"), String(\"u42\")]",
                "[String(\"a1\"), Null]",
                "[String(\"a2\"), Null]",
            ],
        ),
        (
            "MATCH (a:A) OPTIONAL MATCH (b:N) WHERE b.uid = 'u42' AND a.x < 3 \
             RETURN a.name, b.uid",
            &[
                "[String(\"a0\"), String(\"u42\")]",
                "[String(\"a1\"), String(\"u42\")]",
                "[String(\"a2\"), Null]",
            ],
        ),
        (
            "MATCH (a:A) OPTIONAL MATCH (b:N) WHERE b.uid = 'nope' RETURN a.name, b.uid",
            &[
                "[String(\"a0\"), Null]",
                "[String(\"a1\"), Null]",
                "[String(\"a2\"), Null]",
            ],
        ),
    ];
    let mut failures = Vec::new();
    for (query, rows) in cases {
        let result = db.session().query_with(query).fetch_all().await?;
        let mut got: Vec<String> = result
            .rows()
            .iter()
            .map(|r| format!("{:?}", r.values()))
            .collect();
        got.sort();
        let want: Vec<String> = rows.iter().map(|r| (*r).to_string()).collect();
        let metrics = result.metrics();
        // The index answers the `uid` lookup in a handful of rows; a full
        // label scan reads all `n`. 64 sits far below the smaller scale.
        // For a lookup that finds nothing, `index_scans` depends on what ran
        // before it: the mandatory `MATCH (b:N) WHERE b.uid = 'nope'` read 1
        // as a store's first query and 0 after other lookups. Its bound is the
        // rows-read cap alone.
        let needs_index = !query.contains("'nope'");
        if got != want || (needs_index && metrics.index_scans == 0) || metrics.rows_scanned > 64 {
            failures.push(format!(
                "{query}\n  got {got:?}\n  want {want:?}\n  rows_scanned {} index_scans {}",
                metrics.rows_scanned, metrics.index_scans
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "n = {n}: {} failing:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn optional_where_bound_var_pushdown_counters_1000() -> Result<()> {
    check_counters(1000).await
}

#[tokio::test]
async fn optional_where_bound_var_pushdown_counters_4000() -> Result<()> {
    check_counters(4000).await
}
