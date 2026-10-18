// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Guards for P9 of `docs/proposals/open_issues_plan_2026_10_17.md`.
//!
//! - **A7, sideways information passing.** `MATCH (a:N {uid: $x}), (b:N)
//!   WHERE b.uid = a.name` hash-joined a one-row `a` against a full scan of
//!   `b`. The bound key values of `a` are now pushed into `b`'s scan as an
//!   indexed `IN` lookup (`PropertyLookupJoinExec`), with a run-time fall back
//!   to the full scan when the key set is large.
//! - **A6, named path anchored on the far end.** `MATCH p = (a)-[:R]->(b {uid:
//!   $x})` used to refuse anchor reversal because `p` binds in traversal order.
//!   It now reverses and binds `p` from the reversed node/edge lists, so `p` is
//!   identical to the unreversed plan.
//! - **A10, `{ext_id: …}` on a labelled node.** Probed at two scales.
//!
//! Every counter guard runs at two scales, so a cost linear in the label size
//! cannot pass at both. Every differential compares against a formulation the
//! optimization cannot take, and checks that it indeed did not.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{Session, Uni, Value};
use uni_query::plan_shape;

use crate::plan_shape::plan_ops;

const SCALES: [usize; 2] = [400, 1600];

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

/// `n` nodes `:N {id, uid, name, x, ext_id}` (uid and x indexed; `name` is the
/// uid of the next node), `n / 8` nodes `:S {uid, name, k, f}` and `2n` `:R {w}`
/// edges `i -> i+1`, `i -> 7i+3`.
///
/// `S` is the "many bound rows" side: its `name` points at `u{8j}`, except
/// every 5th is NULL and every 7th repeats its predecessor's key. `k` is the
/// same key as an INT and `f` as a FLOAT, for the type-mismatch arms.
async fn fixture(n: usize, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    for ddl in [
        "CREATE LABEL N (id INT, uid STRING, name STRING, x INT)",
        "CREATE LABEL S (uid STRING, name STRING, k INT, f FLOAT)",
        "CREATE EDGE TYPE R (w INT) FROM N TO N",
        "CREATE INDEX idx_n_uid FOR (n:N) ON (n.uid)",
        "CREATE INDEX idx_n_x FOR (n:N) ON (n.x)",
    ] {
        tx.execute(ddl).await?;
    }
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let props: Vec<HashMap<String, Value>> = (0..n)
        .map(|i| {
            HashMap::from([
                ("id".to_string(), Value::Int(i as i64)),
                ("uid".to_string(), s(&format!("u{i}"))),
                ("name".to_string(), s(&format!("u{}", (i + 1) % n))),
                ("x".to_string(), Value::Int(i as i64)),
                ("ext_id".to_string(), s(&format!("e{i}"))),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("N", props).await?;
    let sprops: Vec<HashMap<String, Value>> = (0..n / 8)
        .map(|j| {
            let key = if j % 7 == 6 { (j - 1) * 8 } else { j * 8 };
            let mut m = HashMap::from([
                ("uid".to_string(), s(&format!("s{j}"))),
                ("k".to_string(), Value::Int(key as i64)),
                ("f".to_string(), Value::Float(key as f64)),
            ]);
            if j % 5 != 4 {
                m.insert("name".to_string(), s(&format!("u{key}")));
            }
            m
        })
        .collect();
    bulk.insert_vertices("S", sprops).await?;
    let mut edges = Vec::with_capacity(2 * n);
    for i in 0..n {
        for (dst, w) in [((i + 1) % n, i % 10), ((i * 7 + 3) % n, (i * 3) % 10)] {
            edges.push(uni_bulk::EdgeData::new(
                vids[i],
                vids[dst],
                HashMap::from([("w".to_string(), Value::Int(w as i64))]),
            ));
        }
    }
    bulk.insert_edges("R", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// A deterministic rendering of a value: property maps sorted by key, nodes,
/// edges and paths with every identity field, in order.
fn canon(v: &Value) -> String {
    fn props(m: &HashMap<String, Value>) -> String {
        let mut kv: Vec<String> = m.iter().map(|(k, v)| format!("{k}:{}", canon(v))).collect();
        kv.sort();
        format!("{{{}}}", kv.join(","))
    }
    fn node(n: &uni_db::Node) -> String {
        format!(
            "(v{}:{:?}{})",
            n.vid.as_u64(),
            n.labels,
            props(&n.properties)
        )
    }
    fn edge(e: &uni_db::Edge) -> String {
        format!(
            "[e{}:{} v{}->v{}{}]",
            e.eid.as_u64(),
            e.edge_type,
            e.src.as_u64(),
            e.dst.as_u64(),
            props(&e.properties)
        )
    }
    match v {
        Value::List(xs) => format!("[{}]", xs.iter().map(canon).collect::<Vec<_>>().join(",")),
        Value::Map(m) => props(m),
        Value::Node(n) => node(n),
        Value::Edge(e) => edge(e),
        Value::Path(p) => format!(
            "<{} | {}>",
            p.nodes.iter().map(node).collect::<Vec<_>>().join(","),
            p.edges.iter().map(edge).collect::<Vec<_>>().join(",")
        ),
        other => format!("{other:?}"),
    }
}

/// Rows as a sorted bag of their canonical renderings.
fn bag_of(rows: &[uni_db::Row]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|r| {
            format!(
                "[{}]",
                r.values().iter().map(canon).collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    out.sort();
    out
}

async fn bag(session: &Session, q: &str) -> Result<Vec<String>> {
    let r = session
        .query(q)
        .await
        .map_err(|e| anyhow::anyhow!("`{q}` failed: {e}"))?;
    Ok(bag_of(r.rows()))
}

/// `(rows, rows_scanned, index_scans)` of a profiled query.
async fn counters(session: &Session, q: &str) -> Result<(usize, usize, usize)> {
    let (r, _p) = session.query_with(q).profile().await?;
    let m = r.metrics();
    Ok((r.len(), m.rows_scanned, m.index_scans as usize))
}

// ---------------------------------------------------------------------------
// A7 — sideways information passing
// ---------------------------------------------------------------------------

const A7_ANCHORED: &str = "MATCH (a:N {uid: 'u42'}), (b:N) WHERE b.uid = a.name RETURN b.id";
const A7_TWO_MATCH: &str = "MATCH (a:N {uid: 'u42'}) MATCH (b:N) WHERE b.uid = a.name RETURN b.id";

/// The anchored disjoint pattern reads O(answer) rows of `b`, not the label.
///
/// Before: `rows_scanned` = 1 + |N| (401 / 1,601) through a `HashJoinExec`.
/// After: the one `a` row plus the one `b` row the index returns, at both
/// scales.
#[tokio::test]
async fn a7_anchored_disjoint_pattern_reads_only_the_matching_rows() -> Result<()> {
    for n in SCALES {
        let db = fixture(n, true).await?;
        let session = db.session();
        for q in [A7_ANCHORED, A7_TWO_MATCH] {
            let (rows, scanned, index_scans) = counters(&session, q).await?;
            eprintln!("A7 N={n} rows={rows} rows_scanned={scanned} index_scans={index_scans} {q}");
            assert_eq!(rows, 1, "N={n}: {q}");
            assert_eq!(bag(&session, q).await?, vec!["[Int(43)]".to_string()]);
            assert!(
                scanned <= 2,
                "N={n}: `{q}` scanned {scanned} rows; the bound key should be looked up, \
                 not joined against a scan of all {n} `N` rows"
            );
            assert!(
                index_scans >= 2,
                "N={n}: both sides should use the index ({q})"
            );
            let ops = plan_ops(&session, q).await;
            plan_shape::assert_uses(&ops, "PropertyLookupJoinExec", q);
        }
    }
    Ok(())
}

/// A bound side too large for a lookup to pay switches to one scan at run time,
/// so the many-rows case reads no more than the hash join it replaces.
///
/// The build here is every `N` (|N| distinct keys, all of the label), so the
/// lookup arm would ask for the whole table key by key.
#[tokio::test]
async fn a7_large_bound_side_falls_back_to_one_scan() -> Result<()> {
    for n in SCALES {
        let db = fixture(n, true).await?;
        let session = db.session();
        let q = "MATCH (a:N), (b:N) WHERE b.uid = a.name RETURN count(*) AS c";
        let (_, scanned, _) = counters(&session, q).await?;
        let r = session.query(q).await?;
        assert_eq!(r.rows()[0].get::<i64>("c")?, n as i64);
        eprintln!("A7 large N={n} rows_scanned={scanned}");
        assert!(
            scanned <= 2 * n,
            "N={n}: the large-build arm must not read more than both labels once \
             (got {scanned})"
        );
    }
    Ok(())
}

/// Reference formulations: the same join with a computed build key
/// (`a.name + ''`), which `PropertyLookupJoinExec` does not take. Each pair is
/// (optimized, reference).
fn a7_pairs() -> Vec<(&'static str, &'static str)> {
    vec![
        // Single bound row.
        (
            "MATCH (a:N {uid: 'u42'}), (b:N) WHERE b.uid = a.name RETURN a.id, b.id",
            "MATCH (a:N {uid: 'u42'}), (b:N) WHERE b.uid = a.name + '' RETURN a.id, b.id",
        ),
        // Many bound rows, NULL keys and duplicate keys.
        (
            "MATCH (a:S), (b:N) WHERE b.uid = a.name RETURN a.uid, b.id",
            "MATCH (a:S), (b:N) WHERE b.uid = a.name + '' RETURN a.uid, b.id",
        ),
        // Probe on the left of the equality and of the comma.
        (
            "MATCH (b:N), (a:S {uid: 's3'}) WHERE a.name = b.uid RETURN a.uid, b.id",
            "MATCH (b:N), (a:S {uid: 's3'}) WHERE a.name + '' = b.uid RETURN a.uid, b.id",
        ),
        // INT key against an indexed INT property.
        (
            "MATCH (a:S), (b:N) WHERE b.x = a.k RETURN a.uid, b.id",
            "MATCH (a:S), (b:N) WHERE b.x = a.k + 0 RETURN a.uid, b.id",
        ),
        // Type mismatches: FLOAT against INT (Cypher: 8 = 8.0), INT against
        // STRING (never equal).
        (
            "MATCH (a:S), (b:N) WHERE b.x = a.f RETURN a.uid, b.id",
            "MATCH (a:S), (b:N) WHERE b.x = a.f + 0.0 RETURN a.uid, b.id",
        ),
        (
            "MATCH (a:S), (b:N) WHERE b.uid = a.k RETURN a.uid, b.id",
            "MATCH (a:S), (b:N) WHERE b.uid = a.k + 0 RETURN a.uid, b.id",
        ),
        // A residual cross-side predicate and a probe-side filter.
        (
            "MATCH (a:S), (b:N) WHERE b.uid = a.name AND b.x > a.k - 1 AND b.id % 2 = 0 \
             RETURN a.uid, b.id",
            "MATCH (a:S), (b:N) WHERE b.uid = a.name + '' AND b.x > a.k - 1 AND b.id % 2 = 0 \
             RETURN a.uid, b.id",
        ),
        // OPTIONAL MATCH: unmatched and NULL-keyed `a` rows keep a NULL `b`.
        (
            "MATCH (a:S) OPTIONAL MATCH (b:N) WHERE b.uid = a.name RETURN a.uid, b.id",
            "MATCH (a:S) OPTIONAL MATCH (b:N) WHERE b.uid = a.name + '' RETURN a.uid, b.id",
        ),
        (
            "MATCH (a:N {uid: 'u3'}) OPTIONAL MATCH (b:N) WHERE b.uid = a.uid + 'zz' \
             RETURN a.id, b.id",
            "MATCH (a:N {uid: 'u3'}) OPTIONAL MATCH (b:N) WHERE b.uid = a.uid + 'zz' + '' \
             RETURN a.id, b.id",
        ),
        // The bound value is a whole-entity reference afterwards.
        (
            "MATCH (a:N {uid: 'u42'}), (b:N) WHERE b.uid = a.name RETURN b",
            "MATCH (a:N {uid: 'u42'}), (b:N) WHERE b.uid = a.name + '' RETURN b",
        ),
    ]
}

/// Queries of [`a7_pairs`] the optimization must take (on a flushed store).
const A7_MUST_FIRE: [usize; 5] = [0, 1, 2, 3, 7];

async fn assert_a7_pairs(session: &Session, ctx: &str, check_ops: bool) -> Result<()> {
    for (i, (opt, reference)) in a7_pairs().into_iter().enumerate() {
        let got = bag(session, opt).await?;
        let want = bag(session, reference).await?;
        assert_eq!(got, want, "{ctx}: `{opt}` differs from `{reference}`");
        let ref_ops = plan_ops(session, reference).await;
        plan_shape::assert_avoids(&ref_ops, "PropertyLookupJoinExec", reference);
        if check_ops && A7_MUST_FIRE.contains(&i) {
            let ops = plan_ops(session, opt).await;
            plan_shape::assert_uses(&ops, "PropertyLookupJoinExec", opt);
        }
    }
    Ok(())
}

#[tokio::test]
async fn a7_lookup_join_matches_the_hash_join_flushed_and_l0() -> Result<()> {
    for flush in [true, false] {
        let db = fixture(400, flush).await?;
        let session = db.session();
        assert_a7_pairs(&session, &format!("flush={flush}"), flush).await?;

        // Mixed: flushed rows superseded by L0 writes. `u10` is renamed away
        // (its stale flushed row must not match) and a new `u10` appears.
        let tx = session.tx().await?;
        tx.execute("MATCH (n:N {uid: 'u10'}) SET n.uid = 'moved10'")
            .await?;
        tx.execute("CREATE (:N {id: 9999, uid: 'u10', name: 'u11', x: 10})")
            .await?;
        tx.execute("MATCH (n:N {uid: 'u17'}) DETACH DELETE n")
            .await?;
        tx.commit().await?;
        assert_a7_pairs(&session, &format!("flush={flush} +L0 writes"), false).await?;
        let got = bag(
            &session,
            "MATCH (a:N {uid: 'u9'}), (b:N) WHERE b.uid = a.name RETURN b.id",
        )
        .await?;
        assert_eq!(got, vec!["[Int(9999)]".to_string()], "flush={flush}");
        let got = bag(
            &session,
            "MATCH (a:N {uid: 'u16'}), (b:N) WHERE b.uid = a.name RETURN b.id",
        )
        .await?;
        assert!(
            got.is_empty(),
            "flush={flush}: deleted u17 still joined: {got:?}"
        );
    }
    Ok(())
}

/// Inside a transaction the probe must see the transaction's own writes and
/// none of the rows they supersede.
#[tokio::test]
async fn a7_lookup_join_inside_a_transaction() -> Result<()> {
    let db = fixture(400, true).await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:N {uid: 'u43'}) SET n.uid = 'gone43'")
        .await?;
    tx.execute("CREATE (:N {id: 7777, uid: 'u43', name: 'x', x: -1})")
        .await?;
    let q = "MATCH (a:N {uid: 'u42'}), (b:N) WHERE b.uid = a.name RETURN b.id";
    let r = tx.query_with(q).fetch_all().await?;
    assert_eq!(bag_of(r.rows()), vec!["[Int(7777)]".to_string()]);
    for (opt, reference) in a7_pairs() {
        let got = bag_of(tx.query_with(opt).fetch_all().await?.rows());
        let want = bag_of(tx.query_with(reference).fetch_all().await?.rows());
        assert_eq!(got, want, "in tx: `{opt}`");
    }
    let (_r, profile) = tx.query_with(q).profile().await?;
    plan_shape::assert_uses(&plan_shape::op_names(&profile), "PropertyLookupJoinExec", q);
    tx.rollback();
    Ok(())
}

/// On a fork, the probe reads through the fork's branch: fork writes are seen,
/// primary writes after the fork point are not.
#[tokio::test]
async fn a7_lookup_join_on_a_fork() -> Result<()> {
    let db = fixture(400, true).await?;
    let session = db.session();
    let fork = session.fork("a7").await?;
    let tx = fork.tx().await?;
    tx.execute("MATCH (n:N {uid: 'u43'}) SET n.uid = 'fork43'")
        .await?;
    tx.execute("CREATE (:N {id: 5555, uid: 'u43', name: 'x', x: -1})")
        .await?;
    tx.commit().await?;
    let tx = session.tx().await?;
    tx.execute("CREATE (:N {id: 6666, uid: 'u43', name: 'x', x: -1})")
        .await?;
    tx.commit().await?;

    let q = "MATCH (a:N {uid: 'u42'}), (b:N) WHERE b.uid = a.name RETURN b.id";
    assert_eq!(bag(&fork, q).await?, vec!["[Int(5555)]".to_string()]);
    plan_shape::assert_uses(&plan_ops(&fork, q).await, "PropertyLookupJoinExec", q);
    let mut primary = bag(&session, q).await?;
    primary.sort();
    assert_eq!(
        primary,
        vec!["[Int(43)]".to_string(), "[Int(6666)]".to_string()]
    );
    assert_a7_pairs(&fork, "fork", false).await?;
    fork.flush().await?;
    assert_eq!(bag(&fork, q).await?, vec!["[Int(5555)]".to_string()]);
    assert_a7_pairs(&fork, "fork flushed", false).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// A6 — named path anchored on the far end
// ---------------------------------------------------------------------------

/// Pairs of (anchored on the far end, unanchored reference). The reference
/// spells the equality through `+ ''`, which pins nothing, so it is planned in
/// written order — the plan whose `p` the reversed plan must reproduce.
fn a6_pairs() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "MATCH p = (a:N)-[:R]->(b:N {uid: 'u42'}) RETURN p",
            "MATCH p = (a:N)-[:R]->(b:N) WHERE b.uid + '' = 'u42' RETURN p",
        ),
        (
            "MATCH p = (a:N)-[r:R]->(b:N) WHERE b.uid = 'u42' \
             RETURN p, nodes(p), relationships(p), r, length(p)",
            "MATCH p = (a:N)-[r:R]->(b:N) WHERE b.uid + '' = 'u42' \
             RETURN p, nodes(p), relationships(p), r, length(p)",
        ),
        // Two hops, mixed directions, anonymous middle node and edge.
        (
            "MATCH p = (a:N)-[:R]->()<-[r2:R]-(b:N {uid: 'u42'}) RETURN p, r2",
            "MATCH p = (a:N)-[:R]->()<-[r2:R]-(b:N) WHERE b.uid + '' = 'u42' RETURN p, r2",
        ),
        // Undirected hop.
        (
            "MATCH p = (a:N)-[:R]-(b:N {uid: 'u42'}) RETURN p",
            "MATCH p = (a:N)-[:R]-(b:N) WHERE b.uid + '' = 'u42' RETURN p",
        ),
        // Three hops with a named middle node returned on its own.
        (
            "MATCH p = (a:N)-[:R]->(m:N)-[:R]->(c)-[:R]->(b:N {uid: 'u42'}) \
             RETURN p, m.id, c.id",
            "MATCH p = (a:N)-[:R]->(m:N)-[:R]->(c)-[:R]->(b:N) WHERE b.uid + '' = 'u42' \
             RETURN p, m.id, c.id",
        ),
        // OPTIONAL MATCH from a bound row, path anchored on its far end.
        (
            "MATCH (s:S {uid: 's2'}) OPTIONAL MATCH p = (a:N)-[:R]->(b:N {uid: 'u16'}) \
             RETURN s.uid, p",
            "MATCH (s:S {uid: 's2'}) OPTIONAL MATCH p = (a:N)-[:R]->(b:N) \
             WHERE b.uid + '' = 'u16' RETURN s.uid, p",
        ),
        (
            "OPTIONAL MATCH p = (a:N)-[:R]->(b:N {uid: 'nope'}) RETURN p",
            "OPTIONAL MATCH p = (a:N)-[:R]->(b:N) WHERE b.uid + '' = 'nope' RETURN p",
        ),
    ]
}

async fn assert_a6_pairs(session: &Session, ctx: &str) -> Result<()> {
    for (opt, reference) in a6_pairs() {
        let got = bag(session, opt).await?;
        let want = bag(session, reference).await?;
        assert!(
            !want.is_empty(),
            "{ctx}: reference `{reference}` returned nothing"
        );
        assert_eq!(got, want, "{ctx}: `{opt}` differs from `{reference}`");
    }
    Ok(())
}

/// A named path anchored on the far end starts at the anchor.
///
/// Before: `rows_scanned` = |N| + the walk (the scan started at `a`). After:
/// the one anchor row, at both scales — the same as the unnamed control.
#[tokio::test]
async fn a6_named_path_anchored_on_the_far_end_starts_at_the_anchor() -> Result<()> {
    for n in SCALES {
        let db = fixture(n, true).await?;
        let session = db.session();
        let named = "MATCH p = (a:N)-[:R]->(b:N {uid: 'u42'}) RETURN count(p) AS c";
        let ctl = "MATCH (a:N)-[:R]->(b:N {uid: 'u42'}) RETURN count(*) AS c";
        let (_, named_scanned, _) = counters(&session, named).await?;
        let (_, ctl_scanned, _) = counters(&session, ctl).await?;
        let c = session.query(named).await?.rows()[0].get::<i64>("c")?;
        // u42 is reached from u41 (i -> i+1) and from every i with 7i+3 ≡ 42.
        let expected = (0..n).filter(|&i| (i + 1) % n == 42).count()
            + (0..n).filter(|&i| (i * 7 + 3) % n == 42).count();
        assert_eq!(c, expected as i64, "N={n}");
        eprintln!("A6 N={n} named rows_scanned={named_scanned} ctl={ctl_scanned}");
        assert!(
            named_scanned <= ctl_scanned + 2,
            "N={n}: named path scanned {named_scanned} rows, unnamed control {ctl_scanned}"
        );
        assert!(
            named_scanned < n / 4,
            "N={n}: named path scanned {named_scanned}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a6_reversed_named_path_is_identical_flushed_l0_tx_fork() -> Result<()> {
    for flush in [true, false] {
        let db = fixture(400, flush).await?;
        let session = db.session();
        assert_a6_pairs(&session, &format!("flush={flush}")).await?;

        // Edges created after the flush live in L0 only.
        let tx = session.tx().await?;
        tx.execute("MATCH (a:N {uid: 'u300'}), (b:N {uid: 'u42'}) CREATE (a)-[:R {w: 77}]->(b)")
            .await?;
        let (opt, reference) = a6_pairs()[1];
        let got = bag_of(tx.query_with(opt).fetch_all().await?.rows());
        let want = bag_of(tx.query_with(reference).fetch_all().await?.rows());
        assert_eq!(got, want, "flush={flush} in tx");
        assert!(
            got.iter().any(|r| r.contains("77")),
            "tx edge missing: {got:?}"
        );
        tx.commit().await?;
        assert_a6_pairs(&session, &format!("flush={flush} +L0 edge")).await?;

        let fork = session.fork(&format!("a6_{flush}")).await?;
        let tx = fork.tx().await?;
        tx.execute("MATCH (a:N {uid: 'u301'}), (b:N {uid: 'u42'}) CREATE (b)-[:R {w: 88}]->(a)")
            .await?;
        tx.commit().await?;
        assert_a6_pairs(&fork, &format!("flush={flush} fork")).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// A10 — `{ext_id: …}` on a labelled node
// ---------------------------------------------------------------------------

/// `(n:N {ext_id: $x})` returns the one vertex, and reads O(1) rows at both
/// scales on a flushed store.
///
/// Was red for a correctness reason before a cost one. Probed 2026-10-18
/// (N=400 / N=1600, flushed): both spellings returned **no rows**, with
/// `rows_scanned` = N and `index_scans` = 0. A flush moves a string `ext_id`
/// into the per-label table's `ext_id` column and out of `overflow_json`, and
/// the labelled readers looked only in the blob. Fixed by folding the column
/// back in wherever a per-label table's undeclared properties are read
/// (`property_builder::restore_ext_id_column`, `columnar_scan.rs`), which
/// gave the right row at `rows_scanned` = N; the equality is now also pushed
/// to the column's BTree index: `rows_scanned` = 1, `index_scans` = 1 at both
/// scales. The full read matrix is `bugs::ext_id_on_labelled_nodes`.
#[tokio::test]
async fn a10_ext_id_equality_on_a_labelled_node() -> Result<()> {
    let mut failures = Vec::new();
    for n in SCALES {
        let db = fixture(n, true).await?;
        let session = db.session();
        for q in [
            "MATCH (n:N {ext_id: 'e42'}) RETURN n.id",
            "MATCH (n:N) WHERE n.ext_id = 'e42' RETURN n.id",
        ] {
            let (rows, scanned, index_scans) = counters(&session, q).await?;
            eprintln!("A10 N={n} rows={rows} rows_scanned={scanned} index_scans={index_scans} {q}");
            if bag(&session, q).await? != vec!["[Int(42)]".to_string()] || scanned > 2 {
                failures.push(format!("N={n}: {q}: rows={rows} rows_scanned={scanned}"));
            }
        }
        // The same vertex through the unlabelled spelling, and a CREATEd one
        // before and after a flush.
        assert_eq!(
            bag(&session, "MATCH (n {ext_id: 'e42'}) RETURN n.id").await?,
            vec!["[Int(42)]".to_string()]
        );
        let tx = session.tx().await?;
        tx.execute("CREATE (:N {id: -5, uid: 'c', ext_id: 'x5'})")
            .await?;
        tx.commit().await?;
        let q = "MATCH (n:N {ext_id: 'x5'}) RETURN n.id";
        let before = bag(&session, q).await?;
        db.flush().await?;
        let after = bag(&session, q).await?;
        if before != after {
            failures.push(format!(
                "N={n}: `{q}` before flush {before:?}, after {after:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    Ok(())
}
