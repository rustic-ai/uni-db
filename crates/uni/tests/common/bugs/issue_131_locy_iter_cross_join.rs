// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression for #131 — a recursive Locy rule keyed by a per-iteration anchor
//! (`(it:Iter)`) must NOT materialize an `Iter × <recursive-relation>` cross
//! join.
//!
//! The recursive clause re-introduces `it`/`sup` via `MATCH` and constrains them
//! with `(it, sup) IS reach TO a`. That predicate is a pure 3-way equi-join
//! (`it._vid = reach.it`, `sup._vid = reach.sup`, `a._vid = reach.b`), so the
//! recursive step must be recovered as a `HashJoinExec`, not a `CrossJoinExec`
//! + post-filter. When it degrades to a cross join, the per-pass row volume
//! grows quadratically in the number of `:Iter` anchors even though the derived
//! output (`reach`) grows only linearly.
//!
//! This test runs the program at increasing iteration counts, sums the
//! `CrossJoinExec` rows reported by the recursive stratum's per-iteration
//! profile, and asserts the row volume does NOT grow quadratically. It also
//! pins `reach`'s fact count at exactly `iters × buses` so the optimization is
//! proven semantics-preserving.

use uni_db::Uni;

const NUM_BUSES: usize = 5;

const REACH: &str = r#"
CREATE RULE reach AS
  MATCH (it:Iter),(sup:Super),(g:Gen)-[:AT]->(b:Bus)
  YIELD KEY it, KEY sup, KEY b
CREATE RULE reach AS
  MATCH (it:Iter),(sup:Super),(a:Bus)-[l:LINE]->(b:Bus)
  WHERE (it, sup) IS reach TO a
  YIELD KEY it, KEY sup, KEY b
CREATE RULE reach AS
  MATCH (it:Iter),(sup:Super),(a:Bus)<-[l:LINE]-(b:Bus)
  WHERE (it, sup) IS reach TO a
  YIELD KEY it, KEY sup, KEY b
"#;

/// Build the 5-bus line graph, one generator at bus 1, one `:Super`, and
/// `iters` `:Iter` anchors. Schemaless (the issue's repro uses no schema).
async fn build(iters: usize) -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let session = db.session();
    let tx = session.tx().await.unwrap();
    for bid in 1..=NUM_BUSES {
        tx.query_with("CREATE (:Bus {id: $i})")
            .param("i", bid as i64)
            .fetch_all()
            .await
            .unwrap();
    }
    for a in 1..NUM_BUSES {
        tx.query_with("MATCH (x:Bus {id:$a}),(y:Bus {id:$b}) CREATE (x)-[:LINE]->(y)")
            .param("a", a as i64)
            .param("b", (a + 1) as i64)
            .fetch_all()
            .await
            .unwrap();
    }
    tx.execute("MATCH (b:Bus {id:1}) CREATE (g:Gen)-[:AT]->(b)")
        .await
        .unwrap();
    tx.execute("CREATE (:Super)").await.unwrap();
    tx.query_with("UNWIND range(0, $hi) AS i CREATE (:Iter {idx: i})")
        .param("hi", (iters - 1) as i64)
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// Run `reach` under profiling; return (summed CrossJoinExec rows across the
/// recursive stratum's iterations, `reach` fact count).
async fn profile(iters: usize) -> (usize, usize) {
    let db = build(iters).await;
    let session = db.session();
    let (result, profile) = session.locy_with(REACH).profile().await.unwrap();

    let xj_rows: usize = profile
        .profile
        .strata
        .iter()
        .flat_map(|st| st.rules.iter())
        .flat_map(|r| r.iterations.iter())
        .flat_map(|it| it.operators.iter())
        .filter(|op| op.operator == "CrossJoinExec")
        .map(|op| op.actual_rows)
        .sum();

    // `LocyResult` derefs to `uni_locy::LocyResult`, whose `derived_facts`
    // returns the per-rule fact rows.
    let facts = result
        .derived_rule("reach")
        .await
        .unwrap()
        .map(|v| v.len())
        .unwrap_or(0);

    (xj_rows, facts)
}

#[tokio::test]
async fn issue_131_recursive_iter_is_not_quadratic() {
    // One-time: print the operator labels actually present so the
    // `"CrossJoinExec"` string is verifiable against the profiler's naming.
    {
        let db = build(10).await;
        let session = db.session();
        let (_r, p) = session.locy_with(REACH).profile().await.unwrap();
        let mut names: Vec<&str> = p
            .profile
            .strata
            .iter()
            .flat_map(|st| st.rules.iter())
            .flat_map(|r| r.iterations.iter())
            .flat_map(|it| it.operators.iter())
            .map(|op| op.operator.as_str())
            .collect();
        names.sort_unstable();
        names.dedup();
        eprintln!("operator labels present: {names:?}");
    }

    let (xj10, f10) = profile(10).await;
    let (xj20, f20) = profile(20).await;
    let (xj40, f40) = profile(40).await;

    eprintln!("iters= 10  CrossJoinExec_rows={xj10:>8}  reach_facts={f10}");
    eprintln!("iters= 20  CrossJoinExec_rows={xj20:>8}  reach_facts={f20}");
    eprintln!("iters= 40  CrossJoinExec_rows={xj40:>8}  reach_facts={f40}");

    // Correctness is independent of the join strategy: every iteration reaches
    // every bus, so `reach` = iters × buses, exactly.
    assert_eq!(f10, 10 * NUM_BUSES, "reach fact count wrong at N=10");
    assert_eq!(f20, 20 * NUM_BUSES, "reach fact count wrong at N=20");
    assert_eq!(f40, 40 * NUM_BUSES, "reach fact count wrong at N=40");

    // The defect: CrossJoinExec rows ≈ k·N² (≈4× per doubling). The fix turns
    // the IS-ref into a HashJoin, so its CrossJoinExec contribution collapses to
    // ~0 and total cross-join volume scales (at worst) linearly. A 4× input
    // jump (N=10→40) must not produce a ~16× row jump.
    //
    // Guard against div-by-zero / trivially-zero baselines by requiring the
    // observed growth ratio to stay well under quadratic.
    let quadratic_ratio = (40.0 / 10.0_f64).powi(2); // 16×
    let observed = if xj10 == 0 {
        // No cross-join rows at the smallest size already means the IS-ref join
        // is not a cross product — that's the fixed state.
        0.0
    } else {
        xj40 as f64 / xj10 as f64
    };
    assert!(
        observed < quadratic_ratio * 0.5,
        "CrossJoinExec rows scale quadratically: N=10 -> {xj10}, N=40 -> {xj40} \
         (ratio {observed:.2}×, quadratic would be {quadratic_ratio:.0}×). \
         The recursive IS-ref join is a CrossJoinExec instead of a HashJoinExec (#131)."
    );
}
