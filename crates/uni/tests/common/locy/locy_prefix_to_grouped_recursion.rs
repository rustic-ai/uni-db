// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! REQ-1 / REQ-5b regression tests (uniscape × uni-db gap report).
//!
//! # REQ-1 — per-group recursive transitive closure via prefix-`TO`
//!
//! A recursive Locy rule whose KEY carries a *grouping* dimension (here the
//! Monte-Carlo iteration `it`) used to stall at a single hop: the only way to
//! grow the recursion frontier was the scalar `mid IS reach TO b` form, and the
//! tuple-subject form `(it, m, b) IS reach` only *constrained* — it never bound
//! a fresh value out of the recursive relation. The fix adds a tuple-subject +
//! `TO` grammar alternative, `(it, m) IS reach TO b`, which reuses the existing
//! growth-binding machinery (the target binds to the KEY column after the
//! subjects). These tests assert the recursion reaches a per-group fixpoint
//! (multi-hop) and that independent groups stay partitioned.
//!
//! # REQ-5b — `AND` between IS predicates
//!
//! `WHERE (it,a,b) IS x AND (it,a,b) IS y` used to be a parse error ("AND is a
//! reserved keyword"); only comma separation worked. The grammar now accepts
//! `AND` as an alias for the comma separator in a rule `WHERE`.

use std::time::Duration;

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::{LocyConfig, LocyResult};

fn default_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

/// Two iterations over three buses A→B→C, with edges *partitioned by iteration*
/// through a `LINE.it` property:
///   - iteration 0: A→B and B→C  (a 2-hop chain)
///   - iteration 1: A→B only
///
/// So the per-iteration transitive closure must be:
///   - it 0: (A,B), (B,C), (A,C)   ← (A,C) only exists if recursion composes
///   - it 1: (A,B)                 ← (A,C) must NOT leak in from it 0
async fn build_grid(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (i0:Iter {idx: 0}), (i1:Iter {idx: 1}),
                (a:Bus {name: 'A'}), (b:Bus {name: 'B'}), (c:Bus {name: 'C'}),
                (a)-[:LINE {it: 0}]->(b),
                (b)-[:LINE {it: 0}]->(c),
                (a)-[:LINE {it: 1}]->(b)",
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Count derived facts for a rule whose grouping KEY's first column equals
/// `it_vid` — i.e. how many reach pairs belong to one iteration. The KEY
/// columns are `it`, `a`, `b`; the `it` value is the Iter node's identity.
fn reach_count_total(result: &LocyResult) -> usize {
    result.derived.get("reach").map(|v| v.len()).unwrap_or(0)
}

/// REQ-1 core: `(it, a) IS reach TO m, (it, m) IS active TO b` reaches the
/// per-group fixpoint.
///
/// The total derived `reach` count uniquely discriminates the three behaviours
/// on this fixture (it 0: A→B→C, it 1: A→B):
///   - **4** ⇒ correct: it0 = {AB, BC, AC}, it1 = {AB}. The transitive (A,C)
///     pair proves multi-hop composition; its absence from it1 proves the
///     carried `it` key keeps groups partitioned.
///   - **3** ⇒ the historical one-hop stall (it0 = {AB, BC}, missing AC).
///   - **5** ⇒ a cross-group leak (it1 wrongly gains AC from it0's B→C edge).
#[tokio::test]
async fn prefix_to_grouped_recursion_reaches_fixpoint() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    build_grid(&db).await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE active AS \
               MATCH (it:Iter),(a:Bus)-[e:LINE]->(b:Bus) \
               WHERE toInteger(e.it) = toInteger(it.idx) \
               YIELD KEY it, KEY a, KEY b \n\
             CREATE RULE reach AS \
               MATCH (it:Iter),(a:Bus),(b:Bus) WHERE (it,a,b) IS active \
               YIELD KEY it, KEY a, KEY b \n\
             CREATE RULE reach AS \
               MATCH (it:Iter),(a:Bus) WHERE (it,a) IS reach TO m, (it,m) IS active TO b \
               YIELD KEY it, KEY a, KEY b",
        )
        .with_config(default_config())
        .run()
        .await?;

    let total = reach_count_total(&result);
    assert_eq!(
        total,
        4,
        "per-group transitive closure must derive 4 facts \
         (it0: AB,BC,AC; it1: AB). Got {total} — 3 means the recursion stalled at one hop \
         (missing the transitive AC pair), 5 means iteration groups leaked. Facts: {:?}",
        result.derived.get("reach")
    );
    Ok(())
}

/// Regression: the prefix-`TO` form on a *2-key* relation (degenerate group of
/// one carried key) must agree with the scalar `TO` form. Uses a single
/// iteration so `(it,a) IS reach TO b` reduces to ordinary transitive closure.
#[tokio::test]
async fn prefix_to_single_group_matches_scalar_to() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (i0:Iter {idx: 0}),
                (a:Bus {name: 'A'}), (b:Bus {name: 'B'}),
                (c:Bus {name: 'C'}), (d:Bus {name: 'D'}),
                (a)-[:LINE {it: 0}]->(b),
                (b)-[:LINE {it: 0}]->(c),
                (c)-[:LINE {it: 0}]->(d)",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE active AS \
               MATCH (it:Iter),(a:Bus)-[e:LINE]->(b:Bus) \
               WHERE toInteger(e.it) = toInteger(it.idx) \
               YIELD KEY it, KEY a, KEY b \n\
             CREATE RULE reach AS \
               MATCH (it:Iter),(a:Bus),(b:Bus) WHERE (it,a,b) IS active \
               YIELD KEY it, KEY a, KEY b \n\
             CREATE RULE reach AS \
               MATCH (it:Iter),(a:Bus) WHERE (it,a) IS reach TO m, (it,m) IS active TO b \
               YIELD KEY it, KEY a, KEY b",
        )
        .with_config(default_config())
        .run()
        .await?;

    let reach = result.derived.get("reach").expect("rule 'reach' missing");
    // 4-node chain A→B→C→D in a single iteration: 6 reachable pairs.
    assert_eq!(
        reach.len(),
        6,
        "single-group prefix-TO closure on a 4-chain must derive 6 pairs, got {}: {:?}",
        reach.len(),
        reach
    );
    Ok(())
}

/// REQ-5b: `AND` between two IS predicates parses and evaluates identically to
/// the comma-separated form. `(it,a,b) IS active AND (it,a,b) IS reach` selects
/// exactly `active` (active ⊆ reach).
#[tokio::test]
async fn and_between_is_predicates_equals_comma() -> Result<()> {
    let program = |sep: &str| {
        format!(
            "CREATE RULE active AS \
               MATCH (it:Iter),(a:Bus)-[e:LINE]->(b:Bus) \
               WHERE toInteger(e.it) = toInteger(it.idx) \
               YIELD KEY it, KEY a, KEY b \n\
             CREATE RULE reach AS \
               MATCH (it:Iter),(a:Bus),(b:Bus) WHERE (it,a,b) IS active \
               YIELD KEY it, KEY a, KEY b \n\
             CREATE RULE reach AS \
               MATCH (it:Iter),(a:Bus) WHERE (it,a) IS reach TO m, (it,m) IS active TO b \
               YIELD KEY it, KEY a, KEY b \n\
             CREATE RULE both AS \
               MATCH (it:Iter),(a:Bus),(b:Bus) \
               WHERE (it,a,b) IS active {sep} (it,a,b) IS reach \
               YIELD KEY it, KEY a, KEY b"
        )
    };

    let db_and = Uni::in_memory().build().await?;
    build_grid(&db_and).await?;
    let res_and = db_and
        .session()
        .locy_with(&program("AND"))
        .with_config(default_config())
        .run()
        .await?;

    let db_comma = Uni::in_memory().build().await?;
    build_grid(&db_comma).await?;
    let res_comma = db_comma
        .session()
        .locy_with(&program(","))
        .with_config(default_config())
        .run()
        .await?;

    let n_and = res_and
        .derived
        .get("both")
        .expect("rule 'both' missing")
        .len();
    let n_comma = res_comma
        .derived
        .get("both")
        .expect("rule 'both' missing")
        .len();

    // `both` = active (active ⊆ reach): 3 active facts (A→B it0, B→C it0, A→B it1).
    assert_eq!(
        n_and, 3,
        "AND-conjoined IS predicates derived {n_and} facts, expected 3"
    );
    assert_eq!(
        n_and, n_comma,
        "AND form ({n_and}) and comma form ({n_comma}) must agree"
    );
    Ok(())
}
