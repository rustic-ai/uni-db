// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A positive `IS` reference to a PROB rule is a probabilistic condition.
//!
//! The row holds only when the referenced fact does, so the referenced
//! probability is multiplied into the row's PROB value (default mode), and
//! exact mode evaluates each fact as `OP over rows of (row event AND each
//! referenced fact)` under possible-worlds semantics. A clause whose PROB
//! expression already reads the referenced probability as a product factor
//! counts it once.
//!
//! Every expected value is checked against a brute-force enumeration of the
//! possible worlds of the graph's independent edges.

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{DataType, Uni, Value};

/// `reach` over `E` edges, folding the first hop's weight.
const REACH: &str = "CREATE RULE reach AS \
     MATCH (a:N)-[e:E]->(b:N) \
     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
     CREATE RULE reach AS \
     MATCH (a:N)-[e:E]->(mid:N) WHERE mid IS reach TO b \
     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
     QUERY reach RETURN a.name AS src, b.name AS dst, prob";

/// The same reachability with `MPROD`.
const REACH_MPROD: &str = "CREATE RULE reach AS \
     MATCH (a:N)-[e:E]->(b:N) \
     FOLD prob = MPROD(e.p) YIELD KEY a, KEY b, prob \n\
     CREATE RULE reach AS \
     MATCH (a:N)-[e:E]->(mid:N) WHERE mid IS reach TO b \
     FOLD prob = MPROD(e.p) YIELD KEY a, KEY b, prob \n\
     QUERY reach RETURN a.name AS src, b.name AS dst, prob";

/// The diamond s -0.3-> m -0.5-> t, s -0.7-> t.
const DIAMOND: &[(&str, &str, f64)] = &[("s", "t", 0.7), ("s", "m", 0.3), ("m", "t", 0.5)];

async fn seeded(edges: &[(&str, &str, f64)]) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("E", &["N"], &["N"])
        .property("p", DataType::Float64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    let mut names: Vec<&str> = edges.iter().flat_map(|(a, b, _)| [*a, *b]).collect();
    names.sort_unstable();
    names.dedup();
    for n in names {
        tx.execute(&format!("CREATE (:N {{name: '{n}'}})")).await?;
    }
    for (a, b, p) in edges {
        tx.execute(&format!(
            "MATCH (x:N {{name:'{a}'}}), (y:N {{name:'{b}'}}) CREATE (x)-[:E {{p: {p}}}]->(y)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// A program's `(src, dst) -> prob` rows and warning codes.
struct Outcome {
    probs: Vec<((String, String), f64)>,
    warnings: Vec<String>,
}

impl Outcome {
    fn prob(&self, src: &str, dst: &str) -> f64 {
        self.probs
            .iter()
            .find(|((s, d), _)| s == src && d == dst)
            .map(|(_, p)| *p)
            .unwrap_or_else(|| panic!("no fact for ({src}, {dst}) in {:?}", self.probs))
    }

    fn warned(&self, code: &str) -> bool {
        self.warnings.iter().any(|w| w.contains(code))
    }
}

async fn run(db: &Uni, program: &str, exact: bool) -> Result<Outcome> {
    let config = LocyConfig {
        max_iterations: 1000,
        exact_probability: exact,
        ..Default::default()
    };
    let result = db
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await?;
    let probs = result
        .rows()
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    let name =
                        |c: &str| r.get(c).and_then(Value::as_str).unwrap_or("?").to_string();
                    let p = r.get("prob").and_then(Value::as_f64).unwrap_or(f64::NAN);
                    ((name("src"), name("dst")), p)
                })
                .collect()
        })
        .unwrap_or_default();
    let warnings = result
        .warnings()
        .iter()
        .map(|w| format!("{:?}", w.code))
        .collect();
    Ok(Outcome { probs, warnings })
}

/// P(`dst` reachable from `src`) over the possible worlds of `edges`, each
/// present independently with its probability.
fn reach_probability(edges: &[(&str, &str, f64)], src: &str, dst: &str) -> f64 {
    let mut total = 0.0;
    for world in 0u32..(1 << edges.len()) {
        let mut weight = 1.0;
        for (i, (_, _, p)) in edges.iter().enumerate() {
            weight *= if world & (1 << i) != 0 { *p } else { 1.0 - p };
        }
        // Reachable by a path of at least one edge.
        let mut frontier = vec![src];
        let mut seen: Vec<&str> = Vec::new();
        while let Some(node) = frontier.pop() {
            for (i, (a, b, _)) in edges.iter().enumerate() {
                if world & (1 << i) != 0 && *a == node && !seen.contains(b) {
                    seen.push(b);
                    frontier.push(b);
                }
            }
        }
        if seen.contains(&dst) {
            total += weight;
        }
    }
    total
}

fn assert_close(actual: f64, expected: f64, what: &str) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "{what}: expected {expected}, got {actual}"
    );
}

/// The diamond: X ∨ (Y ∧ Z) = 1 − 0.3 · (1 − 0.15) = 0.745 in both modes.
/// Its two proofs share no edge, so there is no shared-lineage warning, and
/// independence evaluation is already exact.
#[tokio::test]
async fn diamond_mnor_counts_the_referenced_probability() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    assert_close(
        reach_probability(DIAMOND, "s", "t"),
        0.745,
        "possible worlds",
    );
    for exact in [false, true] {
        let out = run(&db, REACH, exact).await?;
        assert_close(
            out.prob("s", "t"),
            0.745,
            &format!("reach(s,t), exact={exact}"),
        );
        assert_close(out.prob("s", "m"), 0.3, "reach(s,m)");
        assert_close(out.prob("m", "t"), 0.5, "reach(m,t)");
        assert!(
            !out.warned("SharedProbabilisticDependency"),
            "the diamond's proofs share nothing: {:?}",
            out.warnings
        );
    }
    Ok(())
}

/// `MPROD` is the conjunction of the rows: X ∧ (Y ∧ Z) = 0.7 · 0.3 · 0.5.
#[tokio::test]
async fn diamond_mprod_counts_the_referenced_probability() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    for exact in [false, true] {
        let out = run(&db, REACH_MPROD, exact).await?;
        assert_close(
            out.prob("s", "t"),
            0.105,
            &format!("reach(s,t), exact={exact}"),
        );
    }
    Ok(())
}

/// Two parallel s→t edges at 0.5: (X1 ∨ X2) ∨ (Y ∧ Z) = 1 − 0.25 · 0.85.
#[tokio::test]
async fn parallel_equal_weight_edges_are_two_events() -> Result<()> {
    let edges = [
        ("s", "t", 0.5),
        ("s", "t", 0.5),
        ("s", "m", 0.3),
        ("m", "t", 0.5),
    ];
    let db = seeded(&edges).await?;
    assert_close(
        reach_probability(&edges, "s", "t"),
        0.7875,
        "possible worlds",
    );
    for exact in [false, true] {
        let out = run(&db, REACH, exact).await?;
        assert_close(
            out.prob("s", "t"),
            0.7875,
            &format!("reach(s,t), exact={exact}"),
        );
    }
    Ok(())
}

/// Two proofs of reach(A,D) both need C→D: exact mode counts it once.
///
/// Possible worlds: CD ∧ (AC ∨ (AB ∧ BC)) = 0.6 · (1 − 0.7 · 0.8) = 0.264.
/// Independence treats the two proofs (0.5 · 0.24 and 0.3 · 0.6) as
/// independent: 1 − 0.88 · 0.82 = 0.2784.
#[tokio::test]
async fn shared_edge_is_corrected_by_exact_mode_only() -> Result<()> {
    let edges = [
        ("A", "B", 0.5),
        ("A", "C", 0.3),
        ("B", "C", 0.4),
        ("C", "D", 0.6),
    ];
    let db = seeded(&edges).await?;
    assert_close(
        reach_probability(&edges, "A", "D"),
        0.264,
        "possible worlds",
    );
    let default = run(&db, REACH, false).await?;
    assert_close(default.prob("A", "D"), 0.2784, "independence");
    assert!(default.warned("SharedProbabilisticDependency"));
    let exact = run(&db, REACH, true).await?;
    assert_close(exact.prob("A", "D"), 0.264, "exact");
    assert!(exact.warned("SharedProbabilisticDependency"));
    assert!(!exact.warned("BddLimitExceeded"));
    Ok(())
}

/// A cycle: a derivation through itself adds nothing in exact mode.
///
/// Paths s→t and s→a→t: 1 − 0.8 · 0.75 = 0.4.
#[tokio::test]
async fn cycle_is_exact_in_exact_mode() -> Result<()> {
    let edges = [
        ("s", "a", 0.5),
        ("a", "s", 0.5),
        ("a", "t", 0.5),
        ("s", "t", 0.2),
    ];
    let db = seeded(&edges).await?;
    assert_close(reach_probability(&edges, "s", "t"), 0.4, "possible worlds");
    let exact = run(&db, REACH, true).await?;
    assert_close(exact.prob("s", "t"), 0.4, "exact");
    assert!(exact.warned("SharedProbabilisticDependency"));
    Ok(())
}

/// A clause that reads the referenced probability as its fold input counts
/// it once (`FOLD p = MNOR(p)`): the hop itself carries no weight.
///
/// risk(A,D) via B and via C both need C→D (0.6): the independence value is
/// 1 − 0.4 · 0.4 = 0.84, the possible-worlds value 0.6.
#[tokio::test]
async fn a_fold_over_the_referenced_probability_counts_it_once() -> Result<()> {
    let edges = [
        ("A", "B", 0.5),
        ("A", "C", 0.3),
        ("B", "C", 0.4),
        ("C", "D", 0.6),
    ];
    let db = seeded(&edges).await?;
    let program = "CREATE RULE risk AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD p = MNOR(e.p) YIELD KEY a, KEY b, p \n\
         CREATE RULE risk AS \
         MATCH (a:N)-[:E]->(mid:N) WHERE mid IS risk TO b \
         FOLD p = MNOR(p) YIELD KEY a, KEY b, p \n\
         QUERY risk RETURN a.name AS src, b.name AS dst, p AS prob";
    let default = run(&db, program, false).await?;
    assert_close(default.prob("A", "D"), 0.84, "independence");
    // risk(A,C): direct 0.3, via B risk(B,C) = 0.4: 1 − 0.7 · 0.6.
    assert_close(default.prob("A", "C"), 0.58, "risk(A,C)");
    assert!(!default.warned("ProbabilityReadNotFactor"));
    let exact = run(&db, program, true).await?;
    assert_close(exact.prob("A", "D"), 0.6, "exact");
    Ok(())
}

/// A product with the referenced probability as one factor counts it once:
/// `MNOR(e.p * prob)` over the diamond is the same 0.745.
#[tokio::test]
async fn a_product_factor_counts_the_referenced_probability_once() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let program = "CREATE RULE hop AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD w = MNOR(e.p) YIELD KEY a, KEY b, w \n\
         CREATE RULE reach2 AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
         CREATE RULE reach2 AS \
         MATCH (a:N)-[e:E]->(mid:N) WHERE (mid, b, w) IS hop \
         FOLD prob = MNOR(e.p * w) YIELD KEY a, KEY b, prob \n\
         QUERY reach2 RETURN a.name AS src, b.name AS dst, prob";
    for exact in [false, true] {
        let out = run(&db, program, exact).await?;
        // s→t direct 0.7; s→m→t through hop(m,t) = 0.5: 0.3 · 0.5 once.
        assert_close(out.prob("s", "t"), 0.745, &format!("exact={exact}"));
        assert!(!out.warned("ProbabilityReadNotFactor"));
    }
    Ok(())
}

/// A read of the referenced probability that is not one product factor is
/// not multiplied in again, and is reported.
#[tokio::test]
async fn a_non_factor_read_is_not_multiplied_and_is_reported() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let program = "CREATE RULE hop AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD w = MNOR(e.p) YIELD KEY a, KEY b, w \n\
         CREATE RULE miss AS \
         MATCH (a:N)-[e:E]->(b:N) WHERE (a, b, w) IS hop \
         FOLD prob = MNOR(1.0 - w) YIELD KEY a, KEY b, prob \n\
         QUERY miss RETURN a.name AS src, b.name AS dst, prob";
    let out = run(&db, program, false).await?;
    assert!(out.warned("ProbabilityReadNotFactor"), "{:?}", out.warnings);
    // 1 − 0.7, not (1 − 0.7) · 0.7.
    assert_close(out.prob("s", "t"), 0.3, "miss(s,t)");
    Ok(())
}

/// A non-fold PROB rule over a positive reference carries the referenced
/// probability, the mirror of `IS NOT`'s complement: the two sum to one.
#[tokio::test]
async fn positive_reference_mirrors_the_complement() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let rules = "CREATE RULE risky AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD r = MNOR(e.p) YIELD KEY a, r \n\
         CREATE RULE flagged AS \
         MATCH (a:N) WHERE a IS risky \
         YIELD KEY a, 1.0 AS prob PROB \n\
         CREATE RULE clear AS \
         MATCH (a:N) WHERE a IS NOT risky \
         YIELD KEY a, 1.0 AS prob PROB \n";
    let flagged = run(
        &db,
        &format!("{rules}QUERY flagged RETURN a.name AS src, 'x' AS dst, prob"),
        false,
    )
    .await?;
    let clear = run(
        &db,
        &format!("{rules}QUERY clear RETURN a.name AS src, 'x' AS dst, prob"),
        false,
    )
    .await?;
    // risky(s) = 1 − 0.3 · 0.7 = 0.79; risky(m) = 0.5.
    assert_close(flagged.prob("s", "x"), 0.79, "flagged(s)");
    assert_close(clear.prob("s", "x"), 0.21, "clear(s)");
    assert_close(
        flagged.prob("m", "x") + clear.prob("m", "x"),
        1.0,
        "m sums to one",
    );
    // t has no outgoing edge: not risky at all.
    assert_close(clear.prob("t", "x"), 1.0, "clear(t)");
    Ok(())
}

/// Clauses of one rule that differ in what their lineage columns hold — a
/// row event over an edge, a constant weight (no event), a reference in one
/// clause only — still union into one relation.
#[tokio::test]
async fn clauses_with_and_without_lineage_columns_union() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let program = "CREATE RULE base AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
         CREATE RULE two AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
         CREATE RULE two AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD prob = MNOR(0.5) YIELD KEY a, KEY b, prob \n\
         CREATE RULE two AS \
         MATCH (a:N)-[e:E]->(mid:N) WHERE mid IS base TO b \
         FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
         QUERY two RETURN a.name AS src, b.name AS dst, prob";
    for exact in [false, true] {
        let out = run(&db, program, exact).await?;
        // 0.7 direct, 0.5 per edge, 0.3 · 0.5 through base(m,t).
        assert_close(
            out.prob("s", "t"),
            1.0 - 0.3 * 0.5 * 0.85,
            &format!("two(s,t), exact={exact}"),
        );
    }
    Ok(())
}

/// A small deterministic generator of graphs.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

const NODES: [&str; 5] = ["n0", "n1", "n2", "n3", "n4"];

/// Over random small graphs (cycles, parallel edges and self-loops
/// included): exact mode equals the possible-worlds probability of every
/// pair, and wherever no shared-lineage warning is raised the default mode
/// equals it too.
#[tokio::test]
async fn exact_matches_possible_worlds_and_default_agrees_without_sharing() -> Result<()> {
    let mut rng = Lcg(0x5eed_1234);
    let mut agreed = 0;
    let mut shared = 0;
    for _ in 0..24 {
        let node_count = 3 + rng.next(3) as usize;
        let edge_count = 2 + rng.next(5) as usize;
        let edges: Vec<(&str, &str, f64)> = (0..edge_count)
            .map(|_| {
                let a = NODES[rng.next(node_count as u64) as usize];
                let b = NODES[rng.next(node_count as u64) as usize];
                let p = (1 + rng.next(9)) as f64 / 10.0;
                (a, b, p)
            })
            .collect();
        let db = seeded(&edges).await?;
        let exact = run(&db, REACH, true).await?;
        let default = run(&db, REACH, false).await?;
        for ((src, dst), p) in &exact.probs {
            let worlds = reach_probability(&edges, src, dst);
            assert_close(
                *p,
                worlds,
                &format!("exact reach({src},{dst}) over {edges:?}"),
            );
        }
        if default.warned("SharedProbabilisticDependency") {
            shared += 1;
        } else {
            agreed += 1;
            for ((src, dst), p) in &default.probs {
                assert_close(
                    *p,
                    reach_probability(&edges, src, dst),
                    &format!("default reach({src},{dst}) over {edges:?} (no sharing)"),
                );
            }
        }
    }
    // Both arms must be exercised for the property to mean anything.
    assert!(agreed > 0 && shared > 0, "agreed {agreed}, shared {shared}");
    Ok(())
}
