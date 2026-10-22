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

// ── Negation inside exact formulas ─────────────────────────────────────────

/// One possible world of a graph's edges: each edge present or not.
struct World<'a> {
    edges: &'a [(&'a str, &'a str, f64)],
    mask: u32,
}

impl World<'_> {
    /// Some `a → b` edge is present.
    fn link(&self, a: &str, b: &str) -> bool {
        self.edges
            .iter()
            .enumerate()
            .any(|(i, (x, y, _))| self.mask & (1 << i) != 0 && *x == a && *y == b)
    }

    /// `b` is reachable from `a` by a path of at least one present edge.
    fn reach(&self, a: &str, b: &str) -> bool {
        let mut frontier = vec![a];
        let mut seen: Vec<&str> = Vec::new();
        while let Some(node) = frontier.pop() {
            for (i, (x, y, _)) in self.edges.iter().enumerate() {
                if self.mask & (1 << i) != 0 && *x == node && !seen.contains(y) {
                    seen.push(y);
                    frontier.push(y);
                }
            }
        }
        seen.contains(&b)
    }
}

/// The probability, over the possible worlds of `edges`, that `holds`.
fn worlds_probability(edges: &[(&str, &str, f64)], holds: impl Fn(&World<'_>) -> bool) -> f64 {
    let mut total = 0.0;
    for mask in 0u32..(1 << edges.len()) {
        let world = World { edges, mask };
        if holds(&world) {
            let mut weight = 1.0;
            for (i, (_, _, p)) in edges.iter().enumerate() {
                weight *= if mask & (1 << i) != 0 { *p } else { 1.0 - p };
            }
            total += weight;
        }
    }
    total
}

/// `link`: one fact per connected pair, the noisy-OR of its edges.
const LINK: &str = "CREATE RULE link AS \
     MATCH (a:N)-[e:E]->(b:N) \
     FOLD p = MNOR(e.p) YIELD KEY a, KEY b, p \n";

/// A rule `odd` over `reach` and `link` with an `IS NOT` whose negated facts
/// can share edges with the positive part, and its possible-worlds meaning.
struct NegatedProgram {
    /// The `odd` rule's text.
    rule: &'static str,
    /// Whether `odd` keys on a middle node `m` too.
    has_mid: bool,
    /// `odd(a, m, b)` in one world (`m` is ignored without a middle key).
    holds: fn(&World<'_>, &str, &str, &str) -> bool,
}

const NEGATED_PROGRAMS: &[NegatedProgram] = &[
    // reach(a, b) AND NOT link(m, b)
    NegatedProgram {
        rule: "CREATE RULE odd AS \
             MATCH (a:N), (m:N) WHERE a IS reach TO b, m IS NOT link TO b \
             FOLD prob = MNOR(1.0) YIELD KEY a, KEY m, KEY b, prob \n",
        has_mid: true,
        holds: |w, a, m, b| w.reach(a, b) && !w.link(m, b),
    },
    // reach(a, b) AND NOT link(a, b): reachable, but not directly
    NegatedProgram {
        rule: "CREATE RULE odd AS \
             MATCH (a:N) WHERE a IS reach TO b, a IS NOT link TO b \
             FOLD prob = MNOR(1.0) YIELD KEY a, KEY b, prob \n",
        has_mid: false,
        holds: |w, a, _, b| w.reach(a, b) && !w.link(a, b),
    },
    // reach(a, b) AND NOT reach(b, a)
    NegatedProgram {
        rule: "CREATE RULE odd AS \
             MATCH (a:N) WHERE a IS reach TO b, b IS NOT reach TO a \
             FOLD prob = MNOR(1.0) YIELD KEY a, KEY b, prob \n",
        has_mid: false,
        holds: |w, a, _, b| w.reach(a, b) && !w.reach(b, a),
    },
    // an edge a → b AND NOT reach(a, b): the edge is part of the negated
    // fact's lineage, so the conjunction is impossible
    NegatedProgram {
        rule: "CREATE RULE odd AS \
             MATCH (a:N)-[e:E]->(b:N) WHERE a IS NOT reach TO b \
             FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n",
        has_mid: false,
        holds: |w, a, _, b| w.link(a, b) && !w.reach(a, b),
    },
];

/// The `reach` rules alone, without their `QUERY`.
fn reach_rules() -> &'static str {
    REACH.split("QUERY").next().expect("REACH has rules")
}

/// A program's `odd` facts as `((a, m, b), prob)` (`m` empty without a
/// middle key), and its warning codes.
async fn run_odd(
    db: &Uni,
    case: &NegatedProgram,
    config: LocyConfig,
) -> Result<(Vec<((String, String, String), f64)>, Vec<String>)> {
    let ret = if case.has_mid {
        "QUERY odd RETURN a.name AS src, m.name AS mid, b.name AS dst, prob"
    } else {
        "QUERY odd RETURN a.name AS src, '' AS mid, b.name AS dst, prob"
    };
    let program = format!("{}\n{LINK}{}{ret}", reach_rules(), case.rule);
    let result = db
        .session()
        .locy_with(&program)
        .with_config(config)
        .run()
        .await?;
    let rows = result
        .rows()
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    let name =
                        |c: &str| r.get(c).and_then(Value::as_str).unwrap_or("?").to_string();
                    let p = r.get("prob").and_then(Value::as_f64).unwrap_or(f64::NAN);
                    ((name("src"), name("mid"), name("dst")), p)
                })
                .collect()
        })
        .unwrap_or_default();
    let warnings = result
        .warnings()
        .iter()
        .map(|w| format!("{:?}", w.code))
        .collect();
    Ok((rows, warnings))
}

fn mode(exact: bool) -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        exact_probability: exact,
        ..Default::default()
    }
}

/// `reach(s, t) AND NOT link(m, t)` on the diamond: both rest on `m → t`.
///
/// Possible worlds: (X ∨ (Y ∧ Z)) ∧ ¬Z = X ∧ ¬Z = 0.7 · 0.5 = 0.35, where X
/// = s→t, Y = s→m, Z = m→t. Default mode multiplies the independent factors,
/// 0.745 · (1 − 0.5) = 0.3725, and must say it is not exact.
#[tokio::test]
async fn negated_fact_sharing_an_edge_is_a_negated_literal_in_exact_mode() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let case = &NEGATED_PROGRAMS[0];
    let expected = worlds_probability(DIAMOND, |w| (case.holds)(w, "s", "m", "t"));
    assert_close(expected, 0.35, "possible worlds");
    let key = ("s".to_string(), "m".to_string(), "t".to_string());
    let (exact, _) = run_odd(&db, case, mode(true)).await?;
    let p = exact.iter().find(|(k, _)| *k == key).map(|(_, p)| *p);
    assert_close(p.expect("odd(s, m, t)"), 0.35, "exact odd(s, m, t)");
    let (default, warnings) = run_odd(&db, case, mode(false)).await?;
    let p = default.iter().find(|(k, _)| *k == key).map(|(_, p)| *p);
    assert_close(p.expect("odd(s, m, t)"), 0.3725, "default odd(s, m, t)");
    assert!(
        warnings
            .iter()
            .any(|w| w == "SharedProbabilisticDependency"),
        "the negated fact shares m → t with the positive one: {warnings:?}"
    );
    Ok(())
}

/// An edge and the negation of a reachability resting on it cannot both
/// hold: exact mode gives 0, default mode `p · (1 − reach)` and a warning.
#[tokio::test]
async fn an_edge_and_the_negation_of_its_own_reachability_is_impossible() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let case = &NEGATED_PROGRAMS[3];
    let (exact, _) = run_odd(&db, case, mode(true)).await?;
    for (key, p) in &exact {
        assert_close(*p, 0.0, &format!("exact odd{key:?}"));
    }
    assert!(!exact.is_empty());
    let (_, warnings) = run_odd(&db, case, mode(false)).await?;
    assert!(
        warnings
            .iter()
            .any(|w| w == "SharedProbabilisticDependency"),
        "{warnings:?}"
    );
    Ok(())
}

/// Over random small graphs and programs with `IS NOT` over lineage shared
/// with the positive part: exact mode equals the possible-worlds probability
/// of every fact, every fact of nonzero probability is derived, and wherever
/// no shared-lineage warning is raised the default mode equals it too.
#[tokio::test]
async fn negation_exact_matches_possible_worlds_and_default_agrees_without_sharing() -> Result<()> {
    let mut rng = Lcg(0x0dd_5eed);
    let mut agreed = 0;
    let mut shared = 0;
    for round in 0..24 {
        let node_count = 3 + rng.next(2) as usize;
        let edge_count = 2 + rng.next(4) as usize;
        let edges: Vec<(&str, &str, f64)> = (0..edge_count)
            .map(|_| {
                let a = NODES[rng.next(node_count as u64) as usize];
                let b = NODES[rng.next(node_count as u64) as usize];
                let p = (1 + rng.next(9)) as f64 / 10.0;
                (a, b, p)
            })
            .collect();
        let case = &NEGATED_PROGRAMS[round % NEGATED_PROGRAMS.len()];
        let db = seeded(&edges).await?;
        let mut names: Vec<&str> = edges.iter().flat_map(|(a, b, _)| [*a, *b]).collect();
        names.sort_unstable();
        names.dedup();
        let mids: Vec<&str> = if case.has_mid {
            names.clone()
        } else {
            vec![""]
        };
        let (exact, _) = run_odd(&db, case, mode(true)).await?;
        let (default, warnings) = run_odd(&db, case, mode(false)).await?;
        let sharing = warnings
            .iter()
            .any(|w| w == "SharedProbabilisticDependency");
        if sharing {
            shared += 1;
        } else {
            agreed += 1;
        }
        for a in &names {
            for m in &mids {
                for b in &names {
                    let key = (a.to_string(), m.to_string(), b.to_string());
                    let worlds = worlds_probability(&edges, |w| (case.holds)(w, a, m, b));
                    let what = format!("odd{key:?} over {edges:?} in\n{}", case.rule);
                    match exact.iter().find(|(k, _)| *k == key) {
                        Some((_, p)) => assert_close(*p, worlds, &format!("exact {what}")),
                        None => assert_close(0.0, worlds, &format!("missing {what}")),
                    }
                    if !sharing && let Some((_, p)) = default.iter().find(|(k, _)| *k == key) {
                        assert_close(*p, worlds, &format!("default (no sharing) {what}"));
                    }
                }
            }
        }
    }
    assert!(agreed > 0 && shared > 0, "agreed {agreed}, shared {shared}");
    Ok(())
}

// ── Facts of a PROB rule without a fold inside exact formulas ──────────────

/// Programs referencing a PROB rule without a fold (`edge`, one fact per
/// edge, or `hop`, an edge then a reachability) from an `MNOR` rule. Exact
/// mode must read each referenced fact as its own formula (the edge, or the
/// edge and the reachability it rests on), not as a fresh variable.
const NON_FOLD_PROGRAMS: &[NegatedProgram] = &[
    // reach(a, b) AND NOT edge(a, b), `edge` one fact per edge
    NegatedProgram {
        rule: "CREATE RULE edge AS \
             MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY b, e.p AS p PROB \n\
             CREATE RULE odd AS \
             MATCH (a:N) WHERE a IS reach TO b, a IS NOT edge TO b \
             FOLD prob = MNOR(1.0) YIELD KEY a, KEY b, prob \n",
        has_mid: false,
        holds: |w, a, _, b| w.reach(a, b) && !w.link(a, b),
    },
    // reach(a, b) AND NOT hop(a, b, _): reachable, but not by an edge a → m
    // then a path m ⇝ b (`TO b` binds `hop`'s second KEY column)
    NegatedProgram {
        rule: "CREATE RULE hop AS \
             MATCH (a:N)-[e:E]->(m:N) WHERE m IS reach TO b \
             YIELD KEY a, KEY b, KEY m, e.p AS p PROB \n\
             CREATE RULE odd AS \
             MATCH (a:N) WHERE a IS reach TO b, a IS NOT hop TO b \
             FOLD prob = MNOR(1.0) YIELD KEY a, KEY b, prob \n",
        has_mid: false,
        holds: |w, a, _, b| {
            let hop = NODES.iter().any(|m| w.link(a, m) && w.reach(m, b));
            w.reach(a, b) && !hop
        },
    },
    // edge(a, b) AND reach(a, b), both positive: the edge implies the
    // reachability, so the conjunction is the edge alone
    NegatedProgram {
        rule: "CREATE RULE edge AS \
             MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY b, e.p AS p PROB \n\
             CREATE RULE odd AS \
             MATCH (a:N) WHERE a IS edge TO b, a IS reach TO b \
             FOLD prob = MNOR(1.0) YIELD KEY a, KEY b, prob \n",
        has_mid: false,
        holds: |w, a, _, b| w.link(a, b) && w.reach(a, b),
    },
    // reach(a, b) AND NOT twice(a, b), `twice` an edge times the
    // reachability that edge is part of: a non-fold fact whose own value
    // reads one edge twice
    NegatedProgram {
        rule: "CREATE RULE twice AS \
             MATCH (a:N)-[e:E]->(b:N) WHERE a IS reach TO b YIELD KEY a, KEY b, e.p AS p PROB \n\
             CREATE RULE odd AS \
             MATCH (a:N) WHERE a IS reach TO b, a IS NOT twice TO b \
             FOLD prob = MNOR(1.0) YIELD KEY a, KEY b, prob \n",
        has_mid: false,
        holds: |w, a, _, b| w.reach(a, b) && !(w.link(a, b) && w.reach(a, b)),
    },
];

/// The PROB rule without a fold each [`NON_FOLD_PROGRAMS`] entry defines,
/// queried directly: `(src, mid, dst, prob)` per fact, and when each fact
/// holds in a world (`mid` empty without a middle KEY).
const NON_FOLD_DIRECT: &[(&str, fn(&World<'_>, &str, &str, &str) -> bool)] = &[
    (
        "QUERY edge RETURN a.name AS src, '' AS mid, b.name AS dst, p AS prob",
        |w, a, _, b| w.link(a, b),
    ),
    (
        "QUERY hop RETURN a.name AS src, m.name AS mid, b.name AS dst, p AS prob",
        |w, a, m, b| w.link(a, m) && w.reach(m, b),
    ),
    (
        "QUERY edge RETURN a.name AS src, '' AS mid, b.name AS dst, p AS prob",
        |w, a, _, b| w.link(a, b),
    ),
    (
        "QUERY twice RETURN a.name AS src, '' AS mid, b.name AS dst, p AS prob",
        |w, a, _, b| w.link(a, b) && w.reach(a, b),
    ),
];

/// The facts `query` returns over `case`'s program, as `((src, mid, dst),
/// prob)`.
async fn run_direct(
    db: &Uni,
    case: &NegatedProgram,
    query: &str,
    config: LocyConfig,
) -> Result<Vec<((String, String, String), f64)>> {
    let program = format!("{}\n{LINK}{}{query}", reach_rules(), case.rule);
    let result = db
        .session()
        .locy_with(&program)
        .with_config(config)
        .run()
        .await?;
    Ok(result
        .rows()
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    let name =
                        |c: &str| r.get(c).and_then(Value::as_str).unwrap_or("?").to_string();
                    let p = r.get("prob").and_then(Value::as_f64).unwrap_or(f64::NAN);
                    ((name("src"), name("mid"), name("dst")), p)
                })
                .collect()
        })
        .unwrap_or_default())
}

/// `twice(s, t)` on the diamond, queried directly: the edge `s → t` times
/// `reach(s, t)`, which that edge is part of. Possible worlds: X ∧ (X ∨ (Y ∧
/// Z)) = X = 0.7. Its own value was the independent product 0.7 · 0.745 in
/// exact mode too.
#[tokio::test]
async fn a_fact_without_a_fold_has_its_exact_value_in_exact_mode() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let (case, (query, _)) = (&NON_FOLD_PROGRAMS[3], NON_FOLD_DIRECT[3]);
    let key = ("s".to_string(), String::new(), "t".to_string());
    let exact = run_direct(&db, case, query, mode(true)).await?;
    let p = exact.iter().find(|(k, _)| *k == key).map(|(_, p)| *p);
    assert_close(p.expect("twice(s, t)"), 0.7, "exact twice(s, t)");
    let default = run_direct(&db, case, query, mode(false)).await?;
    let p = default.iter().find(|(k, _)| *k == key).map(|(_, p)| *p);
    assert_close(p.expect("twice(s, t)"), 0.7 * 0.745, "default twice(s, t)");
    Ok(())
}

/// `reach(s, t) AND NOT edge(s, t)` on the diamond, `edge` a PROB rule
/// without a fold. Possible worlds: (X ∨ (Y ∧ Z)) ∧ ¬X = 0.3 · 0.3 · 0.5 =
/// 0.045 (X = s→t, Y = s→m, Z = m→t). Read as a fresh variable, `edge(s, t)`
/// is independent of `reach(s, t)`'s X and exact mode gave 0.745 · 0.3.
#[tokio::test]
async fn a_negated_fact_without_a_fold_is_its_own_formula_in_exact_mode() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let case = &NON_FOLD_PROGRAMS[0];
    let expected = worlds_probability(DIAMOND, |w| (case.holds)(w, "s", "", "t"));
    assert_close(expected, 0.045, "possible worlds");
    let key = ("s".to_string(), String::new(), "t".to_string());
    let (exact, _) = run_odd(&db, case, mode(true)).await?;
    let p = exact.iter().find(|(k, _)| *k == key).map(|(_, p)| *p);
    assert_close(p.expect("odd(s, t)"), 0.045, "exact odd(s, t)");
    let (default, warnings) = run_odd(&db, case, mode(false)).await?;
    let p = default.iter().find(|(k, _)| *k == key).map(|(_, p)| *p);
    assert_close(p.expect("odd(s, t)"), 0.745 * 0.3, "default odd(s, t)");
    assert!(
        warnings
            .iter()
            .any(|w| w == "SharedProbabilisticDependency"),
        "the negated fact shares s → t with the positive one: {warnings:?}"
    );
    Ok(())
}

/// Over random small graphs, programs referencing PROB rules without a fold
/// (negated and positive, directly over edges and over a reachability):
/// exact mode equals the possible-worlds probability of every fact, and
/// wherever no shared-lineage warning is raised the default mode equals it
/// too.
#[tokio::test]
async fn non_fold_references_exact_matches_possible_worlds() -> Result<()> {
    let mut rng = Lcg(0x0f01d);
    let mut directly = 0;
    let mut agreed = 0;
    let mut shared = 0;
    for round in 0..32 {
        let node_count = 3 + rng.next(2) as usize;
        let edge_count = 2 + rng.next(4) as usize;
        let edges: Vec<(&str, &str, f64)> = (0..edge_count)
            .map(|_| {
                let a = NODES[rng.next(node_count as u64) as usize];
                let b = NODES[rng.next(node_count as u64) as usize];
                let p = (1 + rng.next(9)) as f64 / 10.0;
                (a, b, p)
            })
            .collect();
        let case = &NON_FOLD_PROGRAMS[round % NON_FOLD_PROGRAMS.len()];
        let db = seeded(&edges).await?;
        let mut names: Vec<&str> = edges.iter().flat_map(|(a, b, _)| [*a, *b]).collect();
        names.sort_unstable();
        names.dedup();
        let (exact, _) = run_odd(&db, case, mode(true)).await?;
        let (default, warnings) = run_odd(&db, case, mode(false)).await?;
        let sharing = warnings
            .iter()
            .any(|w| w == "SharedProbabilisticDependency");
        if sharing {
            shared += 1;
        } else {
            agreed += 1;
        }
        for a in &names {
            for b in &names {
                let key = (a.to_string(), String::new(), b.to_string());
                let worlds = worlds_probability(&edges, |w| (case.holds)(w, a, "", b));
                let what = format!("odd{key:?} over {edges:?} in\n{}", case.rule);
                match exact.iter().find(|(k, _)| *k == key) {
                    Some((_, p)) => assert_close(*p, worlds, &format!("exact {what}")),
                    None => assert_close(0.0, worlds, &format!("missing {what}")),
                }
                if !sharing && let Some((_, p)) = default.iter().find(|(k, _)| *k == key) {
                    assert_close(*p, worlds, &format!("default (no sharing) {what}"));
                }
            }
        }
        // The rule without a fold's own facts, queried directly. A KEY with
        // several facts (parallel edges of different weights) is several
        // values of one KEY, which `holds` does not tell apart: skipped.
        let (query, holds) = NON_FOLD_DIRECT[round % NON_FOLD_DIRECT.len()];
        let exact = run_direct(&db, case, query, mode(true)).await?;
        let (default, warnings) = {
            let program = format!("{}\n{LINK}{}{query}", reach_rules(), case.rule);
            let result = db
                .session()
                .locy_with(&program)
                .with_config(mode(false))
                .run()
                .await?;
            let warned: Vec<String> = result
                .warnings()
                .iter()
                .map(|w| format!("{:?}", w.code))
                .collect();
            (run_direct(&db, case, query, mode(false)).await?, warned)
        };
        let sharing = warnings
            .iter()
            .any(|w| w == "SharedProbabilisticDependency");
        for (key, p) in &exact {
            if exact.iter().filter(|(k, _)| k == key).count() > 1 {
                continue;
            }
            directly += 1;
            let (a, m, b) = key;
            let worlds = worlds_probability(&edges, |w| holds(w, a, m, b));
            let what = format!("{query} {key:?} over {edges:?}");
            assert_close(*p, worlds, &format!("exact {what}"));
            if !sharing && let Some((_, d)) = default.iter().find(|(k, _)| k == key) {
                assert_close(*d, worlds, &format!("default (no sharing) {what}"));
            }
        }
    }
    assert!(agreed > 0 && shared > 0, "agreed {agreed}, shared {shared}");
    assert!(directly > 0, "no fact of a rule without a fold was checked");
    Ok(())
}

// ── EXPLAIN and TopKProofs read the same references as the lineage ─────────

/// The name of a node bound in an EXPLAIN node's bindings.
fn bound_name(node: &uni_locy::DerivationNode, var: &str) -> String {
    match node.bindings.get(var) {
        Some(Value::Node(n)) => n
            .properties
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string(),
        other => panic!("`{var}` is not a node in {:?}: {other:?}", node.bindings),
    }
}

/// The EXPLAIN tree of `reach`, every fact.
async fn explain_reach(db: &Uni, config: LocyConfig) -> Result<uni_locy::DerivationNode> {
    let program = format!("{}\nEXPLAIN RULE reach", reach_rules());
    let result = db
        .session()
        .locy_with(&program)
        .with_config(config)
        .run()
        .await?;
    Ok(result
        .command_results()
        .iter()
        .find_map(|c| c.as_explain())
        .expect("an EXPLAIN result")
        .clone())
}

/// Every derivation EXPLAIN shows for `reach` references exactly the facts
/// of `reach`'s lineage, over random graphs: a direct edge none, a step
/// `a → m` then `reach(m, b)` that one fact — even though `mid` is not
/// yielded. Each fact's derivations are one per edge and per (edge,
/// referenced fact), whatever their values.
#[tokio::test]
async fn explain_shows_the_referenced_fact_of_every_derivation() -> Result<()> {
    let mut rng = Lcg(0xe_c0de);
    let mut referenced = 0;
    for _ in 0..12 {
        let node_count = 3 + rng.next(3) as usize;
        let edge_count = 2 + rng.next(5) as usize;
        let edges: Vec<(&str, &str, f64)> = (0..edge_count)
            .map(|_| {
                let a = NODES[rng.next(node_count as u64) as usize];
                let b = NODES[rng.next(node_count as u64) as usize];
                // Few distinct values, so derivations collide on value.
                let p = (1 + rng.next(2)) as f64 / 4.0;
                (a, b, p)
            })
            .collect();
        let db = seeded(&edges).await?;
        let all = World {
            edges: &edges,
            mask: u32::MAX,
        };
        for exact in [false, true] {
            let tree = explain_reach(&db, mode(exact)).await?;
            // (a, b) -> [referenced (m, b) per derivation, `None` if direct]
            let mut derivations: Vec<((String, String), Option<(String, String)>)> = Vec::new();
            for d in &tree.children {
                let fact = (bound_name(d, "a"), bound_name(d, "b"));
                if d.clause_index == 0 {
                    assert!(d.children.is_empty(), "a direct edge references nothing");
                    derivations.push((fact, None));
                    continue;
                }
                // A referenced fact is expanded where it first appears and a
                // leaf carrying its bindings elsewhere, so every child names it.
                let targets: Vec<(String, String)> = d
                    .children
                    .iter()
                    .map(|c| {
                        assert_eq!(c.rule, "reach");
                        (bound_name(c, "a"), bound_name(c, "b"))
                    })
                    .collect();
                let target = targets
                    .first()
                    .unwrap_or_else(|| panic!("derivation of {fact:?} references no fact"))
                    .clone();
                assert!(
                    targets.iter().all(|t| *t == target),
                    "one derivation references one fact: {targets:?}"
                );
                assert_eq!(target.1, fact.1, "reach(m, b) for reach(a, b)");
                derivations.push((fact, Some(target)));
                referenced += 1;
            }
            // Expected from the graph: one derivation per edge a → b, and per
            // edge a → m with b reachable from m.
            let mut expected: Vec<((String, String), Option<(String, String)>)> = Vec::new();
            for (a, m, _) in &edges {
                expected.push(((a.to_string(), m.to_string()), None));
                for b in NODES {
                    if all.reach(m, b) {
                        expected.push((
                            (a.to_string(), b.to_string()),
                            Some((m.to_string(), b.to_string())),
                        ));
                    }
                }
            }
            derivations.sort();
            expected.sort();
            assert_eq!(derivations, expected, "exact={exact} over {edges:?}");
        }
    }
    assert!(referenced > 0);
    Ok(())
}

/// `TopKProofs { k }` evaluates each fact's `k` most probable rows exactly,
/// the referenced facts included: with every row kept it is the
/// possible-worlds probability, the shared `C → D` counted once.
#[tokio::test]
async fn topk_proofs_evaluate_the_lineage_of_their_rows() -> Result<()> {
    let edges = [
        ("A", "B", 0.5),
        ("A", "C", 0.3),
        ("B", "C", 0.4),
        ("C", "D", 0.6),
    ];
    let db = seeded(&edges).await?;
    let topk = |k: u32| LocyConfig {
        max_iterations: 1000,
        semiring: uni_locy::SemiringKind::TopKProofs { k },
        ..Default::default()
    };
    let all = run_with(&db, REACH, topk(8)).await?;
    for ((src, dst), p) in &all.probs {
        assert_close(
            *p,
            reach_probability(&edges, src, dst),
            &format!("TopKProofs(8) reach({src},{dst})"),
        );
    }
    assert_close(all.prob("A", "D"), 0.264, "TopKProofs(8) reach(A, D)");
    // k = 1: reach(A, D)'s best row is A→C then reach(C, D) (0.18 against
    // 0.12 through B), so the value is P(AC ∧ CD).
    let one = run_with(&db, REACH, topk(1)).await?;
    assert_close(one.prob("A", "D"), 0.3 * 0.6, "TopKProofs(1) reach(A, D)");
    // reach(A, C)'s best row is the direct 0.3 edge, over A→B→C's 0.2.
    assert_close(one.prob("A", "C"), 0.3, "TopKProofs(1) reach(A, C)");
    Ok(())
}

/// [`run`] under an explicit configuration.
async fn run_with(db: &Uni, program: &str, config: LocyConfig) -> Result<Outcome> {
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

/// Two `IS NOT` references to one PROB rule in a clause each contribute
/// their own complement: (1 − risky(a)) · (1 − risky(b)).
#[tokio::test]
async fn two_negations_of_one_rule_each_contribute_their_complement() -> Result<()> {
    let db = seeded(DIAMOND).await?;
    let program = "CREATE RULE risky AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD r = MNOR(e.p) YIELD KEY a, r \n\
         CREATE RULE both_clear AS \
         MATCH (a:N)-[:E]->(b:N) WHERE a IS NOT risky, b IS NOT risky \
         YIELD KEY a, KEY b, 1.0 AS prob PROB \n\
         QUERY both_clear RETURN a.name AS src, b.name AS dst, prob";
    for exact in [false, true] {
        let out = run(&db, program, exact).await?;
        // risky(s) = 1 − 0.3 · 0.7 = 0.79, risky(m) = 0.5, t is not risky.
        assert_close(out.prob("s", "m"), 0.21 * 0.5, "both_clear(s, m)");
        assert_close(out.prob("m", "t"), 0.5, "both_clear(m, t)");
        assert_close(out.prob("s", "t"), 0.21, "both_clear(s, t)");
    }
    Ok(())
}
