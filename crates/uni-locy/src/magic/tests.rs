// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shape tests for the goal-directed rewrite: what it emits and when it
//! refuses. Answer equality is tested against the engine in
//! `crates/uni/tests/common/locy/locy_goal_directed.rs` and the oracle.

use super::*;

const TC_RIGHT: &str = "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
    CREATE RULE reach AS MATCH (a:N)-[:E]->(m:N) WHERE m IS reach TO b YIELD KEY a, KEY b\n";

const TC_LEFT: &str = "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
    CREATE RULE reach AS MATCH (m:N)-[:E]->(b:N) WHERE a IS reach TO m YIELD KEY a, KEY b\n";

fn compiled(src: &str) -> CompiledProgram {
    crate::compile(&uni_cypher::parse_locy(src).expect("parses")).expect("compiles")
}

fn rewrite(src: &str, mode: GoalDirected) -> (CompiledProgram, GoalDirectedReport) {
    rewrite_goal_directed(compiled(src), mode, &HashMap::new())
}

fn evaluated(program: &CompiledProgram) -> Vec<String> {
    let mut names: Vec<String> = program
        .strata
        .iter()
        .flat_map(|s| s.rules.iter().map(|r| r.name.clone()))
        .collect();
    names.sort();
    names
}

fn fallback(report: &GoalDirectedReport) -> Fallback {
    match &report.goals[0].outcome {
        GoalOutcome::Fallback(f) => f.clone(),
        other => panic!("expected a fallback, got {other:?}"),
    }
}

fn query_rule(program: &CompiledProgram) -> String {
    program
        .commands
        .iter()
        .find_map(|c| match c {
            CompiledCommand::GoalQuery(q) => Some(q.rule_name.to_string()),
            _ => None,
        })
        .expect("a QUERY")
}

#[test]
fn off_is_the_identity() {
    let src = format!("{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b");
    let (program, report) = rewrite(&src, GoalDirected::Off);
    assert!(report.goals.is_empty());
    assert_eq!(evaluated(&program), ["reach"]);
    assert_eq!(query_rule(&program), "reach");
}

/// Right-linear TC: the recursive call's source is bound through the MATCH
/// edge, so magic propagates along the edge and the original is dropped.
#[test]
fn right_linear_tc_propagates_magic_and_drops_the_original() {
    let src = format!("{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b");
    let (program, report) = rewrite(&src, GoalDirected::On);
    assert_eq!(report.rewritten(), 1, "{report:?}");
    assert_eq!(
        evaluated(&program),
        ["__locy_gd0_reach_bf", "__locy_gdm0_reach_bf"]
    );
    assert_eq!(query_rule(&program), "__locy_gd0_reach_bf");
    let magic = &program.rule_catalog["__locy_gdm0_reach_bf"];
    // The seed plus one propagation clause from the recursive clause.
    assert_eq!(magic.clauses.len(), 2);
    assert_eq!(magic.yield_schema.len(), 1);
    // The magic rule is recursive in itself and below the copy.
    let pos = |name: &str| {
        program
            .strata
            .iter()
            .position(|s| s.rules.iter().any(|r| r.name == name))
            .unwrap()
    };
    assert!(pos("__locy_gdm0_reach_bf") < pos("__locy_gd0_reach_bf"));
    assert!(program.strata[pos("__locy_gdm0_reach_bf")].is_recursive);
    // Every generated clause has a binding plan.
    for name in ["__locy_gdm0_reach_bf", "__locy_gd0_reach_bf"] {
        for clause in &program.rule_catalog[name].clauses {
            assert!(clause.binding.plan().is_some(), "{name} has no plan");
        }
    }
}

/// Left-linear TC: the recursive call passes the head binding through
/// unchanged, so there is no propagation clause and the magic rule is only
/// its seed.
#[test]
fn left_linear_tc_needs_only_the_seed() {
    let src = format!("{TC_LEFT}QUERY reach WHERE a.name = 'x' RETURN b");
    let (program, report) = rewrite(&src, GoalDirected::On);
    assert_eq!(report.rewritten(), 1, "{report:?}");
    let magic = &program.rule_catalog["__locy_gdm0_reach_bf"];
    assert_eq!(magic.clauses.len(), 1, "only the seed");
    let seed = &magic.clauses[0];
    // `a` is bound by an IS reference in the recursive clause, so no label
    // is known for every clause and the seed scans unlabelled.
    assert_eq!(seed_node_labels(seed), LabelExpr::Empty);
}

fn seed_node_labels(seed: &CompiledClause) -> LabelExpr {
    let PatternElement::Node(node) = &seed.match_pattern.paths[0].elements[0] else {
        panic!("seed is a node");
    };
    node.labels.clone()
}

/// Every clause binds `a` as `(a:N)`, so the seed carries the label and can
/// use a label index.
#[test]
fn seed_carries_a_label_every_clause_requires() {
    let src = format!("{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b");
    let (program, _) = rewrite(&src, GoalDirected::On);
    let magic = &program.rule_catalog["__locy_gdm0_reach_bf"];
    let seed = magic.clauses.last().unwrap();
    assert_eq!(
        seed_node_labels(seed),
        LabelExpr::Conjunction(vec!["N".into()])
    );
}

/// Shadow keeps every original rule, so `derived` is unchanged.
#[test]
fn shadow_keeps_the_originals() {
    let src = format!("{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b");
    let (program, report) = rewrite(&src, GoalDirected::Shadow);
    assert_eq!(report.rewritten(), 1);
    assert!(evaluated(&program).contains(&"reach".to_string()));
    assert_eq!(query_rule(&program), "__locy_gd0_reach_bf");
}

/// An original another evaluated rule reads stays evaluated in full.
#[test]
fn original_read_by_another_rule_is_kept() {
    let src = format!(
        "{TC_RIGHT}CREATE RULE other AS MATCH (x:N) WHERE x IS reach YIELD KEY x\n\
         QUERY reach WHERE a.name = 'x' RETURN b"
    );
    let (program, report) = rewrite(&src, GoalDirected::On);
    assert_eq!(report.rewritten(), 1);
    let names = evaluated(&program);
    assert!(names.contains(&"reach".to_string()), "{names:?}");
    assert!(names.contains(&"other".to_string()));
}

#[test]
fn a_parameter_seeds_only_with_a_plain_value() {
    let src = format!("{TC_RIGHT}QUERY reach WHERE a.name = $who RETURN b");
    let mut params = HashMap::new();
    let (_, report) = rewrite_goal_directed(compiled(&src), GoalDirected::On, &params);
    assert_eq!(fallback(&report), Fallback::NoBoundKey, "unbound parameter");
    params.insert("who".to_string(), Value::Float(1.0));
    let (_, report) = rewrite_goal_directed(compiled(&src), GoalDirected::On, &params);
    assert_eq!(fallback(&report), Fallback::NoBoundKey, "float parameter");
    params.insert("who".to_string(), Value::String("x".into()));
    let (_, report) = rewrite_goal_directed(compiled(&src), GoalDirected::On, &params);
    assert_eq!(report.rewritten(), 1);
}

#[test]
fn value_columns_and_unseedable_conjuncts_do_not_bind() {
    for goal in [
        "QUERY reach WHERE a = a RETURN b",
        "QUERY reach WHERE a.name = 'x' OR b.name = 'y' RETURN b",
        "QUERY reach WHERE a.w = 1.5 RETURN b",
        "QUERY reach WHERE a.name = b.name RETURN b",
        "QUERY reach RETURN b",
    ] {
        let (_, report) = rewrite(&format!("{TC_RIGHT}{goal}"), GoalDirected::On);
        assert_eq!(fallback(&report), Fallback::NoBoundKey, "{goal}");
    }
    let src = "CREATE RULE w AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, b\n\
               QUERY w WHERE b.name = 'x' RETURN a";
    let (_, report) = rewrite(src, GoalDirected::On);
    assert_eq!(fallback(&report), Fallback::NoBoundKey, "b is not a KEY");
}

#[test]
fn both_keys_bound_seed_a_pair() {
    let src = format!("{TC_RIGHT}QUERY reach WHERE a.name = 'x' AND b.name IN ['y', 'z']");
    let (program, report) = rewrite(&src, GoalDirected::On);
    assert_eq!(report.rewritten(), 1, "{report:?}");
    let magic = &program.rule_catalog["__locy_gdm0_reach_bb"];
    let seed = magic.clauses.last().unwrap();
    assert_eq!(seed.match_pattern.paths.len(), 2);
    assert_eq!(magic.yield_schema.len(), 2);
}

#[test]
fn unsafe_constructs_fall_back() {
    let cases: [(&str, Unsafe); 4] = [
        (
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY b, e.w AS c\n\
             CREATE RULE r AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS r TO b \
               ALONG c = prev.c + e.w YIELD KEY a, KEY b, c\n\
             QUERY r WHERE a.name = 'x'",
            Unsafe::Along,
        ),
        (
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY b, e.w AS c\n\
             CREATE RULE r AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS r TO b \
               FOLD c = MMAX(e.w) YIELD KEY a, KEY b, c\n\
             QUERY r WHERE a.name = 'x'",
            Unsafe::RecursiveAggregation,
        ),
        (
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY b, e.p AS p PROB\n\
             QUERY r WHERE a.name = 'x'",
            Unsafe::Prob,
        ),
        (
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD p = MNOR(e.p) YIELD KEY a, p\n\
             QUERY r WHERE a.name = 'x'",
            // An MNOR fold makes its column a PROB column.
            Unsafe::Prob,
        ),
    ];
    for (src, expected) in cases {
        let (program, report) = rewrite(src, GoalDirected::On);
        match fallback(&report) {
            Fallback::Unsafe { reason, .. } => assert_eq!(reason, expected, "{src}"),
            other => panic!("{src}: {other:?}"),
        }
        assert!(
            program
                .rule_catalog
                .keys()
                .all(|k| !is_goal_directed_rule(k)),
            "a fallback leaves the program unchanged"
        );
    }
}

/// A non-recursive FOLD keeps or drops whole KEY groups under the guard.
#[test]
fn non_recursive_fold_is_adorned() {
    let src = "CREATE RULE deg AS MATCH (a:N)-[:E]->(b:N) FOLD n = COUNT(b) YIELD KEY a, n\n\
               QUERY deg WHERE a.name = 'x' RETURN n";
    let (program, report) = rewrite(src, GoalDirected::On);
    assert_eq!(report.rewritten(), 1, "{report:?}");
    assert_eq!(
        evaluated(&program),
        ["__locy_gd0_deg_b", "__locy_gdm0_deg_b"]
    );
}

/// `IS NOT` reads its target in full, and the target is never adorned.
#[test]
fn negation_reads_the_original() {
    let src = format!(
        "{TC_RIGHT}CREATE RULE blocked AS MATCH (x:N) WHERE x.bad = true YIELD KEY x\n\
         CREATE RULE ok AS MATCH (a:N)-[:E]->(b:N) WHERE a IS reach TO b, b IS NOT blocked \
           YIELD KEY a, KEY b\n\
         QUERY ok WHERE a.name = 'x' RETURN b"
    );
    let (program, report) = rewrite(&src, GoalDirected::On);
    assert_eq!(report.rewritten(), 1, "{report:?}");
    let names = evaluated(&program);
    assert!(names.contains(&"blocked".to_string()), "{names:?}");
    // reach is called with both ends bound by the MATCH edge from `a`.
    assert!(
        names.iter().any(|n| n.starts_with("__locy_gd0_reach_")),
        "{names:?}"
    );
    let copy = &program.rule_catalog["__locy_gd0_ok_bf"];
    let negated: Vec<String> = copy.clauses[0]
        .where_conditions
        .iter()
        .filter_map(|c| match c {
            RuleCondition::IsReference(r) if r.negated => Some(r.rule_name.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(negated, ["blocked"]);
}

/// Non-linear recursion: the second call's binding comes from the first
/// call, which this SIP does not pass, so it reads its callee in full.
#[test]
fn non_linear_inner_call_reads_the_original() {
    let src = "CREATE RULE tc AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
               CREATE RULE tc AS MATCH (a:N) WHERE a IS tc TO m, m IS tc TO b YIELD KEY a, KEY b\n\
               QUERY tc WHERE a.name = 'x' RETURN b";
    let (program, report) = rewrite(src, GoalDirected::On);
    assert_eq!(report.rewritten(), 1, "{report:?}");
    assert!(evaluated(&program).contains(&"tc".to_string()));
}

#[test]
fn two_goals_get_their_own_rules() {
    let src = format!(
        "{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b\nQUERY reach WHERE a.name = 'y' RETURN b"
    );
    let (program, report) = rewrite(&src, GoalDirected::On);
    assert_eq!(report.rewritten(), 2);
    assert!(program.rule_catalog.contains_key("__locy_gd0_reach_bf"));
    assert!(program.rule_catalog.contains_key("__locy_gd1_reach_bf"));
}

/// A rule inside a `MODULE` is catalogued qualified; generated names carry
/// no dot, so no bare reference can resolve to them.
#[test]
fn module_rules_are_rewritten_under_dot_free_names() {
    let src = format!("MODULE m\n{TC_RIGHT}QUERY reach WHERE a.name = 'x' RETURN b");
    let (program, report) = rewrite(&src, GoalDirected::On);
    assert_eq!(report.rewritten(), 1, "{report:?}");
    assert_eq!(query_rule(&program), "__locy_gd0_m_reach_bf");
    assert!(
        program
            .rule_catalog
            .keys()
            .filter(|k| is_goal_directed_rule(k))
            .all(|k| !k.contains('.'))
    );
}

#[test]
fn explain_derive_and_assume_are_not_rewritten() {
    let src = format!(
        "{TC_RIGHT}EXPLAIN RULE reach WHERE a.name = 'x'\n\
         ASSUME {{ CREATE (:N {{name: 'q'}}) }} THEN {{ QUERY reach WHERE a.name = 'x' RETURN b }}"
    );
    let (program, report) = rewrite(&src, GoalDirected::On);
    assert!(report.goals.is_empty());
    assert_eq!(evaluated(&program), ["reach"]);
}
