// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A FOLD counts rows with `MCOUNT(*)`, `MCOUNT()`, `COUNT(*)` or `COUNT()`.
//!
//! All four parse to the same zero-argument call, so every later stage sees
//! one row-count form.

use uni_cypher::ast::Expr;
use uni_cypher::locy_ast::LocyStatement;
use uni_cypher::parse_locy;

/// The aggregate of the program's one rule's one FOLD binding.
fn fold_aggregate(program: &str) -> Expr {
    let parsed = parse_locy(program).unwrap_or_else(|e| panic!("{program}: {e}"));
    let rule = match &parsed.statements[0] {
        LocyStatement::Rule(r) => r,
        other => panic!("expected a rule, got {other:?}"),
    };
    rule.fold[0].aggregate.clone()
}

fn rule_with(fold: &str) -> String {
    format!("CREATE RULE r AS MATCH (a)-[:E]->(b) FOLD c = {fold} YIELD KEY a, c")
}

#[test]
fn every_row_count_spelling_is_a_zero_argument_call() {
    for spelling in [
        "MCOUNT(*)",
        "mcount( * )",
        "MCOUNT()",
        "COUNT(*)",
        "COUNT()",
    ] {
        match fold_aggregate(&rule_with(spelling)) {
            Expr::FunctionCall { name, args, .. } => {
                assert!(
                    name.eq_ignore_ascii_case("MCOUNT") || name.eq_ignore_ascii_case("COUNT"),
                    "{spelling}: unexpected name {name}"
                );
                assert!(
                    args.is_empty(),
                    "{spelling}: expected no arguments, got {args:?}"
                );
            }
            other => panic!("{spelling}: expected a call, got {other:?}"),
        }
    }
}

#[test]
fn counting_a_column_keeps_its_argument() {
    match fold_aggregate(&rule_with("MCOUNT(b)")) {
        Expr::FunctionCall { args, .. } => assert_eq!(args.len(), 1),
        other => panic!("expected a call, got {other:?}"),
    }
}

#[test]
fn mcount_star_works_beside_other_folds() {
    let program = "CREATE RULE r AS MATCH (a)-[:E]->(b) \
                   FOLD c = MCOUNT(*), m = MMAX(b.w) YIELD KEY a, c, m";
    let parsed = parse_locy(program).expect("parse");
    let LocyStatement::Rule(rule) = &parsed.statements[0] else {
        panic!("expected a rule");
    };
    assert_eq!(rule.fold.len(), 2);
}
