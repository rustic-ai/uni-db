//! ABDUCE and EXPLAIN RULE compile inside an `ASSUME ... THEN` body, at any
//! nesting depth, and a body EXPLAIN makes the program record provenance.
//!
//! Both were compile errors while a hypothetical state recorded no
//! provenance and ABDUCE validated against the parent's strata only. The
//! runtime now records provenance in every re-evaluation of a program that
//! needs it and validates a body ABDUCE against the body's own rules, so the
//! compiler keeps both commands in the body, in order.

use uni_cypher::parse_locy;
use uni_locy::types::CompiledCommand;
use uni_locy::{CompiledProgram, compile};

const RULE: &str = "CREATE RULE reachable AS \
    MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b ";

fn compile_src(src: &str) -> CompiledProgram {
    let program = parse_locy(src).expect("the program must parse");
    compile(&program).unwrap_or_else(|e| panic!("the program must compile: {e}"))
}

/// The kinds of an ASSUME body's commands, nested bodies in brackets.
fn shape(commands: &[CompiledCommand]) -> Vec<String> {
    commands
        .iter()
        .map(|c| match c {
            CompiledCommand::GoalQuery(_) => "QUERY".to_string(),
            CompiledCommand::Abduce(_) => "ABDUCE".to_string(),
            CompiledCommand::ExplainRule(_) => "EXPLAIN".to_string(),
            CompiledCommand::Assume(a) => format!("ASSUME{:?}", shape(&a.body_commands)),
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn abduce_and_explain_compile_in_an_assume_body_in_order() {
    let compiled = compile_src(&format!(
        "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
           QUERY reachable RETURN a \
           ABDUCE NOT reachable WHERE a.name = 'A' \
           EXPLAIN RULE reachable WHERE a.name = 'A' \
         }}"
    ));
    assert_eq!(
        shape(&compiled.commands),
        vec![r#"ASSUME["QUERY", "ABDUCE", "EXPLAIN"]"#.to_string()]
    );
}

#[test]
fn abduce_and_explain_compile_in_a_nested_assume_body() {
    let compiled = compile_src(&format!(
        "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
           ASSUME {{ CREATE (:Node {{name: 'Y'}}) }} \
           THEN {{ ABDUCE NOT reachable WHERE a.name = 'A' \
                   EXPLAIN RULE reachable WHERE a.name = 'A' }} \
         }}"
    ));
    assert_eq!(
        shape(&compiled.commands),
        vec![r#"ASSUME["ASSUME[\"ABDUCE\", \"EXPLAIN\"]"]"#.to_string()]
    );
}

/// A body ABDUCE may name a rule the body defines.
#[test]
fn a_body_abduce_names_a_body_rule() {
    let compiled = compile_src(&format!(
        "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
           CREATE RULE hub AS MATCH (a:Node) WHERE a IS reachable TO b YIELD KEY a \
           ABDUCE NOT hub WHERE a.name = 'A' \
         }}"
    ));
    assert_eq!(
        shape(&compiled.commands),
        vec![r#"ASSUME["ABDUCE"]"#.to_string()]
    );
}

/// The runtime attaches a provenance tracker exactly when the program needs
/// one, and an EXPLAIN in an ASSUME body, at any depth, needs one. A program
/// with neither an EXPLAIN nor a probabilistic fold needs none.
#[test]
fn a_body_explain_needs_a_provenance_tracker() {
    let none = compile_src(&format!(
        "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ QUERY reachable RETURN a }}"
    ));
    assert!(!none.needs_provenance_tracker());
    let body = compile_src(&format!(
        "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
           EXPLAIN RULE reachable WHERE a.name = 'A' }}"
    ));
    assert!(body.needs_provenance_tracker());
    let nested = compile_src(&format!(
        "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
           ASSUME {{ CREATE (:Node {{name: 'Y'}}) }} \
           THEN {{ EXPLAIN RULE reachable WHERE a.name = 'A' }} }}"
    ));
    assert!(nested.needs_provenance_tracker());
}

/// A probabilistic fold defined only in an ASSUME body needs a tracker too:
/// exact probabilities are corrected only with one.
#[test]
fn a_body_probabilistic_fold_needs_a_provenance_tracker() {
    let compiled = compile_src(&format!(
        "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
           CREATE RULE risk AS MATCH (a:Node)-[e:EDGE]->(b:Node) \
             FOLD p = MNOR(e.p) YIELD KEY b, p \
           QUERY risk RETURN p }}"
    ));
    assert!(compiled.needs_provenance_tracker());
}
