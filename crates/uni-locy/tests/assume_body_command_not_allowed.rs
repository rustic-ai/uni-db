//! ABDUCE and EXPLAIN RULE inside an `ASSUME ... THEN` body are compile errors.
//!
//! An ASSUME returns only the rows of its body's QUERY and Cypher commands.
//! The runtime used to run a body ABDUCE or EXPLAIN and drop its result, so
//! the program "succeeded" with nothing to show for the command. The compiler
//! now rejects both, at any nesting depth, and names the alternatives.

use uni_cypher::parse_locy;
use uni_locy::{LocyCompileError, compile};

const RULE: &str = "CREATE RULE reachable AS \
    MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b ";

fn compile_src(src: &str) -> Result<uni_locy::CompiledProgram, LocyCompileError> {
    let program = parse_locy(src).expect("the program must parse");
    compile(&program)
}

fn assert_rejected(src: &str, command: &str) {
    match compile_src(src) {
        Err(err @ LocyCompileError::AssumeBodyCommandNotAllowed { .. }) => {
            assert_eq!(
                err,
                LocyCompileError::AssumeBodyCommandNotAllowed {
                    command: command.to_string()
                }
            );
            let message = err.to_string();
            for alternative in ["top level", "tx()", "rollback()", "Session::scratch()"] {
                assert!(
                    message.contains(alternative),
                    "the error must point to `{alternative}`: {message}"
                );
            }
        }
        other => panic!("expected AssumeBodyCommandNotAllowed({command}), got {other:?}"),
    }
}

#[test]
fn abduce_in_assume_body_is_rejected() {
    assert_rejected(
        &format!(
            "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} \
             THEN {{ ABDUCE NOT reachable WHERE a.name = 'A' }}"
        ),
        "ABDUCE",
    );
}

#[test]
fn abduce_after_query_in_assume_body_is_rejected() {
    assert_rejected(
        &format!(
            "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
               QUERY reachable RETURN a \
               ABDUCE reachable WHERE a.name = 'A' \
             }}"
        ),
        "ABDUCE",
    );
}

#[test]
fn explain_in_assume_body_is_rejected() {
    assert_rejected(
        &format!(
            "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} \
             THEN {{ EXPLAIN RULE reachable WHERE a.name = 'A' }}"
        ),
        "EXPLAIN RULE",
    );
}

#[test]
fn abduce_in_nested_assume_body_is_rejected() {
    assert_rejected(
        &format!(
            "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
               ASSUME {{ CREATE (:Node {{name: 'Y'}}) }} \
               THEN {{ ABDUCE NOT reachable WHERE a.name = 'A' }} \
             }}"
        ),
        "ABDUCE",
    );
}

#[test]
fn explain_in_nested_assume_body_is_rejected() {
    assert_rejected(
        &format!(
            "{RULE} ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
               ASSUME {{ CREATE (:Node {{name: 'Y'}}) }} \
               THEN {{ EXPLAIN RULE reachable WHERE a.name = 'A' }} \
             }}"
        ),
        "EXPLAIN RULE",
    );
}

/// The alternatives the error names compile: ABDUCE and EXPLAIN at the top
/// level beside an ASSUME, and a nested ASSUME whose body only queries.
#[test]
fn top_level_abduce_and_nested_assume_still_compile() {
    let src = format!(
        "{RULE} \
         ASSUME {{ CREATE (:Node {{name: 'X'}}) }} THEN {{ \
           QUERY reachable RETURN a \
           ASSUME {{ CREATE (:Node {{name: 'Y'}}) }} THEN {{ QUERY reachable RETURN b }} \
         }} \
         ABDUCE NOT reachable WHERE a.name = 'A' \
         EXPLAIN RULE reachable WHERE a.name = 'A'"
    );
    let compiled = compile_src(&src);
    assert!(
        compiled.is_ok(),
        "expected the program to compile: {compiled:?}"
    );
}
