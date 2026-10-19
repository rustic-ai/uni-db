// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A quoted identifier is the same name wherever it appears, and openCypher's
//! doubled-backtick escape (`` `a``b` `` is the name ``a`b``) parses.
//!
//! Two defects lived here:
//!
//! * The grammar's quoted form was `` "`" ~ (!"`" ~ ANY)* ~ "`" `` with no
//!   escape, so `` `a``b` `` failed to parse and no name containing a backtick
//!   could be written at all.
//! * Only labels, types and property keys were unquoted by the walker.
//!   Variables (pattern, UNWIND, FOREACH, comprehension, quantifier, REDUCE,
//!   SET/REMOVE targets, map-projection selectors), procedure names,
//!   parameters, index and constraint names, and DDL property names kept their
//!   backticks, so `` MATCH (`n`) RETURN n `` bound `` `n` `` but read `n`.
//!
//! Oracle: a query written with every identifier quoted parses to the same AST
//! (modulo the verbatim `source_text`) as the same query written bare.

use crate::repro_backtick_property_name::debug_without_source_text;
use uni_cypher::locy_ast::{LocyStatement, RuleCondition};

fn ast(src: &str) -> String {
    let q = uni_cypher::parse(src).unwrap_or_else(|e| panic!("parse `{src}`: {e:?}"));
    debug_without_source_text(&format!("{q:?}"))
}

fn assert_same_ast(quoted: &str, bare: &str) {
    assert_eq!(
        ast(quoted),
        ast(bare),
        "quoted `{quoted}` and bare `{bare}` parse differently"
    );
}

#[test]
fn quoted_variables_equal_bare_variables() {
    let cases = [
        (
            "MATCH p = (`n`:`L`)-[`r`:`T`]->(`m`) WHERE `n`.`a` = 1 RETURN `n`, `r`, `m`, `p`",
            "MATCH p = (n:L)-[r:T]->(m) WHERE n.a = 1 RETURN n, r, m, p",
        ),
        ("MATCH `p` = (n) RETURN p", "MATCH p = (n) RETURN p"),
        ("UNWIND [1] AS `x` RETURN x", "UNWIND [1] AS x RETURN x"),
        (
            "MATCH (n) FOREACH (`x` IN [1] | SET n.a = x)",
            "MATCH (n) FOREACH (x IN [1] | SET n.a = x)",
        ),
        (
            "RETURN [`x` IN [1, 2] WHERE x > 1 | x] AS l",
            "RETURN [x IN [1, 2] WHERE x > 1 | x] AS l",
        ),
        (
            "RETURN all(`x` IN [1] WHERE x > 0) AS b",
            "RETURN all(x IN [1] WHERE x > 0) AS b",
        ),
        (
            "RETURN reduce(`acc` = 0, `x` IN [1] | acc + x) AS s",
            "RETURN reduce(acc = 0, x IN [1] | acc + x) AS s",
        ),
        (
            "MATCH (n) RETURN [`q` = (n)-->() | q] AS ps",
            "MATCH (n) RETURN [q = (n)-->() | q] AS ps",
        ),
        (
            "MATCH (n), (m) RETURN n{`m`, .a} AS mp",
            "MATCH (n), (m) RETURN n{m, .a} AS mp",
        ),
        (
            "MATCH (`n`) SET `n` += {a: 1}, `n`:`L`, `n`.`a` = 2",
            "MATCH (n) SET n += {a: 1}, n:L, n.a = 2",
        ),
        ("MATCH (`n`) SET `n` = {a: 1}", "MATCH (n) SET n = {a: 1}"),
        (
            "MATCH (`n`) REMOVE `n`:`L`, `n`.`a`",
            "MATCH (n) REMOVE n:L, n.a",
        ),
        (
            "MATCH (`n`) WITH `n` AS `m` RETURN m",
            "MATCH (n) WITH n AS m RETURN m",
        ),
        ("RETURN $`p` AS v", "RETURN $p AS v"),
        ("MATCH (n {a: $`p`}) RETURN n", "MATCH (n {a: $p}) RETURN n"),
        ("RETURN `toUpper`('a') AS v", "RETURN toUpper('a') AS v"),
        (
            "CALL `db`.`labels`() YIELD `label` AS `l` RETURN l",
            "CALL db.labels() YIELD label AS l RETURN l",
        ),
    ];
    for (quoted, bare) in cases {
        assert_same_ast(quoted, bare);
    }
}

#[test]
fn quoted_ddl_names_equal_bare_names() {
    let cases = [
        (
            "CREATE INDEX `i` FOR (`p`:`L`) ON (`p`.`a`)",
            "CREATE INDEX i FOR (p:L) ON (p.a)",
        ),
        (
            "CREATE VECTOR INDEX `v` FOR (`p`:`L`) ON `p`.`e`",
            "CREATE VECTOR INDEX v FOR (p:L) ON p.e",
        ),
        (
            "CREATE FULLTEXT INDEX `f` FOR (`p`:`L`) ON EACH [`p`.`a`, `p`.`b`]",
            "CREATE FULLTEXT INDEX f FOR (p:L) ON EACH [p.a, p.b]",
        ),
        (
            "CREATE JSON FULLTEXT INDEX `j` FOR (`p`:`L`) ON `doc`",
            "CREATE JSON FULLTEXT INDEX j FOR (p:L) ON doc",
        ),
        ("DROP INDEX `i`", "DROP INDEX i"),
        ("DROP CONSTRAINT `c`", "DROP CONSTRAINT c"),
        (
            "CREATE CONSTRAINT `c` ON (`p`:`L`) ASSERT `p`.`a` IS UNIQUE",
            "CREATE CONSTRAINT c ON (p:L) ASSERT p.a IS UNIQUE",
        ),
        (
            "CREATE CONSTRAINT `c` ON (`p`:`L`) ASSERT EXISTS(`p`.`a`)",
            "CREATE CONSTRAINT c ON (p:L) ASSERT EXISTS(p.a)",
        ),
        (
            "CREATE CONSTRAINT `c` ON (`p`:`L`) ASSERT (`a`, `b`) IS UNIQUE",
            "CREATE CONSTRAINT c ON (p:L) ASSERT (a, b) IS UNIQUE",
        ),
        (
            "CREATE CONSTRAINT `c` ON (`p`:`L`) ASSERT `p`.`a` > 0",
            "CREATE CONSTRAINT c ON (p:L) ASSERT p.a > 0",
        ),
        (
            "CREATE CONSTRAINT `c` ON ()-[`r`:`T`]-() ASSERT `r`.`a` IS UNIQUE",
            "CREATE CONSTRAINT c ON ()-[r:T]-() ASSERT r.a IS UNIQUE",
        ),
        (
            "CREATE LABEL `L` (`a` STRING, `b` INT NOT NULL)",
            "CREATE LABEL L (a STRING, b INT NOT NULL)",
        ),
        (
            "CREATE EDGE TYPE `T` (`w` FLOAT) FROM `A` TO `B`",
            "CREATE EDGE TYPE T (w FLOAT) FROM A TO B",
        ),
        (
            "ALTER LABEL `L` ADD PROPERTY `c` STRING",
            "ALTER LABEL L ADD PROPERTY c STRING",
        ),
        (
            "ALTER LABEL `L` DROP PROPERTY `c`",
            "ALTER LABEL L DROP PROPERTY c",
        ),
        (
            "ALTER EDGE TYPE `T` RENAME PROPERTY `a` TO `b`",
            "ALTER EDGE TYPE T RENAME PROPERTY a TO b",
        ),
        (
            "ALTER LABEL `L` SET PROPERTY `a` DESCRIPTION 'd'",
            "ALTER LABEL L SET PROPERTY a DESCRIPTION 'd'",
        ),
        (
            "ALTER LABEL `L` DROP PROPERTY `a` DESCRIPTION",
            "ALTER LABEL L DROP PROPERTY a DESCRIPTION",
        ),
        ("DROP LABEL `L`", "DROP LABEL L"),
        ("DROP EDGE TYPE `T`", "DROP EDGE TYPE T"),
        ("SHOW CONSTRAINTS ON (`L`)", "SHOW CONSTRAINTS ON (L)"),
        ("COPY `L` TO 'f.csv'", "COPY L TO 'f.csv'"),
        ("COPY `L` FROM 'f.csv'", "COPY L FROM 'f.csv'"),
    ];
    for (quoted, bare) in cases {
        assert_same_ast(quoted, bare);
    }
}

/// `` `a``b` `` denotes the one name ``a`b`` in every identifier position.
#[test]
fn doubled_backtick_is_an_escaped_backtick() {
    let dbg = ast(
        "MATCH (`a``b`:`L``1` {`k``1`: $`p``1`})-[`r``1`:`T``1`]->(m) \
         WITH `a``b`, `r``1` UNWIND [1] AS `x``1` \
         RETURN `a``b`.`q``1` AS `o``1`, `x``1`, {`m``1`: 1} AS mm",
    );
    for name in [
        "a`b", "L`1", "k`1", "p`1", "r`1", "T`1", "x`1", "q`1", "o`1", "m`1",
    ] {
        assert!(
            dbg.contains(&format!("\"{name}\"")),
            "expected the unescaped name {name:?} in the AST:\n{dbg}"
        );
    }
    assert!(!dbg.contains("``"), "a doubled backtick survived:\n{dbg}");

    // Escapes at the edges: a name that is a lone backtick, and one that starts
    // and ends with one.
    let dbg = ast("RETURN 1 AS ````, 2 AS ```x```");
    assert!(dbg.contains("\"`\""), "lone-backtick alias missing:\n{dbg}");
    assert!(
        dbg.contains("\"`x`\""),
        "edge-backtick alias missing:\n{dbg}"
    );
}

/// The escaped name and the plain name are different names: `` `a``b` `` is not
/// `` `ab` `` nor `` `a` `` followed by garbage.
#[test]
fn doubled_backtick_does_not_collapse_or_split() {
    assert_ne!(ast("RETURN 1 AS `a``b`"), ast("RETURN 1 AS `ab`"));
    assert!(uni_cypher::parse("RETURN 1 AS `a`b`").is_err());
}

#[test]
fn ddl_names_carry_escaped_backticks() {
    let dbg = ast("CREATE INDEX `i``x` FOR (p:`L``1`) ON (p.`a``b`)");
    for name in ["i`x", "L`1", "a`b"] {
        assert!(
            dbg.contains(&format!("\"{name}\"")),
            "expected {name:?} in:\n{dbg}"
        );
    }
    let dbg = ast("CREATE LABEL `L``1` (`a``b` STRING)");
    for name in ["L`1", "a`b"] {
        assert!(
            dbg.contains(&format!("\"{name}\"")),
            "expected {name:?} in:\n{dbg}"
        );
    }
}

/// Locy shares the identifier escape: rule names, columns, generator names
/// and outputs, `prev` fields.
#[test]
fn locy_identifiers_unescape() {
    let program = uni_cypher::parse_locy(
        "CREATE RULE `r``1` AS MATCH (`a``b`:L) YIELD KEY `a``b`, 1 AS `c``1`",
    )
    .expect("parse_locy");
    let dbg = format!("{program:?}");
    for name in ["r`1", "a`b", "c`1"] {
        assert!(
            dbg.contains(&format!("\"{name}\"")),
            "expected {name:?} in:\n{dbg}"
        );
    }
    assert!(!dbg.contains("``"), "a doubled backtick survived:\n{dbg}");

    let program = uni_cypher::parse_locy(
        "CREATE RULE r AS MATCH (a:L) WHERE `g`.`f`(a.x) -> (`o``1`) YIELD KEY a, o`1",
    );
    // The bare `o`1` is not valid; the quoted output must be referenced quoted.
    assert!(program.is_err());
    let program = uni_cypher::parse_locy(
        "CREATE RULE r AS MATCH (a:L) WHERE `g`.`f`(a.x) -> (`o``1`) YIELD KEY a, `o``1`",
    )
    .expect("parse_locy generator");
    let LocyStatement::Rule(rule) = &program.statements[0] else {
        panic!("expected a rule");
    };
    let generator = rule
        .where_conditions
        .iter()
        .find_map(|c| match c {
            RuleCondition::Generator(g) => Some(g),
            _ => None,
        })
        .expect("generator condition");
    assert_eq!(generator.name, "g.f");
    assert_eq!(generator.outputs, vec!["o`1".to_string()]);
}
