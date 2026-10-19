// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A backtick-quoted property name is the same name wherever it appears.
//!
//! Property *access* (`n.`my prop``) stripped the backticks, but a `SET`
//! target, a map-literal key (`CREATE (n {`order`: 1})`, a pattern property
//! map) and a map-projection selector kept them. So `SET n.`order` = 'x'`
//! wrote a property literally named `` `order` `` (an overflow property next
//! to the declared `order` column) and `n.`order`` then read NULL: silent data
//! loss, found while testing pushed filters on SQL-keyword property names.
//!
//! Oracle: after parsing, no backtick survives anywhere in the AST (none of
//! these names contains one), except in a return item's `source_text`, which
//! is the verbatim query text by design.

/// The AST's `Debug` form with every `source_text: Some("…")` value removed.
pub(crate) fn debug_without_source_text(dbg: &str) -> String {
    const OPEN: &str = "source_text: Some(\"";
    let mut out = String::new();
    let mut rest = dbg;
    while let Some(i) = rest.find(OPEN) {
        out.push_str(&rest[..i]);
        rest = &rest[i + OPEN.len()..];
        // Skip the escaped string body up to its closing, unescaped quote.
        let mut chars = rest.char_indices();
        let mut end = rest.len();
        while let Some((j, c)) = chars.next() {
            match c {
                '\\' => {
                    chars.next();
                }
                '"' => {
                    end = j + 1;
                    break;
                }
                _ => {}
            }
        }
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

fn assert_no_backtick(src: &str) {
    let ast = uni_cypher::parse(src).unwrap_or_else(|e| panic!("parse `{src}`: {e:?}"));
    let dbg = debug_without_source_text(&format!("{ast:?}"));
    assert!(
        !dbg.contains('`'),
        "a backtick survived parsing `{src}`:\n{dbg}"
    );
}

#[test]
fn set_target_property_is_unquoted() {
    assert_no_backtick("MATCH (n) SET n.`my prop` = 1");
    assert_no_backtick("MATCH (n) SET n.`order` = 1, n.`null` = 2");
}

#[test]
fn map_literal_keys_are_unquoted() {
    assert_no_backtick("CREATE (n:L {`order`: 1, `my prop`: 'x'})");
    assert_no_backtick("MATCH (n:L {`select`: 1}) RETURN n");
    assert_no_backtick("MATCH (a)-[:R*1..2 {`order`: 'b'}]->(b) RETURN b");
    assert_no_backtick("RETURN {`k k`: 1} AS m");
}

#[test]
fn map_projection_selectors_are_unquoted() {
    assert_no_backtick("MATCH (n) RETURN n{.`order`, `my key`: 1} AS m");
}

#[test]
fn property_access_stays_unquoted() {
    assert_no_backtick("MATCH (n) WHERE n.`my prop` = 1 RETURN n.`order`");
}
