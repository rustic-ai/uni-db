//! Regression guard: the Cypher/Locy grammar must not backtrack exponentially.
//!
//! The nightly `locy_parse` fuzz target timed out on a 130-byte input —
//! `unwind\r\r` followed by runs of unmatched `[` interleaved with `a` — which
//! took ~1.5s locally and exceeded the 10s per-input fuzz budget on CI. Root
//! cause: in `cypher.pest`, `postfix_suffix`'s index (`[e]`) and slice
//! (`[e?..e?]`) alternatives both re-parsed `expression` after `[`, so every `[`
//! doubled the work — O(2^N) on N stacked brackets. Factoring the shared
//! `[ expression` prefix into the `index_or_slice` rule made it linear.
//!
//! **A second instance of the same class** turned up in the 2026-09-18 nightly,
//! in a different rule pair. `primary_expression` offers both
//! `pattern_expression` (via `node_pattern`) and `"(" ~ expression ~ ")"`, and
//! both begin with `(`. `node_pattern` reaches arbitrary nested expressions
//! through `node_predicate` -> `properties` -> `map_literal` -> `map_entry` ->
//! `expression`, and a pattern expression needs a relationship after that first
//! node pattern — which the parser only discovers once the whole interior is
//! parsed. With no relationship there, `"(" ~ expression ~ ")"` then parses the
//! same characters again, doubling the work per nesting level.
//!
//! That one was not confined to malformed input: the **valid** query
//! `RETURN (a {k: (a {k: ... 1 ...})})` took 197 ms at depth 10 and ~3x more per
//! level, and it reached `uni_cypher::parse`, so every Cypher query shared the
//! exposure. Reordering the two alternatives does not fix it — PEG commits to
//! the first alternative that succeeds and never retries, so the parenthesized
//! form first makes `RETURN (a)-->(b)` fail outright, while the malformed case
//! (where both alternatives fail anyway) stays exponential. The fix is the
//! `&pattern_ahead` guard: one linear scan for the matching `)` and the `-`/`<`
//! that every `relationship_pattern` alternative starts with, so the parser
//! never descends into an interior it is about to discard.
//!
//! **A third instance**, found by the PR fuzz lane on 2026-10-11:
//! `list_expression` tried a list comprehension and then a list literal, and
//! `[x IN src ...` starts both — `x IN src` is a comparison, so a valid first
//! element. When the comprehension failed after parsing `src`, the literal parsed
//! `src` again, doubling the work per nesting level. Valid input was exposed as
//! well: `RETURN [x IN [x IN ... [1], 1], 1]` took 2.6 s at depth 14. Both
//! readings parse `src` identically, so `list_comprehension_body` now parses it
//! once and lets the next token decide (`,` / AND / XOR / OR continue a literal),
//! and the literal alternative refuses the `x IN` prefix.
//!
//! These tests assert the pathological inputs parse near-instantly and that the
//! index/slice/comprehension and pattern/parenthesis surfaces still parse, so a
//! future grammar edit that reintroduces either ambiguity fails loudly instead
//! of silently hanging the fuzzer.

use std::time::{Duration, Instant};

/// Run both parser entry points on a worker thread and fail if they do not finish
/// within `budget`. Using a thread (rather than a bare wall-clock assertion on
/// the calling thread) means a regression to exponential time fails cleanly
/// instead of hanging the whole test binary.
fn assert_parses_within(input: &str, budget: Duration) {
    let owned = input.to_string();
    let handle = std::thread::spawn(move || {
        let start = Instant::now();
        let _ = uni_cypher::parse(&owned);
        let _ = uni_cypher::parse_locy(&owned);
        start.elapsed()
    });

    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if handle.is_finished() {
            let elapsed = handle.join().expect("parse thread panicked");
            assert!(
                elapsed < budget,
                "parse took {elapsed:?}, over budget {budget:?}"
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "parse did not finish within {budget:?} for input of {} bytes — \
         exponential backtracking regressed",
        input.len()
    );
}

#[test]
fn fuzz_timeout_artifact_parses_fast() {
    // The exact libFuzzer `locy_parse` timeout artifact
    // (timeout-226c0fb00a6e5a350dbcbb24062758309afd0b67).
    let bytes: &[u8] = &[
        117, 110, 119, 105, 110, 100, 13, 13, 91, 91, 91, 91, 91, 91, 91, 97, 91, 91, 91, 91, 91,
        91, 91, 91, 91, 91, 91, 97, 91, 91, 91, 91, 110, 119, 105, 110, 100, 13, 13, 91, 91, 91,
        91, 91, 91, 91, 97, 91, 91, 91, 75, 91, 91, 97, 91, 91, 110, 119, 105, 110, 100, 13, 13,
        91, 91, 91, 91, 91, 91, 91, 97, 91, 91, 91, 91, 91, 97, 91, 91, 91, 91, 110, 119, 105, 110,
        100, 13, 13, 91, 91, 91, 91, 91, 91, 91, 97, 91, 91, 91, 75, 91, 91, 97, 91, 91, 110, 119,
        105, 110, 100, 13, 13, 91, 91, 91, 91, 91, 91, 75, 91, 91, 97, 91, 91, 91, 75, 62, 61, 120,
        79,
    ];
    let input = std::str::from_utf8(bytes).unwrap();
    assert_parses_within(input, Duration::from_secs(5));
}

#[test]
fn stacked_unmatched_brackets_parse_fast() {
    // Far beyond the artifact: pre-fix this would take longer than the age of
    // the universe; post-fix it is microseconds.
    let input = format!("unwind\r\r{}", "[a".repeat(120));
    assert_parses_within(&input, Duration::from_secs(5));
}

#[test]
fn index_and_slice_surface_still_parses() {
    // The grammar factoring must not change the accepted language.
    let queries = [
        "RETURN xs[0]",
        "RETURN xs[1 + 2]",
        "RETURN xs[1..3]",
        "RETURN xs[..3]",
        "RETURN xs[1..]",
        "RETURN xs[..]",
        "RETURN xs[0][1]",
        "RETURN [1, 2, 3]",
        "RETURN []",
        "RETURN m['key']",
        "RETURN [x IN range(0, 10) WHERE x > 2 | x * 2]",
    ];
    for q in queries {
        assert!(
            uni_cypher::parse_locy(q).is_ok(),
            "expected {q:?} to parse after the index_or_slice factoring"
        );
    }
}

// ── 2026-09-18: pattern_expression vs "(" ~ expression ~ ")" ────────────────

/// The nightly `locy_parse` artifact, trimmed to its repeating core.
///
/// `G=({G:` stacks an unclosed `(` and `{` per repetition. The full 942-byte
/// artifact exceeded 25s locally pre-fix; this shape reached 53s at 12
/// repetitions.
#[test]
fn stacked_paren_brace_map_keys_parse_fast() {
    let input = format!("set\rE{}", "G=({G:".repeat(40));
    assert_parses_within(&input, Duration::from_secs(5));
}

/// The same defect on **valid**, fully-closed input — the reason a guard keyed
/// on malformed or unclosed text would not have been enough.
#[test]
fn deeply_nested_valid_map_patterns_parse_fast() {
    let mut q = String::from("1");
    for _ in 0..40 {
        q = format!("(a {{k: {q}}})");
    }
    assert_parses_within(&format!("RETURN {q}"), Duration::from_secs(5));
}

/// A pattern expression must still be recognised, and the lookahead must find
/// the *matching* `)` — not the first one it meets.
#[test]
fn pattern_and_parenthesis_surface_still_parses() {
    let queries = [
        // Pattern expressions: the guard must let these through.
        "RETURN (a)-->(b)",
        "RETURN (a)--(b)",
        "RETURN (a)<--(b)",
        "RETURN (a)-[r:T*1..2]->(b)",
        "RETURN (a)-->(b)-->(c)",
        // Nested parens inside the first node's properties: a scan that stopped
        // at the first `)` would miss the relationship and reject these.
        "RETURN (a {k: (1 + 2)})-->(b)",
        "RETURN (a {k: abs(-1)})-->(b)",
        "RETURN (a {k: [1, (2)]})-->(b)",
        // A bracket inside a string literal must not unbalance the scan.
        "RETURN (a {k: ')'})-->(b)",
        "RETURN (a {k: '()('})-->(b)",
        // Trivia between the node pattern and its relationship.
        "RETURN (a) /* c */ -->(b)",
        "RETURN (a) // c\n-->(b)",
        // Parenthesized expressions: the guard must NOT divert these.
        "RETURN (1 + 2) * 3",
        "RETURN (a {k: 1})",
        "RETURN ((a))",
        "RETURN (a) - (b)",
    ];
    for q in queries {
        assert!(
            uni_cypher::parse(q).is_ok(),
            "expected {q:?} to parse after the pattern_ahead guard"
        );
    }
}

// ── 2026-10-11: list comprehension vs list literal ────────────────────────────

/// The PR-lane `locy_parse` artifact (timeout-231f8bed8a1a3ff4e85491cfe8e4dea560e8b0b3):
/// unclosed `[x IN` runs, 36 s under the fuzz build before the fix.
#[test]
fn list_comprehension_fuzz_artifact_parses_fast() {
    let bytes: &[u8] = &[
        117, 110, 119, 105, 110, 100, 13, 13, 91, 45, 91, 91, 91, 110, 119, 100, 13, 105, 110, 13,
        91, 114, 114, 122, 100, 13, 13, 91, 91, 91, 91, 100, 13, 105, 110, 32, 32, 32, 32, 32, 100,
        13, 13, 91, 91, 91, 91, 100, 13, 105, 110, 32, 32, 91, 45, 91, 91, 91, 100, 13, 105, 110,
        32, 32, 91, 45, 91, 91, 91, 110, 119, 100, 13, 105, 110, 13, 91, 114, 114, 122, 100, 13,
        13, 91, 91, 91, 91, 100, 13, 105, 110, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 100,
        13, 13, 91, 91, 91, 91, 91, 110, 119, 100, 13, 105, 110, 13, 91, 114, 114, 122, 100, 13,
        13, 91, 91, 91, 91, 100, 13, 105, 110, 32, 32, 32, 32, 45, 91, 91, 91, 110, 119, 100, 13,
        105, 110, 13, 91, 114, 114, 122, 100, 13, 13, 91, 91, 91, 91, 100, 13, 105, 110, 32, 32,
        32, 32, 32, 100, 13, 13, 91, 91, 91, 91, 100, 13, 105, 110, 32, 32, 91, 45, 91, 91, 91,
        100, 13, 105, 110, 32, 32, 91, 45, 91, 91, 91, 110, 119, 100, 13, 105, 110, 13, 91, 114,
        114, 122, 100, 13, 13, 91, 91, 91, 91, 100, 13, 105, 110, 32, 32, 32, 32, 32, 32, 32, 32,
        32, 32, 32, 32, 100, 13, 13, 91, 91, 91, 91, 91, 110, 119, 100, 13, 105, 110, 13, 91, 114,
        114, 122, 100, 13, 13, 91, 91, 91, 91, 100, 13, 105, 110, 32, 32, 32, 32, 32, 32, 32, 32,
        32, 32, 32, 32, 100, 13, 13, 91, 91, 91, 91, 58, 100, 13, 105, 110, 32, 32, 32, 32, 32, 32,
        32, 32, 83, 0, 0, 0, 32, 32, 32, 32, 32, 32, 32, 32, 100, 13, 13, 91, 91, 91, 91, 58, 100,
        13, 105, 110, 32, 32, 32, 32, 32, 32, 32, 32, 83, 0, 0, 0, 0, 0, 0, 0, 32, 32, 61, 120, 79,
    ];
    let input = String::from_utf8_lossy(bytes);
    assert_parses_within(&input, Duration::from_secs(5));
}

#[test]
fn unclosed_nested_comprehension_sources_parse_fast() {
    let input = format!("UNWIND {}", "[x IN ".repeat(60));
    assert_parses_within(&input, Duration::from_secs(5));
}

/// Valid literals led by `x IN src`, nested: each level's comprehension reading
/// fails only after its whole interior is parsed.
#[test]
fn deeply_nested_valid_in_led_literals_parse_fast() {
    for tail in [", 1]", " AND true]", " OR false]", " XOR true]"] {
        let q = format!("RETURN {}[1]{}", "[x IN ".repeat(40), tail.repeat(40));
        assert_parses_within(&q, Duration::from_secs(5));
        assert!(uni_cypher::parse(&q).is_ok(), "expected {q:?} to parse");
    }
}

/// The factored path must build what the literal alternative built. The oracle
/// parenthesises the first element, which keeps it off the `x IN` path entirely.
#[test]
fn in_led_list_literals_build_the_same_ast() {
    let cases = [
        ("[x IN l, 1]", "[(x IN l), 1]"),
        ("[x IN l AND a]", "[(x IN l AND a)]"),
        ("[x IN l AND a, 2, 3]", "[(x IN l AND a), 2, 3]"),
        ("[x IN l OR a AND b XOR c]", "[(x IN l OR a AND b XOR c)]"),
        (
            "[x IN l AND NOT a XOR b OR c, d]",
            "[(x IN l AND NOT a XOR b OR c), d]",
        ),
        ("[x IN l = y, 1]", "[(x IN l = y), 1]"),
        ("[x IN l < y <= z AND a]", "[(x IN l < y <= z AND a)]"),
        ("[x IN l IS NULL, 1]", "[(x IN l IS NULL), 1]"),
        ("[x IN l + [2], 1]", "[(x IN l + [2]), 1]"),
        ("[x IN [y IN m, 1], 2]", "[(x IN [(y IN m), 1]), 2]"),
        ("[`x` IN l, 1]", "[(`x` IN l), 1]"),
        ("[count IN l, 1]", "[(count IN l), 1]"),
    ];
    for (q, oracle) in cases {
        // Compare whole results: a refusal must match too. `x IN l IS NULL`
        // stacks two predicates, which the literal path always refused.
        let built = uni_cypher::parse_expression(q).map_err(|e| e.to_string());
        let expected = uni_cypher::parse_expression(oracle).map_err(|e| e.to_string());
        assert_eq!(built, expected, "{q:?} must build what {oracle:?} builds");
    }
    assert!(uni_cypher::parse_expression("[x IN l, 1]").is_ok());
}

/// A comprehension stays a comprehension, including when its source is a chain
/// that a literal could also have read (`[x IN l = y]`).
#[test]
fn list_comprehensions_still_parse_as_comprehensions() {
    let cases = [
        ("[x IN l]", "[x IN (l)]"),
        ("[x IN l WHERE x > 1]", "[x IN (l) WHERE x > 1]"),
        ("[x IN l | x * 2]", "[x IN (l) | x * 2]"),
        (
            "[x IN l WHERE x > 1 | x * 2]",
            "[x IN (l) WHERE x > 1 | x * 2]",
        ),
        ("[x IN l = y]", "[x IN (l = y)]"),
        ("[x IN l IS NULL | x]", "[x IN (l IS NULL) | x]"),
        ("[x IN [y IN m | y] | x]", "[x IN ([y IN (m) | y]) | x]"),
    ];
    for (q, oracle) in cases {
        let built = uni_cypher::parse_expression(q).unwrap_or_else(|e| panic!("{q:?}: {e}"));
        assert!(
            matches!(built, uni_cypher::ast::Expr::ListComprehension { .. }),
            "{q:?} must be a comprehension, got {built:?}"
        );
        assert_eq!(
            built,
            uni_cypher::parse_expression(oracle).unwrap_or_else(|e| panic!("{oracle:?}: {e}")),
            "{q:?} must build the same AST as {oracle:?}"
        );
    }
}

/// Malformed `x IN`-led lists are still refused.
#[test]
fn malformed_in_led_lists_are_still_refused() {
    for q in [
        "[x IN l WHERE p, 1]",
        "[x IN l | x, 1]",
        "[x IN l AND]",
        "[x IN , 1]",
        "[x IN l",
    ] {
        assert!(
            uni_cypher::parse_expression(q).is_err(),
            "expected {q:?} to be refused"
        );
    }
}

// The 2026-10-27 Cypher fuzz timeout also exposes valid list literals: their
// pattern-comprehension reading descends into nested properties before failing.
#[test]
fn nested_list_map_projections_parse_fast() {
    let q = format!("RETURN {}1{}", "[(a {k: ".repeat(40), "})]".repeat(40));
    assert_parses_within(&q, Duration::from_secs(5));
    assert!(uni_cypher::parse(&q).is_ok());
}

#[test]
fn cypher_list_pattern_timeout_artifact_parses_fast() {
    let input = include_str!("data/cypher_list_pattern_timeout.cypher");
    assert_parses_within(input, Duration::from_secs(5));
    assert!(uni_cypher::parse(input).is_err());
}

#[test]
fn pattern_comprehension_lookahead_preserves_nested_and_quoted_pipes() {
    for q in [
        "RETURN [(a)-->(b) | b]",
        "RETURN [p = (a)-->(b) WHERE a.x > 1 | p]",
        "RETURN [(a {k: [1, 2]})-->(b) | b]",
        "RETURN [(a:`a|b` {k: '])|['})-->(b) | b]",
        "RETURN [(a {k: 1}) /* ) ] | */ -->(b) | b]",
        "RETURN [(a)-->(b) WHERE b.x IN [x IN [1, 2] | x] | b]",
        "RETURN [(a)-->(b) | [(b)-->(c) | c]]",
        "RETURN [(a WHERE a.x > 1)-->(b) | b]",
        "RETURN [(a {k: 1} WHERE a.x > 1)-->(b) | b]",
    ] {
        assert!(uni_cypher::parse(q).is_ok(), "expected {q:?} to parse");
    }
    for q in [
        "RETURN [(a {k: 1})]",
        "RETURN [(a {k: '|'})]",
        "RETURN [1, [2]]",
    ] {
        assert!(uni_cypher::parse(q).is_ok(), "expected {q:?} to parse");
    }
    for q in ["RETURN [(a)-->(b) |]", "RETURN [(a)-->(b) WHERE | b]"] {
        assert!(uni_cypher::parse(q).is_err(), "expected {q:?} to fail");
    }
}

// ── 2026-10-27 (second pass): CREATE pattern & pattern_ahead / rel_predicate ──

#[test]
fn cypher_create_pattern_timeout_artifact_parses_fast() {
    let input = include_str!("data/cypher_create_pattern_timeout.cypher");
    assert_parses_within(input, Duration::from_secs(5));
    assert!(uni_cypher::parse(input).is_err());
}

#[test]
fn nested_paren_map_subtraction_and_comparison_parse_fast() {
    // Previously `pattern_ahead` checked only for a bare `-` or `<` after the
    // closing `)` of a `(`-led primary expression, so `(a {k: ...}) - 1`,
    // `(a {k: ...}) < 1`, `(a {k: ...})--(1)` (subtraction of `-1`), and
    // comments/backtick identifiers containing `)-` inside `(a {k: ...})`
    // matched `pattern_ahead`, parsed the nested map as `node_pattern`, failed
    // on the relationship/target node, and re-parsed the whole interior via
    // `"(" ~ expression ~ ")"`.
    for tail in [") - 1", ") < 1", ") <= 1", ") <> 1", ")--(1)", ")"] {
        let open = if tail == ")" {
            "(a {/*)-*/ `)-`: "
        } else {
            "(a {k: "
        };
        let q = format!(
            "RETURN {}1{}",
            open.repeat(40),
            format!("}}{tail}").repeat(40)
        );
        assert_parses_within(&q, Duration::from_secs(5));
        assert!(uni_cypher::parse(&q).is_ok(), "expected {q:?} to parse");
    }
}

#[test]
fn nested_rel_predicate_properties_without_where_parse_fast() {
    // `rel_predicate` previously tried `properties ~ WHERE ~ expression` before
    // `properties`, re-parsing relationship property maps whenever `WHERE` was
    // omitted.
    let q = format!(
        "RETURN {}1{}",
        "[(a)-[r {k: ".repeat(30),
        "}]->(b) | b]".repeat(30)
    );
    assert_parses_within(&q, Duration::from_secs(5));
    assert!(uni_cypher::parse(&q).is_ok());
}

#[test]
fn probability_projection_keeps_shorthand_and_explicit_aliases() {
    use uni_cypher::locy_ast::{LocyStatement, RuleOutput};

    for (projection, expected_alias, is_prob) in [
        ("p AS PROB", Some("p"), true),
        ("p PROB", Some("p"), true),
        ("p AS score PROB", Some("score"), true),
        ("n.p AS PROB", Some("p"), true),
        ("p AS PROB PROB", Some("PROB"), true),
        ("p AS score", Some("score"), false),
        ("p AS PROBfoo", Some("PROBfoo"), false),
    ] {
        let source = format!("CREATE RULE r AS MATCH (n) YIELD {projection}");
        let program = uni_cypher::parse_locy(&source).expect("parse rule");
        let LocyStatement::Rule(rule) = &program.statements[0] else {
            panic!("expected rule");
        };
        let RuleOutput::Yield(output) = &rule.output else {
            panic!("expected YIELD");
        };
        assert_eq!(output.items.len(), 1);
        assert_eq!(output.items[0].is_prob, is_prob, "{projection}");
        assert_eq!(
            output.items[0].alias.as_deref(),
            expected_alias,
            "{projection}"
        );
    }
}
