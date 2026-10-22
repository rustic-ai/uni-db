pub(crate) mod locy_parser;
mod locy_walker;
mod walker;

use crate::ast::{Expr, Query};
use crate::locy_ast::LocyProgram;
use pest::Parser;
use pest_derive::Parser;

/// Error type for Cypher parsing failures.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ParseError {
    message: String,
}

impl ParseError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

#[derive(Parser)]
#[grammar = "grammar/cypher.pest"]
pub struct CypherParser;

/// Maximum supported nesting depth of bracketing constructs and `CASE`
/// expressions in a query.
///
/// Both the pest parser and the AST walker are recursive-descent, so a query
/// with thousands of nested parens / lists / maps / `CASE` expressions (or
/// nested parenthesized patterns) would otherwise exhaust the thread stack and
/// `abort()` the host process — an uncatchable crash for an embedded library
/// triggered by a query string. The ceiling sits far above any legitimate
/// query's nesting yet well below the depth at which the parser overflows even
/// a small (1 MiB) stack.
const MAX_NESTING_DEPTH: u32 = 200;

/// Rejects an `input` that nests bracketing constructs / `CASE` expressions
/// deeper than [`MAX_NESTING_DEPTH`], before any recursive parsing begins.
///
/// Counts `(`/`[`/`{` and the `CASE` keyword as opening a level and `)`/`]`/`}`
/// and the `END` keyword as closing one, tracking the maximum live depth.
/// Brackets and keywords inside string / backtick literals and `//` or `/* */`
/// comments are skipped. This is a deliberately conservative O(n) check: it may
/// over-count, but never under-counts the nesting the parser would recurse into.
///
/// # Errors
///
/// Returns [`ParseError`] when nesting exceeds [`MAX_NESTING_DEPTH`].
fn check_nesting_depth(input: &str) -> Result<(), ParseError> {
    let bytes = input.as_bytes();
    let mut i = 0usize;
    let mut depth: i32 = 0;
    let mut max_depth: i32 = 0;

    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'\'' | b'"') => {
                // String literal: skip to the matching quote, honoring `\` escapes.
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        c if c == quote => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
            }
            b'`' => {
                // Backtick-quoted identifier. A doubled backtick (the escape)
                // scans as two adjacent quoted spans, which skips the same text.
                i += 1;
                while i < bytes.len() && bytes[i] != b'`' {
                    i += 1;
                }
                i += 1;
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i += 2;
            }
            b'(' | b'[' | b'{' => {
                depth += 1;
                max_depth = max_depth.max(depth);
                i += 1;
            }
            b')' | b']' | b'}' => {
                depth = (depth - 1).max(0);
                i += 1;
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                // Read a whole word so only the bare keywords CASE / END count.
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &input[start..i];
                if word.eq_ignore_ascii_case("case") {
                    depth += 1;
                    max_depth = max_depth.max(depth);
                } else if word.eq_ignore_ascii_case("end") {
                    // `end` is a non-reserved keyword, so `end(...)` is a legal
                    // recursive function call — NOT a `CASE` close. Treating it as
                    // a close would cancel the following `(`'s increment, letting
                    // `end(end(...))` recurse to native-stack exhaustion while the
                    // counter stays near zero (under-counting the depth the parser
                    // recurses into, which this guard must never do). Only decrement
                    // when the word is not immediately followed (past whitespace) by
                    // an opening paren; a real `CASE ... END` never is.
                    let mut j = i;
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if bytes.get(j) != Some(&b'(') {
                        depth = (depth - 1).max(0);
                    }
                }
            }
            _ => i += 1,
        }

        if max_depth as u32 > MAX_NESTING_DEPTH {
            return Err(ParseError::new(format!(
                "SyntaxError: NestingTooDeep - query nesting exceeds the maximum \
                 supported depth ({MAX_NESTING_DEPTH})"
            )));
        }
    }

    Ok(())
}

pub fn parse(input: &str) -> Result<Query, ParseError> {
    check_nesting_depth(input)?;
    let pairs = CypherParser::parse(Rule::query, input).map_err(|e| map_pest_error(input, e))?;

    walker::build_query(pairs)
}

/// Parse `input` like [`parse`], and also return the name of every parameter
/// it references, in order of first appearance and without repeats.
///
/// Read from the parse tree, so a parameter anywhere in the statement counts
/// (subqueries, `SKIP`/`LIMIT`, procedure arguments, property maps), and text
/// that only looks like one (inside a string literal or a comment) does not.
/// openCypher reports missing parameters before execution, all at once.
///
/// # Errors
///
/// As [`parse`].
///
/// # Examples
/// ```
/// let (_, names) = uni_cypher::parse_with_parameters(
///     "MATCH (n) WHERE n.x = $b RETURN $a, '$c', $b",
/// )
/// .unwrap();
/// assert_eq!(names, ["b", "a"]);
/// ```
pub fn parse_with_parameters(input: &str) -> Result<(Query, Vec<String>), ParseError> {
    check_nesting_depth(input)?;
    let pairs = CypherParser::parse(Rule::query, input).map_err(|e| map_pest_error(input, e))?;
    let mut names: Vec<String> = Vec::new();
    for pair in pairs.clone().flatten() {
        if pair.as_rule() == Rule::parameter {
            let name = walker::parameter_name(&pair);
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    Ok((walker::build_query(pairs)?, names))
}

pub fn parse_expression(input: &str) -> Result<Expr, ParseError> {
    check_nesting_depth(input)?;
    // Parse via the SOI/EOI-anchored `standalone_expression` so the entire input
    // must be a single complete expression; trailing garbage now errors instead
    // of being silently dropped (callers include untrusted plugin trigger
    // conditions and UDF bodies that rely on rejecting malformed source).
    let pairs = CypherParser::parse(Rule::standalone_expression, input)
        .map_err(|e| map_pest_error(input, e))?;
    let standalone = pairs.into_iter().next().unwrap();
    let expr = standalone
        .into_inner()
        .find(|p| p.as_rule() == Rule::expression)
        .expect("standalone_expression always wraps an expression");
    walker::build_expression(expr)
}

pub fn parse_locy(input: &str) -> Result<LocyProgram, ParseError> {
    use locy_parser::LocyParser;
    use locy_parser::Rule as LocyRule;

    check_nesting_depth(input)?;
    let pairs = LocyParser::parse(LocyRule::locy_query, input)
        .map_err(|e| map_locy_pest_error(input, e))?;

    locy_walker::build_program(pairs.into_iter().next().unwrap())
}

/// Parse a Locy program like [`parse_locy`], and also return the name of every
/// parameter it references, in order of first appearance and without repeats.
///
/// # Errors
///
/// As [`parse_locy`].
///
/// # Examples
/// ```
/// let (_, names) = uni_cypher::parse_locy_with_parameters(
///     "CREATE RULE r AS MATCH (n) WHERE n.k = $x YIELD KEY n QUERY r WHERE n.v = $y RETURN n",
/// )
/// .unwrap();
/// assert_eq!(names, ["x", "y"]);
/// ```
pub fn parse_locy_with_parameters(input: &str) -> Result<(LocyProgram, Vec<String>), ParseError> {
    use locy_parser::LocyParser;
    use locy_parser::Rule as LocyRule;

    check_nesting_depth(input)?;
    let pairs = LocyParser::parse(LocyRule::locy_query, input)
        .map_err(|e| map_locy_pest_error(input, e))?;
    let mut names: Vec<String> = Vec::new();
    for pair in pairs.clone().flatten() {
        if pair.as_rule() == LocyRule::parameter {
            let name = walker::normalize_identifier(&pair.as_str()[1..]);
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    let program = locy_walker::build_program(pairs.into_iter().next().unwrap())?;
    Ok((program, names))
}

/// Returns true if the pest error expects an identifier-like rule at the error position.
/// Used to gate the reserved-keyword check so it only fires when a keyword is used
/// where a variable name is expected, not when it appears after unrelated syntax errors.
fn expects_identifier(e: &pest::error::Error<Rule>) -> bool {
    use pest::error::ErrorVariant;
    match &e.variant {
        ErrorVariant::ParsingError { positives, .. } => positives
            .iter()
            .any(|r| matches!(r, Rule::identifier | Rule::identifier_or_keyword)),
        _ => false,
    }
}

/// Locy analogue of [`expects_identifier`]: true only when the parser failed at a
/// position where an identifier was a valid next token. Used to gate the
/// reserved-keyword diagnostic so a genuine syntax error (e.g. a misplaced
/// operator) is not mislabelled as "X is a reserved keyword". Mirrors the guard
/// the Cypher path already applies (`map_pest_error`).
fn expects_locy_identifier(e: &pest::error::Error<locy_parser::Rule>) -> bool {
    use pest::error::ErrorVariant;
    match &e.variant {
        ErrorVariant::ParsingError { positives, .. } => positives
            .iter()
            .any(|r| matches!(r, locy_parser::Rule::locy_identifier)),
        _ => false,
    }
}

fn error_position<R: pest::RuleType>(e: &pest::error::Error<R>) -> usize {
    match e.location {
        pest::error::InputLocation::Pos(p) => p,
        pest::error::InputLocation::Span((s, _)) => s,
    }
}

fn extract_token_span_at(input: &str, pos: usize) -> Option<(usize, usize)> {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    let mut p = pos.min(bytes.len() - 1);

    let is_token_char =
        |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'#' | b'$');

    if !is_token_char(bytes[p]) {
        if p == 0 || !is_token_char(bytes[p - 1]) {
            return None;
        }
        p -= 1;
    }

    let mut start = p;
    while start > 0 && is_token_char(bytes[start - 1]) {
        start -= 1;
    }

    let mut end = p;
    while end < bytes.len() && is_token_char(bytes[end]) {
        end += 1;
    }

    Some((start, end))
}

fn is_map_key_like_context(input: &str, start: usize, end: usize) -> bool {
    let bytes = input.as_bytes();
    if bytes.is_empty() || start >= bytes.len() || end > bytes.len() {
        return false;
    }

    let mut colon_pos = end;
    while colon_pos < bytes.len() && bytes[colon_pos].is_ascii_whitespace() {
        colon_pos += 1;
    }
    if colon_pos >= bytes.len() || bytes[colon_pos] != b':' {
        return false;
    }

    let mut prev_pos = start;
    while prev_pos > 0 && bytes[prev_pos - 1].is_ascii_whitespace() {
        prev_pos -= 1;
    }
    if prev_pos == 0 {
        return false;
    }

    matches!(bytes[prev_pos - 1], b'{' | b',')
}

fn relationship_bracket_segment(input: &str, pos: usize) -> Option<&str> {
    let pos = pos.min(input.len());
    let before = &input[..pos];
    let start = before.rfind('[')?;

    // Restrict to relationship patterns: ...-[ ... ]-...
    let prefix = &input[..start];
    if !prefix.trim_end().ends_with('-') {
        return None;
    }

    let after = &input[start..];
    let end = after.find(']').map(|i| start + i + 1).unwrap_or(pos);
    Some(&input[start..end])
}

fn is_invalid_relationship_pattern(input: &str, pos: usize) -> bool {
    let Some(segment) = relationship_bracket_segment(input, pos) else {
        return false;
    };
    // [:LIKES..] (missing `*`) or [:LIKES*-2] (negative range bound)
    (segment.contains("..") && !segment.contains('*')) || segment.contains("*-")
}

fn is_invalid_number_literal(input: &str, pos: usize) -> bool {
    let Some((start, end)) = extract_token_span_at(input, pos) else {
        return false;
    };
    if is_map_key_like_context(input, start, end) {
        return false;
    }
    let token = &input[start..end];

    let t = token.strip_prefix('-').unwrap_or(token);
    if !t.as_bytes().first().is_some_and(|b| b.is_ascii_digit()) {
        return false;
    }

    let has_only = |digits: &str, valid: fn(&char) -> bool| {
        digits.is_empty() || !digits.chars().all(|c| valid(&c) || c == '_')
    };

    if let Some(digits) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        return has_only(digits, char::is_ascii_hexdigit);
    }
    if let Some(digits) = t.strip_prefix("0o").or_else(|| t.strip_prefix("0O")) {
        return has_only(digits, |c| matches!(c, '0'..='7'));
    }

    // Decimal-like token with alphabetic suffix/midfix, e.g. 9223372h54775808
    t.chars().any(|c| c.is_ascii_alphabetic())
}

fn invalid_unicode_character(input: &str, pos: usize) -> Option<char> {
    let ch = input.get(pos..)?.chars().next()?;
    matches!(ch, '—' | '–' | '−').then_some(ch)
}

/// All Cypher reserved keywords (from `keyword_reserved` in cypher.pest).
/// Stored lowercase for case-insensitive comparison.
const CYPHER_RESERVED_KEYWORDS: &[&str] = &[
    "match",
    "optional",
    "where",
    "create",
    "merge",
    "set",
    "remove",
    "delete",
    "detach",
    "return",
    "with",
    "unwind",
    "union",
    "call",
    "yield",
    "distinct",
    "order",
    "by",
    "asc",
    "desc",
    "skip",
    "limit",
    "as",
    "and",
    "or",
    "xor",
    "not",
    "in",
    "contains",
    "starts",
    "ends",
    "is",
    "null",
    "true",
    "false",
    "case",
    "when",
    "then",
    "else",
    "if",
    "from",
    "to",
    "on",
    "drop",
    "alter",
    "show",
    "over",
    "partition",
    "explain",
    "recursive",
    "valid_at",
    "each",
];

/// Additional Locy-only reserved keywords (from `locy_keyword_reserved` in locy.pest).
const LOCY_RESERVED_KEYWORDS: &[&str] = &[
    "rule", "along", "prev", "fold", "best", "derive", "assume", "abduce", "query",
];

/// If the token at the error position is a reserved keyword, return it.
fn reserved_keyword_at(input: &str, pos: usize, extra_keywords: &[&str]) -> Option<String> {
    let (start, end) = extract_token_span_at(input, pos)?;
    let token = &input[start..end];
    let lower = token.to_lowercase();
    if CYPHER_RESERVED_KEYWORDS.contains(&lower.as_str())
        || extra_keywords.contains(&lower.as_str())
    {
        Some(token.to_string())
    } else {
        None
    }
}

/// Categorize a Locy parse error based on context before the error position.
fn locy_context_category(input: &str, pos: usize) -> Option<&'static str> {
    let before = input[..pos].trim_end();
    let before_upper = before.to_uppercase();
    // Check in reverse order of specificity
    if before_upper.ends_with("BEST BY") {
        return Some("InvalidBestByClause");
    }
    if before_upper.ends_with("ALONG") {
        return Some("InvalidAlongClause");
    }
    if before_upper.ends_with("FOLD") {
        return Some("InvalidFoldClause");
    }
    if before_upper.ends_with("ASSUME") {
        return Some("InvalidAssumeBlock");
    }
    if before_upper.ends_with("DERIVE") {
        return Some("InvalidDeriveCommand");
    }
    // Check for CREATE RULE (may have name/priority between)
    if before_upper.contains("CREATE RULE") {
        return Some("InvalidRuleDefinition");
    }
    // Standalone QUERY (not part of CREATE RULE ... YIELD ... QUERY)
    if before_upper.ends_with("QUERY") && !before_upper.contains("CREATE RULE") {
        return Some("InvalidGoalQuery");
    }
    None
}

fn map_locy_pest_error(input: &str, e: pest::error::Error<locy_parser::Rule>) -> ParseError {
    let pos = error_position(&e);

    // Reuse input-based heuristics from the Cypher parser
    if is_invalid_relationship_pattern(input, pos) {
        return ParseError::new(format!("LocySyntaxError: InvalidRelationshipPattern - {e}"));
    }
    if is_invalid_number_literal(input, pos) {
        return ParseError::new(format!("LocySyntaxError: InvalidNumberLiteral - {e}"));
    }
    if let Some(ch) = invalid_unicode_character(input, pos) {
        return ParseError::new(format!(
            "LocySyntaxError: InvalidUnicodeCharacter - Invalid character '{ch}'"
        ));
    }
    if let Some(kw) = expects_locy_identifier(&e)
        .then(|| reserved_keyword_at(input, pos, LOCY_RESERVED_KEYWORDS))
        .flatten()
    {
        return ParseError::new(format!(
            "LocySyntaxError: ReservedKeyword - \"{kw}\" is a reserved keyword \
             and cannot be used as a variable name. Use backtick-quoting: `{kw}`\n{e}"
        ));
    }

    // Locy-specific context categorization
    if let Some(category) = locy_context_category(input, pos) {
        return ParseError::new(format!("LocySyntaxError: {category} - {e}"));
    }

    ParseError::new(format!("LocySyntaxError: {e}"))
}

fn map_pest_error(input: &str, e: pest::error::Error<Rule>) -> ParseError {
    let pos = error_position(&e);
    if is_invalid_relationship_pattern(input, pos) {
        return ParseError::new(format!("SyntaxError: InvalidRelationshipPattern - {e}"));
    }
    if is_invalid_number_literal(input, pos) {
        return ParseError::new(format!("SyntaxError: InvalidNumberLiteral - {e}"));
    }
    if let Some(ch) = invalid_unicode_character(input, pos) {
        return ParseError::new(format!(
            "SyntaxError: InvalidUnicodeCharacter - Invalid character '{ch}'"
        ));
    }
    if let Some(kw) = expects_identifier(&e)
        .then(|| reserved_keyword_at(input, pos, &[]))
        .flatten()
    {
        return ParseError::new(format!(
            "SyntaxError: ReservedKeyword - \"{kw}\" is a reserved keyword \
             and cannot be used as a variable name. Use backtick-quoting: `{kw}`\n{e}"
        ));
    }

    ParseError::new(format!("UnexpectedSyntax: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expression_parsing() {
        let cases = [
            ("1", Rule::integer),
            ("3.14", Rule::float),
            ("'hello'", Rule::string),
            ("n.name", Rule::expression),
            ("1 + 2", Rule::expression),
            ("a AND b OR c", Rule::expression),
        ];

        for (input, rule) in cases {
            let result = CypherParser::parse(rule, input);
            assert!(
                result.is_ok(),
                "Failed to parse '{}' as {:?}: {:?}",
                input,
                rule,
                result.err()
            );
        }
    }

    #[test]
    fn test_list_expressions() {
        // Empty list
        assert!(parse_expression("[]").is_ok());

        // List literal
        assert!(parse_expression("[1, 2, 3]").is_ok());

        // List comprehension
        assert!(parse_expression("[x IN range(1,10) | x * 2]").is_ok());
        assert!(parse_expression("[x IN list WHERE x > 5 | x]").is_ok());

        // Pattern comprehension - THE KEY TEST
        assert!(parse_expression("[(n)-[:KNOWS]->(m) | m.name]").is_ok());
        assert!(parse_expression("[p = (n)-->(m) WHERE m.age > 30 | p]").is_ok());
    }

    #[test]
    fn test_ambiguous_cases() {
        // These caused LR(1) conflicts before
        assert!(parse_expression("[n]").is_ok()); // List with variable
        assert!(parse_expression("[n.name]").is_ok()); // List with property
        assert!(parse_expression("[n IN list]").is_ok()); // Comprehension? No, missing |, so list with boolean IN expression?
        // Wait, [n IN list] in Cypher is valid list literal containing one boolean expression `n IN list`.
        // UNLESS it's a comprehension. Comprehension MUST have `|`.
        // My grammar handles this:
        // list_expression = { ... | "[" ~ list_comprehension_body ~ "]" | ... }
        // list_comprehension_body = { identifier ~ IN ~ comprehension_expr ~ ... ~ pipe ~ expression }
        // So `[n IN list]` matches `list_literal` containing `expression(n IN list)`.
        // It does NOT match `list_comprehension_body` because of missing pipe.
        // Correct.

        assert!(parse_expression("[(n)]").is_ok()); // Pattern comprehension? No, pattern comprehension must have pattern.
        // `[(n)]` -> List literal containing parenthesized expression `(n)` (node pattern used as expr? No, `(n)` is node pattern).
        // But `(n)` as expression?
        // `primary_expression` -> `(` expression `)`.
        // If `n` is identifier, `(n)` is expression.
        // So `[(n)]` is list literal.
        // `[(n)-->(m)]`? List literal containing boolean pattern expression?
        // Yes, `pattern_expression` is valid in `boolean_primary`.
        // `pattern_comprehension` requires `|`.
        // `[(n)-->(m) | x]` is comprehension.
        // `[(n)-->(m)]` is list of pattern expression.
    }

    fn parse_err_msg(input: &str) -> String {
        parse(input).unwrap_err().to_string()
    }

    #[test]
    fn test_invalid_relationship_pattern_missing_star_error_code() {
        let msg = parse_err_msg("MATCH (a:A)\nMATCH (a)-[:LIKES..]->(c)\nRETURN c.name");
        assert!(
            msg.contains("InvalidRelationshipPattern"),
            "expected InvalidRelationshipPattern, got: {msg}"
        );
    }

    #[test]
    fn test_invalid_number_literal_error_code_decimal_alpha() {
        let msg = parse_err_msg("RETURN 9223372h54775808 AS literal");
        assert!(
            msg.contains("InvalidNumberLiteral"),
            "expected InvalidNumberLiteral, got: {msg}"
        );
    }

    #[test]
    fn test_invalid_number_literal_error_code_hex_prefix_only() {
        let msg = parse_err_msg("RETURN 0x AS literal");
        assert!(
            msg.contains("InvalidNumberLiteral"),
            "expected InvalidNumberLiteral, got: {msg}"
        );
    }

    #[test]
    fn test_invalid_unicode_character_error_code() {
        let msg = parse_err_msg("RETURN 42 — 41");
        assert!(
            msg.contains("InvalidUnicodeCharacter"),
            "expected InvalidUnicodeCharacter, got: {msg}"
        );
    }

    #[test]
    fn test_symbol_in_number_stays_unexpected_syntax() {
        let msg = parse_err_msg("RETURN 9223372#54775808 AS literal");
        assert!(
            msg.contains("UnexpectedSyntax"),
            "expected UnexpectedSyntax, got: {msg}"
        );
    }

    #[test]
    fn test_map_key_starting_with_number_stays_unexpected_syntax() {
        let msg = parse_err_msg("RETURN {1B2c3e67:1} AS literal");
        assert!(
            msg.contains("UnexpectedSyntax"),
            "expected UnexpectedSyntax, got: {msg}"
        );
    }

    #[test]
    fn test_unary_minus_double() {
        use crate::ast::{CypherLiteral, Expr};
        // --5 → Integer(5)
        let expr = parse_expression("--5").expect("--5 should parse");
        assert_eq!(expr, Expr::Literal(CypherLiteral::Integer(5)));
    }

    #[test]
    fn test_unary_minus_single() {
        use crate::ast::{CypherLiteral, Expr};
        // -5 → Integer(-5)
        let expr = parse_expression("-5").expect("-5 should parse");
        assert_eq!(expr, Expr::Literal(CypherLiteral::Integer(-5)));
    }

    #[test]
    fn test_unary_minus_triple() {
        use crate::ast::{CypherLiteral, Expr};
        // ---5 → Integer(-5)
        let expr = parse_expression("---5").expect("---5 should parse");
        assert_eq!(expr, Expr::Literal(CypherLiteral::Integer(-5)));
    }

    #[test]
    fn test_unary_minus_folds_float_literals() {
        use crate::ast::{CypherLiteral, Expr};
        let lit = |s: &str| parse_expression(s).unwrap_or_else(|e| panic!("{s}: {e}"));
        assert_eq!(lit("-2.5"), Expr::Literal(CypherLiteral::Float(-2.5)));
        assert_eq!(lit("--2.5"), Expr::Literal(CypherLiteral::Float(2.5)));
        assert_eq!(lit("-+1e3"), Expr::Literal(CypherLiteral::Float(-1000.0)));
        let Expr::Literal(CypherLiteral::Float(z)) = lit("-0.0") else {
            panic!("-0.0 is not a float literal");
        };
        assert!(z == 0.0 && z.is_sign_negative(), "-0.0 must keep its sign");
    }

    #[test]
    fn test_unary_plus_identity() {
        use crate::ast::{CypherLiteral, Expr};
        // +5 → Integer(5)
        let expr = parse_expression("+5").expect("+5 should parse");
        assert_eq!(expr, Expr::Literal(CypherLiteral::Integer(5)));
    }

    #[test]
    fn test_unary_plus_minus() {
        use crate::ast::{CypherLiteral, Expr};
        // +-5 → Integer(-5)
        let expr = parse_expression("+-5").expect("+-5 should parse");
        assert_eq!(expr, Expr::Literal(CypherLiteral::Integer(-5)));
    }

    #[test]
    fn test_unary_minus_plus() {
        use crate::ast::{CypherLiteral, Expr};
        // -+5 → Integer(-5)
        let expr = parse_expression("-+5").expect("-+5 should parse");
        assert_eq!(expr, Expr::Literal(CypherLiteral::Integer(-5)));
    }

    #[test]
    fn test_unary_double_minus_overflow() {
        // --9223372036854775808 → overflow error
        let result = parse_expression("--9223372036854775808");
        assert!(
            result.is_err(),
            "expected overflow error, got: {:?}",
            result
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("IntegerOverflow"),
            "expected IntegerOverflow, got: {msg}"
        );
    }

    #[test]
    fn test_unary_minus_i64_min() {
        use crate::ast::{CypherLiteral, Expr};
        // -9223372036854775808 → Integer(i64::MIN) (valid)
        let expr = parse_expression("-9223372036854775808").expect("-i64::MIN should parse");
        assert_eq!(expr, Expr::Literal(CypherLiteral::Integer(i64::MIN)));
    }

    #[test]
    fn test_stacked_predicates_is_null_is_not_null() {
        // x IS NULL IS NOT NULL → error
        let result = parse("RETURN x IS NULL IS NOT NULL");
        assert!(
            result.is_err(),
            "expected parse error for stacked IS NULL IS NOT NULL"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("InvalidPredicateChain"),
            "expected InvalidPredicateChain, got: {msg}"
        );
    }

    #[test]
    fn test_stacked_predicates_starts_with() {
        // x STARTS WITH 'a' STARTS WITH 'b' → error
        let result = parse("RETURN x STARTS WITH 'a' STARTS WITH 'b'");
        assert!(
            result.is_err(),
            "expected parse error for stacked STARTS WITH"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("InvalidPredicateChain"),
            "expected InvalidPredicateChain, got: {msg}"
        );
    }

    #[test]
    fn test_stacked_predicates_in() {
        // x IN [1] IN [true] → error
        let result = parse("RETURN x IN [1] IN [true]");
        assert!(result.is_err(), "expected parse error for stacked IN");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("InvalidPredicateChain"),
            "expected InvalidPredicateChain, got: {msg}"
        );
    }

    #[test]
    fn test_stacked_predicates_contains_ends_with() {
        // x CONTAINS 'a' ENDS WITH 'b' → error
        let result = parse("RETURN x CONTAINS 'a' ENDS WITH 'b'");
        assert!(
            result.is_err(),
            "expected parse error for stacked CONTAINS/ENDS WITH"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("InvalidPredicateChain"),
            "expected InvalidPredicateChain, got: {msg}"
        );
    }

    #[test]
    fn test_label_stacking_allowed() {
        // x :Person :Employee → OK (label stacking is valid)
        // Note: label predicates in comparison context are valid
        assert!(
            parse("MATCH (x) WHERE x:Person:Employee RETURN x").is_ok(),
            "label stacking should be allowed"
        );
    }

    #[test]
    fn test_range_chaining_allowed() {
        // 1 < n.num < 3 → OK (required by TCK Comparison3)
        assert!(
            parse("MATCH (n) WHERE 1 < n.num < 3 RETURN n").is_ok(),
            "range chaining 1 < n.num < 3 should be allowed"
        );
    }

    #[test]
    fn test_reserved_keyword_as_variable_name() {
        let msg = parse_err_msg("MATCH (match:N) RETURN match");
        assert!(
            msg.contains("ReservedKeyword"),
            "expected ReservedKeyword, got: {msg}"
        );
        assert!(
            msg.contains("backtick-quoting"),
            "expected backtick hint, got: {msg}"
        );
    }

    #[test]
    fn test_reserved_keyword_return_as_variable() {
        let msg = parse_err_msg("MATCH (return:N) RETURN return");
        assert!(
            msg.contains("ReservedKeyword"),
            "expected ReservedKeyword, got: {msg}"
        );
    }

    #[test]
    fn test_non_reserved_keyword_allowed() {
        // `end` was moved to keyword_nonreserved — should parse fine
        assert!(
            parse("MATCH (end:N) RETURN end").is_ok(),
            "non-reserved keyword 'end' should be allowed as variable name"
        );
    }

    #[test]
    fn test_backtick_escaped_reserved_keyword() {
        assert!(
            parse("MATCH (`match`:N) RETURN `match`").is_ok(),
            "backtick-escaped reserved keyword should be allowed"
        );
    }
}
