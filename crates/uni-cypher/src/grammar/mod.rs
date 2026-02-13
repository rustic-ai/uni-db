mod walker;

use crate::ast::{Expr, Query};
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

pub fn parse(input: &str) -> Result<Query, ParseError> {
    let pairs = CypherParser::parse(Rule::query, input).map_err(|e| map_pest_error(input, e))?;

    walker::build_query(pairs)
}

pub fn parse_expression(input: &str) -> Result<Expr, ParseError> {
    let pairs =
        CypherParser::parse(Rule::expression, input).map_err(|e| map_pest_error(input, e))?;

    walker::build_expression(pairs.into_iter().next().unwrap())
}

fn error_position(e: &pest::error::Error<Rule>) -> usize {
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

    let mut p = pos.min(bytes.len().saturating_sub(1));
    if p == bytes.len() && p > 0 {
        p -= 1;
    }

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

    if start == end {
        None
    } else {
        Some((start, end))
    }
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

    // Example: [:LIKES..] (missing `*`)
    if segment.contains("..") && !segment.contains('*') {
        return true;
    }

    // Example: [:LIKES*-2] (negative range bound)
    if segment.contains("*-") {
        return true;
    }

    false
}

fn is_invalid_number_literal(input: &str, pos: usize) -> bool {
    let Some((start, end)) = extract_token_span_at(input, pos) else {
        return false;
    };
    if is_map_key_like_context(input, start, end) {
        return false;
    }
    let token = &input[start..end];
    if token.is_empty() {
        return false;
    }

    let t = token.strip_prefix('-').unwrap_or(token);
    if t.is_empty() || !t.as_bytes()[0].is_ascii_digit() {
        return false;
    }

    if t.starts_with("0x") || t.starts_with("0X") {
        let digits = &t[2..];
        if digits.is_empty() {
            return true;
        }
        return digits.chars().any(|c| !(c.is_ascii_hexdigit() || c == '_'));
    }

    if t.starts_with("0o") || t.starts_with("0O") {
        let digits = &t[2..];
        if digits.is_empty() {
            return true;
        }
        return digits
            .chars()
            .any(|c| !(matches!(c, '0'..='7') || c == '_'));
    }

    // Decimal-like token with alphabetic suffix/midfix, e.g. 9223372h54775808
    t.chars().any(|c| c.is_ascii_alphabetic())
}

fn invalid_unicode_character(input: &str, pos: usize) -> Option<char> {
    let ch = input.get(pos..)?.chars().next()?;
    if matches!(ch, '—' | '–' | '−') {
        Some(ch)
    } else {
        None
    }
}

fn map_pest_error(input: &str, e: pest::error::Error<Rule>) -> ParseError {
    let pos = error_position(&e);
    if is_invalid_relationship_pattern(input, pos) {
        return ParseError::new(format!("SyntaxError: InvalidRelationshipPattern - {}", e));
    }
    if is_invalid_number_literal(input, pos) {
        return ParseError::new(format!("SyntaxError: InvalidNumberLiteral - {}", e));
    }
    if let Some(ch) = invalid_unicode_character(input, pos) {
        return ParseError::new(format!(
            "SyntaxError: InvalidUnicodeCharacter - Invalid character '{}'",
            ch
        ));
    }

    let msg = format!("{}", e);
    ParseError::new(format!("UnexpectedSyntax: {}", msg))
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
}
