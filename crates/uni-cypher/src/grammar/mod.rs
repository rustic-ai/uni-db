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
    let pairs = CypherParser::parse(Rule::query, input).map_err(map_pest_error)?;

    walker::build_query(pairs)
}

pub fn parse_expression(input: &str) -> Result<Expr, ParseError> {
    let pairs = CypherParser::parse(Rule::expression, input).map_err(map_pest_error)?;

    walker::build_expression(pairs.into_iter().next().unwrap())
}

fn map_pest_error(e: pest::error::Error<Rule>) -> ParseError {
    ParseError::new(format!("{}", e))
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
}
