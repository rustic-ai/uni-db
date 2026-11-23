// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The query language of a full-text search (`uni.fts.query`).
//!
//! ```text
//! query   := or_expr
//! or_expr := and_expr ( [OR] and_expr )*     adjacency means OR
//! and_expr:= unary ( AND unary )*
//! unary   := ( NOT | '-' ) unary | primary
//! primary := WORD | '"' phrase '"' | '(' or_expr ')'
//! ```
//!
//! `AND`, `OR` and `NOT` are operators only in upper case; in any other case
//! they are words. A `-` negates only when it starts a word, phrase or group
//! (`-fox`, `-"brown fox"`, `-(a b)`); inside a word (`e-mail`) or on its own it
//! is text. A query with no syntax at all, a plain word list, is searched
//! exactly as before the syntax existed: any of the words, ranked by BM25.
//!
//! A negation excludes rows and does not score. Every group, the query
//! included, needs a term that is not negated: `-fox` alone has nothing to
//! rank and is refused. A blank query matches nothing. An unbalanced quote or
//! parenthesis, or an operator with no operand, is an error.

// Rust guideline compliant

use anyhow::{Result, anyhow, bail};

/// A parsed full-text query.
#[derive(Debug, Clone, PartialEq)]
pub enum FtsQueryNode {
    /// A word: matches a row holding any of the tokens it analyzes to.
    Term(String),
    /// A quoted phrase: its tokens, adjacent and in order.
    Phrase(String),
    /// Every positive child matches and no negated one does.
    And(Vec<FtsQueryNode>),
    /// At least one positive child matches and no negated one does.
    Or(Vec<FtsQueryNode>),
    /// Excludes rows its child matches; only valid inside a group with a
    /// positive member.
    Not(Box<FtsQueryNode>),
}

impl FtsQueryNode {
    /// Whether this is a plain word list, searched as one OR match of the
    /// original text.
    pub fn is_plain(&self) -> bool {
        match self {
            Self::Term(_) => true,
            Self::Or(children) => children.iter().all(|c| matches!(c, Self::Term(_))),
            _ => false,
        }
    }

    /// The text of every leaf that is not negated, joined by spaces: the
    /// tokens a ranking needs statistics for.
    pub fn positive_text(&self) -> String {
        let mut out = Vec::new();
        self.collect_positive(&mut out);
        out.join(" ")
    }

    fn collect_positive<'a>(&'a self, out: &mut Vec<&'a str>) {
        match self {
            Self::Term(t) | Self::Phrase(t) => out.push(t),
            Self::And(children) | Self::Or(children) => {
                for c in children {
                    c.collect_positive(out);
                }
            }
            Self::Not(_) => {}
        }
    }

    /// Splits a group's children into positive ones and the inner queries of
    /// negated ones.
    pub fn split_negated(children: &[FtsQueryNode]) -> (Vec<&FtsQueryNode>, Vec<&FtsQueryNode>) {
        let mut positive = Vec::new();
        let mut negated = Vec::new();
        for c in children {
            match c {
                Self::Not(inner) => negated.push(inner.as_ref()),
                other => positive.push(other),
            }
        }
        (positive, negated)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Phrase(String),
    LParen,
    RParen,
    And,
    Or,
    Not,
    Minus,
}

/// Parses `query`. `Ok(None)` for a blank query, which matches nothing.
///
/// # Errors
/// Returns an error for an unbalanced quote or parenthesis, an operator
/// without an operand, or a group (the query included) whose every member is
/// negated.
pub fn parse_fts_query(query: &str) -> Result<Option<FtsQueryNode>> {
    let tokens = lex(query)?;
    if tokens.is_empty() {
        return Ok(None);
    }
    let mut parser = Parser { tokens, pos: 0 };
    let node = parser.or_expr()?;
    if let Some(tok) = parser.peek() {
        return Err(match tok {
            Token::RParen => anyhow!("full-text query {query:?}: unbalanced parenthesis ')'"),
            other => anyhow!("full-text query {query:?}: unexpected {other:?}"),
        });
    }
    let node = simplify(node);
    validate(&node, true).map_err(|e| anyhow!("full-text query {query:?}: {e}"))?;
    Ok(Some(node))
}

fn lex(query: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = query.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '(' {
            tokens.push(Token::LParen);
            i += 1;
        } else if c == ')' {
            tokens.push(Token::RParen);
            i += 1;
        } else if c == '"' {
            let start = i + 1;
            let Some(len) = chars[start..].iter().position(|&ch| ch == '"') else {
                bail!("full-text query {query:?}: unbalanced quote '\"'");
            };
            tokens.push(Token::Phrase(chars[start..start + len].iter().collect()));
            i = start + len + 1;
        } else if c == '-'
            && chars
                .get(i + 1)
                .is_some_and(|n| !n.is_whitespace() && *n != ')' && *n != '-')
        {
            tokens.push(Token::Minus);
            i += 1;
        } else {
            let start = i;
            while i < chars.len()
                && !chars[i].is_whitespace()
                && !matches!(chars[i], '(' | ')' | '"')
            {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            tokens.push(match word.as_str() {
                "AND" => Token::And,
                "OR" => Token::Or,
                "NOT" => Token::Not,
                _ => Token::Word(word),
            });
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    /// Whether the next token can start an operand.
    fn starts_operand(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::Word(_) | Token::Phrase(_) | Token::LParen | Token::Not | Token::Minus)
        )
    }

    fn or_expr(&mut self) -> Result<FtsQueryNode> {
        let mut children = vec![self.and_expr()?];
        loop {
            match self.peek() {
                Some(Token::Or) => {
                    self.pos += 1;
                    if !self.starts_operand() {
                        bail!("OR needs a term after it");
                    }
                    children.push(self.and_expr()?);
                }
                _ if self.starts_operand() => children.push(self.and_expr()?),
                _ => break,
            }
        }
        Ok(if children.len() == 1 {
            children.pop().expect("one child")
        } else {
            FtsQueryNode::Or(children)
        })
    }

    fn and_expr(&mut self) -> Result<FtsQueryNode> {
        let mut children = vec![self.unary()?];
        while matches!(self.peek(), Some(Token::And)) {
            self.pos += 1;
            if !self.starts_operand() {
                bail!("AND needs a term after it");
            }
            children.push(self.unary()?);
        }
        Ok(if children.len() == 1 {
            children.pop().expect("one child")
        } else {
            FtsQueryNode::And(children)
        })
    }

    fn unary(&mut self) -> Result<FtsQueryNode> {
        match self.peek() {
            Some(Token::Not) | Some(Token::Minus) => {
                let op = if matches!(self.peek(), Some(Token::Not)) {
                    "NOT"
                } else {
                    "-"
                };
                self.pos += 1;
                if !self.starts_operand() {
                    bail!("{op} needs a term after it");
                }
                Ok(FtsQueryNode::Not(Box::new(self.unary()?)))
            }
            _ => self.primary(),
        }
    }

    fn primary(&mut self) -> Result<FtsQueryNode> {
        match self.next() {
            Some(Token::Word(w)) => Ok(FtsQueryNode::Term(w)),
            Some(Token::Phrase(p)) => Ok(FtsQueryNode::Phrase(p)),
            Some(Token::LParen) => {
                if matches!(self.peek(), Some(Token::RParen)) {
                    bail!("empty parentheses");
                }
                let inner = self.or_expr()?;
                match self.next() {
                    Some(Token::RParen) => Ok(inner),
                    _ => bail!("unbalanced parenthesis '('"),
                }
            }
            Some(Token::RParen) => bail!("unbalanced parenthesis ')'"),
            Some(Token::And) => bail!("AND needs a term before it"),
            Some(Token::Or) => bail!("OR needs a term before it"),
            Some(other) => bail!("unexpected {other:?}"),
            None => bail!("the query ends where a term is expected"),
        }
    }
}

/// Removes double negations and flattens nested groups of the same kind.
fn simplify(node: FtsQueryNode) -> FtsQueryNode {
    match node {
        FtsQueryNode::Not(inner) => match simplify(*inner) {
            FtsQueryNode::Not(x) => *x,
            other => FtsQueryNode::Not(Box::new(other)),
        },
        FtsQueryNode::And(children) => {
            let mut out = Vec::with_capacity(children.len());
            for c in children {
                match simplify(c) {
                    FtsQueryNode::And(inner) => out.extend(inner),
                    other => out.push(other),
                }
            }
            FtsQueryNode::And(out)
        }
        FtsQueryNode::Or(children) => {
            let mut out = Vec::with_capacity(children.len());
            for c in children {
                match simplify(c) {
                    FtsQueryNode::Or(inner) => out.extend(inner),
                    other => out.push(other),
                }
            }
            FtsQueryNode::Or(out)
        }
        leaf => leaf,
    }
}

/// Refuses a group whose every member is negated.
fn validate(node: &FtsQueryNode, top: bool) -> Result<()> {
    match node {
        FtsQueryNode::Term(_) | FtsQueryNode::Phrase(_) => Ok(()),
        FtsQueryNode::Not(inner) => {
            if top {
                bail!(
                    "a query of only negated terms has nothing to rank; \
                     add a term that is not negated"
                );
            }
            validate(inner, false)
        }
        FtsQueryNode::And(children) | FtsQueryNode::Or(children) => {
            let (positive, negated) = FtsQueryNode::split_negated(children);
            if positive.is_empty() {
                bail!(
                    "a group of only negated terms has nothing to rank; \
                     add a term that is not negated"
                );
            }
            for c in positive {
                validate(c, false)?;
            }
            for c in negated {
                // The inner query of a negation is matched, not ranked: a
                // negated group may itself hold negations as long as it has a
                // positive member.
                validate_negated(c)?;
            }
            Ok(())
        }
    }
}

fn validate_negated(node: &FtsQueryNode) -> Result<()> {
    match node {
        FtsQueryNode::Not(_) => Ok(()),
        other => validate(other, false),
    }
}

#[cfg(test)]
mod tests {
    use super::FtsQueryNode::*;
    use super::*;

    fn term(s: &str) -> FtsQueryNode {
        Term(s.to_string())
    }

    fn not(n: FtsQueryNode) -> FtsQueryNode {
        Not(Box::new(n))
    }

    #[test]
    fn plain_words_are_one_or() {
        let q = parse_fts_query("quick brown fox").unwrap().unwrap();
        assert_eq!(q, Or(vec![term("quick"), term("brown"), term("fox")]));
        assert!(q.is_plain());
        assert!(parse_fts_query("fox").unwrap().unwrap().is_plain());
        // Lower-case operators and inner hyphens are text.
        assert!(
            parse_fts_query("cats and e-mail or not")
                .unwrap()
                .unwrap()
                .is_plain()
        );
        assert!(parse_fts_query("a - b").unwrap().unwrap().is_plain());
    }

    #[test]
    fn blank_is_none() {
        assert_eq!(parse_fts_query("").unwrap(), None);
        assert_eq!(parse_fts_query("  \t ").unwrap(), None);
    }

    #[test]
    fn precedence() {
        // AND binds tighter than adjacency/OR.
        let q = parse_fts_query("a b AND c OR d").unwrap().unwrap();
        assert_eq!(
            q,
            Or(vec![term("a"), And(vec![term("b"), term("c")]), term("d")])
        );
        let q = parse_fts_query("(a OR b) AND \"c d\" -e").unwrap().unwrap();
        assert_eq!(
            q,
            Or(vec![
                And(vec![Or(vec![term("a"), term("b")]), Phrase("c d".into())]),
                not(term("e")),
            ])
        );
        assert!(!q.is_plain());
        assert_eq!(q.positive_text(), "a b c d");
    }

    #[test]
    fn negations() {
        assert_eq!(
            parse_fts_query("fox NOT quick").unwrap().unwrap(),
            Or(vec![term("fox"), not(term("quick"))])
        );
        assert_eq!(
            parse_fts_query("fox AND NOT NOT quick").unwrap().unwrap(),
            And(vec![term("fox"), term("quick")])
        );
        assert_eq!(
            parse_fts_query("quick AND (-fox)").unwrap().unwrap(),
            And(vec![term("quick"), not(term("fox"))])
        );
    }

    #[test]
    fn malformed_is_an_error() {
        for (q, fragment) in [
            ("\"open", "quote"),
            ("(a b", "parenthesis"),
            ("a b)", "parenthesis"),
            ("()", "empty parentheses"),
            ("a AND", "AND"),
            ("AND a", "AND"),
            ("a OR", "OR"),
            ("NOT", "NOT"),
            ("-a", "negated"),
            ("NOT a -b", "negated"),
            ("a AND (-b -c)", "negated"),
        ] {
            let err = parse_fts_query(q).expect_err(q).to_string();
            assert!(err.contains(fragment), "{q}: {err}");
        }
    }
}
