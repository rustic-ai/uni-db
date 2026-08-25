//! Renders a generated `uni_cypher::ast::Query` back to canonical Cypher text.
//!
//! The metamorphic oracles (TLP, NoREC) build queries as ASTs, transform them
//! by editing `WHERE` expressions, then execute the rendered text. This module
//! is the AST -> string half; `uni_cypher::parse` is the string -> AST half.
//!
//! # Round-trip contract
//!
//! [`render`] is designed so that, for every AST the generator emits,
//! `normalize(parse(render(ast))) == normalize(ast)`. Two disciplines make this
//! hold (both verified by the generator's round-trip property test):
//!
//! - **Full parenthesization.** Every compound expression ([`Expr::BinaryOp`],
//!   [`Expr::UnaryOp`], `IS NULL`, ...) is wrapped in parens. Cypher has no
//!   parenthesis AST node — grouping parens dissolve into tree structure — so
//!   parenthesizing adds no nodes yet forces the parser to rebuild the identical
//!   tree regardless of operator precedence.
//! - **Float literals carry a decimal point.** `f64` is rendered with `{:?}`
//!   (e.g. `30.0`, never `30`) so it re-parses as a float, not an integer.
//!
//! The generator additionally avoids constructs that cannot round-trip (negative
//! numeric literals, which the parser reads as `UnaryOp::Neg` over a positive
//! literal); see `querygen/mod.rs`. [`normalize`] erases the one parse-incidental AST
//! field, [`ReturnItem::Expr::source_text`], which the parser fills from source
//! text but a generator leaves empty.

use uni_cypher::ast::{
    BinaryOp, Clause, CypherLiteral, Direction, Expr, LabelExpr, MatchClause, NodePattern,
    PathPattern, Pattern, PatternElement, Query, Range, RelationshipPattern, ReturnClause,
    ReturnItem, SortItem, Statement, UnaryOp, WithClause,
};

/// Renders a query AST to canonical, fully-parenthesized Cypher text.
///
/// Covers exactly the subgrammar the metamorphic generator emits: `MATCH`
/// (single node or one relationship), optional `WHERE`, `WITH` (the NoREC
/// barrier), and `RETURN` with optional `ORDER BY`/`SKIP`/`LIMIT`.
///
/// # Panics
///
/// Panics if the AST contains a clause or expression variant outside that
/// subgrammar. Such an AST is a generator bug (M-PANIC-ON-BUG), not a runtime
/// error: the generator is the sole producer of inputs here.
pub fn render(q: &Query) -> String {
    match q {
        Query::Single(stmt) => render_statement(stmt),
        // Symmetric with `normalize`, which has always recursed through
        // `Explain`. The motivation is **routing assertions**, not plan-text
        // witnesses: `EXPLAIN` names the operator a query planned through, which
        // is how a test proves it reaches the code path it claims to. That guard
        // is what the #135 trap needed — a fixture can supply the right data and
        // the right transition and still route around the fix site entirely,
        // because a declared edge type plans to `Traverse` while the defect lives
        // under `TraverseMainByType`.
        //
        // Deliberately **not** for differential comparison. Plan text is
        // `format!("{:#?}", plan)` over structures holding `HashSet`/`HashMap`
        // fields, so its ordering is not stable across processes; and the two
        // sides of a lever are *expected* to plan differently. Use it for
        // substring containment within one process, nothing more.
        Query::Explain(inner) => format!("EXPLAIN {}", render(inner)),
        other => panic!(
            "render: only Query::Single and Query::Explain are in the generated \
             subgrammar, got {other:?}"
        ),
    }
}

/// Erases parse-incidental AST fields so a rendered-then-reparsed query compares
/// equal to its source AST.
///
/// Currently clears [`ReturnItem::Expr::source_text`] in every `RETURN`/`WITH`
/// projection. Apply to both sides before asserting equality.
pub fn normalize(q: &mut Query) {
    match q {
        Query::Single(stmt) => {
            for clause in &mut stmt.clauses {
                match clause {
                    Clause::Return(r) => clear_source_text(&mut r.items),
                    Clause::With(w) => clear_source_text(&mut w.items),
                    _ => {}
                }
            }
        }
        Query::Union { left, right, .. } => {
            normalize(left);
            normalize(right);
        }
        Query::Explain(inner) | Query::TimeTravel { query: inner, .. } => normalize(inner),
        Query::Schema(_) => {}
    }
}

fn clear_source_text(items: &mut [ReturnItem]) {
    for item in items {
        if let ReturnItem::Expr { source_text, .. } = item {
            *source_text = None;
        }
    }
}

fn render_statement(stmt: &Statement) -> String {
    stmt.clauses
        .iter()
        .map(render_clause)
        .collect::<Vec<_>>()
        .join(" ")
}

fn render_clause(clause: &Clause) -> String {
    match clause {
        Clause::Match(m) => render_match(m),
        Clause::With(w) => render_with(w),
        Clause::Return(r) => render_return(r),
        other => panic!("render_clause: unsupported clause in generated subgrammar: {other:?}"),
    }
}

fn render_match(m: &MatchClause) -> String {
    let mut s = String::new();
    if m.optional {
        s.push_str("OPTIONAL ");
    }
    s.push_str("MATCH ");
    s.push_str(&render_pattern(&m.pattern));
    if let Some(w) = &m.where_clause {
        s.push_str(" WHERE ");
        s.push_str(&render_expr(w));
    }
    s
}

fn render_with(w: &WithClause) -> String {
    let mut s = String::from("WITH ");
    if w.distinct {
        s.push_str("DISTINCT ");
    }
    s.push_str(&render_items(&w.items));
    s.push_str(&render_tail(
        w.order_by.as_deref(),
        w.skip.as_ref(),
        w.limit.as_ref(),
    ));
    if let Some(p) = &w.where_clause {
        s.push_str(" WHERE ");
        s.push_str(&render_expr(p));
    }
    s
}

fn render_return(r: &ReturnClause) -> String {
    let mut s = String::from("RETURN ");
    if r.distinct {
        s.push_str("DISTINCT ");
    }
    s.push_str(&render_items(&r.items));
    s.push_str(&render_tail(
        r.order_by.as_deref(),
        r.skip.as_ref(),
        r.limit.as_ref(),
    ));
    s
}

fn render_tail(order_by: Option<&[SortItem]>, skip: Option<&Expr>, limit: Option<&Expr>) -> String {
    let mut s = String::new();
    if let Some(items) = order_by {
        s.push_str(" ORDER BY ");
        s.push_str(
            &items
                .iter()
                .map(|si| {
                    let dir = if si.ascending { " ASC" } else { " DESC" };
                    format!("{}{dir}", render_expr(&si.expr))
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    if let Some(e) = skip {
        s.push_str(" SKIP ");
        s.push_str(&render_expr(e));
    }
    if let Some(e) = limit {
        s.push_str(" LIMIT ");
        s.push_str(&render_expr(e));
    }
    s
}

fn render_items(items: &[ReturnItem]) -> String {
    items
        .iter()
        .map(|item| match item {
            ReturnItem::All => "*".to_string(),
            ReturnItem::Expr { expr, alias, .. } => match alias {
                Some(a) => format!("{} AS {a}", render_expr(expr)),
                None => render_expr(expr),
            },
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_pattern(p: &Pattern) -> String {
    p.paths
        .iter()
        .map(render_path)
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_path(path: &PathPattern) -> String {
    let mut s = String::new();
    if let Some(v) = &path.variable {
        s.push_str(v);
        s.push_str(" = ");
    }
    for element in &path.elements {
        match element {
            PatternElement::Node(n) => s.push_str(&render_node(n)),
            PatternElement::Relationship(r) => s.push_str(&render_relationship(r)),
            other => panic!(
                "render_path: unsupported pattern element in generated subgrammar: {other:?}"
            ),
        }
    }
    s
}

fn render_node(n: &NodePattern) -> String {
    let mut s = String::from("(");
    if let Some(v) = &n.variable {
        s.push_str(v);
    }
    s.push_str(&render_label_expr(&n.labels));
    if let Some(props) = &n.properties {
        s.push(' ');
        s.push_str(&render_expr(props));
    }
    s.push(')');
    s
}

fn render_relationship(r: &RelationshipPattern) -> String {
    let mut inner = String::new();
    if let Some(v) = &r.variable {
        inner.push_str(v);
    }
    inner.push_str(&render_label_expr(&r.types));
    if let Some(range) = &r.range {
        inner.push_str(&render_range(range));
    }
    let bracket = if inner.is_empty() {
        String::new()
    } else {
        format!("[{inner}]")
    };
    match r.direction {
        Direction::Outgoing => format!("-{bracket}->"),
        Direction::Incoming => format!("<-{bracket}-"),
        Direction::Both => format!("-{bracket}-"),
    }
}

fn render_range(range: &Range) -> String {
    match (range.min, range.max) {
        (None, None) => "*".to_string(),
        (Some(min), None) => format!("*{min}.."),
        (None, Some(max)) => format!("*..{max}"),
        (Some(min), Some(max)) => format!("*{min}..{max}"),
    }
}

fn render_label_expr(le: &LabelExpr) -> String {
    match le {
        LabelExpr::Empty => String::new(),
        // Single-label and `:A:B` both reduce to colon-prefixed names.
        LabelExpr::Conjunction(v) => v.iter().map(|s| format!(":{s}")).collect(),
        LabelExpr::Disjunction(v) => format!(":{}", v.join("|")),
    }
}

/// Renders an expression with every compound node fully parenthesized.
///
/// # Panics
///
/// Panics on an expression variant outside the generated subgrammar — a
/// generator bug (M-PANIC-ON-BUG).
pub fn render_expr(e: &Expr) -> String {
    match e {
        Expr::Literal(l) => render_literal(l),
        Expr::Variable(v) => v.clone(),
        Expr::Parameter(p) => format!("${p}"),
        Expr::Wildcard => "*".to_string(),
        // Base is a `Variable` in the subgrammar; no parens needed.
        Expr::Property(base, prop) => format!("{}.{prop}", render_expr(base)),
        Expr::List(items) => format!(
            "[{}]",
            items.iter().map(render_expr).collect::<Vec<_>>().join(", ")
        ),
        Expr::Map(entries) => format!(
            "{{{}}}",
            entries
                .iter()
                .map(|(k, v)| format!("{k}: {}", render_expr(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            let mut inner = String::new();
            if *distinct {
                inner.push_str("DISTINCT ");
            }
            inner.push_str(&args.iter().map(render_expr).collect::<Vec<_>>().join(", "));
            format!("{name}({inner})")
        }
        Expr::BinaryOp { left, op, right } => {
            format!("({} {op} {})", render_expr(left), render_expr(right))
        }
        Expr::UnaryOp { op, expr } => match op {
            UnaryOp::Not => format!("(NOT {})", render_expr(expr)),
            UnaryOp::Neg => format!("(-{})", render_expr(expr)),
        },
        Expr::IsNull(inner) => format!("({} IS NULL)", render_expr(inner)),
        Expr::IsNotNull(inner) => format!("({} IS NOT NULL)", render_expr(inner)),
        Expr::In { expr, list } => format!("({} IN {})", render_expr(expr), render_expr(list)),
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            let mut s = String::from("CASE");
            if let Some(scrutinee) = expr {
                s.push(' ');
                s.push_str(&render_expr(scrutinee));
            }
            for (when, then) in when_then {
                s.push_str(&format!(
                    " WHEN {} THEN {}",
                    render_expr(when),
                    render_expr(then)
                ));
            }
            if let Some(otherwise) = else_expr {
                s.push_str(&format!(" ELSE {}", render_expr(otherwise)));
            }
            s.push_str(" END");
            s
        }
        other => panic!("render_expr: unsupported expr in generated subgrammar: {other:?}"),
    }
}

fn render_literal(l: &CypherLiteral) -> String {
    match l {
        CypherLiteral::Null => "null".to_string(),
        CypherLiteral::Bool(b) => b.to_string(),
        CypherLiteral::Integer(i) => i.to_string(),
        // `{:?}` keeps the decimal point (`30.0`) so it re-parses as Float.
        CypherLiteral::Float(v) => format!("{v:?}"),
        CypherLiteral::String(s) => render_string(s),
        CypherLiteral::Bytes(_) => {
            panic!("render_literal: Bytes literal is outside the generated subgrammar")
        }
    }
}

fn render_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}
