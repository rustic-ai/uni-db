use std::collections::HashMap;

use pest::iterators::{Pair, Pairs};
use uni_common::Value;

use super::{ParseError, Rule};
use crate::ast::*;

/// Check if the next token in a peekable iterator matches the given rule.
fn peek_is(inner: &mut std::iter::Peekable<Pairs<Rule>>, rule: Rule) -> bool {
    inner.peek().is_some_and(|p| p.as_rule() == rule)
}

/// The name an identifier token denotes.
///
/// A bare token is its own name. A backtick-quoted token (`escaped_name` in
/// `cypher.pest`) loses its enclosing backticks, and each doubled backtick
/// inside stands for one literal backtick: `` `a``b` `` is the name ``a`b``.
/// Must be applied exactly once per token: the result of unescaping can itself
/// look quoted (`` ```x``` `` is the name `` `x` ``).
pub(crate) fn normalize_identifier(s: &str) -> String {
    match s.strip_prefix('`').and_then(|s| s.strip_suffix('`')) {
        Some(body) => body.replace("``", "`"),
        None => s.to_string(),
    }
}

/// The name an identifier pair denotes; see [`normalize_identifier`].
fn ident(pair: &Pair<Rule>) -> String {
    normalize_identifier(pair.as_str())
}

/// The name a `$parameter` token denotes: `$p`, `$1` or `` $`p q` ``.
pub(super) fn parameter_name(pair: &Pair<Rule>) -> String {
    normalize_identifier(&pair.as_str()[1..])
}

/// Extract the expression from a `where_clause` pair (skips the WHERE keyword).
fn extract_where_expr(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    build_expression(pair.into_inner().nth(1).unwrap())
}

/// Consume an identifier token if it is the next token, returning its string value.
fn consume_identifier(inner: &mut std::iter::Peekable<Pairs<Rule>>) -> Option<String> {
    if peek_is(inner, Rule::identifier) {
        Some(normalize_identifier(inner.next().unwrap().as_str()))
    } else {
        None
    }
}

fn match_query(pattern: Pattern, where_clause: Option<Expr>) -> Query {
    Query::Single(Statement {
        clauses: vec![Clause::Match(MatchClause {
            optional: false,
            pattern,
            where_clause,
            for_update: false,
        })],
    })
}

pub fn build_query(pairs: Pairs<Rule>) -> Result<Query, ParseError> {
    let pair = pairs.into_iter().next().unwrap();
    debug_assert_eq!(pair.as_rule(), Rule::query);

    let mut inner = pair.into_inner();
    let query = build_union_query(inner.next().unwrap())?;

    if let Some(tt_pair) = inner.next()
        && tt_pair.as_rule() == Rule::time_travel_clause
    {
        let spec = build_time_travel_spec(tt_pair)?;
        return Ok(Query::TimeTravel {
            query: Box::new(query),
            spec,
        });
    }
    Ok(query)
}

fn build_time_travel_spec(pair: Pair<Rule>) -> Result<TimeTravelSpec, ParseError> {
    let mut inner = pair.into_inner();
    let keyword = inner.next().unwrap();

    let string_pair = inner
        .find(|p| p.as_rule() == Rule::string)
        .ok_or_else(|| ParseError::new("Expected string literal in time_travel_clause".into()))?;
    let value = build_string_literal(string_pair)?;

    match keyword.as_rule() {
        Rule::VERSION => Ok(TimeTravelSpec::Version(value)),
        Rule::TIMESTAMP_KW => Ok(TimeTravelSpec::Timestamp(value)),
        other => Err(ParseError::new(format!(
            "Unexpected keyword in time_travel_clause: {:?}",
            other
        ))),
    }
}

fn build_union_query(pair: Pair<Rule>) -> Result<Query, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();
    let mut left = build_single_query(first)?;

    while let Some(op_pair) = inner.next() {
        let all = op_pair.into_inner().count() > 1; // UNION has 1 token, UNION ALL has 2
        let right = build_single_query(inner.next().unwrap())?;
        left = Query::Union {
            left: Box::new(left),
            right: Box::new(right),
            all,
        };
    }

    Ok(left)
}

fn build_single_query(pair: Pair<Rule>) -> Result<Query, ParseError> {
    // Unwrap single_query to get its inner alternative
    let inner = if pair.as_rule() == Rule::single_query {
        pair.into_inner().next().unwrap()
    } else {
        pair
    };

    match inner.as_rule() {
        Rule::explain_query => {
            let mut explain_inner = inner.into_inner();
            explain_inner.next(); // Skip EXPLAIN keyword token
            let stmt = explain_inner.next().unwrap(); // Get the actual statement/schema_command

            match stmt.as_rule() {
                Rule::statement => Ok(Query::Explain(Box::new(Query::Single(build_statement(
                    stmt,
                )?)))),
                Rule::schema_command => Ok(Query::Explain(Box::new(Query::Schema(Box::new(
                    build_schema_command(stmt)?,
                ))))),
                _ => unreachable!("Unexpected explain inner rule: {:?}", stmt.as_rule()),
            }
        }
        Rule::statement => Ok(Query::Single(build_statement(inner)?)),
        Rule::schema_command => Ok(Query::Schema(Box::new(build_schema_command(inner)?))),
        _ => unreachable!("Unexpected single_query rule: {:?}", inner.as_rule()),
    }
}

pub(crate) fn build_statement(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let clauses = pair
        .into_inner()
        .map(build_clause)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Statement { clauses })
}

pub(crate) fn build_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    build_clause_inner(inner)
}

/// Dispatch on an already-unwrapped clause pair.
///
/// Split out of [`build_clause`] for `FOREACH`: its body is a silent grammar
/// rule (`foreach_body_clause`), so the update clauses arrive as `create_clause`
/// / `set_clause` / … directly rather than wrapped in a `clause`, and calling
/// `build_clause` on one would unwrap a level too far.
fn build_clause_inner(inner: Pair<Rule>) -> Result<Clause, ParseError> {
    match inner.as_rule() {
        Rule::match_clause => build_match_clause(inner),
        Rule::create_clause => build_create_clause(inner),
        Rule::return_clause => build_return_clause(inner),
        Rule::with_recursive_clause => build_with_recursive_clause(inner),
        Rule::with_clause => build_with_clause(inner),
        Rule::set_clause => build_set_clause(inner),
        Rule::delete_clause => build_delete_clause(inner),
        Rule::merge_clause => build_merge_clause(inner),
        Rule::unwind_clause => build_unwind_clause(inner),
        Rule::remove_clause => build_remove_clause(inner),
        Rule::call_clause => build_call_clause(inner),
        Rule::foreach_clause => build_foreach_clause(inner),
        _ => unreachable!("Unexpected clause: {:?}", inner.as_rule()),
    }
}

fn build_match_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner().peekable();

    let optional = consume_if_present(&mut inner, Rule::OPTIONAL);
    inner.next(); // MATCH

    let pattern = build_pattern(inner.next().unwrap())?;

    // `where_clause` and `for_update_clause` are both optional and trailing, so
    // dispatch the remaining pairs by rule rather than positionally.
    let mut where_clause = None;
    let mut for_update = false;
    for p in inner {
        match p.as_rule() {
            Rule::where_clause => where_clause = Some(extract_where_expr(p)?),
            Rule::for_update_clause => for_update = true,
            _ => {}
        }
    }

    Ok(Clause::Match(MatchClause {
        optional,
        pattern,
        where_clause,
        for_update,
    }))
}

fn build_create_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    let pattern = build_pattern(inner.next().unwrap())?;
    Ok(Clause::Create(CreateClause { pattern }))
}

/// Parsed projection modifiers (ORDER BY, SKIP, LIMIT, WHERE).
struct ProjectionModifiers {
    order_by: Option<Vec<SortItem>>,
    skip: Option<Expr>,
    limit: Option<Expr>,
    where_clause: Option<Expr>,
}

/// Parse projection modifiers from a clause's remaining pairs.
fn parse_projection_modifiers(pairs: Pairs<Rule>) -> Result<ProjectionModifiers, ParseError> {
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;
    let mut where_clause = None;

    for p in pairs {
        match p.as_rule() {
            Rule::order_clause => {
                order_by = Some(build_sort_items(p.into_inner().nth(2).unwrap())?);
            }
            Rule::skip_clause => {
                skip = Some(build_expression(p.into_inner().nth(1).unwrap())?);
            }
            Rule::limit_clause => {
                limit = Some(build_expression(p.into_inner().nth(1).unwrap())?);
            }
            Rule::where_clause => where_clause = Some(extract_where_expr(p)?),
            _ => {}
        }
    }

    Ok(ProjectionModifiers {
        order_by,
        skip,
        limit,
        where_clause,
    })
}

/// Check and consume DISTINCT keyword from the front of the iterator.
fn consume_distinct(inner: &mut Pairs<Rule>) -> bool {
    if inner.peek().is_some_and(|p| p.as_rule() == Rule::DISTINCT) {
        inner.next();
        true
    } else {
        false
    }
}

fn build_return_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // RETURN

    let distinct = consume_distinct(&mut inner);
    let items = build_return_items(inner.next().unwrap())?;
    let mods = parse_projection_modifiers(inner)?;

    Ok(Clause::Return(ReturnClause {
        distinct,
        items,
        order_by: mods.order_by,
        skip: mods.skip,
        limit: mods.limit,
    }))
}

fn build_with_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // WITH

    let distinct = consume_distinct(&mut inner);
    let items = build_return_items(inner.next().unwrap())?;
    let mods = parse_projection_modifiers(inner)?;

    Ok(Clause::With(WithClause {
        distinct,
        items,
        order_by: mods.order_by,
        skip: mods.skip,
        limit: mods.limit,
        where_clause: mods.where_clause,
    }))
}

fn build_with_recursive_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // WITH
    inner.next(); // RECURSIVE
    let name = ident(&inner.next().unwrap());
    inner.next(); // AS
    // Note: parentheses are not separate tokens
    let union_query = inner.next().unwrap();

    // Build the union_query into a Query
    let query = Box::new(build_union_query(union_query)?);

    // For now, items is empty - could be extracted from the query's RETURN clause
    let items = vec![];

    Ok(Clause::WithRecursive(WithRecursiveClause {
        name,
        query,
        items,
    }))
}

fn build_unwind_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // UNWIND
    let expr = build_expression(inner.next().unwrap())?;
    inner.next(); // AS
    let variable = ident(&inner.next().unwrap());
    Ok(Clause::Unwind(UnwindClause { expr, variable }))
}

/// `FOREACH (x IN list | <update clauses>)`.
///
/// Mirrors [`build_unwind_clause`] for the `variable`/`expr` half — the two
/// clauses carry the same pair, in the opposite order (`FOREACH x IN e` versus
/// `UNWIND e AS x`) — then walks the body clauses through
/// [`build_clause_inner`], which is what lets a nested `FOREACH` recurse.
fn build_foreach_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // FOREACH
    let variable = ident(&inner.next().unwrap());
    inner.next(); // IN
    let expr = build_expression(inner.next().unwrap())?;
    let body = inner
        .map(build_clause_inner)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Clause::Foreach(ForeachClause {
        variable,
        expr,
        body,
    }))
}

fn build_delete_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner().peekable();

    let detach = consume_if_present(&mut inner, Rule::DETACH);
    inner.next(); // DELETE
    let items = build_expression_list(inner.next().unwrap())?;

    Ok(Clause::Delete(DeleteClause { detach, items }))
}

fn build_set_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // SET
    let items = inner.map(build_set_item).collect::<Result<Vec<_>, _>>()?;
    Ok(Clause::Set(SetClause { items }))
}

/// Build a property access expression from a `property_expr` parse node.
///
/// Handles both plain (`n.prop`) and parenthesized (`(n).prop`) forms,
/// constructing an `Expr::Property` chain.
fn build_property_expr_chain(pair: Pair<Rule>) -> Expr {
    let mut p_inner = pair.into_inner();
    let first_token = p_inner.next().unwrap();
    let var = if first_token.as_str() == "(" {
        ident(&p_inner.next().unwrap())
    } else {
        ident(&first_token)
    };
    p_inner
        .filter(|p| p.as_str() != ")")
        .fold(Expr::Variable(var), |expr, p| {
            // Unquoted like property access is: `SET n.`order` = …` must
            // write the property `n.`order`` reads.
            Expr::Property(Box::new(expr), normalize_identifier(p.as_str()))
        })
}

fn build_set_item(pair: Pair<Rule>) -> Result<SetItem, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    match first.as_rule() {
        Rule::identifier => {
            let next = inner.next().unwrap();
            match next.as_rule() {
                Rule::plus_eq => {
                    let expr = build_expression(inner.next().unwrap())?;
                    Ok(SetItem::VariablePlus {
                        variable: ident(&first),
                        value: expr,
                    })
                }
                Rule::node_labels => {
                    let has_pipe = next.clone().into_inner().any(|t| t.as_rule() == Rule::pipe);
                    if has_pipe {
                        return Err(ParseError::new(format!(
                            "SET `{}:A|B` is not valid Cypher; use `SET {0}:A:B` to assign multiple labels",
                            first.as_str()
                        )));
                    }
                    let labels = next
                        .into_inner()
                        .filter(|t| {
                            matches!(t.as_rule(), Rule::identifier | Rule::identifier_or_keyword)
                        })
                        .map(|l| normalize_identifier(l.as_str()))
                        .collect();
                    Ok(SetItem::Labels {
                        variable: ident(&first),
                        labels,
                    })
                }
                Rule::eq => {
                    let expr = build_expression(inner.next().unwrap())?;
                    Ok(SetItem::Variable {
                        variable: ident(&first),
                        value: expr,
                    })
                }
                _ => unreachable!(),
            }
        }
        Rule::property_expr => {
            let expr = build_property_expr_chain(first);
            inner.next(); // eq
            let val = build_expression(inner.next().unwrap())?;
            Ok(SetItem::Property { expr, value: val })
        }
        _ => unreachable!(),
    }
}

fn build_remove_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // REMOVE
    let items = inner
        .map(build_remove_item)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Clause::Remove(RemoveClause { items }))
}

fn build_remove_item(pair: Pair<Rule>) -> Result<RemoveItem, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();
    match first.as_rule() {
        Rule::property_expr => Ok(RemoveItem::Property(build_property_expr_chain(first))),
        Rule::identifier => {
            let id = ident(&first);
            let labels_pair = inner.next().unwrap();
            let labels = labels_pair
                .into_inner()
                .filter(|t| matches!(t.as_rule(), Rule::identifier | Rule::identifier_or_keyword))
                // Strip backticks (consistent with the SET branch) so `REMOVE
                // n:`Person`` targets the normalized label `Person` instead of a
                // literal "`Person`" that never matches and silently no-ops.
                .map(|l| normalize_identifier(l.as_str()))
                .collect();
            Ok(RemoveItem::Labels {
                variable: id,
                labels,
            })
        }
        _ => unreachable!(),
    }
}

fn build_merge_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // MERGE

    let pattern_path = build_path_pattern(inner.next().unwrap())?;
    let pattern = Pattern {
        paths: vec![pattern_path],
    };

    let mut on_match = vec![];
    let mut on_create = vec![];

    for action in inner {
        let mut action_inner = action.into_inner();
        action_inner.next(); // ON
        let kind = action_inner.next().unwrap();
        let set_pair = action_inner.next().unwrap();

        let items = set_pair
            .into_inner()
            .skip(1)
            .map(build_set_item)
            .collect::<Result<Vec<_>, _>>()?;

        match kind.as_rule() {
            Rule::MATCH => on_match.extend(items),
            Rule::CREATE => on_create.extend(items),
            _ => unreachable!(),
        }
    }

    Ok(Clause::Merge(MergeClause {
        pattern,
        on_match,
        on_create,
    }))
}

fn build_call_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CALL

    let target = inner.next().unwrap();

    // Check by Rule type, not string content
    match target.as_rule() {
        Rule::statement => {
            // Subquery variant: CALL { statement }
            let stmt = build_statement(target)?;
            Ok(Clause::Call(CallClause {
                kind: CallKind::Subquery(Box::new(Query::Single(stmt))),
                yield_items: vec![],
                where_clause: None,
            }))
        }
        Rule::qualified_name => {
            // Procedure variant: CALL qualified_name (...). Each dotted part
            // is its own identifier, so `db`.`labels` names db.labels.
            let procedure = target
                .clone()
                .into_inner()
                .map(|p| ident(&p))
                .collect::<Vec<_>>()
                .join(".");
            let mut args = vec![];
            let mut yield_items = vec![];
            let mut where_clause = None;

            for p in inner {
                match p.as_rule() {
                    Rule::expression_list => args = build_expression_list(p)?,
                    Rule::yield_clause => {
                        let mut y_inner = p.into_inner();
                        y_inner.next(); // YIELD
                        let yield_items_pair = y_inner.next().unwrap();
                        let mut yi_inner = yield_items_pair.into_inner();
                        let first = yi_inner.next().unwrap();
                        if first.as_rule() == Rule::star {
                            yield_items = vec![YieldItem {
                                name: "*".to_string(),
                                alias: None,
                            }];
                        } else {
                            yield_items = std::iter::once(first)
                                .chain(yi_inner)
                                .map(build_yield_item)
                                .collect::<Result<_, _>>()?;
                        }

                        if let Some(where_pair) = y_inner.next() {
                            where_clause = Some(extract_where_expr(where_pair)?);
                        }
                    }
                    _ => {}
                }
            }

            Ok(Clause::Call(CallClause {
                kind: CallKind::Procedure {
                    procedure,
                    arguments: args,
                },
                yield_items,
                where_clause,
            }))
        }
        _ => Err(ParseError::new(format!(
            "Expected statement or qualified_name in CALL clause, got {:?}",
            target.as_rule()
        ))),
    }
}

fn build_yield_item(pair: Pair<Rule>) -> Result<YieldItem, ParseError> {
    let mut inner = pair.into_inner();
    let name = normalize_identifier(inner.next().unwrap().as_str());
    let alias = if inner.next().is_some() {
        // AS
        Some(normalize_identifier(inner.next().unwrap().as_str()))
    } else {
        None
    };
    Ok(YieldItem { name, alias })
}

pub fn build_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    match pair.as_rule() {
        Rule::expression | Rule::comprehension_source => {
            build_expression(pair.into_inner().next().unwrap())
        }
        Rule::or_expression => build_binary_left_assoc(pair, BinaryOp::Or),
        Rule::xor_expression => build_binary_left_assoc(pair, BinaryOp::Xor),
        Rule::and_expression => build_binary_left_assoc(pair, BinaryOp::And),
        Rule::not_expression => build_not_expression(pair),
        Rule::comparison_expression => build_comparison_expression(pair),
        Rule::additive_expression => build_additive_expression(pair),
        Rule::multiplicative_expression => build_multiplicative_expression(pair),
        Rule::power_expression => build_binary_left_assoc(pair, BinaryOp::Pow),
        Rule::unary_expression => build_unary_expression(pair),
        Rule::postfix_expression => build_postfix_expression(pair),
        Rule::primary_expression => build_primary_expression(pair),
        Rule::literal => build_literal(pair),
        Rule::identifier => Ok(Expr::Variable(ident(&pair))),
        Rule::parameter => Ok(Expr::Parameter(parameter_name(&pair))),
        _ => unreachable!("Unexpected expression rule: {:?}", pair.as_rule()),
    }
}

fn build_binary_left_assoc(pair: Pair<Rule>, op: BinaryOp) -> Result<Expr, ParseError> {
    build_binary_left_assoc_dynamic(pair, |_| op)
}

fn build_binary_left_assoc_dynamic(
    pair: Pair<Rule>,
    map_op: impl Fn(Rule) -> BinaryOp,
) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let mut left = build_expression(inner.next().unwrap())?;
    while let Some(op_pair) = inner.next() {
        let op = map_op(op_pair.as_rule());
        let right = build_expression(inner.next().unwrap())?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn build_additive_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    build_binary_left_assoc_dynamic(pair, |rule| match rule {
        Rule::plus => BinaryOp::Add,
        Rule::minus => BinaryOp::Sub,
        _ => unreachable!(),
    })
}

fn build_multiplicative_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    build_binary_left_assoc_dynamic(pair, |rule| match rule {
        Rule::star => BinaryOp::Mul,
        Rule::slash => BinaryOp::Div,
        Rule::percent => BinaryOp::Mod,
        _ => unreachable!(),
    })
}

fn build_not_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner().peekable();
    let mut not_count = 0;
    while peek_is(&mut inner, Rule::NOT) {
        inner.next();
        not_count += 1;
    }
    let mut expr = build_expression(inner.next().unwrap())?;
    for _ in 0..not_count {
        expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        };
    }
    Ok(expr)
}

fn build_comparison_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let first = build_expression(inner.next().unwrap())?;
    build_comparison_chain(first, inner.map(ComparisonTail::Parsed).collect())
}

/// One link of a comparison chain after its first operand.
enum ComparisonTail<'i> {
    /// A `comparison_tail` pair.
    Parsed(Pair<'i, Rule>),
    /// `IN rhs`, already built: the leading `x IN src` of a list literal, whose
    /// `IN` belongs to `list_comprehension_body` rather than to a tail.
    In(Expr),
}

fn build_comparison_chain(first: Expr, tails: Vec<ComparisonTail>) -> Result<Expr, ParseError> {
    if tails.is_empty() {
        return Ok(first);
    }

    let mut operands = vec![first];
    let mut ops = vec![];
    let mut last_was_predicate = false;

    for tail in tails {
        let tail = match tail {
            ComparisonTail::Parsed(tail) => tail,
            ComparisonTail::In(list) => {
                // Not chainable, and not a label, exactly as a parsed `IN` tail.
                if last_was_predicate {
                    return Err(ParseError::new(
                        "InvalidPredicateChain: cannot stack multiple predicates \
                         without a comparison operator between them"
                            .to_string(),
                    ));
                }
                let last = operands.pop().unwrap();
                operands.push(Expr::In {
                    expr: Box::new(last),
                    list: Box::new(list),
                });
                last_was_predicate = true;
                continue;
            }
        };
        let mut tail_inner = tail.into_inner();
        let op_pair = tail_inner.next().unwrap();
        let rule = op_pair.as_rule();

        let chain_op = match rule {
            Rule::eq => Some(BinaryOp::Eq),
            Rule::not_eq => Some(BinaryOp::NotEq),
            Rule::lt => Some(BinaryOp::Lt),
            Rule::gt => Some(BinaryOp::Gt),
            Rule::lt_eq => Some(BinaryOp::LtEq),
            Rule::gt_eq => Some(BinaryOp::GtEq),
            Rule::approx_eq => Some(BinaryOp::ApproxEq),
            _ => None,
        };

        if let Some(op) = chain_op {
            // Chainable comparison operator resets the predicate flag.
            let rhs = build_expression(tail_inner.next().unwrap())?;
            ops.push(op);
            operands.push(rhs);
            last_was_predicate = false;
        } else {
            // Non-chainable operator (IS NULL, IN, STARTS WITH, etc.)
            // Label predicates (identifier_or_keyword) are allowed to stack (a:A:B).
            let is_label = rule == Rule::identifier_or_keyword;

            if last_was_predicate && !is_label {
                return Err(ParseError::new(
                    "InvalidPredicateChain: cannot stack multiple predicates \
                     without a comparison operator between them"
                        .to_string(),
                ));
            }

            // Apply it to the LAST operand in the list.
            let last_idx = operands.len() - 1;
            let last = operands.remove(last_idx);
            let modified = apply_tail_to_expr(last, rule, op_pair, tail_inner)?;

            // Label checks are allowed to stack; other predicates set the flag.
            let produced_label = matches!(&modified, Expr::LabelCheck { .. });
            operands.push(modified);
            last_was_predicate = !produced_label;
        }
    }

    // Now build the result
    if ops.is_empty() {
        return Ok(operands.pop().unwrap());
    }

    // Create chain of ANDs
    let mut final_expr = Expr::BinaryOp {
        left: Box::new(operands[0].clone()),
        op: ops[0],
        right: Box::new(operands[1].clone()),
    };

    for i in 1..ops.len() {
        let next_cmp = Expr::BinaryOp {
            left: Box::new(operands[i].clone()),
            op: ops[i],
            right: Box::new(operands[i + 1].clone()),
        };
        final_expr = Expr::BinaryOp {
            left: Box::new(final_expr),
            op: BinaryOp::And,
            right: Box::new(next_cmp),
        };
    }

    Ok(final_expr)
}

/// A label predicate `n:A` (stacking onto a preceding `:B` as `n:B:A`), or a
/// disjunction `n:A|B` (kept as a multi-label `Expr::LabelCheck` with OR
/// semantics, matching how the planner evaluates `Expr::LabelCheck` and
/// avoiding cloning `left` per disjunct, which would blow up exponentially on
/// nested `((x:A|B):C|D)`). Mixing conjunction and disjunction (`n:A:B|C` or
/// `n:A|B:C`) is refused, as GQL refuses it.
fn label_predicate(left: Expr, first: Pair<Rule>, rest: Pairs<Rule>) -> Result<Expr, ParseError> {
    let mut labels = vec![normalize_identifier(first.as_str())];
    let mut saw_pipe = false;
    let mut saw_conjunction = matches!(left, Expr::LabelCheck { .. });
    for p in rest {
        match p.as_rule() {
            Rule::pipe => saw_pipe = true,
            Rule::identifier_or_keyword => {
                if !saw_pipe {
                    saw_conjunction = true;
                }
                labels.push(normalize_identifier(p.as_str()));
            }
            _ => {}
        }
    }
    if !saw_pipe {
        return Ok(labels.into_iter().fold(left, |expr, label| match expr {
            Expr::LabelCheck { expr, mut labels } => {
                labels.push(label);
                Expr::LabelCheck { expr, labels }
            }
            _ => Expr::LabelCheck {
                expr: Box::new(expr),
                labels: vec![label],
            },
        }));
    }
    if saw_conjunction {
        return Err(ParseError::new(
            "InvalidLabelExpression: a label conjunction (`:A:B`) cannot be combined \
             with a disjunction (`|`)"
                .to_string(),
        ));
    }
    let mut checks = labels.into_iter().map(|label| Expr::LabelCheck {
        expr: Box::new(left.clone()),
        labels: vec![label],
    });
    let first = checks.next().expect("two or more labels");
    Ok(checks.fold(first, |acc, check| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOp::Or,
        right: Box::new(check),
    }))
}

fn apply_tail_to_expr(
    left: Expr,
    rule: Rule,
    op_pair: Pair<Rule>,
    mut tail_inner: Pairs<Rule>,
) -> Result<Expr, ParseError> {
    match rule {
        Rule::IS => {
            let next = tail_inner.next().unwrap();
            match next.as_rule() {
                Rule::NULL => Ok(Expr::IsNull(Box::new(left))),
                Rule::NOT => Ok(Expr::IsNotNull(Box::new(left))),
                Rule::UNIQUE => Ok(Expr::IsUnique(Box::new(left))),
                Rule::identifier_or_keyword => label_predicate(left, next, tail_inner),
                other => unreachable!("Unexpected IS form: {other:?}"),
            }
        }
        Rule::IN => Ok(Expr::In {
            expr: Box::new(left),
            list: Box::new(build_expression(tail_inner.next().unwrap())?),
        }),
        Rule::CONTAINS => Ok(Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Contains,
            right: Box::new(build_expression(tail_inner.next().unwrap())?),
        }),
        Rule::STARTS => {
            tail_inner.next(); // WITH
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::StartsWith,
                right: Box::new(build_expression(tail_inner.next().unwrap())?),
            })
        }
        Rule::ENDS => {
            tail_inner.next(); // WITH
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::EndsWith,
                right: Box::new(build_expression(tail_inner.next().unwrap())?),
            })
        }
        Rule::regex_match => Ok(Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Regex,
            right: Box::new(build_expression(tail_inner.next().unwrap())?),
        }),
        Rule::VALID_AT => {
            let timestamp = build_expression(tail_inner.next().unwrap())?;
            let start_prop = tail_inner.next().map(build_string_literal).transpose()?;
            let end_prop = tail_inner.next().map(build_string_literal).transpose()?;
            Ok(Expr::ValidAt {
                entity: Box::new(left),
                timestamp: Box::new(timestamp),
                start_prop,
                end_prop,
            })
        }
        Rule::identifier_or_keyword => label_predicate(left, op_pair, tail_inner),
        _ => unreachable!("Unexpected non-chainable rule: {:?}", rule),
    }
}

// Build comprehension WHERE expression - limited to single comparison to avoid ambiguity

/// Negate an expression, folding integer literals at parse time.
///
/// Integer literals require special handling because the parser sees the
/// magnitude first (always positive) and the negation arrives separately.
/// For `i64::MIN` (`-9223372036854775808`), the magnitude exceeds
/// `i64::MAX` and is stored as `i64::MIN` by [`parse_integer_safe`].
/// Negating it wraps back to itself, which is the correct result.
fn negate_expression(expr: Expr) -> Expr {
    if let Expr::Literal(CypherLiteral::Integer(int_val)) = &expr {
        return Expr::Literal(CypherLiteral::Integer(int_val.wrapping_neg()));
    }
    // A float literal folds the same way: `-2.5` is the literal -2.5, exactly
    // the value `-(2.5)` evaluates to. Left as `Neg(2.5)` it was not a literal
    // to any matcher downstream, so `n.f > -2.5` on an indexed property was
    // never handed to the index while `n.x > -2` was.
    if let Expr::Literal(CypherLiteral::Float(f)) = &expr {
        return Expr::Literal(CypherLiteral::Float(-f));
    }
    Expr::UnaryOp {
        op: UnaryOp::Neg,
        expr: Box::new(expr),
    }
}

fn build_unary_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner().peekable();
    let mut neg_count = 0u32;

    // Consume leading plus/minus tokens
    while inner
        .peek()
        .is_some_and(|p| matches!(p.as_rule(), Rule::minus | Rule::plus))
    {
        let p = inner.next().unwrap();
        if p.as_rule() == Rule::minus {
            neg_count += 1;
        }
        // plus is identity — skip
    }

    let mut expr = build_expression(inner.next().unwrap())?;

    if neg_count == 0 {
        // No negation: validate no bare overflow literal
        if let Expr::Literal(CypherLiteral::Integer(i64::MIN)) = expr {
            return Err(ParseError::new(
                "IntegerOverflow: value too large".to_string(),
            ));
        }
    } else {
        // Apply negate_expression for each minus
        for _ in 0..neg_count {
            expr = negate_expression(expr);
        }
        // Even negation count on the boundary value wraps back to i64::MIN,
        // which came from parsing a magnitude that was already overflowed.
        if neg_count.is_multiple_of(2)
            && matches!(expr, Expr::Literal(CypherLiteral::Integer(i64::MIN)))
        {
            return Err(ParseError::new(
                "IntegerOverflow: value too large".to_string(),
            ));
        }
    }

    Ok(expr)
}

fn build_postfix_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let base = build_expression(inner.next().unwrap())?;
    inner.try_fold(base, apply_postfix_suffix)
}

/// Build a function call expression from the given base expression.
fn make_function_call(
    base: &Expr,
    args: Vec<Expr>,
    distinct: bool,
    window_spec: Option<WindowSpec>,
) -> Result<Expr, ParseError> {
    let name = extract_dotted_name(base)
        .ok_or_else(|| ParseError::new(format!("Invalid call base: {base:?}")))?;
    Ok(Expr::FunctionCall {
        name,
        args,
        distinct,
        window_spec,
    })
}

fn apply_postfix_suffix(base: Expr, suffix: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = suffix.into_inner();

    // Handle empty function calls like rand()
    let Some(first) = inner.next() else {
        return make_function_call(&base, vec![], false, None);
    };

    // Check for DISTINCT in function calls
    let (distinct, first) = if first.as_rule() == Rule::DISTINCT {
        (true, inner.next())
    } else {
        (false, Some(first))
    };

    let Some(first) = first else {
        // DISTINCT with no args: func(DISTINCT)
        return make_function_call(&base, vec![], distinct, None);
    };

    match first.as_rule() {
        Rule::identifier_or_keyword => Ok(Expr::Property(
            Box::new(base),
            normalize_identifier(first.as_str()),
        )),
        Rule::expression => {
            if let Some(second) = inner.next() {
                if second.as_rule() == Rule::dot_dot {
                    let end = inner.next().map(build_expression).transpose()?;
                    Ok(Expr::ArraySlice {
                        array: Box::new(base),
                        start: Some(Box::new(build_expression(first)?)),
                        end: end.map(Box::new),
                    })
                } else {
                    unreachable!()
                }
            } else {
                Ok(Expr::ArrayIndex {
                    array: Box::new(base),
                    index: Box::new(build_expression(first)?),
                })
            }
        }
        Rule::dot_dot => {
            let end = inner.next().map(build_expression).transpose()?;
            Ok(Expr::ArraySlice {
                array: Box::new(base),
                start: None,
                end: end.map(Box::new),
            })
        }
        Rule::expression_list => {
            let args = build_expression_list(first)?;
            let window_spec = inner.next().map(build_window_spec).transpose()?;
            make_function_call(&base, args, distinct, window_spec)
        }
        Rule::map_projection_items => {
            let items = build_map_projection_items(first)?;
            Ok(Expr::MapProjection {
                base: Box::new(base),
                items,
            })
        }
        Rule::window_spec => {
            let window_spec = Some(build_window_spec(first)?);
            make_function_call(&base, vec![], distinct, window_spec)
        }
        _ => unreachable!(),
    }
}

fn build_primary_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();
    match first.as_rule() {
        Rule::literal => build_literal(first),
        Rule::function_name => {
            // function_name is atomic, just get the string directly
            Ok(Expr::Variable(ident(&first)))
        }
        Rule::identifier => Ok(Expr::Variable(ident(&first))),
        Rule::parameter => Ok(Expr::Parameter(parameter_name(&first))),
        Rule::case_expression => build_case_expression(first),
        Rule::list_expression => build_list_expression(first),
        Rule::map_literal => build_map_literal(first),
        Rule::expression => build_expression(first),
        Rule::count_subquery => {
            let inner = first.into_inner().nth(1).unwrap();
            let query = match inner.as_rule() {
                Rule::statement => Query::Single(build_statement(inner)?),
                Rule::pattern => match_query(build_pattern(inner)?, None),
                _ => unreachable!("Unexpected rule in count_subquery: {:?}", inner.as_rule()),
            };
            Ok(Expr::CountSubquery(Box::new(query)))
        }
        Rule::collect_subquery => {
            let stmt = build_statement(first.into_inner().nth(1).unwrap())?;
            Ok(Expr::CollectSubquery(Box::new(Query::Single(stmt))))
        }
        Rule::exists_expression => {
            let content = first.into_inner().nth(1).unwrap();
            let mut content_inner = content.into_inner();
            let first_item = content_inner.next().unwrap();

            let query = match first_item.as_rule() {
                Rule::statement => Query::Single(build_statement(first_item)?),
                Rule::pattern => {
                    let pattern = build_pattern(first_item)?;
                    let where_clause = content_inner
                        .next()
                        .filter(|p| p.as_rule() == Rule::where_clause)
                        .map(extract_where_expr)
                        .transpose()?;
                    match_query(pattern, where_clause)
                }
                _ => unreachable!(
                    "Unexpected rule in exists_subquery_content: {:?}",
                    first_item.as_rule()
                ),
            };
            Ok(Expr::Exists {
                query: Box::new(query),
                from_pattern_predicate: false,
            })
        }
        Rule::quantifier_expression => build_quantifier_expression(first),
        Rule::reduce_expression => build_reduce_expression(first),
        Rule::pattern_expression => {
            let elements = first
                .into_inner()
                .map(build_pattern_child)
                .collect::<Result<Vec<_>, _>>()?;
            let pattern = Pattern {
                paths: vec![PathPattern {
                    variable: None,
                    elements,
                    shortest_path_mode: None,
                }],
            };
            Ok(Expr::Exists {
                query: Box::new(match_query(pattern, None)),
                from_pattern_predicate: true,
            })
        }
        Rule::COUNT => {
            let mut distinct = false;
            let mut args = vec![];
            let mut window_spec = None;
            for p in inner {
                match p.as_rule() {
                    Rule::DISTINCT => distinct = true,
                    Rule::count_args => {
                        let ca_inner = p.into_inner().next().unwrap();
                        if ca_inner.as_rule() == Rule::star {
                            args.push(Expr::Wildcard);
                        } else {
                            args = build_expression_list(ca_inner)?;
                        }
                    }
                    Rule::window_spec => window_spec = Some(build_window_spec(p)?),
                    _ => {}
                }
            }
            Ok(Expr::FunctionCall {
                name: first.as_str().to_string(),
                args,
                distinct,
                window_spec,
            })
        }
        Rule::EXISTS => {
            let list = inner.next().unwrap();
            Ok(Expr::FunctionCall {
                name: first.as_str().to_string(),
                args: build_expression_list(list)?,
                distinct: false,
                window_spec: None,
            })
        }
        _ => unreachable!("Unexpected primary: {:?}", first.as_rule()),
    }
}

/// Unescape a Cypher string literal
/// Handles: \\ \' \" \t \n \r \b \f \uXXXX \UXXXXXX and '' doubling
fn unescape_string(s: &str, quote_char: char) -> Result<String, ParseError> {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('\\') => result.push('\\'),
                Some('\'') => result.push('\''),
                Some('"') => result.push('"'),
                Some('t') => result.push('\t'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('b') => result.push('\u{0008}'), // backspace
                Some('f') => result.push('\u{000C}'), // form feed
                Some(esc @ ('u' | 'U')) => {
                    let expected_len = if esc == 'u' { 4 } else { 6 };
                    let hex: String = chars.by_ref().take(expected_len).collect();
                    if hex.len() != expected_len {
                        return Err(ParseError::new(format!(
                            "InvalidUnicodeLiteral: Invalid \\{esc} escape: \\{esc}{hex}"
                        )));
                    }
                    let code = u32::from_str_radix(&hex, 16).map_err(|_| {
                        ParseError::new(format!(
                            "InvalidUnicodeLiteral: Invalid hex in \\{esc}{hex}"
                        ))
                    })?;
                    result.push(char::from_u32(code).ok_or_else(|| {
                        ParseError::new(format!(
                            "InvalidUnicodeLiteral: Invalid unicode \\{esc}{hex}"
                        ))
                    })?);
                }
                Some(other) => {
                    // For regex compatibility, preserve backslash + character literally
                    // This allows regex patterns like \. \d \w etc. in string literals
                    result.push('\\');
                    result.push(other);
                }
                None => {
                    return Err(ParseError::new(
                        "InvalidUnicodeLiteral: Unexpected end after backslash".to_string(),
                    ));
                }
            }
        } else if ch == quote_char {
            // Handle doubled quotes ('' or "")
            if chars.as_str().starts_with(quote_char) {
                chars.next(); // consume the second quote
                result.push(quote_char);
            } else {
                // Single quote at end (shouldn't happen in valid input)
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

/// Parse an unsigned integer string in the given radix with overflow handling.
///
/// Values that fit in `i64` are returned directly. The boundary value
/// `i64::MAX + 1` is stored as `i64::MIN` so that [`negate_expression`]
/// can produce the correct result for `-9223372036854775808`. Larger
/// values result in an error.
fn parse_integer_safe(s: &str, radix: u32) -> Result<i64, ParseError> {
    if let Ok(val) = i64::from_str_radix(s, radix) {
        return Ok(val);
    }

    // i64 parse failed; try u64 to handle the i64::MIN boundary case.
    // The magnitude i64::MAX+1 is stored as i64::MIN so that
    // negate_expression can produce the correct -9223372036854775808.
    let magnitude = u64::from_str_radix(s, radix)
        .map_err(|e| ParseError::new(format!("Invalid integer: {e}")))?;

    if magnitude == i64::MAX as u64 + 1 {
        Ok(i64::MIN)
    } else {
        Err(ParseError::new(
            "IntegerOverflow: value too large".to_string(),
        ))
    }
}

fn build_literal(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::integer => {
            let s_clean = inner.as_str().replace('_', "");
            let value = if let Some(hex) = s_clean
                .strip_prefix("0x")
                .or_else(|| s_clean.strip_prefix("0X"))
            {
                parse_integer_safe(hex, 16)?
            } else if let Some(oct) = s_clean
                .strip_prefix("0o")
                .or_else(|| s_clean.strip_prefix("0O"))
            {
                parse_integer_safe(oct, 8)?
            } else {
                parse_integer_safe(&s_clean, 10)?
            };
            Ok(Expr::Literal(CypherLiteral::Integer(value)))
        }
        Rule::float => {
            let s = inner.as_str().replace('_', "");
            let value = s
                .parse::<f64>()
                .map_err(|e| ParseError::new(format!("Invalid float: {e}")))?;
            if value.is_infinite() {
                return Err(ParseError::new(
                    "FloatingPointOverflow: value too large".to_string(),
                ));
            }
            Ok(Expr::Literal(CypherLiteral::Float(value)))
        }
        Rule::infinity => {
            let value = if inner.as_str().starts_with('-') {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };
            Ok(Expr::Literal(CypherLiteral::Float(value)))
        }
        Rule::nan => Ok(Expr::Literal(CypherLiteral::Float(f64::NAN))),
        Rule::string => {
            let s = inner.as_str();
            let quote_char = s.chars().next().unwrap();
            let content = &s[1..s.len() - 1];
            let unescaped = unescape_string(content, quote_char)?;
            Ok(Expr::Literal(CypherLiteral::String(unescaped)))
        }
        Rule::TRUE => Ok(Expr::Literal(CypherLiteral::Bool(true))),
        Rule::FALSE => Ok(Expr::Literal(CypherLiteral::Bool(false))),
        Rule::NULL => Ok(Expr::Literal(CypherLiteral::Null)),
        _ => unreachable!(),
    }
}

fn build_list_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let Some(first) = inner.next() else {
        return Ok(Expr::List(vec![]));
    };

    match first.as_rule() {
        Rule::pattern_comprehension_body => {
            let mut p_inner = first.into_inner().peekable();
            let variable = consume_identifier(&mut p_inner);
            let path = build_path_pattern(p_inner.next().unwrap())?;
            let pattern = Pattern { paths: vec![path] };

            let where_clause = if peek_is(&mut p_inner, Rule::WHERE) {
                p_inner.next();
                Some(build_expression(p_inner.next().unwrap())?)
            } else {
                None
            };

            p_inner.next(); // consume pipe
            let map_expr = build_expression(p_inner.next().unwrap())?;

            Ok(Expr::PatternComprehension {
                path_variable: variable,
                pattern,
                where_clause: where_clause.map(Box::new),
                map_expr: Box::new(map_expr),
            })
        }
        Rule::list_comprehension_body => {
            let mut l_inner = first.into_inner();
            let var = ident(&l_inner.next().unwrap());
            l_inner.next(); // IN
            let source = l_inner.next().unwrap();

            let next = l_inner.next();
            if let Some(tail) = next.clone()
                && tail.as_rule() == Rule::list_literal_tail
            {
                return build_list_literal_led_by_in(var, source, tail);
            }
            let list = build_expression(source)?;

            let mut where_clause = None;
            let mut map_expr = None;

            // Process optional WHERE clause
            if let Some(next) = next {
                if next.as_rule() == Rule::WHERE {
                    where_clause = Some(build_expression(l_inner.next().unwrap())?);
                    // After WHERE, check for optional pipe
                    if let Some(pipe_or_expr) = l_inner.next()
                        && pipe_or_expr.as_rule() == Rule::pipe
                    {
                        map_expr = Some(build_expression(l_inner.next().unwrap())?);
                    }
                } else if next.as_rule() == Rule::pipe {
                    // No WHERE, just pipe and map expression
                    map_expr = Some(build_expression(l_inner.next().unwrap())?);
                }
            }

            // If no map expression provided, default to identity (return the variable itself)
            let map_expr = map_expr.unwrap_or_else(|| Expr::Variable(var.clone()));

            Ok(Expr::ListComprehension {
                variable: var,
                list: Box::new(list),
                where_clause: where_clause.map(Box::new),
                map_expr: Box::new(map_expr),
            })
        }
        Rule::expression => {
            let exprs: Vec<_> = std::iter::once(first)
                .chain(inner)
                .map(build_expression)
                .collect::<Result<_, _>>()?;
            Ok(Expr::List(exprs))
        }
        _ => unreachable!("Unexpected list rule: {:?}", first.as_rule()),
    }
}

/// The list literal `[x IN src ..., e2, ...]`, parsed through
/// `list_comprehension_body` so that `src` is parsed once (see `list_expression`
/// in the grammar). Builds exactly what the literal alternative would have: the
/// first element is the comparison chain `x IN src`, then its AND, XOR and OR
/// operands, left-associative.
fn build_list_literal_led_by_in(
    var: String,
    source: Pair<Rule>,
    tail: Pair<Rule>,
) -> Result<Expr, ParseError> {
    // `comprehension_source` -> `comparison_expression` -> first operand, tails.
    let comparison = source.into_inner().next().unwrap();
    let mut links = comparison.into_inner();
    let in_rhs = build_expression(links.next().unwrap())?;
    let mut chain = vec![ComparisonTail::In(in_rhs)];
    chain.extend(links.map(ComparisonTail::Parsed));
    let mut element = build_comparison_chain(Expr::Variable(var), chain)?;

    let mut elements = Vec::new();
    for part in tail.into_inner() {
        let op = match part.as_rule() {
            Rule::literal_and_operands => BinaryOp::And,
            Rule::literal_xor_operands => BinaryOp::Xor,
            Rule::literal_or_operands => BinaryOp::Or,
            _ => {
                elements.push(build_expression(part)?);
                continue;
            }
        };
        for operand in part
            .into_inner()
            .filter(|p| !matches!(p.as_rule(), Rule::AND | Rule::XOR | Rule::OR))
        {
            element = Expr::BinaryOp {
                left: Box::new(element),
                op,
                right: Box::new(build_expression(operand)?),
            };
        }
    }
    elements.insert(0, element);
    Ok(Expr::List(elements))
}

fn build_case_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CASE

    let expr = if inner
        .peek()
        .is_some_and(|p| p.as_rule() == Rule::expression)
    {
        Some(Box::new(build_expression(inner.next().unwrap())?))
    } else {
        None
    };

    let mut when_then = vec![];
    let mut else_expr = None;

    for p in inner {
        match p.as_rule() {
            Rule::when_clause => {
                let mut w_inner = p.into_inner();
                w_inner.next(); // WHEN
                let w = build_expression(w_inner.next().unwrap())?;
                w_inner.next(); // THEN
                let t = build_expression(w_inner.next().unwrap())?;
                when_then.push((w, t));
            }
            Rule::else_clause => {
                else_expr = Some(Box::new(build_expression(p.into_inner().nth(1).unwrap())?));
            }
            _ => {}
        }
    }
    Ok(Expr::Case {
        expr,
        when_then,
        else_expr,
    })
}

fn build_quantifier_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let q_rule = inner.next().unwrap().as_rule();
    let quantifier = match q_rule {
        Rule::ALL => Quantifier::All,
        Rule::ANY_KW => Quantifier::Any,
        Rule::SINGLE => Quantifier::Single,
        Rule::NONE => Quantifier::None,
        _ => unreachable!(),
    };

    let variable = ident(&inner.next().unwrap());
    inner.next(); // IN
    let list = build_expression(inner.next().unwrap())?;
    inner.next(); // WHERE
    let predicate = build_expression(inner.next().unwrap())?;
    Ok(Expr::Quantifier {
        quantifier,
        variable,
        list: Box::new(list),
        predicate: Box::new(predicate),
    })
}

fn build_reduce_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // REDUCE
    let accumulator = ident(&inner.next().unwrap());
    inner.next(); // =
    let init = build_expression(inner.next().unwrap())?;
    // , skipped
    let variable = ident(&inner.next().unwrap());
    inner.next(); // IN
    let list = build_expression(inner.next().unwrap())?;
    inner.next(); // pipe
    let expr = build_expression(inner.next().unwrap())?;
    Ok(Expr::Reduce {
        accumulator,
        init: Box::new(init),
        variable,
        list: Box::new(list),
        expr: Box::new(expr),
    })
}

fn build_map_literal(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let entries = pair
        .into_inner()
        .map(|p| {
            let mut inner = p.into_inner();
            let key_pair = inner.next().unwrap();
            let key = match key_pair.as_rule() {
                // Decode escapes / doubled quotes exactly as the value path does,
                // so `{ "a\nb": 1 }` keys on a real newline (not a literal
                // backslash-n) and `{ 'it''s': 1 }` keys on `it's`.
                Rule::string => build_string_literal(key_pair)?,
                _ => normalize_identifier(key_pair.as_str()), // identifier_or_keyword
            };
            let val = build_expression(inner.next().unwrap())?;
            Ok((key, val))
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(Expr::Map(entries))
}

pub(crate) fn build_pattern(pair: Pair<Rule>) -> Result<Pattern, ParseError> {
    let paths = pair
        .into_inner()
        .map(build_path_pattern)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Pattern { paths })
}

/// Build a `PatternElement` from a node_pattern or relationship_pattern pair.
fn build_pattern_child(child: Pair<Rule>) -> Result<PatternElement, ParseError> {
    match child.as_rule() {
        Rule::node_pattern => Ok(PatternElement::Node(build_node_pattern(child)?)),
        Rule::relationship_pattern => Ok(PatternElement::Relationship(build_relationship_pattern(
            child,
        )?)),
        _ => unreachable!("Unexpected pattern child: {:?}", child.as_rule()),
    }
}

/// Process a single pattern_element and append its contents to the elements vector.
fn process_pattern_element(
    elem: Pair<Rule>,
    elements: &mut Vec<PatternElement>,
) -> Result<(), ParseError> {
    if elem.as_rule() != Rule::pattern_element {
        return Ok(());
    }

    let mut elem_inner = elem.into_inner();
    let first_child = elem_inner.next().unwrap();

    match first_child.as_rule() {
        Rule::parenthesized_pattern => {
            elements.push(build_parenthesized_pattern(first_child)?);
        }
        Rule::node_pattern => {
            elements.push(build_pattern_child(first_child)?);
            for child in elem_inner {
                if matches!(
                    child.as_rule(),
                    Rule::node_pattern | Rule::relationship_pattern
                ) {
                    elements.push(build_pattern_child(child)?);
                }
            }
        }
        _ => unreachable!(
            "Unexpected rule in pattern_element: {:?}",
            first_child.as_rule()
        ),
    }
    Ok(())
}

fn build_path_pattern(pair: Pair<Rule>) -> Result<PathPattern, ParseError> {
    let mut inner = pair.into_inner().peekable();

    let variable = consume_identifier(&mut inner);

    let mut shortest_path_mode = None;
    let mut elements = vec![];

    let next = inner.next().unwrap();

    if next.as_rule() == Rule::shortest_path_pattern {
        // Determine mode from matched text
        let matched_lower = next.as_str().to_lowercase();
        shortest_path_mode = Some(if matched_lower.starts_with("allshortest") {
            ShortestPathMode::AllShortest
        } else {
            ShortestPathMode::Shortest
        });

        for elem in next.into_inner() {
            process_pattern_element(elem, &mut elements)?;
        }
    } else {
        // Regular pattern_element+
        process_pattern_element(next, &mut elements)?;
        for p in inner {
            process_pattern_element(p, &mut elements)?;
        }
    }

    Ok(PathPattern {
        variable,
        elements,
        shortest_path_mode,
    })
}

fn build_parenthesized_pattern(pair: Pair<Rule>) -> Result<PatternElement, ParseError> {
    let mut inner = pair.into_inner().peekable();

    let variable = consume_identifier(&mut inner);

    let pattern_part = inner.next().unwrap();
    let elements = pattern_part
        .into_inner()
        .map(build_pattern_child)
        .collect::<Result<Vec<_>, _>>()?;

    let where_clause = if peek_is(&mut inner, Rule::WHERE) {
        inner.next();
        Some(build_expression(inner.next().unwrap())?)
    } else {
        None
    };

    // Optional path quantifier
    let range = inner.next().map(build_path_quantifier).transpose()?;

    // Create a PathPattern from the elements
    let path_pattern = PathPattern {
        variable,
        elements,
        shortest_path_mode: None,
    };

    // Previously this branch emitted an `eprintln!` warning and then
    // dropped the WHERE clause on the floor, producing wrong results
    // for queries like `MATCH ((n)-[r]->(m) WHERE r.x > 0)+ ...`.
    // Until the AST and executor support parenthesized-pattern WHERE
    // end-to-end, refuse to parse the query rather than silently
    // miscompile it. (Tracking issue: AST extension for
    // `PatternElement::Parenthesized { where_clause }`.)
    if where_clause.is_some() {
        return Err(ParseError::new(
            "WHERE inside a parenthesized path pattern is not yet supported; \
             move the predicate to the enclosing MATCH or omit it"
                .to_string(),
        ));
    }

    Ok(PatternElement::Parenthesized {
        pattern: Box::new(path_pattern),
        range,
    })
}

/// Parses a parenthesized-path quantifier bound into a `u32`.
///
/// `u32::from_str` natively rejects negative (`-2`), hexadecimal (`0x2`), and
/// overflowing (`4294967296`) inputs, so this returns an error rather than
/// panicking on any of them.
///
/// # Errors
///
/// Returns [`ParseError`] when `s` is not a valid non-negative `u32`.
fn parse_quantifier_bound(s: &str) -> Result<u32, ParseError> {
    s.parse::<u32>()
        .map_err(|_| ParseError::new(format!("invalid path quantifier bound: {s}")))
}

/// Builds a [`Range`] from a parenthesized-path quantifier (`+`, `*`, `{..}`).
///
/// openCypher semantics: `*`/`{,}` = `1..∞`, `+` = `1..∞`, `{n}` = `n..n`,
/// `{n,}` = `n..∞`, `{n,m}` = `n..m`, `{,m}` = `1..m`. An empty lower bound is
/// represented as `min: None` (mirroring how the relationship form `*..m` is
/// built), which the executor treats as `1`.
///
/// The grammar makes the `{`, `,`, and `}` tokens silent, so the inner pairs
/// only ever expose the optional `integer` bounds; the empty-lower `{,m}` form
/// is therefore distinguished by sniffing the raw token text for a leading
/// comma rather than by pair position.
///
/// # Errors
///
/// Returns [`ParseError`] when a bound is not a valid non-negative `u32`.
fn build_path_quantifier(pair: Pair<Rule>) -> Result<Range, ParseError> {
    let has_comma = pair.as_str().contains(',');
    // `{` and any leading whitespace are silent in the grammar, so a leading
    // comma in the raw text means the lower bound was omitted (`{,m}`).
    let empty_lower = pair
        .as_str()
        .trim_start_matches('{')
        .trim_start()
        .starts_with(',');
    let mut inner = pair.into_inner().peekable();

    let Some(first) = inner.next() else {
        // `{,}` (and any all-silent form) means unbounded both ways: 1..∞.
        return Ok(Range {
            min: None,
            max: None,
        });
    };

    match first.as_rule() {
        Rule::plus => Ok(Range {
            min: Some(1),
            max: None,
        }),
        Rule::star => Ok(Range {
            min: None,
            max: None,
        }),
        Rule::integer => {
            let n = parse_quantifier_bound(first.as_str())?;
            if empty_lower {
                // {,m} - empty lower bound, `n` is actually the max.
                return Ok(Range {
                    min: None,
                    max: Some(n),
                });
            }
            if !has_comma {
                // {n} - exactly n.
                return Ok(Range {
                    min: Some(n),
                    max: Some(n),
                });
            }
            // {n,m} or {n,}.
            let max = inner
                .next()
                .filter(|p| p.as_rule() == Rule::integer)
                .map(|p| parse_quantifier_bound(p.as_str()))
                .transpose()?;
            Ok(Range { min: Some(n), max })
        }
        rule => unreachable!("unexpected rule in path quantifier: {rule:?}"),
    }
}

/// Parse a predicate pair (node_predicate or rel_predicate) into properties and WHERE clause.
///
/// Predicate structure: `properties WHERE expr | WHERE expr | properties`
fn parse_predicate(pair: Pair<Rule>) -> Result<(Option<Expr>, Option<Expr>), ParseError> {
    let mut pred_inner = pair.into_inner();
    let Some(first) = pred_inner.next() else {
        return Ok((None, None));
    };

    match first.as_rule() {
        Rule::properties => {
            let properties = Some(build_properties(first)?);
            let where_clause = if pred_inner
                .next()
                .is_some_and(|p| p.as_rule() == Rule::WHERE)
            {
                Some(build_expression(pred_inner.next().unwrap())?)
            } else {
                None
            };
            Ok((properties, where_clause))
        }
        Rule::WHERE => {
            let where_clause = Some(build_expression(pred_inner.next().unwrap())?);
            Ok((None, where_clause))
        }
        _ => unreachable!("Unexpected rule in predicate: {:?}", first.as_rule()),
    }
}

fn build_node_pattern(pair: Pair<Rule>) -> Result<NodePattern, ParseError> {
    let mut variable = None;
    let mut labels = LabelExpr::Empty;
    let mut properties = None;
    let mut where_clause = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier => variable = Some(ident(&p)),
            Rule::node_labels => {
                let has_pipe = p.clone().into_inner().any(|t| t.as_rule() == Rule::pipe);
                let names: Vec<String> = p
                    .into_inner()
                    .filter(|t| {
                        matches!(t.as_rule(), Rule::identifier | Rule::identifier_or_keyword)
                    })
                    .map(|t| normalize_identifier(t.as_str()))
                    .collect();
                labels = if has_pipe {
                    LabelExpr::Disjunction(names)
                } else {
                    LabelExpr::Conjunction(names)
                };
            }
            Rule::node_predicate => {
                let (props, wc) = parse_predicate(p)?;
                properties = props;
                where_clause = wc;
            }
            Rule::properties => properties = Some(build_properties(p)?),
            _ => {}
        }
    }
    Ok(NodePattern {
        variable,
        labels,
        properties,
        where_clause,
    })
}

fn build_relationship_pattern(pair: Pair<Rule>) -> Result<RelationshipPattern, ParseError> {
    let mut has_left = false;
    let mut has_right = false;
    let mut variable = None;
    let mut types = LabelExpr::Empty;
    let mut range = None;
    let mut properties = None;
    let mut where_clause = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::arrow_left => has_left = true,
            Rule::arrow_right => has_right = true,
            Rule::arrow_bidirectional => {
                has_left = true;
                has_right = true;
            }
            Rule::relationship_detail => {
                for detail in p.into_inner() {
                    match detail.as_rule() {
                        Rule::identifier => variable = Some(ident(&detail)),
                        Rule::relationship_types => {
                            // Relationship types are always disjunction
                            // semantically — `[r:A|B]` matches edges of
                            // either type. Even the single-type form
                            // `[r:A]` is encoded the same way; reduction
                            // to the singleton case is a planner concern.
                            let names: Vec<String> = detail
                                .into_inner()
                                .filter(|p| {
                                    matches!(
                                        p.as_rule(),
                                        Rule::identifier | Rule::identifier_or_keyword
                                    )
                                })
                                .map(|t| normalize_identifier(t.as_str()))
                                .collect();
                            types = LabelExpr::Disjunction(names);
                        }
                        Rule::range_literal => range = Some(build_range(detail)?),
                        Rule::rel_predicate => {
                            let (props, wc) = parse_predicate(detail)?;
                            properties = props;
                            where_clause = wc;
                        }
                        Rule::properties => properties = Some(build_properties(detail)?),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    let direction = match (has_left, has_right) {
        (true, false) => Direction::Incoming,
        (false, true) => Direction::Outgoing,
        _ => Direction::Both,
    };

    Ok(RelationshipPattern {
        variable,
        types,
        direction,
        range,
        properties,
        where_clause,
    })
}

fn build_single_return_item(pair: Pair<Rule>) -> Result<ReturnItem, ParseError> {
    let mut inner = pair.into_inner();
    let expr_pair = inner.next().unwrap();
    let source_text = expr_pair.as_str().trim().to_string();
    let expr = build_expression(expr_pair)?;
    let alias = if inner.next().is_some() {
        // AS keyword consumed; next is the alias identifier
        Some(normalize_identifier(inner.next().unwrap().as_str()))
    } else {
        None
    };
    Ok(ReturnItem::Expr {
        expr,
        alias,
        source_text: Some(source_text),
    })
}

pub(crate) fn build_return_items(pair: Pair<Rule>) -> Result<Vec<ReturnItem>, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    if first.as_rule() == Rule::star {
        let mut items = vec![ReturnItem::All];
        for p in inner {
            items.push(build_single_return_item(p)?);
        }
        return Ok(items);
    }

    std::iter::once(first)
        .chain(inner)
        .map(build_single_return_item)
        .collect()
}

pub(crate) fn build_sort_items(pair: Pair<Rule>) -> Result<Vec<SortItem>, ParseError> {
    pair.into_inner()
        .map(|p| {
            let mut i = p.into_inner();
            let expr = build_expression(i.next().unwrap())?;
            let ascending = i.next().is_none_or(|o| o.as_rule() == Rule::ASC);
            Ok(SortItem { expr, ascending })
        })
        .collect()
}

fn build_window_spec(pair: Pair<Rule>) -> Result<WindowSpec, ParseError> {
    let mut partition_by = vec![];
    let mut order_by = vec![];

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::partition_clause => {
                // partition_clause = { PARTITION ~ BY ~ expression_list }
                // So inner elements are: [0]=PARTITION, [1]=BY, [2]=expression_list
                let list = p.into_inner().nth(2).unwrap();
                partition_by = build_expression_list(list)?;
            }
            Rule::order_clause => {
                // order_clause = { ORDER ~ BY ~ sort_items }
                // So inner elements are: [0]=ORDER, [1]=BY, [2]=sort_items
                let list = p.into_inner().nth(2).unwrap();
                order_by = build_sort_items(list)?;
            }
            _ => {}
        }
    }
    Ok(WindowSpec {
        partition_by,
        order_by,
    })
}

pub(crate) fn build_properties(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::map_literal => build_map_literal(inner),
        Rule::parameter => Ok(Expr::Parameter(parameter_name(&inner))),
        _ => unreachable!(),
    }
}

/// Parse a range bound token into a non-negative u32.
fn parse_range_bound(pair: &Pair<Rule>) -> Result<u32, ParseError> {
    let val: i64 = pair.as_str().parse().map_err(|e| {
        ParseError::new(format!(
            "SyntaxError: InvalidRelationshipPattern - Invalid range bound '{}': {}",
            pair.as_str(),
            e
        ))
    })?;
    if val < 0 {
        return Err(ParseError::new(format!(
            "SyntaxError: InvalidRelationshipPattern - Negative range bound '{}' is not allowed",
            val
        )));
    }
    Ok(val as u32)
}

fn build_range(pair: Pair<Rule>) -> Result<Range, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // star
    let mut min = None;
    let mut max = None;

    if let Some(first) = inner.next() {
        if first.as_rule() == Rule::integer {
            let val = parse_range_bound(&first)?;
            min = Some(val);
            if inner.next().is_some() {
                // dot_dot consumed; check for upper bound
                if let Some(second) = inner.next() {
                    max = Some(parse_range_bound(&second)?);
                }
            } else {
                // No dot_dot means exact bound: *N
                max = Some(val);
            }
        } else if first.as_rule() == Rule::dot_dot
            && let Some(second) = inner.next()
        {
            max = Some(parse_range_bound(&second)?);
        }
    } else {
        // [*] alone means 1 or more (OpenCypher standard default)
        min = Some(1);
    }

    Ok(Range { min, max })
}

fn build_expression_list(pair: Pair<Rule>) -> Result<Vec<Expr>, ParseError> {
    pair.into_inner().map(build_expression).collect()
}

fn build_map_projection_items(pair: Pair<Rule>) -> Result<Vec<MapProjectionItem>, ParseError> {
    pair.into_inner()
        .map(|p| {
            let full_str = p.as_str();
            let mut inner = p.into_inner();

            // Check if this is a property selector (starts with .)
            if full_str.starts_with('.') {
                let first = inner.next().unwrap();
                if first.as_rule() == Rule::star {
                    // .*
                    Ok(MapProjectionItem::AllProperties)
                } else {
                    // .property_name
                    Ok(MapProjectionItem::Property(normalize_identifier(
                        first.as_str(),
                    )))
                }
            } else if full_str.contains(':') {
                // Computed property: key: expr
                // Inner contains: [identifier, expression]
                let key = normalize_identifier(inner.next().unwrap().as_str());
                let val = build_expression(inner.next().unwrap())?;
                Ok(MapProjectionItem::LiteralEntry(key, Box::new(val)))
            } else {
                // Variable selector: just an identifier
                let first = inner.next().unwrap();
                Ok(MapProjectionItem::Variable(ident(&first)))
            }
        })
        .collect()
}

fn build_schema_command(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::create_vector_index => build_create_vector_index(inner),
        Rule::create_fulltext_index => build_create_fulltext_index(inner),
        Rule::create_scalar_index => build_create_scalar_index(inner),
        Rule::create_json_index => build_create_json_index(inner),
        Rule::drop_index => build_drop_index(inner),
        Rule::create_constraint => build_create_constraint(inner),
        Rule::drop_constraint => build_drop_constraint(inner),
        Rule::create_label => build_create_label(inner),
        Rule::create_edge_type => build_create_edge_type(inner),
        Rule::alter_label => build_alter_label(inner),
        Rule::alter_edge_type => build_alter_edge_type(inner),
        Rule::drop_label => build_drop_label(inner),
        Rule::drop_edge_type => build_drop_edge_type(inner),
        Rule::show_constraints => build_show_constraints(inner),
        Rule::show_indexes => build_show_indexes(inner),
        Rule::show_database => Ok(SchemaCommand::ShowDatabase),
        Rule::show_config => Ok(SchemaCommand::ShowConfig),
        Rule::show_statistics => Ok(SchemaCommand::ShowStatistics),
        Rule::vacuum_command => Ok(SchemaCommand::Vacuum),
        Rule::checkpoint_command => Ok(SchemaCommand::Checkpoint),
        Rule::backup_command => build_backup_command(inner),
        Rule::copy_to => build_copy_to(inner),
        Rule::copy_from => build_copy_from(inner),
        _ => unreachable!("Unexpected schema command: {:?}", inner.as_rule()),
    }
}

// Schema command helper functions

/// Parse label from optional `var:Label` or just `Label` pattern.
///
/// Consumes the first token (which is either a label or variable), then peeks
/// to see if there's a following `identifier_or_keyword` (the actual label).
fn parse_label_binding(inner: &mut std::iter::Peekable<Pairs<Rule>>) -> String {
    let first_token = inner.next().unwrap();
    match inner.peek() {
        Some(p) if p.as_rule() == Rule::identifier_or_keyword => {
            // var:Label pattern - consume and return the label
            normalize_identifier(inner.next().unwrap().as_str())
        }
        _ => normalize_identifier(first_token.as_str()),
    }
}

/// Extract the property name from the last segment of a `property_expr`.
///
/// e.g., "d.embedding" -> "embedding". Reads the last identifier token rather
/// than splitting the text on `.`, which a quoted name may contain
/// (`` d.`a.b` `` is the property `a.b`).
fn extract_property_name(pair: Pair<Rule>) -> String {
    let fallback = pair.as_str().trim().to_string();
    pair.into_inner()
        .rfind(|p| matches!(p.as_rule(), Rule::identifier | Rule::identifier_or_keyword))
        .map_or(fallback, |p| ident(&p))
}

/// Parse OPTIONS clause if present, returning empty map otherwise.
fn parse_options<'a, I>(inner: &mut I) -> Result<HashMap<String, Value>, ParseError>
where
    I: Iterator<Item = Pair<'a, Rule>>,
{
    if let Some(p) = inner.next()
        && p.as_rule() == Rule::OPTIONS
    {
        return build_map_options(inner.next().unwrap());
    }
    Ok(HashMap::new())
}

fn build_create_vector_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // CREATE
    inner.next(); // VECTOR
    inner.next(); // INDEX

    let name = ident(&inner.next().unwrap());
    let if_not_exists = consume_if_present(&mut inner, Rule::if_not_exists);
    inner.next(); // FOR

    let label = parse_label_binding(&mut inner);
    inner.next(); // ON

    let property = extract_property_name(inner.next().unwrap());
    let options = parse_options(&mut inner)?;

    Ok(SchemaCommand::CreateVectorIndex(CreateVectorIndex {
        name,
        label,
        property,
        options,
        if_not_exists,
    }))
}

fn build_create_fulltext_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // CREATE
    inner.next(); // FULLTEXT
    inner.next(); // INDEX

    let name = ident(&inner.next().unwrap());
    let if_not_exists = consume_if_present(&mut inner, Rule::if_not_exists);
    inner.next(); // FOR

    let label = parse_label_binding(&mut inner);
    inner.next(); // ON

    consume_if_present(&mut inner, Rule::EACH);

    // Parse property_expression_list
    let prop_list = inner.next().unwrap();
    let properties = if prop_list.as_rule() == Rule::property_expression_list {
        prop_list.into_inner().map(extract_property_name).collect()
    } else {
        vec![prop_list.as_str().to_string()]
    };

    let options = parse_options(&mut inner)?;

    Ok(SchemaCommand::CreateFullTextIndex(CreateFullTextIndex {
        name,
        label,
        properties,
        options,
        if_not_exists,
    }))
}

fn build_create_scalar_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // CREATE
    inner.next(); // INDEX

    let if_not_exists = consume_if_present(&mut inner, Rule::if_not_exists);
    let name = ident(&inner.next().unwrap());

    // FOR clause: expect identifier ":" label
    // Parentheses are not separate tokens
    inner.next(); // FOR
    let _variable = ident(&inner.next().unwrap()); // variable name (e.g., "p")
    let label = normalize_identifier(inner.next().unwrap().as_str()); // label name (e.g., "Person")
    inner.next(); // ON

    // ON clause: index_expression_list
    let expr_list = inner.next().unwrap();
    let expressions: Vec<Expr> = expr_list
        .into_inner()
        .map(build_expression)
        .collect::<Result<Vec<_>, _>>()?;

    let mut where_clause = None;
    let mut options = HashMap::new();

    // The OPTIONS keyword carries no data; the following map_literal holds it.
    for p in inner {
        match p.as_rule() {
            Rule::where_clause => where_clause = Some(extract_where_expr(p)?),
            Rule::map_literal => {
                options = build_map_options(p)?;
            }
            _ => {}
        }
    }

    Ok(SchemaCommand::CreateScalarIndex(CreateScalarIndex {
        name,
        label,
        expressions,
        where_clause,
        options,
        if_not_exists,
    }))
}

fn build_create_json_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // CREATE
    inner.next(); // JSON
    inner.next(); // FULLTEXT
    inner.next(); // INDEX

    let name = ident(&inner.next().unwrap());
    let if_not_exists = consume_if_present(&mut inner, Rule::if_not_exists);
    inner.next(); // FOR

    let _variable = inner.next().unwrap(); // e.g., "a"
    let label = normalize_identifier(inner.next().unwrap().as_str());
    inner.next(); // ON
    let column = ident(&inner.next().unwrap());

    let options = parse_options(&mut inner)?;

    Ok(SchemaCommand::CreateJsonFtsIndex(CreateJsonFtsIndex {
        name,
        label,
        column,
        options,
        if_not_exists,
    }))
}

fn build_drop_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // DROP_KW
    inner.next(); // INDEX

    let if_exists = consume_if_present(&mut inner, Rule::if_exists);
    let name = ident(&inner.next().unwrap());

    Ok(SchemaCommand::DropIndex(DropIndex { name, if_exists }))
}

fn build_create_constraint(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // CREATE
    inner.next(); // CONSTRAINT

    let _if_not_exists = consume_if_present(&mut inner, Rule::if_not_exists);

    // Optional constraint name - check if next token is an identifier or ON
    let name = {
        let next_pair = inner.next().unwrap();
        if matches!(
            next_pair.as_rule(),
            Rule::identifier | Rule::identifier_or_keyword
        ) {
            inner.next(); // Skip ON
            Some(normalize_identifier(next_pair.as_str()))
        } else {
            None
        }
    };

    // The target is a node pattern `(n:Label)` or a relationship pattern
    // `()-[r:TYPE]-()`. Parens/colon/arrows are string literals (not tokens); the
    // pattern rule's inner tokens are [var, label-or-edge-type].
    let pattern = inner.next().unwrap();
    let on_relationship = matches!(pattern.as_rule(), Rule::constraint_rel_pattern);
    let mut pat_inner = pattern.into_inner();
    let var = ident(&pat_inner.next().unwrap());
    let label = normalize_identifier(pat_inner.next().unwrap().as_str());
    inner.next(); // ASSERT

    let assertion = inner.next().unwrap();
    let (constraint_type, properties, expression) = build_constraint_assertion(assertion, &var)?;

    Ok(SchemaCommand::CreateConstraint(CreateConstraint {
        name,
        constraint_type,
        label,
        properties,
        expression,
        on_relationship,
    }))
}

fn build_constraint_assertion(
    pair: Pair<Rule>,
    var: &str,
) -> Result<(ConstraintType, Vec<String>, Option<Expr>), ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    match first.as_rule() {
        Rule::EXISTS => {
            // EXISTS(n.prop) - parentheses are not separate tokens
            let prop_expr = inner.next().unwrap();
            let props = extract_property_names_from_expr(prop_expr, var)?;
            Ok((ConstraintType::Exists, props, None))
        }
        Rule::property_expr => {
            // n.prop IS UNIQUE or n.prop IS KEY
            let props = extract_property_names_from_expr(first, var)?;
            inner.next(); // IS
            let mut kw = inner.next().unwrap();
            if kw.as_rule() == Rule::NODE {
                kw = inner.next().unwrap();
            }
            let ctype = match kw.as_rule() {
                Rule::UNIQUE => ConstraintType::Unique,
                Rule::KEY => ConstraintType::NodeKey,
                _ => return Err(ParseError::new("Expected UNIQUE or KEY".to_string())),
            };
            Ok((ctype, props, None))
        }
        Rule::expression => {
            // CHECK constraint with arbitrary expression. Walk the AST
            // for property accesses rooted at `var` so downstream
            // constraint validators know which properties the predicate
            // touches. Previously this returned `vec![]` with a TODO,
            // which made the constraint's property dependency list
            // empty — silent for the parser, but downstream constraint
            // reasoning (e.g. delete-cascade impact analysis) treated
            // the constraint as if it referenced no columns.
            let expr = build_expression(first)?;
            let mut props = Vec::new();
            collect_property_refs_into(&expr, var, &mut props);
            // Deduplicate while preserving first-seen order so the
            // emitted list is stable for snapshot tests.
            let mut seen = std::collections::HashSet::new();
            props.retain(|p| seen.insert(p.clone()));
            Ok((ConstraintType::Check, props, Some(expr)))
        }
        Rule::identifier_list => {
            // Composite form `(a, b) IS [NODE|RELATIONSHIP] [UNIQUE|KEY]`. The
            // parentheses are silent literals, so `first` IS the identifier_list;
            // its inner pairs are the bare property names.
            let properties = first.into_inner().map(|p| ident(&p)).collect();
            inner.next(); // IS

            let mut next = inner.next().unwrap();
            if matches!(next.as_rule(), Rule::NODE | Rule::RELATIONSHIP) {
                next = inner.next().unwrap();
            }

            let ctype = if next.as_rule() == Rule::UNIQUE {
                ConstraintType::Unique
            } else {
                ConstraintType::NodeKey
            };

            Ok((ctype, properties, None))
        }
        other => Err(ParseError::new(format!(
            "Unexpected constraint assertion form: {other:?}"
        ))),
    }
}

fn extract_property_names_from_expr(
    pair: Pair<Rule>,
    expected_var: &str,
) -> Result<Vec<String>, ParseError> {
    let mut parts: Vec<String> = Vec::new();

    fn collect_identifiers(pair: Pair<Rule>, parts: &mut Vec<String>) {
        if matches!(
            pair.as_rule(),
            Rule::identifier | Rule::identifier_or_keyword
        ) {
            parts.push(ident(&pair));
        } else {
            for child in pair.into_inner() {
                collect_identifiers(child, parts);
            }
        }
    }

    collect_identifiers(pair, &mut parts);

    if parts.first().is_some_and(|p| p == expected_var) {
        parts.remove(0);
    }

    Ok(parts)
}

/// Collect every property name referenced on `expected_var` inside
/// `expr`, in source order.
///
/// Walks the [`Expr`] AST recursively, descending through binary ops,
/// function calls, lists, maps, case branches, quantifiers, and so on.
/// Only `Property(Variable(expected_var), name)` accesses contribute;
/// accesses rooted at other variables, parameters, or deeper nested
/// property chains (`n.foo.bar`) are not included (the immediate property
/// of `expected_var` in such a chain is still collected via recursion).
fn collect_property_refs_into(expr: &Expr, expected_var: &str, out: &mut Vec<String>) {
    match expr {
        Expr::Property(base, name) => {
            if let Expr::Variable(v) = base.as_ref()
                && v == expected_var
            {
                out.push(name.clone());
            }
            collect_property_refs_into(base, expected_var, out);
        }
        Expr::Quantifier { list, .. } => {
            // The bound variable inside a quantifier shadows `expected_var`;
            // we conservatively still descend into the list expression
            // (which is evaluated in the outer scope) but not the predicate.
            collect_property_refs_into(list, expected_var, out);
        }
        // Compound forms whose direct children are all evaluated in the same
        // scope: descend into each via `Expr::for_each_child`.
        Expr::List(_)
        | Expr::Map(_)
        | Expr::FunctionCall { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::Case { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsUnique(_)
        | Expr::In { .. }
        | Expr::ArrayIndex { .. }
        | Expr::ArraySlice { .. } => {
            expr.for_each_child(&mut |child| collect_property_refs_into(child, expected_var, out));
        }
        // Leaf forms and subquery forms (Exists/CountSubquery/CollectSubquery)
        // do not contribute simple property references on `expected_var`.
        _ => {}
    }
}

fn build_drop_constraint(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // DROP_KW
    inner.next(); // CONSTRAINT

    let if_exists = consume_if_present(&mut inner, Rule::if_exists);
    let name = ident(&inner.next().unwrap());

    Ok(SchemaCommand::DropConstraint(DropConstraint {
        name,
        if_exists,
    }))
}

fn build_create_label(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // CREATE
    inner.next(); // LABEL

    let if_not_exists = consume_if_present(&mut inner, Rule::if_not_exists);
    let name = normalize_identifier(inner.next().unwrap().as_str());

    let prop_defs = inner.next().unwrap();
    let properties = build_property_definitions(prop_defs)?;

    let description = if inner.peek().map(|p| p.as_rule()) == Some(Rule::description_clause) {
        Some(build_description_clause(inner.next().unwrap())?)
    } else {
        None
    };

    Ok(SchemaCommand::CreateLabel(CreateLabel {
        name,
        properties,
        if_not_exists,
        description,
    }))
}

fn build_create_edge_type(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // CREATE
    inner.next(); // EDGE
    inner.next(); // TYPE

    let if_not_exists = consume_if_present(&mut inner, Rule::if_not_exists);
    let name = normalize_identifier(inner.next().unwrap().as_str());

    let mut properties = vec![];

    // Next token could be property_definitions or FROM
    let next = inner.next().unwrap();
    if next.as_rule() == Rule::property_definitions {
        properties = build_property_definitions(next)?;
        inner.next(); // Skip FROM
    }
    // If next is FROM, we've already consumed it and don't need to skip

    let src_label = normalize_identifier(inner.next().unwrap().as_str());
    inner.next(); // TO
    let dst_label = normalize_identifier(inner.next().unwrap().as_str());

    let description = if inner.peek().map(|p| p.as_rule()) == Some(Rule::description_clause) {
        Some(build_description_clause(inner.next().unwrap())?)
    } else {
        None
    };

    Ok(SchemaCommand::CreateEdgeType(CreateEdgeType {
        name,
        src_labels: vec![src_label],
        dst_labels: vec![dst_label],
        properties,
        if_not_exists,
        description,
    }))
}

fn build_alter_label(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // ALTER
    inner.next(); // LABEL
    let name = normalize_identifier(inner.next().unwrap().as_str());
    let action_pair = inner.next().unwrap();
    let action = build_alter_action(action_pair)?;

    Ok(SchemaCommand::AlterLabel(AlterLabel { name, action }))
}

fn build_alter_edge_type(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // ALTER
    inner.next(); // EDGE
    inner.next(); // TYPE
    let name = normalize_identifier(inner.next().unwrap().as_str());
    let action_pair = inner.next().unwrap();
    let action = build_alter_action(action_pair)?;

    Ok(SchemaCommand::AlterEdgeType(AlterEdgeType { name, action }))
}

fn build_alter_action(pair: Pair<Rule>) -> Result<AlterAction, ParseError> {
    let mut inner = pair.into_inner().peekable();
    let first = inner.next().unwrap();

    match first.as_rule() {
        Rule::ADD => {
            inner.next(); // PROPERTY
            let prop_def = inner.next().unwrap();
            Ok(AlterAction::AddProperty(build_property_definition(
                prop_def,
            )?))
        }
        Rule::DROP_KW => {
            let second = inner.next().unwrap();
            match second.as_rule() {
                Rule::PROPERTY => {
                    let name = ident(&inner.next().unwrap());
                    // Check if followed by DESCRIPTION_KW (drop property description)
                    if inner.peek().map(|p| p.as_rule()) == Some(Rule::DESCRIPTION_KW) {
                        inner.next(); // consume DESCRIPTION_KW
                        Ok(AlterAction::SetPropertyDescription {
                            property: name,
                            description: None,
                        })
                    } else {
                        Ok(AlterAction::DropProperty(name))
                    }
                }
                Rule::DESCRIPTION_KW => Ok(AlterAction::SetDescription(None)),
                _ => unreachable!(),
            }
        }
        Rule::RENAME => {
            inner.next(); // PROPERTY
            let old_name = ident(&inner.next().unwrap());
            inner.next(); // TO
            let new_name = ident(&inner.next().unwrap());
            Ok(AlterAction::RenameProperty { old_name, new_name })
        }
        Rule::SET => {
            let second = inner.next().unwrap();
            match second.as_rule() {
                Rule::PROPERTY => {
                    let name = ident(&inner.next().unwrap());
                    inner.next(); // DESCRIPTION_KW
                    let string_pair = inner.next().unwrap();
                    let desc = build_string_literal(string_pair)?;
                    Ok(AlterAction::SetPropertyDescription {
                        property: name,
                        description: Some(desc),
                    })
                }
                Rule::DESCRIPTION_KW => {
                    let string_pair = inner.next().unwrap();
                    let desc = build_string_literal(string_pair)?;
                    Ok(AlterAction::SetDescription(Some(desc)))
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

fn build_drop_label(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // DROP_KW
    inner.next(); // LABEL
    let if_exists = consume_if_present(&mut inner, Rule::if_exists);
    let name = normalize_identifier(inner.next().unwrap().as_str());

    Ok(SchemaCommand::DropLabel(DropLabel { name, if_exists }))
}

fn build_drop_edge_type(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // DROP_KW
    inner.next(); // EDGE
    inner.next(); // TYPE
    let if_exists = consume_if_present(&mut inner, Rule::if_exists);
    let name = normalize_identifier(inner.next().unwrap().as_str());

    Ok(SchemaCommand::DropEdgeType(DropEdgeType {
        name,
        if_exists,
    }))
}

fn build_show_constraints(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // SHOW
    inner.next(); // CONSTRAINTS

    let target = inner.next().map(build_constraint_target).transpose()?;

    Ok(SchemaCommand::ShowConstraints(ShowConstraints { target }))
}

fn build_constraint_target(pair: Pair<Rule>) -> Result<ConstraintTarget, ParseError> {
    let mut inner = pair.into_inner().peekable();
    inner.next(); // ON

    let is_edge = consume_if_present(&mut inner, Rule::EDGE);
    // NOTE: the parentheses in `constraint_target` (cypher.pest) are bare
    // string literals, so pest does not surface them as inner pairs — only
    // `ON`, `EDGE` and the identifier are captured. Do not try to consume a
    // `(` here: doing so eats the identifier and leaves nothing to name the
    // target, which used to panic on the valid `SHOW CONSTRAINTS ON (Label)`.
    let name = inner
        .next()
        .map(|p| normalize_identifier(p.as_str()))
        .ok_or_else(|| {
            ParseError::new(
                "UnexpectedSyntax: SHOW CONSTRAINTS target is missing a name".to_string(),
            )
        })?;
    Ok(if is_edge {
        ConstraintTarget::EdgeType(name)
    } else {
        ConstraintTarget::Label(name)
    })
}

fn build_show_indexes(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // SHOW

    let filter = inner.next().and_then(|p| match p.as_rule() {
        Rule::VECTOR | Rule::FULLTEXT => Some(p.as_str().to_uppercase()),
        _ => None,
    });

    Ok(SchemaCommand::ShowIndexes(ShowIndexes { filter }))
}

fn build_backup_command(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // BACKUP
    inner.next(); // TO
    let path_pair = inner.next().unwrap();
    let path = build_string_literal(path_pair)?;

    Ok(SchemaCommand::Backup { path })
}

/// Parse WITH options clause if present.
fn parse_with_options(inner: &mut Pairs<Rule>) -> Result<HashMap<String, Value>, ParseError> {
    if let Some(p) = inner.next()
        && p.as_rule() == Rule::WITH
    {
        return build_map_options(inner.next().unwrap());
    }
    Ok(HashMap::new())
}

fn parse_copy_fields(
    pair: Pair<Rule>,
) -> Result<(String, String, String, HashMap<String, Value>), ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // COPY
    let label = normalize_identifier(inner.next().unwrap().as_str());
    inner.next(); // TO or FROM
    let path = build_string_literal(inner.next().unwrap())?;
    let options = parse_with_options(&mut inner)?;
    let format = detect_file_format(&path, &options);
    Ok((label, path, format, options))
}

fn build_copy_to(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let (label, path, format, options) = parse_copy_fields(pair)?;
    Ok(SchemaCommand::CopyTo(CopyToCommand {
        label,
        path,
        format,
        options,
    }))
}

fn build_copy_from(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let (label, path, format, options) = parse_copy_fields(pair)?;
    Ok(SchemaCommand::CopyFrom(CopyFromCommand {
        label,
        path,
        format,
        options,
    }))
}

// Helper functions for schema commands

/// Detect file format from options or file extension.
///
/// Priority:
/// 1. Explicit 'format' option in the WITH clause
/// 2. File extension (.csv, .parquet, etc.)
/// 3. Default to 'parquet' if neither is available
fn detect_file_format(path: &str, options: &HashMap<String, Value>) -> String {
    if let Some(format_value) = options.get("format")
        && let Some(format_str) = format_value.as_str()
    {
        return format_str.to_lowercase();
    }

    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    match ext.to_lowercase().as_str() {
        "csv" => "csv",
        "parquet" | "pq" => "parquet",
        _ => "parquet",
    }
    .to_string()
}

fn consume_if_present(inner: &mut std::iter::Peekable<Pairs<Rule>>, rule: Rule) -> bool {
    if peek_is(inner, rule) {
        inner.next();
        true
    } else {
        false
    }
}

fn build_property_definitions(pair: Pair<Rule>) -> Result<Vec<PropertyDefinition>, ParseError> {
    pair.into_inner().map(build_property_definition).collect()
}

fn build_property_definition(pair: Pair<Rule>) -> Result<PropertyDefinition, ParseError> {
    let mut inner = pair.into_inner();
    let name = ident(&inner.next().unwrap());
    let data_type = inner.next().unwrap().as_str().to_string();

    let mut nullable = true;
    let mut unique = false;
    let mut default = None;
    let mut description = None;

    for p in inner {
        match p.as_rule() {
            Rule::nullable_constraint => {
                let constraint_inner = p.into_inner().next().unwrap();
                if constraint_inner.as_rule() == Rule::NOT {
                    nullable = false;
                }
            }
            Rule::UNIQUE => unique = true,
            Rule::default_value => {
                default = Some(build_expression(p.into_inner().nth(1).unwrap())?);
            }
            Rule::description_clause => {
                description = Some(build_description_clause(p)?);
            }
            _ => {}
        }
    }

    Ok(PropertyDefinition {
        name,
        data_type,
        nullable,
        unique,
        default,
        description,
    })
}

/// Convert an expression to a JSON value for OPTIONS.
///
/// Supports:
/// - Literals: strings, numbers, booleans, null
/// - Maps: converted to Value::Map recursively
/// - Lists: converted to Value::List recursively
fn expr_to_value(expr: Expr) -> Result<Value, ParseError> {
    match expr {
        Expr::Literal(lit) => Ok(lit.to_value()),
        Expr::Map(entries) => {
            let map = entries
                .into_iter()
                .map(|(k, v)| Ok((k, expr_to_value(v)?)))
                .collect::<Result<_, ParseError>>()?;
            Ok(Value::Map(map))
        }
        Expr::List(items) => Ok(Value::List(
            items
                .into_iter()
                .map(expr_to_value)
                .collect::<Result<_, _>>()?,
        )),
        _ => Err(ParseError::new(format!(
            "OPTIONS values must be literals, maps, or lists. Got: {:?}",
            expr
        ))),
    }
}

fn build_map_options(pair: Pair<Rule>) -> Result<HashMap<String, Value>, ParseError> {
    let Expr::Map(entries) = build_map_literal(pair)? else {
        unreachable!("build_map_literal always returns Expr::Map");
    };
    entries
        .into_iter()
        .map(|(key, expr)| Ok((key, expr_to_value(expr)?)))
        .collect()
}

fn build_description_clause(pair: Pair<Rule>) -> Result<String, ParseError> {
    let s = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::string)
        .unwrap();
    build_string_literal(s)
}

pub(crate) fn build_string_literal(pair: Pair<Rule>) -> Result<String, ParseError> {
    if pair.as_rule() == Rule::string {
        let s = pair.as_str();
        let quote_char = s.chars().next().unwrap();
        let content = &s[1..s.len() - 1];
        unescape_string(content, quote_char)
    } else {
        Err(ParseError::new("Expected string literal".to_string()))
    }
}

#[cfg(test)]
mod show_constraints_tests {
    //! Regression cover for the `SHOW CONSTRAINTS ON (...)` target walker.
    //!
    //! The parentheses in `constraint_target` are bare string literals, so pest
    //! does not surface them as inner pairs. `build_constraint_target` used to
    //! consume one anyway, which ate the identifier and made the following
    //! `.unwrap()` panic on input the grammar accepts.

    use crate::ast::{ConstraintTarget, Query, SchemaCommand};

    fn show_constraints_target(q: &str) -> Option<ConstraintTarget> {
        match crate::parse(q).expect("grammar accepts this") {
            Query::Schema(cmd) => match *cmd {
                SchemaCommand::ShowConstraints(sc) => sc.target,
                other => panic!("not SHOW CONSTRAINTS: {other:?}"),
            },
            other => panic!("not a schema command: {other:?}"),
        }
    }

    #[test]
    fn show_constraints_on_label_does_not_panic() {
        assert_eq!(
            show_constraints_target("SHOW CONSTRAINTS ON (Paper)"),
            Some(ConstraintTarget::Label("Paper".to_string()))
        );
    }

    #[test]
    fn show_constraints_on_edge_type() {
        assert_eq!(
            show_constraints_target("SHOW CONSTRAINTS ON EDGE (CITES)"),
            Some(ConstraintTarget::EdgeType("CITES".to_string()))
        );
    }

    #[test]
    fn show_constraints_without_target() {
        assert_eq!(show_constraints_target("SHOW CONSTRAINTS"), None);
    }
}
