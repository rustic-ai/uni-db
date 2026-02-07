use super::ParseError;
use super::Rule;
use crate::ast::*;
use pest::iterators::{Pair, Pairs};
use serde_json::Value;

// ═══════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

/// Strip backticks from identifier_or_keyword tokens
/// Converts `Order` -> Order, `my-var` -> my-var, plain -> plain
fn normalize_identifier(s: &str) -> String {
    if s.starts_with('`') && s.ends_with('`') && s.len() > 1 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// QUERY BUILDING
// ═══════════════════════════════════════════════════════════════════════════

pub fn build_query(pairs: Pairs<Rule>) -> Result<Query, ParseError> {
    let pair = pairs.into_iter().next().unwrap();

    match pair.as_rule() {
        Rule::query => {
            let mut inner = pair.into_inner();
            let query_pair = inner.next().unwrap();
            let query = build_union_query(query_pair)?;

            // Check for trailing time_travel_clause
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
        _ => unreachable!("Expected query rule, got {:?}", pair.as_rule()),
    }
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
        Rule::transaction_command => Ok(Query::Transaction(build_transaction_command(inner)?)),
        _ => unreachable!("Unexpected single_query rule: {:?}", inner.as_rule()),
    }
}

fn build_statement(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let clauses = pair
        .into_inner()
        .map(build_clause)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Statement { clauses })
}

fn build_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let inner = pair.into_inner().next().unwrap();

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
        Rule::load_csv_clause => build_load_csv_clause(inner),
        _ => unreachable!("Unexpected clause: {:?}", inner.as_rule()),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CLAUSE IMPLEMENTATIONS
// ═══════════════════════════════════════════════════════════════════════════

fn build_match_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner().peekable();

    let optional = inner
        .peek()
        .map(|p| p.as_rule() == Rule::OPTIONAL)
        .unwrap_or(false);
    if optional {
        inner.next();
    }

    inner.next(); // MATCH

    let pattern = build_pattern(inner.next().unwrap())?;

    let where_clause = if let Some(p) = inner.next() {
        let mut where_inner = p.into_inner();
        where_inner.next(); // Skip WHERE keyword
        Some(build_expression(where_inner.next().unwrap())?)
    } else {
        None
    };

    Ok(Clause::Match(MatchClause {
        optional,
        pattern,
        where_clause,
    }))
}

fn build_create_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    let pattern = build_pattern(inner.next().unwrap())?;
    Ok(Clause::Create(CreateClause { pattern }))
}

fn build_return_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // RETURN

    let mut distinct = false;
    if let Some(p) = inner.clone().next()
        && p.as_rule() == Rule::DISTINCT
    {
        distinct = true;
        inner.next();
    }

    let items = build_return_items(inner.next().unwrap())?;

    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;

    for p in inner {
        match p.as_rule() {
            Rule::order_clause => {
                order_by = Some(build_sort_items(p.into_inner().nth(2).unwrap())?)
            }
            Rule::skip_clause => skip = Some(build_expression(p.into_inner().nth(1).unwrap())?),
            Rule::limit_clause => limit = Some(build_expression(p.into_inner().nth(1).unwrap())?),
            _ => {}
        }
    }

    Ok(Clause::Return(ReturnClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
    }))
}

fn build_with_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // WITH

    let mut distinct = false;
    if let Some(p) = inner.clone().next()
        && p.as_rule() == Rule::DISTINCT
    {
        distinct = true;
        inner.next();
    }

    let items = build_return_items(inner.next().unwrap())?;

    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;
    let mut where_clause = None;

    for p in inner {
        match p.as_rule() {
            Rule::order_clause => {
                order_by = Some(build_sort_items(p.into_inner().nth(2).unwrap())?)
            }
            Rule::skip_clause => skip = Some(build_expression(p.into_inner().nth(1).unwrap())?),
            Rule::limit_clause => limit = Some(build_expression(p.into_inner().nth(1).unwrap())?),
            Rule::where_clause => {
                let mut wc_inner = p.into_inner();
                wc_inner.next(); // Skip WHERE keyword
                where_clause = Some(build_expression(wc_inner.next().unwrap())?)
            }
            _ => {}
        }
    }

    Ok(Clause::With(WithClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
        where_clause,
    }))
}

fn build_with_recursive_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // WITH
    inner.next(); // RECURSIVE
    let name = inner.next().unwrap().as_str().to_string();
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
    let variable = inner.next().unwrap().as_str().to_string();
    Ok(Clause::Unwind(UnwindClause { expr, variable }))
}

fn build_delete_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner().peekable();

    let detach = if inner
        .peek()
        .map(|p| p.as_rule() == Rule::DETACH)
        .unwrap_or(false)
    {
        inner.next();
        true
    } else {
        false
    };

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
                        variable: first.as_str().to_string(),
                        value: expr,
                    })
                }
                Rule::node_labels => {
                    let labels = next
                        .into_inner()
                        .map(|l| normalize_identifier(l.as_str()))
                        .collect();
                    Ok(SetItem::Labels {
                        variable: first.as_str().to_string(),
                        labels,
                    })
                }
                Rule::eq => {
                    let expr = build_expression(inner.next().unwrap())?;
                    Ok(SetItem::Variable {
                        variable: first.as_str().to_string(),
                        value: expr,
                    })
                }
                _ => unreachable!(),
            }
        }
        Rule::property_expr => {
            let mut p_inner = first.into_inner();
            let first_token = p_inner.next().unwrap();
            // Check if it's parenthesized identifier: (id) or just id
            let var = if first_token.as_str() == "(" {
                // Parenthesized: skip "(" and get identifier
                p_inner.next().unwrap().as_str().to_string()
                // Will also need to skip ")" but it's consumed automatically
            } else {
                first_token.as_str().to_string()
            };
            let mut expr = Expr::Variable(var);
            for p in p_inner {
                // Skip closing paren if present
                if p.as_str() != ")" {
                    expr = Expr::Property(Box::new(expr), p.as_str().to_string());
                }
            }

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
        Rule::property_expr => {
            let mut p_inner = first.into_inner();
            let first_token = p_inner.next().unwrap();
            // Check if it's parenthesized identifier: (id) or just id
            let var = if first_token.as_str() == "(" {
                // Parenthesized: skip "(" and get identifier
                p_inner.next().unwrap().as_str().to_string()
            } else {
                first_token.as_str().to_string()
            };
            let mut expr = Expr::Variable(var);
            for p in p_inner {
                // Skip closing paren if present
                if p.as_str() != ")" {
                    expr = Expr::Property(Box::new(expr), p.as_str().to_string());
                }
            }
            Ok(RemoveItem::Property(expr))
        }
        Rule::identifier => {
            let id = first.as_str().to_string();
            let labels_pair = inner.next().unwrap();
            let labels = labels_pair
                .into_inner()
                .map(|l| l.as_str().to_string())
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
            // Procedure variant: CALL qualified_name (...)
            let procedure = target.as_str().to_string();
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
                        match yield_items_pair.as_rule() {
                            Rule::yield_items => {
                                let mut yi_inner = yield_items_pair.into_inner();
                                let first = yi_inner.next().unwrap();
                                if first.as_rule() == Rule::star {
                                    // YIELD * - empty yield items means all
                                    yield_items = vec![];
                                } else {
                                    // Regular yield items
                                    yield_items.push(build_yield_item(first)?);
                                    for yi in yi_inner {
                                        yield_items.push(build_yield_item(yi)?);
                                    }
                                }
                            }
                            _ => unreachable!(
                                "Unexpected rule in yield_clause: {:?}",
                                yield_items_pair.as_rule()
                            ),
                        }

                        // Check for optional WHERE clause after yield items
                        if let Some(where_pair) = y_inner.next() {
                            let mut where_inner = where_pair.into_inner();
                            where_inner.next(); // Skip WHERE keyword
                            where_clause = Some(build_expression(where_inner.next().unwrap())?);
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
    let name = inner.next().unwrap().as_str().to_string();
    let alias = if inner.next().is_some() {
        // AS
        Some(inner.next().unwrap().as_str().to_string())
    } else {
        None
    };
    Ok(YieldItem { name, alias })
}

fn build_load_csv_clause(pair: Pair<Rule>) -> Result<Clause, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // LOAD
    inner.next(); // CSV

    let mut with_headers = false;
    let next = inner.peek().unwrap();
    if next.as_rule() == Rule::WITH {
        inner.next();
        inner.next(); // HEADERS
        with_headers = true;
    }

    inner.next(); // FROM
    let url_expr = build_expression(inner.next().unwrap())?;
    let url = match url_expr {
        Expr::Literal(CypherLiteral::String(s)) => s,
        _ => {
            return Err(ParseError::new(
                "LOAD CSV URL must be a string literal".to_string(),
            ));
        }
    };

    inner.next(); // AS
    let variable = inner.next().unwrap().as_str().to_string();

    let mut field_terminator = None;
    if inner.next().is_some() {
        // FIELDTERMINATOR
        let s = inner.next().unwrap().as_str();
        let clean = &s[1..s.len() - 1];
        if clean.len() != 1 {
            return Err(ParseError::new(
                "FIELDTERMINATOR must be a single character".to_string(),
            ));
        }
        field_terminator = Some(clean.chars().next().unwrap());
    }

    Ok(Clause::LoadCsv(LoadCsvClause {
        url,
        variable,
        with_headers,
        field_terminator,
    }))
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPRESSIONS
// ═══════════════════════════════════════════════════════════════════════════

pub fn build_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    match pair.as_rule() {
        Rule::expression => build_expression(pair.into_inner().next().unwrap()),
        Rule::comprehension_source => {
            // comprehension_source wraps comparison_expression
            build_expression(pair.into_inner().next().unwrap())
        }
        Rule::or_expression => build_binary_left_assoc(pair, BinaryOp::Or),
        Rule::xor_expression => build_binary_left_assoc(pair, BinaryOp::Xor),
        Rule::and_expression => build_binary_left_assoc(pair, BinaryOp::And),
        Rule::not_expression => build_not_expression(pair),
        Rule::comparison_expression => build_comparison_expression(pair),
        Rule::additive_expression => build_additive_expression(pair),
        Rule::multiplicative_expression => build_multiplicative_expression(pair),
        Rule::power_expression => build_binary_right_assoc(pair, BinaryOp::Pow),
        Rule::unary_expression => build_unary_expression(pair),
        Rule::postfix_expression => build_postfix_expression(pair),
        Rule::primary_expression => build_primary_expression(pair),
        Rule::literal => build_literal(pair),
        Rule::identifier => Ok(Expr::Variable(pair.as_str().to_string())),
        Rule::parameter => Ok(Expr::Parameter(pair.as_str()[1..].to_string())),
        _ => unreachable!("Unexpected expression rule: {:?}", pair.as_rule()),
    }
}

// Helpers for binary ops
fn build_binary_left_assoc(pair: Pair<Rule>, op: BinaryOp) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let mut left = build_expression(inner.next().unwrap())?;
    while let Some(_) = inner.next() {
        let right = build_expression(inner.next().unwrap())?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn build_binary_right_assoc(pair: Pair<Rule>, op: BinaryOp) -> Result<Expr, ParseError> {
    let inner: Vec<_> = pair.into_inner().collect();
    if inner.len() == 1 {
        return build_expression(inner.into_iter().next().unwrap());
    }
    let mut iter = inner.into_iter().step_by(2).rev();
    let mut right = build_expression(iter.next().unwrap())?;
    for left_pair in iter {
        let left = build_expression(left_pair)?;
        right = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(right)
}

fn build_additive_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let mut left = build_expression(inner.next().unwrap())?;
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_rule() {
            Rule::plus => BinaryOp::Add,
            Rule::minus => BinaryOp::Sub,
            _ => unreachable!(),
        };
        let right = build_expression(inner.next().unwrap())?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn build_multiplicative_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let mut left = build_expression(inner.next().unwrap())?;
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_rule() {
            Rule::star => BinaryOp::Mul,
            Rule::slash => BinaryOp::Div,
            Rule::percent => BinaryOp::Mod,
            _ => unreachable!(),
        };
        let right = build_expression(inner.next().unwrap())?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn build_not_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner().peekable();
    let mut not_count = 0;
    while inner
        .peek()
        .map(|p| p.as_rule() == Rule::NOT)
        .unwrap_or(false)
    {
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
    let mut expr = build_expression(inner.next().unwrap())?;

    // Process all comparison tails (supports chaining like "a IS NULL <> b IS NULL")
    for tail in inner {
        expr = build_comparison_with_tail(expr, tail)?;
    }

    Ok(expr)
}

fn build_comparison_with_tail(left: Expr, tail: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut tail_inner = tail.into_inner();
    let op_pair = tail_inner.next().unwrap();
    match op_pair.as_rule() {
        Rule::IS => {
            let next = tail_inner.next().unwrap();
            match next.as_rule() {
                Rule::NULL => Ok(Expr::IsNull(Box::new(left))),
                Rule::NOT => Ok(Expr::IsNotNull(Box::new(left))),
                Rule::UNIQUE => Ok(Expr::IsUnique(Box::new(left))),
                _ => {
                    // IS :Label - check if variable has label
                    // Skip the colon, get the label name
                    let label = tail_inner.next().unwrap().as_str().to_string();
                    // Convert to function call: hasLabel(left, 'label')
                    Ok(Expr::FunctionCall {
                        name: "hasLabel".to_string(),
                        args: vec![left, Expr::Literal(CypherLiteral::String(label))],
                        distinct: false,
                        window_spec: None,
                    })
                }
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
            tail_inner.next();
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::StartsWith,
                right: Box::new(build_expression(tail_inner.next().unwrap())?),
            })
        }
        Rule::ENDS => {
            tail_inner.next();
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
            // VALID_AT has two forms:
            // 1. Simple: e VALID_AT timestamp
            // 2. Custom: e VALID_AT(timestamp, 'start_prop', 'end_prop')
            //
            // Pest only includes named rules in the iterator, not literals like "(" or ","
            // So we get: [expression] or [expression, string, string]

            let timestamp = build_expression(tail_inner.next().unwrap())?;

            // Check if there are additional string pairs (custom property names)
            let start_prop = if let Some(start_pair) = tail_inner.next() {
                if start_pair.as_rule() == Rule::string {
                    let s = start_pair.as_str();
                    let _quote_char = s.chars().next().unwrap();
                    let content = &s[1..s.len() - 1];
                    Some(content.to_string())
                } else {
                    return Err(ParseError::new(format!(
                        "Expected string for VALID_AT start property, got {:?}",
                        start_pair.as_rule()
                    )));
                }
            } else {
                None
            };

            let end_prop = if let Some(end_pair) = tail_inner.next() {
                if end_pair.as_rule() == Rule::string {
                    let s = end_pair.as_str();
                    let _quote_char = s.chars().next().unwrap();
                    let content = &s[1..s.len() - 1];
                    Some(content.to_string())
                } else {
                    return Err(ParseError::new(format!(
                        "Expected string for VALID_AT end property, got {:?}",
                        end_pair.as_rule()
                    )));
                }
            } else {
                None
            };

            Ok(Expr::ValidAt {
                entity: Box::new(left),
                timestamp: Box::new(timestamp),
                start_prop,
                end_prop,
            })
        }
        Rule::identifier_or_keyword => {
            // Bare :Label predicate (without IS)
            let label = normalize_identifier(op_pair.as_str());
            Ok(Expr::FunctionCall {
                name: "hasLabel".to_string(),
                args: vec![left, Expr::Literal(CypherLiteral::String(label))],
                distinct: false,
                window_spec: None,
            })
        }
        _ => {
            let op = match op_pair.as_rule() {
                Rule::eq => BinaryOp::Eq,
                Rule::not_eq => BinaryOp::NotEq,
                Rule::lt => BinaryOp::Lt,
                Rule::gt => BinaryOp::Gt,
                Rule::lt_eq => BinaryOp::LtEq,
                Rule::gt_eq => BinaryOp::GtEq,
                Rule::approx_eq => BinaryOp::ApproxEq,
                _ => unreachable!(),
            };
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(build_expression(tail_inner.next().unwrap())?),
            })
        }
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
    Expr::UnaryOp {
        op: UnaryOp::Neg,
        expr: Box::new(expr),
    }
}

fn build_unary_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner().peekable();
    let neg = if inner
        .peek()
        .map(|p| p.as_rule() == Rule::minus)
        .unwrap_or(false)
    {
        inner.next();
        true
    } else {
        false
    };
    let mut expr = build_expression(inner.next().unwrap())?;
    if neg {
        expr = negate_expression(expr);
    } else {
        // Validation: If we have a positive integer literal that equals i64::MIN,
        // it means we parsed a magnitude of i64::MAX + 1 (9223372036854775808).
        // Since it wasn't negated, this is a positive overflow.
        if let Expr::Literal(CypherLiteral::Integer(i64::MIN)) = expr {
            return Err(ParseError::new("Integer overflow".to_string()));
        }
    }
    Ok(expr)
}

fn build_postfix_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let mut expr = build_expression(inner.next().unwrap())?;
    for suffix in inner {
        expr = apply_postfix_suffix(expr, suffix)?;
    }
    Ok(expr)
}

fn apply_postfix_suffix(base: Expr, suffix: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = suffix.into_inner();

    // Handle empty function calls like rand()
    let Some(first) = inner.next() else {
        // Empty postfix suffix means function call with no arguments
        let name = match &base {
            Expr::Variable(n) => n.clone(),
            Expr::Property(..) => {
                fn extract_dotted(e: &Expr) -> Option<String> {
                    match e {
                        Expr::Variable(n) => Some(n.clone()),
                        Expr::Property(b, p) => extract_dotted(b).map(|s| format!("{}.{}", s, p)),
                        _ => None,
                    }
                }
                extract_dotted(&base)
                    .ok_or_else(|| ParseError::new(format!("Invalid call base: {:?}", base)))?
            }
            _ => return Err(ParseError::new(format!("Invalid call base: {:?}", base))),
        };
        return Ok(Expr::FunctionCall {
            name,
            args: vec![],
            distinct: false,
            window_spec: None,
        });
    };

    // Check for DISTINCT in function calls
    let (distinct, first) = if first.as_rule() == Rule::DISTINCT {
        // DISTINCT present, get next pair which should be expression_list or window_spec
        (true, inner.next())
    } else {
        (false, Some(first))
    };

    let Some(first) = first else {
        // DISTINCT with no args: func(DISTINCT) - treat as empty call with distinct flag
        let name = match &base {
            Expr::Variable(n) => n.clone(),
            Expr::Property(..) => {
                fn extract_dotted(e: &Expr) -> Option<String> {
                    match e {
                        Expr::Variable(n) => Some(n.clone()),
                        Expr::Property(b, p) => extract_dotted(b).map(|s| format!("{}.{}", s, p)),
                        _ => None,
                    }
                }
                extract_dotted(&base)
                    .ok_or_else(|| ParseError::new(format!("Invalid call base: {:?}", base)))?
            }
            _ => return Err(ParseError::new(format!("Invalid call base: {:?}", base))),
        };
        return Ok(Expr::FunctionCall {
            name,
            args: vec![],
            distinct,
            window_spec: None,
        });
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
            let name = match &base {
                Expr::Variable(n) => n.clone(),
                Expr::Property(..) => {
                    fn extract_dotted(e: &Expr) -> Option<String> {
                        match e {
                            Expr::Variable(n) => Some(n.clone()),
                            Expr::Property(b, p) => {
                                extract_dotted(b).map(|s| format!("{}.{}", s, p))
                            }
                            _ => None,
                        }
                    }
                    extract_dotted(&base)
                        .ok_or_else(|| ParseError::new(format!("Invalid call base: {:?}", base)))?
                }
                _ => return Err(ParseError::new(format!("Invalid call base: {:?}", base))),
            };
            Ok(Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            })
        }
        Rule::map_projection_items => {
            let items = build_map_projection_items(first)?;
            Ok(Expr::MapProjection {
                base: Box::new(base),
                items,
            })
        }
        Rule::window_spec => {
            // Function call with DISTINCT but no args, just window spec
            let window_spec = Some(build_window_spec(first)?);
            let name = match &base {
                Expr::Variable(n) => n.clone(),
                Expr::Property(..) => {
                    fn extract_dotted(e: &Expr) -> Option<String> {
                        match e {
                            Expr::Variable(n) => Some(n.clone()),
                            Expr::Property(b, p) => {
                                extract_dotted(b).map(|s| format!("{}.{}", s, p))
                            }
                            _ => None,
                        }
                    }
                    extract_dotted(&base)
                        .ok_or_else(|| ParseError::new(format!("Invalid call base: {:?}", base)))?
                }
                _ => return Err(ParseError::new(format!("Invalid call base: {:?}", base))),
            };
            Ok(Expr::FunctionCall {
                name,
                args: vec![],
                distinct,
                window_spec,
            })
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
            Ok(Expr::Variable(first.as_str().to_string()))
        }
        Rule::identifier => Ok(Expr::Variable(first.as_str().to_string())),
        Rule::parameter => Ok(Expr::Parameter(first.as_str()[1..].to_string())),
        Rule::case_expression => build_case_expression(first),
        Rule::list_expression => build_list_expression(first),
        Rule::map_literal => build_map_literal(first),
        Rule::expression => build_expression(first),
        Rule::count_subquery => {
            let inner = first.into_inner().nth(1).unwrap();
            let query = match inner.as_rule() {
                Rule::statement => Query::Single(build_statement(inner)?),
                Rule::pattern => {
                    // Wrap bare pattern in MATCH clause
                    let pattern = build_pattern(inner)?;
                    let match_clause = Clause::Match(MatchClause {
                        optional: false,
                        pattern,
                        where_clause: None,
                    });
                    Query::Single(Statement {
                        clauses: vec![match_clause],
                    })
                }
                _ => unreachable!("Unexpected rule in count_subquery: {:?}", inner.as_rule()),
            };
            Ok(Expr::CountSubquery(Box::new(query)))
        }
        Rule::collect_subquery => {
            let stmt = build_statement(first.into_inner().nth(1).unwrap())?;
            Ok(Expr::CollectSubquery(Box::new(Query::Single(stmt))))
        }
        Rule::exists_expression => {
            let content = first.into_inner().nth(1).unwrap(); // Get exists_subquery_content
            let mut content_inner = content.into_inner();
            let first_item = content_inner.next().unwrap();

            let query = match first_item.as_rule() {
                Rule::statement => Query::Single(build_statement(first_item)?),
                Rule::pattern => {
                    // Pattern with optional WHERE clause
                    let pattern = build_pattern(first_item)?;
                    // Check for optional WHERE clause
                    let where_clause = if let Some(where_pair) = content_inner.next() {
                        if where_pair.as_rule() == Rule::where_clause {
                            Some(build_expression(where_pair.into_inner().nth(1).unwrap())?)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    let match_clause = Clause::Match(MatchClause {
                        optional: false,
                        pattern,
                        where_clause,
                    });
                    Query::Single(Statement {
                        clauses: vec![match_clause],
                    })
                }
                _ => unreachable!(
                    "Unexpected rule in exists_subquery_content: {:?}",
                    first_item.as_rule()
                ),
            };
            Ok(Expr::Exists(Box::new(query)))
        }
        Rule::quantifier_expression => build_quantifier_expression(first),
        Rule::reduce_expression => build_reduce_expression(first),
        Rule::pattern_expression => {
            // Pattern as boolean: (n)-->(m) is equivalent to EXISTS { MATCH (n)-->(m) }
            let mut elements = vec![];
            for child in first.into_inner() {
                match child.as_rule() {
                    Rule::node_pattern => {
                        elements.push(PatternElement::Node(build_node_pattern(child)?))
                    }
                    Rule::relationship_pattern => elements.push(PatternElement::Relationship(
                        build_relationship_pattern(child)?,
                    )),
                    _ => unreachable!("Unexpected child in pattern_expression"),
                }
            }
            let path = PathPattern {
                variable: None,
                elements,
                shortest_path_mode: None,
            };
            let pattern = Pattern { paths: vec![path] };

            // Convert to EXISTS { MATCH pattern }
            let match_clause = Clause::Match(MatchClause {
                optional: false,
                pattern,
                where_clause: None,
            });
            let statement = Statement {
                clauses: vec![match_clause],
            };
            Ok(Expr::Exists(Box::new(Query::Single(statement))))
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
                name: "count".to_string(),
                args,
                distinct,
                window_spec,
            })
        }
        Rule::EXISTS => {
            let list = inner.next().unwrap();
            Ok(Expr::FunctionCall {
                name: "exists".to_string(),
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
                Some('u') => {
                    // 4-digit unicode: \uXXXX
                    let hex: String = chars.by_ref().take(4).collect();
                    if hex.len() != 4 {
                        return Err(ParseError::new(format!("Invalid \\u escape: \\u{}", hex)));
                    }
                    let code = u32::from_str_radix(&hex, 16)
                        .map_err(|_| ParseError::new(format!("Invalid hex in \\u{}", hex)))?;
                    result.push(
                        char::from_u32(code).ok_or_else(|| {
                            ParseError::new(format!("Invalid unicode \\u{}", hex))
                        })?,
                    );
                }
                Some('U') => {
                    // 6-digit unicode: \UXXXXXX
                    let hex: String = chars.by_ref().take(6).collect();
                    if hex.len() != 6 {
                        return Err(ParseError::new(format!("Invalid \\U escape: \\U{}", hex)));
                    }
                    let code = u32::from_str_radix(&hex, 16)
                        .map_err(|_| ParseError::new(format!("Invalid hex in \\U{}", hex)))?;
                    result.push(
                        char::from_u32(code).ok_or_else(|| {
                            ParseError::new(format!("Invalid unicode \\U{}", hex))
                        })?,
                    );
                }
                Some(other) => {
                    // For regex compatibility, preserve backslash + character literally
                    // This allows regex patterns like \. \d \w etc. in string literals
                    result.push('\\');
                    result.push(other);
                }
                None => {
                    return Err(ParseError::new(
                        "Unexpected end after backslash".to_string(),
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

/// Outcome of parsing an integer literal that may overflow `i64`.
enum IntegerParseResult {
    /// Value fits in a signed 64-bit integer (or is the sentinel `i64::MIN` for `MAX + 1`).
    Integer(i64),
}

/// Parse an unsigned integer string in the given radix with overflow handling.
///
/// Values that fit in `i64` are returned directly. The boundary value
/// `i64::MAX + 1` is stored as `i64::MIN` so that [`negate_expression`]
/// can produce the correct result for `-9223372036854775808`. Larger
/// values result in an error.
fn parse_integer_safe(s: &str, radix: u32) -> Result<IntegerParseResult, ParseError> {
    if let Ok(val) = i64::from_str_radix(s, radix) {
        return Ok(IntegerParseResult::Integer(val));
    }

    let magnitude = u64::from_str_radix(s, radix)
        .map_err(|e| ParseError::new(format!("Invalid integer: {}", e)))?;

    if magnitude <= i64::MAX as u64 {
        Ok(IntegerParseResult::Integer(magnitude as i64))
    } else if magnitude == (i64::MAX as u64 + 1) {
        Ok(IntegerParseResult::Integer(i64::MIN))
    } else {
        Err(ParseError::new("Integer overflow".to_string()))
    }
}

fn build_literal(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::integer => {
            let s = inner.as_str();
            let s_clean = s.replace('_', ""); // Remove underscores
            let result = if s_clean.starts_with("0x") || s_clean.starts_with("0X") {
                // Hexadecimal
                parse_integer_safe(&s_clean[2..], 16)?
            } else if s_clean.starts_with("0o") || s_clean.starts_with("0O") {
                // Octal
                parse_integer_safe(&s_clean[2..], 8)?
            } else {
                // Decimal
                parse_integer_safe(&s_clean, 10)?
            };
            match result {
                IntegerParseResult::Integer(value) => {
                    Ok(Expr::Literal(CypherLiteral::Integer(value)))
                }
            }
        }
        Rule::float => {
            let s = inner.as_str().replace('_', ""); // Remove underscores
            let value = s
                .parse::<f64>()
                .map_err(|e| ParseError::new(format!("Invalid float: {}", e)))?;
            Ok(Expr::Literal(CypherLiteral::Float(value)))
        }
        Rule::infinity => {
            let s = inner.as_str();
            let value = if s.starts_with('-') {
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
    let first = inner.next();
    if first.is_none() {
        return Ok(Expr::List(vec![]));
    }
    let first = first.unwrap();

    match first.as_rule() {
        Rule::pattern_comprehension_body => {
            let mut p_inner = first.into_inner().peekable();
            let mut variable = None;
            if p_inner
                .peek()
                .map(|p| p.as_rule() == Rule::identifier)
                .unwrap_or(false)
            {
                variable = Some(p_inner.next().unwrap().as_str().to_string());
            }
            let pattern_pair = p_inner.next().unwrap();
            let path = build_path_pattern(pattern_pair)?;
            let pattern = Pattern { paths: vec![path] };

            let where_clause = if p_inner
                .peek()
                .map(|p| p.as_rule() == Rule::WHERE)
                .unwrap_or(false)
            {
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
            let var = l_inner.next().unwrap().as_str().to_string();
            l_inner.next(); // IN
            let list = build_expression(l_inner.next().unwrap())?;

            let mut where_clause = None;
            let mut map_expr = None;

            // Process optional WHERE clause
            if let Some(next) = l_inner.next() {
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

fn build_case_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CASE
    let mut expr = None;
    let next = inner.peek().unwrap();
    if next.as_rule() == Rule::expression {
        expr = Some(Box::new(build_expression(inner.next().unwrap())?));
    }

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
            Rule::END => {}
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

    let variable = inner.next().unwrap().as_str().to_string();
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
    let accumulator = inner.next().unwrap().as_str().to_string();
    inner.next(); // =
    let init = build_expression(inner.next().unwrap())?;
    // , skipped
    let variable = inner.next().unwrap().as_str().to_string();
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
                Rule::string => {
                    // Strip quotes from string literals
                    let s = key_pair.as_str();
                    let content = &s[1..s.len() - 1];
                    content.to_string()
                }
                _ => key_pair.as_str().to_string(), // identifier_or_keyword
            };
            let val = build_expression(inner.next().unwrap())?;
            Ok((key, val))
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(Expr::Map(entries))
}

// ═══════════════════════════════════════════════════════════════════════════
// PATTERNS
// ═══════════════════════════════════════════════════════════════════════════

fn build_pattern(pair: Pair<Rule>) -> Result<Pattern, ParseError> {
    let paths = pair
        .into_inner()
        .map(build_path_pattern)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Pattern { paths })
}

fn build_path_pattern(pair: Pair<Rule>) -> Result<PathPattern, ParseError> {
    let mut inner = pair.into_inner().peekable();
    let mut variable = None;

    // Check for optional variable assignment
    if inner
        .peek()
        .map(|p| p.as_rule() == Rule::identifier)
        .unwrap_or(false)
    {
        variable = Some(inner.next().unwrap().as_str().to_string());
    }

    let mut shortest_path_mode = None;
    let mut elements = vec![];

    // Get the next element - either shortest_path_pattern or pattern_element+
    let next = inner.next().unwrap();

    if next.as_rule() == Rule::shortest_path_pattern {
        // Parse shortest path pattern
        let matched_text = next.as_str();

        // Determine mode from the matched text (function name is not a child node)
        let matched_text_lower = matched_text.to_lowercase();

        shortest_path_mode = Some(if matched_text_lower.starts_with("allshortest") {
            ShortestPathMode::AllShortest
        } else {
            ShortestPathMode::Shortest
        });

        // Parse pattern elements inside the shortestPath()
        for elem in next.into_inner() {
            if elem.as_rule() == Rule::pattern_element {
                let mut elem_inner_iter = elem.into_inner();
                let first_child = elem_inner_iter.next().unwrap();

                match first_child.as_rule() {
                    Rule::parenthesized_pattern => {
                        elements.push(build_parenthesized_pattern(first_child)?);
                    }
                    Rule::node_pattern => {
                        // Node pattern followed by relationships
                        elements.push(PatternElement::Node(build_node_pattern(first_child)?));
                        // Continue processing remaining elements in pattern_element
                        for child in elem_inner_iter {
                            match child.as_rule() {
                                Rule::relationship_pattern => {
                                    elements.push(PatternElement::Relationship(
                                        build_relationship_pattern(child)?,
                                    ))
                                }
                                Rule::node_pattern => {
                                    elements.push(PatternElement::Node(build_node_pattern(child)?))
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => unreachable!(
                        "Unexpected rule in pattern_element: {:?}",
                        first_child.as_rule()
                    ),
                }
            }
        }
    } else {
        // Regular pattern_element+
        let mut process_pattern_element = |p: Pair<Rule>| -> Result<(), ParseError> {
            if p.as_rule() == Rule::pattern_element {
                let mut elem_inner_iter = p.into_inner();
                let first_child = elem_inner_iter.next().unwrap();

                match first_child.as_rule() {
                    Rule::parenthesized_pattern => {
                        elements.push(build_parenthesized_pattern(first_child)?);
                    }
                    Rule::node_pattern => {
                        // Node pattern followed by relationships
                        elements.push(PatternElement::Node(build_node_pattern(first_child)?));
                        // Continue processing remaining elements in pattern_element
                        for child in elem_inner_iter {
                            match child.as_rule() {
                                Rule::relationship_pattern => {
                                    elements.push(PatternElement::Relationship(
                                        build_relationship_pattern(child)?,
                                    ))
                                }
                                Rule::node_pattern => {
                                    elements.push(PatternElement::Node(build_node_pattern(child)?))
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => unreachable!(
                        "Unexpected rule in pattern_element: {:?}",
                        first_child.as_rule()
                    ),
                }
            }
            Ok(())
        };

        // Process the first pattern_element
        process_pattern_element(next)?;

        // Process remaining pattern_element+
        for p in inner {
            process_pattern_element(p)?;
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

    // Optional variable assignment
    let mut variable = None;
    if inner.peek().map(|p| p.as_rule()) == Some(Rule::identifier) {
        variable = Some(inner.next().unwrap().as_str().to_string());
        // Skip the '=' which is implicit in the grammar structure
    }

    // Pattern part (required)
    let pattern_part = inner.next().unwrap();
    let mut elements = vec![];
    for child in pattern_part.into_inner() {
        match child.as_rule() {
            Rule::node_pattern => elements.push(PatternElement::Node(build_node_pattern(child)?)),
            Rule::relationship_pattern => elements.push(PatternElement::Relationship(
                build_relationship_pattern(child)?,
            )),
            _ => unreachable!("Unexpected child in pattern_part"),
        }
    }

    // Optional WHERE clause
    let mut where_clause = None;
    if inner.peek().map(|p| p.as_rule()) == Some(Rule::WHERE) {
        inner.next(); // consume WHERE
        where_clause = Some(build_expression(inner.next().unwrap())?);
    }

    // Optional path quantifier
    let range = if let Some(quantifier) = inner.next() {
        Some(build_path_quantifier(quantifier)?)
    } else {
        None
    };

    // Create a PathPattern from the elements
    let path_pattern = PathPattern {
        variable,
        elements,
        shortest_path_mode: None,
    };

    // For now, we'll store the WHERE clause in a comment or ignore it
    // since the current AST doesn't support WHERE in parenthesized patterns
    // TODO: Consider extending AST to support WHERE in parenthesized patterns
    if where_clause.is_some() {
        eprintln!("Warning: WHERE clause in parenthesized pattern is not yet fully supported");
    }

    Ok(PatternElement::Parenthesized {
        pattern: Box::new(path_pattern),
        range,
    })
}

fn build_path_quantifier(pair: Pair<Rule>) -> Result<Range, ParseError> {
    let matched_text = pair.as_str();
    let inner: Vec<_> = pair.into_inner().collect();

    if inner.is_empty() {
        return Ok(Range {
            min: Some(0),
            max: None,
        });
    }

    let first = &inner[0];
    match first.as_rule() {
        Rule::plus => Ok(Range {
            min: Some(1),
            max: None,
        }),
        Rule::star => Ok(Range {
            min: Some(0),
            max: None,
        }),
        Rule::integer if inner.len() == 1 && !matched_text.contains(',') => {
            // {n} - exactly n (no comma means exact count)
            let n: u32 = first.as_str().parse().unwrap();
            Ok(Range {
                min: Some(n),
                max: Some(n),
            })
        }
        Rule::integer => {
            // {n,m} or {n,}
            let n: u32 = first.as_str().parse().unwrap();
            // Check if there's a second integer
            if inner.len() > 1 && inner[1].as_rule() == Rule::integer {
                let max: u32 = inner[1].as_str().parse().unwrap();
                Ok(Range {
                    min: Some(n),
                    max: Some(max),
                })
            } else {
                // {n,}
                Ok(Range {
                    min: Some(n),
                    max: None,
                })
            }
        }
        _ => {
            // {,m} - first element is the max integer
            if !inner.is_empty() {
                let max: u32 = inner[0].as_str().parse().unwrap();
                Ok(Range {
                    min: Some(0),
                    max: Some(max),
                })
            } else {
                // {,}
                Ok(Range {
                    min: Some(0),
                    max: None,
                })
            }
        }
    }
}

fn build_node_pattern(pair: Pair<Rule>) -> Result<NodePattern, ParseError> {
    let mut variable = None;
    let mut labels = vec![];
    let mut properties = None;
    let mut where_clause = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::identifier => variable = Some(p.as_str().to_string()),
            Rule::node_labels => {
                labels = p
                    .into_inner()
                    .map(|l| normalize_identifier(l.as_str()))
                    .collect()
            }
            Rule::node_predicate => {
                // node_predicate can be: properties WHERE expr | WHERE expr | properties
                let mut pred_inner = p.into_inner();
                let first = pred_inner.next();
                if first.is_none() {
                    continue;
                }
                let first = first.unwrap();

                match first.as_rule() {
                    Rule::properties => {
                        properties = Some(build_properties(first)?);
                        // Check if there's a WHERE clause
                        if let Some(where_kw) = pred_inner.next()
                            && where_kw.as_rule() == Rule::WHERE
                        {
                            where_clause = Some(build_expression(pred_inner.next().unwrap())?);
                        }
                    }
                    Rule::WHERE => {
                        where_clause = Some(build_expression(pred_inner.next().unwrap())?);
                    }
                    _ => unreachable!("Unexpected rule in node_predicate: {:?}", first.as_rule()),
                }
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
    let inner: Vec<_> = pair.into_inner().collect();
    let has_left = inner.iter().any(|p| p.as_rule() == Rule::arrow_left);
    let has_right = inner.iter().any(|p| p.as_rule() == Rule::arrow_right);
    let has_bidir = inner
        .iter()
        .any(|p| p.as_rule() == Rule::arrow_bidirectional);

    let direction = match (has_bidir, has_left, has_right) {
        (false, true, false) => Direction::Incoming,
        (false, false, true) => Direction::Outgoing,
        _ => Direction::Both,
    };

    let mut variable = None;
    let mut types = vec![];
    let mut range = None;
    let mut properties = None;
    let mut where_clause = None;

    for p in inner {
        if p.as_rule() == Rule::relationship_detail {
            for detail in p.into_inner() {
                match detail.as_rule() {
                    Rule::identifier => variable = Some(detail.as_str().to_string()),
                    Rule::relationship_types => {
                        types = detail
                            .into_inner()
                            .filter(|p| {
                                // Skip pipe and colon separators, keep only identifiers
                                matches!(
                                    p.as_rule(),
                                    Rule::identifier | Rule::identifier_or_keyword
                                )
                            })
                            .map(|t| normalize_identifier(t.as_str()))
                            .collect()
                    }
                    Rule::range_literal => range = Some(build_range(detail)?),
                    Rule::rel_predicate => {
                        // rel_predicate can be: properties WHERE expr | WHERE expr | properties
                        let mut pred_inner = detail.into_inner();
                        let first = pred_inner.next();
                        if first.is_none() {
                            continue;
                        }
                        let first = first.unwrap();

                        match first.as_rule() {
                            Rule::properties => {
                                properties = Some(build_properties(first)?);
                                // Check if there's a WHERE clause
                                if let Some(where_kw) = pred_inner.next()
                                    && where_kw.as_rule() == Rule::WHERE
                                {
                                    where_clause =
                                        Some(build_expression(pred_inner.next().unwrap())?);
                                }
                            }
                            Rule::WHERE => {
                                where_clause = Some(build_expression(pred_inner.next().unwrap())?);
                            }
                            _ => unreachable!(
                                "Unexpected rule in rel_predicate: {:?}",
                                first.as_rule()
                            ),
                        }
                    }
                    Rule::properties => properties = Some(build_properties(detail)?),
                    _ => {}
                }
            }
        }
    }
    Ok(RelationshipPattern {
        variable,
        types,
        direction,
        range,
        properties,
        where_clause,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

fn build_return_items(pair: Pair<Rule>) -> Result<Vec<ReturnItem>, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    if first.as_rule() == Rule::star {
        // RETURN *, items or just RETURN *
        let mut items = vec![ReturnItem::All];
        for p in inner {
            let mut i = p.into_inner();
            let expr = build_expression(i.next().unwrap())?;
            let alias = if i.next().is_some() {
                // AS
                Some(i.next().unwrap().as_str().to_string())
            } else {
                None
            };
            items.push(ReturnItem::Expr { expr, alias });
        }
        return Ok(items);
    }

    // RETURN items (no star)
    std::iter::once(first)
        .chain(inner)
        .map(|p| {
            let mut i = p.into_inner();
            let expr = build_expression(i.next().unwrap())?;
            let alias = if i.next().is_some() {
                // AS
                Some(i.next().unwrap().as_str().to_string())
            } else {
                None
            };
            Ok(ReturnItem::Expr { expr, alias })
        })
        .collect()
}

fn build_sort_items(pair: Pair<Rule>) -> Result<Vec<SortItem>, ParseError> {
    pair.into_inner()
        .map(|p| {
            let mut i = p.into_inner();
            let expr = build_expression(i.next().unwrap())?;
            let ascending = if let Some(order) = i.next() {
                order.as_rule() == Rule::ASC
            } else {
                true
            };
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

fn build_properties(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::map_literal => build_map_literal(inner),
        Rule::parameter => Ok(Expr::Parameter(inner.as_str()[1..].to_string())),
        _ => unreachable!(),
    }
}

fn build_range(pair: Pair<Rule>) -> Result<Range, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // star
    let mut min = None;
    let mut max = None;

    if let Some(first) = inner.next() {
        if first.as_rule() == Rule::integer {
            let val = first.as_str().parse().map_err(|e| {
                ParseError::new(format!("Invalid range bound '{}': {}", first.as_str(), e))
            })?;
            min = Some(val);
            if inner.next().is_some() {
                if let Some(second) = inner.next() {
                    let val = second.as_str().parse().map_err(|e| {
                        ParseError::new(format!("Invalid range bound '{}': {}", second.as_str(), e))
                    })?;
                    max = Some(val);
                }
            } else {
                max = Some(val);
            }
        } else if first.as_rule() == Rule::dot_dot
            && let Some(second) = inner.next()
        {
            let val = second.as_str().parse().map_err(|e| {
                ParseError::new(format!("Invalid range bound '{}': {}", second.as_str(), e))
            })?;
            max = Some(val);
        }
    } else {
        // [*] alone means 0 or more (Neo4j compatibility)
        min = Some(0);
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
                    Ok(MapProjectionItem::Property(first.as_str().to_string()))
                }
            } else if full_str.contains(':') {
                // Computed property: key: expr
                // Inner contains: [identifier, expression]
                let key = inner.next().unwrap().as_str().to_string();
                let val = build_expression(inner.next().unwrap())?;
                Ok(MapProjectionItem::LiteralEntry(key, Box::new(val)))
            } else {
                // Variable selector: just an identifier
                let first = inner.next().unwrap();
                Ok(MapProjectionItem::Variable(first.as_str().to_string()))
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
fn build_create_vector_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    inner.next(); // VECTOR
    inner.next(); // INDEX

    let name = inner.next().unwrap().as_str().to_string();
    let if_not_exists = check_if_not_exists(&mut inner);

    // Grammar: FOR ~ "(" ~ identifier ~ (":" ~ identifier_or_keyword)? ~ ")" ~ ON ~ "(" ~ property_expr ~ ")"
    inner.next(); // FOR

    // Handle optional variable binding: var or var:Label
    let first_token = inner.next().unwrap();

    // Check if next token is an identifier_or_keyword (the label after colon)
    let label = match inner.peek() {
        Some(p) if p.as_rule() == Rule::identifier_or_keyword => {
            // We have var:Label pattern, consume and use the label
            normalize_identifier(inner.next().unwrap().as_str())
        }
        _ => {
            // Just a label name without variable binding
            normalize_identifier(first_token.as_str())
        }
    };

    inner.next(); // ON

    // Get property expression (might be property_expr rule now)
    let prop_pair = inner.next().unwrap();
    let property = if prop_pair.as_rule() == Rule::property_expr {
        // Extract just the property name from property_expr like "d.embedding"
        let prop_str = prop_pair.as_str().trim();
        // Property expr is like "d.embedding", extract after the last dot
        prop_str
            .split('.')
            .next_back()
            .unwrap_or(prop_str)
            .trim()
            .to_string()
    } else {
        prop_pair.as_str().trim().to_string()
    };

    let options = if let Some(p) = inner.next() {
        if p.as_rule() == Rule::OPTIONS {
            build_map_options(inner.next().unwrap())?
        } else {
            std::collections::HashMap::new()
        }
    } else {
        std::collections::HashMap::new()
    };

    Ok(SchemaCommand::CreateVectorIndex(CreateVectorIndex {
        name,
        label,
        property,
        options,
        if_not_exists,
    }))
}

fn build_create_fulltext_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    inner.next(); // FULLTEXT
    inner.next(); // INDEX

    let name = inner.next().unwrap().as_str().to_string();
    let if_not_exists = check_if_not_exists(&mut inner);

    // Grammar: FOR ~ "(" ~ identifier ~ (":" ~ identifier_or_keyword)? ~ ")" ~ ON ~ EACH? ~ "[" ~ property_expression_list ~ "]"
    inner.next(); // FOR

    // Handle optional variable binding: var or var:Label
    let first_token = inner.next().unwrap();

    // Check if next token is an identifier_or_keyword (the label after colon)
    let label = match inner.peek() {
        Some(p) if p.as_rule() == Rule::identifier_or_keyword => {
            // We have var:Label pattern, consume and use the label
            normalize_identifier(inner.next().unwrap().as_str())
        }
        _ => {
            // Just a label name without variable binding
            normalize_identifier(first_token.as_str())
        }
    };

    inner.next(); // ON

    // Skip optional EACH keyword if present
    if let Some(peek) = inner.peek()
        && peek.as_rule() == Rule::EACH
    {
        inner.next();
    }

    // Parse property_expression_list
    let prop_list = inner.next().unwrap();
    let properties = if prop_list.as_rule() == Rule::property_expression_list {
        prop_list
            .into_inner()
            .map(|prop| {
                // Each item is a property_expr like "a.body"
                let prop_str = prop.as_str();
                // Extract the property name after the last dot
                prop_str
                    .split('.')
                    .next_back()
                    .unwrap_or(prop_str)
                    .to_string()
            })
            .collect()
    } else {
        // Fallback: treat as single property (shouldn't happen with correct grammar)
        vec![prop_list.as_str().to_string()]
    };

    let options = if let Some(p) = inner.next() {
        if p.as_rule() == Rule::OPTIONS {
            build_map_options(inner.next().unwrap())?
        } else {
            std::collections::HashMap::new()
        }
    } else {
        std::collections::HashMap::new()
    };

    Ok(SchemaCommand::CreateFullTextIndex(CreateFullTextIndex {
        name,
        label,
        properties,
        options,
        if_not_exists,
    }))
}

fn build_create_scalar_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    inner.next(); // INDEX

    // Grammar: CREATE ~ INDEX ~ if_not_exists? ~ identifier
    //          ~ FOR ~ "(" ~ identifier ~ ":" ~ identifier_or_keyword ~ ")"
    //          ~ ON ~ "(" ~ property_expression_list ~ ")"
    let if_not_exists = check_if_not_exists(&mut inner);
    let name = inner.next().unwrap().as_str().to_string();

    // FOR clause: expect identifier ":" label
    // Parentheses are not separate tokens
    inner.next(); // FOR
    let _variable = inner.next().unwrap().as_str().to_string(); // variable name (e.g., "p")
    let label = normalize_identifier(inner.next().unwrap().as_str()); // label name (e.g., "Person")
    inner.next(); // ON

    // ON clause: index_expression_list
    let expr_list = inner.next().unwrap();
    let expressions: Vec<Expr> = expr_list
        .into_inner()
        .map(|expr_pair| build_expression(expr_pair))
        .collect::<Result<Vec<_>, _>>()?;

    let mut where_clause = None;
    let mut options = std::collections::HashMap::new();

    for p in inner {
        match p.as_rule() {
            Rule::where_clause => {
                let mut wc_inner = p.into_inner();
                wc_inner.next(); // Skip WHERE keyword
                where_clause = Some(build_expression(wc_inner.next().unwrap())?);
            }
            Rule::OPTIONS => {
                // Next item should be map_literal
            }
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
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    inner.next(); // JSON
    inner.next(); // FULLTEXT
    inner.next(); // INDEX

    let name = inner.next().unwrap().as_str().to_string();
    let if_not_exists = check_if_not_exists(&mut inner);

    // Grammar: FOR ~ "(" ~ identifier ~ ":" ~ identifier_or_keyword ~ ")" ~ ON ~ identifier
    // Parentheses and colon are not separate tokens
    inner.next(); // FOR
    let _variable = inner.next().unwrap().as_str().to_string(); // e.g., "a"
    let label = normalize_identifier(inner.next().unwrap().as_str()); // e.g., "Article"
    inner.next(); // ON
    let column = inner.next().unwrap().as_str().to_string(); // e.g., "_doc"

    let options = if let Some(p) = inner.next() {
        if p.as_rule() == Rule::OPTIONS {
            build_map_options(inner.next().unwrap())?
        } else {
            std::collections::HashMap::new()
        }
    } else {
        std::collections::HashMap::new()
    };

    Ok(SchemaCommand::CreateJsonFtsIndex(CreateJsonFtsIndex {
        name,
        label,
        column,
        options,
        if_not_exists,
    }))
}

fn build_drop_index(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // DROP_KW
    inner.next(); // INDEX

    let _if_exists = check_if_exists(&mut inner);
    let name = inner.next().unwrap().as_str().to_string();

    Ok(SchemaCommand::DropIndex(DropIndex { name }))
}

fn build_create_constraint(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    inner.next(); // CONSTRAINT

    let _if_not_exists = check_if_not_exists(&mut inner);

    // Optional constraint name - check if next token is an identifier or ON
    let name = {
        let next_pair = inner.next().unwrap();
        if next_pair.as_rule() == Rule::identifier
            || next_pair.as_rule() == Rule::identifier_or_keyword
        {
            // It's a constraint name, skip ON and return the name
            inner.next(); // Skip ON
            Some(normalize_identifier(next_pair.as_str()))
        } else {
            // It's ON, we already consumed it, no name
            None
        }
    };

    // Note: parentheses and colon are not separate tokens in the parse tree
    let var = inner.next().unwrap().as_str().to_string();
    let label = normalize_identifier(inner.next().unwrap().as_str());
    inner.next(); // ASSERT

    let assertion = inner.next().unwrap();
    let (constraint_type, properties, expression) = build_constraint_assertion(assertion, &var)?;

    Ok(SchemaCommand::CreateConstraint(CreateConstraint {
        name,
        constraint_type,
        label,
        properties,
        expression,
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
            let constraint_kw = inner.next().unwrap();

            // Check for optional NODE keyword before KEY
            let constraint_kw = if constraint_kw.as_rule() == Rule::NODE {
                inner.next().unwrap() // Get the actual KEY/UNIQUE
            } else {
                constraint_kw
            };

            let ctype = match constraint_kw.as_rule() {
                Rule::UNIQUE => ConstraintType::Unique,
                Rule::KEY => ConstraintType::NodeKey,
                _ => return Err(ParseError::new("Expected UNIQUE or KEY".to_string())),
            };
            Ok((ctype, props, None))
        }
        Rule::expression => {
            // CHECK constraint with arbitrary expression
            let expr = build_expression(first)?;
            // TODO: Extract property names from expression for properties vec
            Ok((ConstraintType::Check, vec![], Some(expr)))
        }
        _ => {
            // Fallback to old syntax: (props) IS [NODE|RELATIONSHIP] [UNIQUE|KEY]
            inner.next(); // Skip (
            let prop_list = inner.next().unwrap();
            let properties = prop_list
                .into_inner()
                .map(|p| p.as_str().to_string())
                .collect();
            inner.next(); // )
            inner.next(); // IS

            // Optional NODE or RELATIONSHIP
            let mut next = inner.next().unwrap();
            if next.as_rule() == Rule::NODE || next.as_rule() == Rule::RELATIONSHIP {
                next = inner.next().unwrap();
            }

            let ctype = if next.as_rule() == Rule::UNIQUE {
                ConstraintType::Unique
            } else {
                ConstraintType::NodeKey
            };

            Ok((ctype, properties, None))
        }
    }
}

fn extract_property_names_from_expr(
    pair: Pair<Rule>,
    expected_var: &str,
) -> Result<Vec<String>, ParseError> {
    // For property_expr like n.prop or n.prop1.prop2, extract the property names
    // We expect the expression to be in the form: var.prop1.prop2...
    let mut parts: Vec<String> = Vec::new();

    fn extract_from_pair(pair: Pair<Rule>, parts: &mut Vec<String>) {
        match pair.as_rule() {
            Rule::property_expr => {
                for child in pair.into_inner() {
                    extract_from_pair(child, parts);
                }
            }
            Rule::identifier | Rule::identifier_or_keyword => {
                // Skip the variable name, only collect property names
                parts.push(pair.as_str().to_string());
            }
            _ => {
                for child in pair.into_inner() {
                    extract_from_pair(child, parts);
                }
            }
        }
    }

    extract_from_pair(pair, &mut parts);

    // Remove the first element which should be the variable name
    if !parts.is_empty() && parts[0] == expected_var {
        parts.remove(0);
    }

    Ok(parts)
}

fn build_drop_constraint(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // DROP_KW
    inner.next(); // CONSTRAINT

    let _if_exists = check_if_exists(&mut inner);
    let name = inner.next().unwrap().as_str().to_string();

    Ok(SchemaCommand::DropConstraint(DropConstraint { name }))
}

fn build_create_label(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    inner.next(); // LABEL

    let if_not_exists = check_if_not_exists(&mut inner);
    let name = normalize_identifier(inner.next().unwrap().as_str());

    let prop_defs = inner.next().unwrap();
    let properties = build_property_definitions(prop_defs)?;

    Ok(SchemaCommand::CreateLabel(CreateLabel {
        name,
        properties,
        if_not_exists,
    }))
}

fn build_create_edge_type(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // CREATE
    inner.next(); // EDGE
    inner.next(); // TYPE

    let if_not_exists = check_if_not_exists(&mut inner);
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

    Ok(SchemaCommand::CreateEdgeType(CreateEdgeType {
        name,
        src_labels: vec![src_label],
        dst_labels: vec![dst_label],
        properties,
        if_not_exists,
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
    let mut inner = pair.into_inner();
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
            inner.next(); // PROPERTY
            let name = inner.next().unwrap().as_str().to_string();
            Ok(AlterAction::DropProperty(name))
        }
        Rule::RENAME => {
            inner.next(); // PROPERTY
            let old_name = inner.next().unwrap().as_str().to_string();
            inner.next(); // TO
            let new_name = inner.next().unwrap().as_str().to_string();
            Ok(AlterAction::RenameProperty { old_name, new_name })
        }
        _ => unreachable!(),
    }
}

fn build_drop_label(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // DROP_KW
    inner.next(); // LABEL
    let if_exists = check_if_exists(&mut inner);
    let name = normalize_identifier(inner.next().unwrap().as_str());

    Ok(SchemaCommand::DropLabel(DropLabel { name, if_exists }))
}

fn build_drop_edge_type(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // DROP_KW
    inner.next(); // EDGE
    inner.next(); // TYPE
    let if_exists = check_if_exists(&mut inner);
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

    let target = if let Some(target_pair) = inner.next() {
        Some(build_constraint_target(target_pair)?)
    } else {
        None
    };

    Ok(SchemaCommand::ShowConstraints(ShowConstraints { target }))
}

fn build_constraint_target(pair: Pair<Rule>) -> Result<ConstraintTarget, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // FOR

    let next = inner.peek().unwrap();
    if next.as_rule() == Rule::EDGE {
        inner.next(); // EDGE
        inner.next(); // (
        let name = normalize_identifier(inner.next().unwrap().as_str());
        Ok(ConstraintTarget::EdgeType(name))
    } else {
        inner.next(); // (
        let name = normalize_identifier(inner.next().unwrap().as_str());
        Ok(ConstraintTarget::Label(name))
    }
}

fn build_show_indexes(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // SHOW

    let filter = if let Some(p) = inner.next() {
        if p.as_rule() == Rule::VECTOR {
            Some("VECTOR".to_string())
        } else if p.as_rule() == Rule::FULLTEXT {
            Some("FULLTEXT".to_string())
        } else {
            None
        }
    } else {
        None
    };

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

fn build_copy_to(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // COPY
    let label = normalize_identifier(inner.next().unwrap().as_str());
    inner.next(); // TO
    let path_pair = inner.next().unwrap();
    let path = build_string_literal(path_pair)?;

    let options = if let Some(p) = inner.next() {
        if p.as_rule() == Rule::WITH {
            build_map_options(inner.next().unwrap())?
        } else {
            std::collections::HashMap::new()
        }
    } else {
        std::collections::HashMap::new()
    };

    let format = detect_file_format(&path, &options);

    Ok(SchemaCommand::CopyTo(CopyToCommand {
        label,
        path,
        format,
        options,
    }))
}

fn build_copy_from(pair: Pair<Rule>) -> Result<SchemaCommand, ParseError> {
    let mut inner = pair.into_inner();
    inner.next(); // COPY
    let label = normalize_identifier(inner.next().unwrap().as_str());
    inner.next(); // FROM
    let path_pair = inner.next().unwrap();
    let path = build_string_literal(path_pair)?;

    let options = if let Some(p) = inner.next() {
        if p.as_rule() == Rule::WITH {
            build_map_options(inner.next().unwrap())?
        } else {
            std::collections::HashMap::new()
        }
    } else {
        std::collections::HashMap::new()
    };

    let format = detect_file_format(&path, &options);

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
fn detect_file_format(
    path: &str,
    options: &std::collections::HashMap<String, serde_json::Value>,
) -> String {
    // Check if format is explicitly provided in options
    if let Some(format_value) = options.get("format")
        && let Some(format_str) = format_value.as_str()
    {
        return format_str.to_lowercase();
    }

    // Detect from file extension
    if let Some(ext) = std::path::Path::new(path).extension()
        && let Some(ext_str) = ext.to_str()
    {
        let ext_lower = ext_str.to_lowercase();
        match ext_lower.as_str() {
            "csv" => return "csv".to_string(),
            "parquet" | "pq" => return "parquet".to_string(),
            _ => {}
        }
    }

    // Default to parquet
    "parquet".to_string()
}

fn check_if_not_exists(inner: &mut Pairs<Rule>) -> bool {
    // Peek at the next element
    let mut temp_iter = inner.clone();
    if let Some(p) = temp_iter.next()
        && p.as_rule() == Rule::if_not_exists
    {
        inner.next(); // Consume it from the real iterator
        return true;
    }
    false
}

fn check_if_exists(inner: &mut Pairs<Rule>) -> bool {
    // Peek at the next element
    let mut temp_iter = inner.clone();
    if let Some(p) = temp_iter.next()
        && p.as_rule() == Rule::if_exists
    {
        inner.next(); // Consume it from the real iterator
        return true;
    }
    false
}

fn build_property_definitions(pair: Pair<Rule>) -> Result<Vec<PropertyDefinition>, ParseError> {
    pair.into_inner().map(build_property_definition).collect()
}

fn build_property_definition(pair: Pair<Rule>) -> Result<PropertyDefinition, ParseError> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let data_type = inner.next().unwrap().as_str().to_string();

    let mut nullable = true;
    let mut unique = false;
    let mut default = None;

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
            _ => {}
        }
    }

    Ok(PropertyDefinition {
        name,
        data_type,
        nullable,
        unique,
        default,
    })
}

/// Convert an expression to a JSON value for OPTIONS.
///
/// Supports:
/// - Literals: strings, numbers, booleans, null
/// - Maps: converted to JSON objects recursively
/// - Lists: converted to JSON arrays recursively
fn expr_to_value(expr: Expr) -> Result<Value, ParseError> {
    match expr {
        Expr::Literal(lit) => Ok(lit.to_json_value()),

        Expr::Map(entries) => {
            let mut obj = serde_json::Map::new();
            for (key, value_expr) in entries {
                obj.insert(key, expr_to_value(value_expr)?);
            }
            Ok(Value::Object(obj))
        }

        Expr::List(items) => {
            let values: Result<Vec<Value>, ParseError> =
                items.into_iter().map(expr_to_value).collect();
            Ok(Value::Array(values?))
        }

        _ => Err(ParseError::new(format!(
            "OPTIONS values must be literals, maps, or lists. Got: {:?}",
            expr
        ))),
    }
}

fn build_map_options(
    pair: Pair<Rule>,
) -> Result<std::collections::HashMap<String, Value>, ParseError> {
    let map = build_map_literal(pair)?;
    if let Expr::Map(entries) = map {
        let mut result = std::collections::HashMap::new();
        for (key, value_expr) in entries {
            result.insert(key, expr_to_value(value_expr)?);
        }
        Ok(result)
    } else {
        Err(ParseError::new(
            "Expected map literal for options".to_string(),
        ))
    }
}

fn build_string_literal(pair: Pair<Rule>) -> Result<String, ParseError> {
    if pair.as_rule() == Rule::string {
        let s = pair.as_str();
        let quote_char = s.chars().next().unwrap();
        let content = &s[1..s.len() - 1];
        unescape_string(content, quote_char)
    } else {
        Err(ParseError::new("Expected string literal".to_string()))
    }
}

fn build_transaction_command(pair: Pair<Rule>) -> Result<TransactionCommand, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::BEGIN => Ok(TransactionCommand::Begin),
        Rule::COMMIT => Ok(TransactionCommand::Commit),
        Rule::ROLLBACK => Ok(TransactionCommand::Rollback),
        _ => unreachable!(),
    }
}
