// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_cypher::ast::{BinaryOp, Clause, CypherLiteral, Expr, Query, ReturnItem};

fn parse_return_expr(input: &str) -> Expr {
    let query = uni_query::parse_cypher(input).unwrap();

    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };

    let return_clause = stmt
        .clauses
        .iter()
        .find_map(|c| {
            if let Clause::Return(r) = c {
                Some(r)
            } else {
                None
            }
        })
        .expect("Expected return clause");

    match &return_clause.items[0] {
        ReturnItem::Expr { expr, .. } => expr.clone(),
        ReturnItem::All => panic!("Expected expression, got RETURN *"),
    }
}

#[test]
fn test_parse_reduce() {
    let expr = parse_return_expr("RETURN reduce(total = 0, x IN [1, 2, 3] | total + x)");

    let Expr::Reduce {
        accumulator,
        init,
        variable,
        list,
        expr,
    } = expr
    else {
        panic!("Expected Expr::Reduce");
    };

    assert_eq!(accumulator, "total");
    assert_eq!(*init, Expr::Literal(CypherLiteral::Integer(0)));
    assert_eq!(variable, "x");

    let Expr::List(items) = *list else {
        panic!("Expected list literal");
    };
    assert_eq!(items.len(), 3);

    let Expr::BinaryOp { left, op, .. } = *expr else {
        panic!("Expected BinaryOp expression");
    };
    assert_eq!(*left, Expr::Variable("total".to_string()));
    assert_eq!(op, BinaryOp::Add);
}
