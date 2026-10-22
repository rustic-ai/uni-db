pub mod ast;
mod grammar;
pub mod locy_ast;
pub mod pattern_requirements;
pub mod plugin_aggregates;

pub use grammar::{
    ParseError, parse, parse_expression, parse_locy, parse_locy_with_parameters,
    parse_with_parameters,
};
pub use plugin_aggregates::{
    is_known_plugin_aggregate, register_plugin_aggregate, unregister_plugin_aggregate,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comprehension_with_complex_where() {
        let test_cases = vec![
            // Basic boolean operators
            (
                "AND operator",
                "RETURN [x IN range(1,100) WHERE x > 10 AND x < 50 | x * 2] AS result",
            ),
            (
                "OR operator",
                "RETURN [x IN nodes WHERE x.active OR x.admin | x.name] AS result",
            ),
            (
                "XOR operator",
                "RETURN [x IN items WHERE x.flag1 XOR x.flag2 | x.id] AS result",
            ),
            // Nested conditions
            (
                "Parenthesized OR with AND",
                "RETURN [x IN list WHERE (x > 0 AND x < 10) OR x = 100 | x] AS result",
            ),
            (
                "Complex nested",
                "RETURN [x IN data WHERE (x.a AND x.b) OR (x.c AND NOT x.d) | x.value] AS result",
            ),
            (
                "Triple nesting",
                "RETURN [x IN items WHERE ((x.a OR x.b) AND x.c) OR (x.d AND NOT x.e) | x] AS result",
            ),
            // NOT operator variations
            (
                "NOT with AND",
                "RETURN [x IN list WHERE NOT x.deleted AND x.active | x] AS result",
            ),
            (
                "NOT with OR",
                "RETURN [x IN list WHERE NOT (x.a OR x.b) | x] AS result",
            ),
            (
                "Multiple NOT",
                "RETURN [x IN list WHERE NOT x.a AND NOT x.b | x] AS result",
            ),
            // Filter-only (no map expression) - Previously broken!
            (
                "Filter-only with AND",
                "RETURN [x IN list WHERE x > 5 AND x < 10] AS filtered",
            ),
            (
                "Filter-only with OR",
                "RETURN [x IN list WHERE x < 0 OR x > 100] AS outliers",
            ),
            (
                "Filter-only complex",
                "RETURN [x IN data WHERE (x.status = 'active' AND x.verified) OR x.admin] AS users",
            ),
            // Pattern comprehensions with complex WHERE
            (
                "Pattern with AND",
                "RETURN [(a)-[:KNOWS]->(b) WHERE b.age > 21 AND b.active | b.name] AS friends",
            ),
            (
                "Pattern with OR",
                "RETURN [(n)-[:LIKES|LOVES]->(m) WHERE m.public OR n.friend | m] AS items",
            ),
            (
                "Pattern complex",
                "RETURN [p = (a)-[r]->(b) WHERE (r.weight > 5 AND b.score > 10) OR a.vip | p] AS paths",
            ),
            // Combining different comparison operators
            (
                "Multiple comparisons",
                "RETURN [x IN items WHERE x.price > 10 AND x.price < 100 AND x.inStock | x] AS affordable",
            ),
            (
                "String operators",
                "RETURN [x IN names WHERE x STARTS WITH 'A' AND NOT x ENDS WITH 'z' | x] AS filtered",
            ),
            (
                "IN with AND",
                "RETURN [x IN numbers WHERE x IN [1,2,3] AND x % 2 = 0 | x * 10] AS even",
            ),
            // Property access in complex conditions
            (
                "Nested properties",
                "RETURN [x IN items WHERE x.meta.active AND (x.meta.score > 5 OR x.priority) | x.id] AS result",
            ),
            (
                "Property with NULL",
                "RETURN [x IN items WHERE x.prop IS NOT NULL AND x.prop > 0 | x] AS valid",
            ),
            // All three operators combined
            (
                "AND OR XOR mix",
                "RETURN [x IN list WHERE (x.a AND x.b) OR (x.c XOR x.d) | x] AS result",
            ),
            (
                "Complex mix",
                "RETURN [x IN data WHERE (x.flag1 OR x.flag2) AND NOT (x.flag3 XOR x.flag4) | x.value] AS result",
            ),
        ];

        println!("\n=== Testing Complex Comprehension WHERE Clauses ===\n");

        for (name, query) in test_cases.iter() {
            match parse(query) {
                Ok(_) => println!("✅ {}: PASSED", name),
                Err(e) => panic!("❌ {} FAILED: {:?}\nQuery: {}", name, e, query),
            }
        }

        println!(
            "\n✅ All {} complex comprehension tests passed!",
            test_cases.len()
        );
    }

    #[test]
    fn test_parse_version_as_of() {
        let q = parse("MATCH (n) RETURN n VERSION AS OF 'snap123'").unwrap();
        match q {
            ast::Query::TimeTravel { query, spec } => {
                assert!(matches!(*query, ast::Query::Single(_)));
                assert_eq!(spec, ast::TimeTravelSpec::Version("snap123".to_string()));
            }
            _ => panic!("Expected TimeTravel query, got {:?}", q),
        }
    }

    #[test]
    fn test_parse_timestamp_as_of() {
        let q = parse("MATCH (n) RETURN n TIMESTAMP AS OF '2025-02-01T12:00:00Z'").unwrap();
        match q {
            ast::Query::TimeTravel { query, spec } => {
                assert!(matches!(*query, ast::Query::Single(_)));
                assert_eq!(
                    spec,
                    ast::TimeTravelSpec::Timestamp("2025-02-01T12:00:00Z".to_string())
                );
            }
            _ => panic!("Expected TimeTravel query, got {:?}", q),
        }
    }

    #[test]
    fn test_parse_version_as_of_with_union() {
        let q =
            parse("MATCH (n:A) RETURN n UNION MATCH (m:B) RETURN m VERSION AS OF 'snap1'").unwrap();
        match q {
            ast::Query::TimeTravel { query, spec } => {
                assert!(matches!(*query, ast::Query::Union { .. }));
                assert_eq!(spec, ast::TimeTravelSpec::Version("snap1".to_string()));
            }
            _ => panic!("Expected TimeTravel query, got {:?}", q),
        }
    }

    #[test]
    fn test_parse_no_time_travel() {
        let q = parse("MATCH (n) RETURN n").unwrap();
        assert!(matches!(q, ast::Query::Single(_)));
    }

    #[test]
    fn test_parse_or_relationship_types() {
        let q = parse("MATCH (n)-[r:KNOWS|HATES]->(x) RETURN r").unwrap();
        if let ast::Query::Single(single) = q
            && let ast::Clause::Match(match_clause) = &single.clauses[0]
            && let ast::PatternElement::Relationship(rel) =
                &match_clause.pattern.paths[0].elements[1]
        {
            assert_eq!(
                rel.types,
                ast::LabelExpr::Disjunction(vec!["KNOWS".to_string(), "HATES".to_string()])
            );
            println!("Parsed types: {:?}", rel.types);
            return;
        }
        panic!("Could not find relationship pattern with OR types");
    }

    #[test]
    fn test_parse_vlp_relationship_variable() {
        // Test that VLP patterns preserve the relationship variable
        let q = parse("MATCH (a)-[r*1..1]->(b) RETURN r").unwrap();
        if let ast::Query::Single(single) = q
            && let ast::Clause::Match(match_clause) = &single.clauses[0]
            && let ast::PatternElement::Relationship(rel) =
                &match_clause.pattern.paths[0].elements[1]
        {
            assert_eq!(
                rel.variable,
                Some("r".to_string()),
                "VLP should preserve relationship variable 'r'"
            );
            assert!(rel.range.is_some(), "VLP should have range");
            let range = rel.range.as_ref().unwrap();
            assert_eq!(range.min, Some(1));
            assert_eq!(range.max, Some(1));
            println!(
                "VLP relationship: variable={:?}, range={:?}",
                rel.variable, rel.range
            );
            return;
        }
        panic!("Could not find VLP relationship pattern");
    }
}

#[cfg(test)]
mod locy_tests {
    use super::*;

    // ══════════════════════════════════════════════════════════════════════
    // Step 1: Cypher passthrough
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_cypher_passthrough_match_return() {
        let program = parse_locy("MATCH (n) RETURN n").unwrap();
        assert!(program.module.is_none());
        assert!(program.uses.is_empty());
        assert_eq!(program.statements.len(), 1);
        assert!(
            matches!(&program.statements[0], locy_ast::LocyStatement::Cypher(_)),
            "Expected Cypher passthrough, got: {:?}",
            program.statements[0]
        );
    }

    #[test]
    fn test_locy_cypher_passthrough_create() {
        let program = parse_locy("CREATE (n:Person {name: 'Alice'})").unwrap();
        assert_eq!(program.statements.len(), 1);
        assert!(matches!(
            &program.statements[0],
            locy_ast::LocyStatement::Cypher(_)
        ));
    }

    #[test]
    fn test_locy_cypher_passthrough_union() {
        let program = parse_locy("MATCH (n:A) RETURN n UNION MATCH (m:B) RETURN m").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let locy_ast::LocyStatement::Cypher(q) = &program.statements[0] {
            assert!(matches!(q, ast::Query::Union { .. }));
        } else {
            panic!("Expected Cypher union");
        }
    }

    #[test]
    fn test_locy_cypher_passthrough_multi_clause() {
        let program = parse_locy("MATCH (n) WHERE n.age > 21 WITH n RETURN n.name").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let locy_ast::LocyStatement::Cypher(ast::Query::Single(stmt)) = &program.statements[0] {
            assert_eq!(stmt.clauses.len(), 3); // MATCH, WITH, RETURN
        } else {
            panic!("Expected Cypher single query with 3 clauses");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 2: CREATE RULE ... YIELD (minimal)
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_create_rule_minimal() {
        let program =
            parse_locy("CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            assert_eq!(rule.name.parts, vec!["reachable"]);
            assert!(rule.priority.is_none());
            assert!(!rule.match_pattern.paths.is_empty());
            assert!(rule.where_conditions.is_empty());
            if let locy_ast::RuleOutput::Yield(yc) = &rule.output {
                let items = &yc.items;
                assert_eq!(items.len(), 2);
                assert!(!items[0].is_key);
                assert!(!items[1].is_key);
            } else {
                panic!("Expected Yield output");
            }
        } else {
            panic!("Expected Rule statement");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 3: PRIORITY clause
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_rule_priority() {
        let program =
            parse_locy("CREATE RULE r PRIORITY 2 AS MATCH (a)-[:E]->(b) YIELD a").unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            assert_eq!(rule.priority, Some(2));
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 4: Unary IS reference
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_is_reference_unary() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (n) WHERE n IS suspicious YIELD n").unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            assert_eq!(rule.where_conditions.len(), 1);
            if let locy_ast::RuleCondition::IsReference(is_ref) = &rule.where_conditions[0] {
                assert_eq!(is_ref.subjects, vec!["n"]);
                assert_eq!(is_ref.rule_name.parts, vec!["suspicious"]);
                assert!(is_ref.target.is_none());
                assert!(!is_ref.negated);
            } else {
                panic!("Expected IsReference");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 5: IS NOT reference
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_is_not_reference() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (n) WHERE n IS NOT clean YIELD n").unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleCondition::IsReference(is_ref) = &rule.where_conditions[0] {
                assert!(is_ref.negated);
                assert_eq!(is_ref.rule_name.parts, vec!["clean"]);
            } else {
                panic!("Expected IsReference");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    #[test]
    fn test_locy_not_is_reference_prefix() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (n) WHERE NOT n IS clean YIELD n").unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleCondition::IsReference(is_ref) = &rule.where_conditions[0] {
                assert!(is_ref.negated);
                assert_eq!(is_ref.rule_name.parts, vec!["clean"]);
            } else {
                panic!("Expected IsReference");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 6: Binary IS ... TO
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_is_reference_binary() {
        let program = parse_locy(
            "CREATE RULE test AS MATCH (a)-[:E]->(b) WHERE a IS reachable TO b YIELD a, b",
        )
        .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleCondition::IsReference(is_ref) = &rule.where_conditions[0] {
                assert_eq!(is_ref.subjects, vec!["a"]);
                assert_eq!(is_ref.rule_name.parts, vec!["reachable"]);
                assert_eq!(is_ref.target, Some("b".to_string()));
                assert!(!is_ref.negated);
            } else {
                panic!("Expected IsReference");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 7: Tuple IS reference
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_is_reference_tuple() {
        let program = parse_locy(
            "CREATE RULE test AS MATCH (x)-[:E]->(y) WHERE (x, y, cost) IS control YIELD x",
        )
        .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleCondition::IsReference(is_ref) = &rule.where_conditions[0] {
                assert_eq!(is_ref.subjects, vec!["x", "y", "cost"]);
                assert_eq!(is_ref.rule_name.parts, vec!["control"]);
                assert!(is_ref.target.is_none());
            } else {
                panic!("Expected IsReference");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 8: Mixed WHERE conditions
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_mixed_where_conditions() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (n) WHERE n IS reachable, n.age > 18 YIELD n")
                .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            assert_eq!(rule.where_conditions.len(), 2);
            assert!(matches!(
                &rule.where_conditions[0],
                locy_ast::RuleCondition::IsReference(_)
            ));
            assert!(matches!(
                &rule.where_conditions[1],
                locy_ast::RuleCondition::Expression(_)
            ));
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 9: ALONG with prev
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_along_clause() {
        let program = parse_locy(
            "CREATE RULE test AS MATCH (a)-[:E]->(b) ALONG hops = prev.hops + 1 YIELD a, b, hops",
        )
        .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            assert_eq!(rule.along.len(), 1);
            assert_eq!(rule.along[0].name, "hops");
            // The expression should be a BinaryOp(PrevRef("hops"), Add, Cypher(1))
            if let locy_ast::LocyExpr::BinaryOp { left, op, right } = &rule.along[0].expr {
                assert!(matches!(left.as_ref(), locy_ast::LocyExpr::PrevRef(f) if f == "hops"));
                assert_eq!(*op, locy_ast::LocyBinaryOp::Add);
                assert!(matches!(right.as_ref(), locy_ast::LocyExpr::Cypher(_)));
            } else {
                panic!("Expected BinaryOp, got: {:?}", rule.along[0].expr);
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 10: FOLD
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_fold_clause() {
        let program = parse_locy(
            "CREATE RULE test AS MATCH (a)-[:E]->(b) FOLD total = SUM(s) YIELD a, total",
        )
        .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            assert_eq!(rule.fold.len(), 1);
            assert_eq!(rule.fold[0].name, "total");
            if let ast::Expr::FunctionCall { name, .. } = &rule.fold[0].aggregate {
                assert_eq!(name.to_uppercase(), "SUM");
            } else {
                panic!("Expected FunctionCall, got: {:?}", rule.fold[0].aggregate);
            }
        } else {
            panic!("Expected Rule");
        }
    }

    #[test]
    fn test_locy_fold_plugin_namespaced_aggregate() {
        // P0.2: a plugin-namespaced (dotted) aggregate must parse in FOLD
        // position, carried through as a dotted FunctionCall name so the
        // resolver can dispatch it by namespace.
        let program = parse_locy(
            "CREATE RULE test AS MATCH (a)-[:E]->(b) FOLD total = myplugin.MYAGG(s) YIELD a, total",
        )
        .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            assert_eq!(rule.fold.len(), 1);
            if let ast::Expr::FunctionCall { name, .. } = &rule.fold[0].aggregate {
                assert_eq!(name, "myplugin.MYAGG", "dotted aggregate name preserved");
            } else {
                panic!(
                    "Expected dotted FunctionCall, got: {:?}",
                    rule.fold[0].aggregate
                );
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 11: BEST BY
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_best_by_clause() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (a)-[:E]->(b) BEST BY cost ASC YIELD a, b")
                .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            let best_by = rule.best_by.as_ref().unwrap();
            assert_eq!(best_by.items.len(), 1);
            assert!(best_by.items[0].ascending);
        } else {
            panic!("Expected Rule");
        }
    }

    #[test]
    fn test_locy_best_by_desc() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (a)-[:E]->(b) BEST BY cost DESC YIELD a, b")
                .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            let best_by = rule.best_by.as_ref().unwrap();
            assert!(!best_by.items[0].ascending);
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 12: DERIVE pattern
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_derive_forward() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (a)-[:KNOWS]->(b) DERIVE (a)-[:FRIEND]->(b)")
                .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleOutput::Derive(locy_ast::DeriveClause::Patterns(pats)) =
                &rule.output
            {
                assert_eq!(pats.len(), 1);
                assert_eq!(pats[0].direction, ast::Direction::Outgoing);
            } else {
                panic!("Expected Derive Patterns terminal");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 13: DERIVE MERGE
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_derive_merge() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (a)-[:SAME]->(b) DERIVE MERGE a, b").unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleOutput::Derive(locy_ast::DeriveClause::Merge(a, b)) = &rule.output
            {
                assert_eq!(a, "a");
                assert_eq!(b, "b");
            } else {
                panic!("Expected Derive Merge");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 14: DERIVE NEW
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_derive_new_backward() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (c) DERIVE (NEW x:Country)<-[:IN]-(c)").unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleOutput::Derive(locy_ast::DeriveClause::Patterns(pats)) =
                &rule.output
            {
                assert_eq!(pats[0].direction, ast::Direction::Incoming);
                assert!(pats[0].source.is_new);
                assert_eq!(pats[0].source.variable, "x");
                assert_eq!(pats[0].source.labels, vec!["Country"]);
            } else {
                panic!("Expected Derive Patterns");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 15: YIELD with KEY
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_yield_with_key() {
        let program =
            parse_locy("CREATE RULE test AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, cost").unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleOutput::Yield(yc) = &rule.output {
                let items = &yc.items;
                assert_eq!(items.len(), 3);
                assert!(items[0].is_key);
                assert!(items[1].is_key);
                assert!(!items[2].is_key);
            } else {
                panic!("Expected Yield output");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    #[test]
    fn test_locy_yield_key_with_property() {
        let program = parse_locy(
            "CREATE RULE r AS MATCH (e:Event) YIELD KEY e.action, KEY e.outcome, n AS support",
        )
        .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleOutput::Yield(yc) = &rule.output {
                let items = &yc.items;
                assert_eq!(items.len(), 3);
                assert!(items[0].is_key);
                assert_eq!(
                    items[0].expr,
                    ast::Expr::Property(
                        Box::new(ast::Expr::Variable("e".to_string())),
                        "action".to_string()
                    )
                );
                assert!(items[1].is_key);
                assert_eq!(
                    items[1].expr,
                    ast::Expr::Property(
                        Box::new(ast::Expr::Variable("e".to_string())),
                        "outcome".to_string()
                    )
                );
                assert!(!items[2].is_key);
                assert_eq!(items[2].alias, Some("support".to_string()));
            } else {
                panic!("Expected Yield output");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    #[test]
    fn test_locy_yield_key_with_alias() {
        let program =
            parse_locy("CREATE RULE r AS MATCH (e:Event) YIELD KEY e.action AS act").unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            if let locy_ast::RuleOutput::Yield(yc) = &rule.output {
                assert!(yc.items[0].is_key);
                assert_eq!(yc.items[0].alias, Some("act".to_string()));
            } else {
                panic!("Expected Yield output");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 16: QUERY (goal-directed)
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_goal_query() {
        let program = parse_locy("QUERY reachable WHERE a.name = 'Alice' RETURN b").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let locy_ast::LocyStatement::GoalQuery(gq) = &program.statements[0] {
            assert_eq!(gq.rule_name.parts, vec!["reachable"]);
            assert!(gq.return_clause.is_some());
        } else {
            panic!("Expected GoalQuery, got: {:?}", program.statements[0]);
        }
    }

    #[test]
    fn test_locy_goal_query_no_return() {
        let program = parse_locy("QUERY reachable WHERE a.name = 'Alice'").unwrap();
        if let locy_ast::LocyStatement::GoalQuery(gq) = &program.statements[0] {
            assert!(gq.return_clause.is_none());
        } else {
            panic!("Expected GoalQuery");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 17: ASSUME ... THEN
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_assume_block() {
        let program = parse_locy("ASSUME { CREATE (x:Temp) } THEN { MATCH (n) RETURN n }").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let locy_ast::LocyStatement::AssumeBlock(ab) = &program.statements[0] {
            assert_eq!(ab.mutations.len(), 1);
            assert!(matches!(&ab.mutations[0], ast::Clause::Create(_)));
            assert_eq!(ab.body.len(), 1);
            assert!(matches!(&ab.body[0], locy_ast::LocyStatement::Cypher(_)));
        } else {
            panic!("Expected AssumeBlock, got: {:?}", program.statements[0]);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 18: ABDUCE
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_abduce_query() {
        let program = parse_locy("ABDUCE NOT reachable WHERE a.name = 'Alice' RETURN b").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let locy_ast::LocyStatement::AbduceQuery(aq) = &program.statements[0] {
            assert!(aq.negated);
            assert_eq!(aq.rule_name.parts, vec!["reachable"]);
            assert!(aq.return_clause.is_some());
        } else {
            panic!("Expected AbduceQuery, got: {:?}", program.statements[0]);
        }
    }

    #[test]
    fn test_locy_abduce_query_positive() {
        let program = parse_locy("ABDUCE reachable WHERE a.name = 'Bob'").unwrap();
        if let locy_ast::LocyStatement::AbduceQuery(aq) = &program.statements[0] {
            assert!(!aq.negated);
            assert!(aq.return_clause.is_none());
        } else {
            panic!("Expected AbduceQuery");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 19: EXPLAIN RULE
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_explain_rule() {
        let program = parse_locy("EXPLAIN RULE reachable WHERE a.name = 'Alice'").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let locy_ast::LocyStatement::ExplainRule(eq) = &program.statements[0] {
            assert_eq!(eq.rule_name.parts, vec!["reachable"]);
            assert!(eq.return_clause.is_none());
        } else {
            panic!("Expected ExplainRule, got: {:?}", program.statements[0]);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Step 20: MODULE / USE
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_module_use() {
        let program =
            parse_locy("MODULE acme.compliance\nUSE acme.common\nMATCH (n) RETURN n").unwrap();
        assert!(program.module.is_some());
        assert_eq!(
            program.module.as_ref().unwrap().name.parts,
            vec!["acme", "compliance"]
        );
        assert_eq!(program.uses.len(), 1);
        assert_eq!(program.uses[0].name.parts, vec!["acme", "common"]);
        assert_eq!(program.statements.len(), 1);
        assert!(matches!(
            &program.statements[0],
            locy_ast::LocyStatement::Cypher(_)
        ));
    }

    #[test]
    fn test_locy_module_multiple_uses() {
        let program =
            parse_locy("MODULE mymod\nUSE dep1\nUSE dep2.sub\nMATCH (n) RETURN n").unwrap();
        assert_eq!(program.uses.len(), 2);
        assert_eq!(program.uses[0].name.parts, vec!["dep1"]);
        assert_eq!(program.uses[1].name.parts, vec!["dep2", "sub"]);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Bonus: Complex multi-clause rule
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_complex_rule_all_clauses() {
        let program = parse_locy(
            "CREATE RULE shortest_path PRIORITY 1 AS \
             MATCH (a)-[:EDGE {weight: w}]->(b) \
             WHERE a IS reachable TO b, w > 0 \
             ALONG dist = prev.dist + w \
             FOLD total = SUM(dist) \
             BEST BY dist ASC \
             YIELD KEY a, KEY b, dist",
        )
        .unwrap();
        if let locy_ast::LocyStatement::Rule(rule) = &program.statements[0] {
            assert_eq!(rule.name.parts, vec!["shortest_path"]);
            assert_eq!(rule.priority, Some(1));
            assert_eq!(rule.where_conditions.len(), 2);
            assert_eq!(rule.along.len(), 1);
            assert_eq!(rule.fold.len(), 1);
            let best_by = rule.best_by.as_ref().unwrap();
            assert_eq!(best_by.items.len(), 1);
            assert!(best_by.items[0].ascending);
            if let locy_ast::RuleOutput::Yield(yc) = &rule.output {
                let items = &yc.items;
                assert_eq!(items.len(), 3);
                assert!(items[0].is_key);
                assert!(items[1].is_key);
                assert!(!items[2].is_key);
            } else {
                panic!("Expected Yield");
            }
        } else {
            panic!("Expected Rule");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Bonus: DERIVE command (top-level)
    // ══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_locy_derive_command() {
        let program = parse_locy("DERIVE reachable WHERE a.name = 'Alice'").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let locy_ast::LocyStatement::DeriveCommand(dc) = &program.statements[0] {
            assert_eq!(dc.rule_name.parts, vec!["reachable"]);
            assert!(dc.where_expr.is_some());
        } else {
            panic!("Expected DeriveCommand, got: {:?}", program.statements[0]);
        }
    }

    #[test]
    fn test_locy_derive_command_no_where() {
        let program = parse_locy("DERIVE reachable").unwrap();
        if let locy_ast::LocyStatement::DeriveCommand(dc) = &program.statements[0] {
            assert!(dc.where_expr.is_none());
        } else {
            panic!("Expected DeriveCommand");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Phase B: CREATE MODEL (neural-predicate preview)
    // ══════════════════════════════════════════════════════════════════════

    fn expect_model(stmt: &locy_ast::LocyStatement) -> &locy_ast::ModelDefinition {
        match stmt {
            locy_ast::LocyStatement::Model(m) => m,
            other => panic!("expected Model, got {other:?}"),
        }
    }

    #[test]
    fn test_create_model_minimal() {
        // INPUT + OUTPUT + USING only.
        let program = parse_locy(
            "CREATE MODEL flag AS \
             INPUT (s) \
             OUTPUT PROB risk \
             USING xervo('classify/flag-v1')",
        )
        .unwrap();
        let m = expect_model(&program.statements[0]);
        assert_eq!(m.name.parts, vec!["flag"]);
        assert_eq!(m.inputs.len(), 1);
        assert_eq!(m.inputs[0].variable, "s");
        assert!(m.inputs[0].label.is_none());
        assert!(m.features.is_empty());
        assert_eq!(m.output.output_type, locy_ast::OutputType::Prob);
        assert_eq!(m.output.name, "risk");
        assert_eq!(m.xervo_alias, "classify/flag-v1");
        assert!(m.calibration.is_none());
        assert!(m.version.is_none());
        assert!(!m.annotations.independent);
    }

    #[test]
    fn test_create_model_full_with_calibration_version() {
        let program = parse_locy(
            "CREATE MODEL supplier_risk_scorer AS \
             INPUT (s:Supplier) \
             FEATURES s.country, s.annual_revenue \
             OUTPUT PROB risk \
             USING xervo('classify/supplier-risk-v3') \
             CALIBRATION platt_scaling \
             VERSION '3.1.0'",
        )
        .unwrap();
        let m = expect_model(&program.statements[0]);
        assert_eq!(m.inputs.len(), 1);
        assert_eq!(m.inputs[0].variable, "s");
        assert_eq!(m.inputs[0].label.as_deref(), Some("Supplier"));
        assert_eq!(m.features.len(), 2);
        assert_eq!(
            m.calibration,
            Some(locy_ast::CalibrationMethod::PlattScaling)
        );
        assert_eq!(m.version.as_deref(), Some("3.1.0"));
    }

    #[test]
    fn test_create_model_independent_annotation() {
        let program = parse_locy(
            "@independent CREATE MODEL m AS \
             INPUT (s) OUTPUT PROB risk USING xervo('classify/m')",
        )
        .unwrap();
        let m = expect_model(&program.statements[0]);
        assert!(m.annotations.independent);
    }

    #[test]
    fn test_create_model_multi_input() {
        let program = parse_locy(
            "CREATE MODEL pair AS \
             INPUT (a:User), (b:Item) \
             OUTPUT SCORE relevance \
             USING xervo('rerank/pair')",
        )
        .unwrap();
        let m = expect_model(&program.statements[0]);
        assert_eq!(m.inputs.len(), 2);
        assert_eq!(m.inputs[0].variable, "a");
        assert_eq!(m.inputs[0].label.as_deref(), Some("User"));
        assert_eq!(m.inputs[1].variable, "b");
        assert_eq!(m.inputs[1].label.as_deref(), Some("Item"));
        assert_eq!(m.output.output_type, locy_ast::OutputType::Score);
    }

    #[test]
    fn test_create_model_all_output_types() {
        for (kw, expected) in [
            ("PROB", locy_ast::OutputType::Prob),
            ("SCORE", locy_ast::OutputType::Score),
            ("LABEL", locy_ast::OutputType::Label),
            ("VECTOR", locy_ast::OutputType::Vector),
        ] {
            let src = format!("CREATE MODEL m AS INPUT (s) OUTPUT {kw} out USING xervo('test/m')");
            let p = parse_locy(&src).unwrap();
            let m = expect_model(&p.statements[0]);
            assert_eq!(m.output.output_type, expected, "kw {kw}");
        }
    }
}
