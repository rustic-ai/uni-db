pub mod ast;
mod grammar;

pub use grammar::{ParseError, parse, parse_expression};

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
            assert_eq!(rel.types, vec!["KNOWS", "HATES"]);
            println!("Parsed types: {:?}", rel.types);
            return;
        }
        panic!("Could not find relationship pattern with OR types");
    }
}
