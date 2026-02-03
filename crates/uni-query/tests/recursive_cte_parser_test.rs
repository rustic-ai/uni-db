// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_cypher::ast::{Clause, Query};

#[test]
fn test_parse_recursive_cte() {
    let input = "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Node) WHERE root.id = 0 RETURN root
            UNION
            MATCH (parent:Node)-[:CHILD]->(child:Node)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n) WHERE n IN hierarchy RETURN n
    ";

    let query = uni_query::parse_cypher(input).unwrap();

    if let Query::Single(stmt) = query {
        // First clause should be WithRecursive
        if let Clause::WithRecursive(cte) = &stmt.clauses[0] {
            assert_eq!(cte.name, "hierarchy");

            // Check that query is a Union (anchor + recursive)
            if let Query::Union { left, right, .. } = &*cte.query {
                // Check anchor part has a MATCH clause
                if let Query::Single(anchor) = &**left {
                    if let Clause::Match(_) = &anchor.clauses[0] {
                        // OK
                    } else {
                        panic!("Expected anchor MATCH clause");
                    }
                } else {
                    panic!("Expected Single Query for anchor");
                }

                // Check recursive part has a MATCH clause
                if let Query::Single(recursive) = &**right {
                    if let Clause::Match(_) = &recursive.clauses[0] {
                        // OK
                    } else {
                        panic!("Expected recursive MATCH clause");
                    }
                } else {
                    panic!("Expected Single Query for recursive part");
                }
            } else {
                panic!("Expected Union in CTE query");
            }
        } else {
            panic!("Expected WithRecursive clause");
        }

        // Second clause should be Match
        if let Clause::Match(_) = &stmt.clauses[1] {
            // OK
        } else {
            panic!("Expected main query Match");
        }
    } else {
        panic!("Expected Single Query");
    }
}
