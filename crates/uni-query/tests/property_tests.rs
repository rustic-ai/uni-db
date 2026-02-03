// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use proptest::prelude::*;

fn valid_identifier_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]*".prop_filter("Reserved words", |s| {
        let reserved = [
            "MATCH",
            "RETURN",
            "WHERE",
            "CREATE",
            "DELETE",
            "SET",
            "MERGE",
            "WITH",
            "LIMIT",
            "SKIP",
            "ORDER",
            "BY",
            "AS",
            "AND",
            "OR",
            "XOR",
            "NOT",
            "IN",
            "IS",
            "NULL",
            "TRUE",
            "FALSE",
            "CALL",
            "YIELD",
            "UNWIND",
            "OPTIONAL",
            "DETACH",
            "REMOVE",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "IF",
            "CONTAINS",
            "STARTS",
            "ENDS",
            "ON",
            "FROM",
            "TO",
            "DISTINCT",
            "ASC",
            "DESC",
            "UNION",
            "DROP",
            "ALTER",
            "SHOW",
            "OVER",
            "PARTITION",
            "EXPLAIN",
            "LOAD",
            "CSV",
            "HEADERS",
            "RECURSIVE",
            "EACH",
        ];
        !reserved.contains(&s.to_uppercase().as_str())
    })
}

fn valid_query_strategy() -> impl Strategy<Value = String> {
    valid_identifier_strategy().prop_map(|var| format!("MATCH ({}) RETURN {}", var, var))
}

proptest! {
    #[test]
    fn parse_valid_queries(query in valid_query_strategy()) {
        let res = uni_query::parse_cypher(&query);
        prop_assert!(res.is_ok(), "Query failed to parse: {}", query);
    }

    #[test]
    fn parse_random_noise_fails(s in "\\PC*") {
        // Random unicode strings should mostly fail, but if they happen to match valid cypher, that's fine.
        // But generally garbage shouldn't panic.
        let _ = uni_query::parse_cypher(&s);
        // We just assert it doesn't crash
    }
}
