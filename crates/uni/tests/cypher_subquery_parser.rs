// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

#[test]
fn test_cypher_subquery_parser() -> anyhow::Result<()> {
    // Just test parsing for now
    let query = "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN n";
    let ast = uni_query::parse_cypher(query);
    assert!(ast.is_ok());

    let query = "CALL { MATCH (n:Person) RETURN n.name } RETURN n.name";
    let ast = uni_query::parse_cypher(query);
    assert!(ast.is_ok());

    Ok(())
}
