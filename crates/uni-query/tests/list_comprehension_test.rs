use uni_cypher::parse;

#[test]
fn test_parse_list_comprehension_basic() {
    let query = "RETURN [x IN [1, 2, 3] | x * 2] AS doubled";
    let result = parse(query);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_parse_list_comprehension_with_where() {
    let query = "RETURN [x IN [1, 2, 3, 4, 5] WHERE x % 2 = 0 | x * x] AS even_squares";
    let result = parse(query);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_parse_list_comprehension_complex() {
    let query = "RETURN [x IN range(1, 10) WHERE x > 5 | x + 100] AS result";
    let result = parse(query);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_parse_list_literal_still_works() {
    // Ensure we didn't break list literals
    let query = "RETURN [1, 2, 3] AS numbers";
    let result = parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse list literal: {:?}",
        result.err()
    );
}

#[test]
fn test_parse_list_literal_with_identifiers() {
    // List literals with identifiers should still work
    let query = "MATCH (n) RETURN [n.name, n.age] AS props";
    let result = parse(query);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_parse_list_literal_complex_expressions() {
    // List literals with complex expressions (use parentheses for binary ops)
    let query = "RETURN [(x + 1), (y * 2), z.prop] AS values";
    let result = parse(query);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_parse_nested_list_comprehension() {
    // Nested list comprehension: outer comprehension contains inner comprehension
    let query = "RETURN [x IN [[1, 2], [3, 4]] | [y IN x | y * 2]] AS nested";
    let result = parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse nested comprehension: {:?}",
        result.err()
    );
}
