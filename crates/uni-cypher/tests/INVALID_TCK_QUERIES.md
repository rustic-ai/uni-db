// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call2.feature
// Expected error: InvalidArgumentPassingMode
// IGNORED: Semantic error - requires parentheses when YIELD is used with other clauses (RETURN). Our grammar intentionally allows optional parentheses.
CALL test.my.proc YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
// Expected error: UnexpectedSyntax
// IGNORED: Semantic error - YIELD * only valid in standalone calls, not with subsequent clauses. Our grammar intentionally allows YIELD * in all contexts.
CALL test.my.proc('Stefan', 1) YIELD *
RETURN city, country_code

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
// Expected error: InvalidRelationshipPattern
MATCH (a:A)
MATCH (a)-[:LIKES..]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
// Expected error: InvalidRelationshipPattern
MATCH (a:A)
MATCH (a)-[:LIKES*-2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
// Expected error: InvalidNumberLiteral
RETURN 9223372h54775808 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
// Expected error: UnexpectedSyntax
RETURN 9223372#54775808 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
// Expected error: InvalidNumberLiteral
RETURN 0x AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
// Expected error: InvalidNumberLiteral
RETURN 0x1A2b3j4D5E6f7 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
// Expected error: InvalidNumberLiteral
RETURN 0x1A2b3c4Z5E6f7 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
// Expected error: InvalidUnicodeLiteral
RETURN '\uH'

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
// Expected error: UnexpectedSyntax
RETURN [, ] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
// Expected error: UnexpectedSyntax
RETURN [[[]] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
// Expected error: UnexpectedSyntax
RETURN [[','[]',']] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
// Expected error: UnexpectedSyntax
RETURN {1B2c3e67:1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
// Expected error: UnexpectedSyntax
RETURN {k1#k: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
// Expected error: UnexpectedSyntax
RETURN {k1.k: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
// Expected error: UnexpectedSyntax
RETURN {, } AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
// Expected error: UnexpectedSyntax
RETURN {1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
// Expected error: UnexpectedSyntax
RETURN {[]} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
// Expected error: UnexpectedSyntax
RETURN {{}} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
// Expected error: UnexpectedSyntax
RETURN {k: {k: {}} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/mathematical/Mathematical3.feature
// Expected error: InvalidUnicodeCharacter
RETURN 42 — 41

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
// Expected error: Integer overflow
RETURN 9223372036854775808 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
// Expected error: Integer overflow
RETURN -9223372036854775809 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
// Expected error: Integer overflow
RETURN 0x8000000000000000 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
// Expected error: Integer overflow
RETURN -0x8000000000000001 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
// Expected error: Integer overflow
RETURN 0o1000000000000000000000 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
// Expected error: Integer overflow
RETURN -0o1000000000000000000001 AS literal