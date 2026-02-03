# uni-tck: openCypher TCK Test Harness for Uni

Complete Technology Compatibility Kit (TCK) test harness for validating Uni's openCypher compliance against the M23 specification.

## Overview

This crate implements a comprehensive test harness that runs all 1,339 TCK scenarios across 220 feature files to verify Uni's compliance with the openCypher specification.

## Structure

```
crates/uni-tck/
├── src/
│   ├── world.rs           # UniWorld - test state management
│   ├── steps/             # Cucumber step definitions
│   │   ├── given.rs       # Setup steps (empty graph, named graphs, etc.)
│   │   ├── when_step.rs   # Execution steps (query execution)
│   │   ├── then.rs        # Assertion steps (result validation)
│   │   └── and.rs         # Side effect verification
│   ├── parser/            # TCK syntax parser
│   │   ├── value.rs       # Parse TCK values (nodes, edges, paths, scalars)
│   │   └── table.rs       # Parse Gherkin tables
│   ├── matcher/           # Result comparison
│   │   ├── result.rs      # Query result matching (order-sensitive & agnostic)
│   │   └── error.rs       # Error classification and matching
│   └── fixtures/          # Named graph loaders
│       └── binary_tree.rs # Example fixtures
├── tests/
│   └── cucumber.rs        # Test runner
└── features/              # 220 TCK feature files (copied from grammar/tck-M23)
    ├── clauses/           # MATCH, CREATE, WHERE, etc.
    ├── expressions/       # Literals, operators, functions
    └── useCases/          # Real-world scenarios
```

## Running Tests

### Run all TCK tests
```bash
cargo test -p uni-tck --test cucumber
```

### Run specific feature file
```bash
cargo test -p uni-tck --test cucumber -- features/expressions/literals/Literals1.feature
```

### Run specific scenario
```bash
cargo test -p uni-tck --test cucumber -- "Return a boolean true"
```

## Implementation Status

### ✅ Completed (Phase 1)

1. **Crate Structure**
   - All dependencies configured
   - Module organization in place
   - Test runner infrastructure

2. **UniWorld State Management**
   - In-memory database initialization
   - Query result/error tracking
   - Side effects capture (nodes, edges, labels)
   - Parameter storage

3. **Step Definitions**
   - **Given**: `an empty graph`, `any graph`, `the (.+) graph`, `having executed:`
   - **When**: `executing query:`, `executing query with parameters:`
   - **Then**: `result should be empty`, `result should be in order/any order`, `error should be raised`
   - **And**: `no side effects`, `side effects should be:`

4. **Value Parser** (nom-based)
   - Scalars: null, bool, int, float, string
   - Collections: lists, maps
   - Graph types: nodes, edges, paths (basic)
   - Escape sequences in strings

5. **Result Matcher**
   - Order-sensitive comparison
   - Order-agnostic comparison (multiset algorithm)
   - Float epsilon comparison (NaN/Infinity handling)
   - Recursive value comparison
   - Graph type comparison (nodes, edges, paths)

6. **Error Matcher**
   - Phase classification (compile-time vs runtime)
   - Error type mapping (SyntaxError, TypeError, etc.)
   - Detail code substring matching

7. **Named Graph Fixtures**
   - Registry pattern
   - Placeholder implementations for binary trees
   - Extensible design

8. **Feature Files**
   - All 220 TCK M23 feature files copied
   - 1,339 scenarios available

9. **Test Runner**
   - Cucumber integration
   - Tracing setup
   - Libtest-compatible output

### 🚧 TODO (Phase 2+)

1. **Complete Step Implementations**
   - Parse Gherkin tables in `then` steps
   - Implement actual result comparison using matcher
   - Implement error matching using error matcher
   - Parse and apply query parameters in `when` steps
   - Parse and verify side effects table

2. **Enhance Value Parser**
   - Complete path syntax parsing (`<n0-[r1]->n1-[r2]->n2>`)
   - Handle all TCK edge cases
   - Support temporal types (if needed)

3. **Implement Named Fixtures**
   - Discover all fixture types from feature files
   - Implement each named graph fixture properly
   - Match exact TCK requirements

4. **Improve Error Handling**
   - Map all UniError variants to TCK error types
   - Handle error detail codes precisely
   - Test error scenarios thoroughly

5. **Optimization**
   - Parallel scenario execution
   - Caching for common fixtures
   - Performance profiling

## Current Limitations

Many step implementations use `todo!()` for:
- Table parsing and comparison (in `then` steps)
- Parameter handling (in `when` steps)
- Named graph fixtures (only stubs exist)
- Side effects table parsing

These will cause tests to panic when reached, which is expected for Phase 1.

## Example TCK Scenario

```gherkin
Scenario: Return a boolean true lower case
  Given any graph
  When executing query:
    """
    RETURN true AS literal
    """
  Then the result should be, in any order:
    | literal |
    | true    |
  And no side effects
```

## Development Workflow

1. **Run subset of tests** to identify which scenarios work
2. **Fix parsers/matchers** based on failures
3. **Implement missing fixtures** as needed
4. **Iterate** until compliance reaches target %

## Success Criteria

- ✅ All 220 feature files load without parse errors
- ✅ Test runner executes scenarios
- ⏳ Clear pass/fail reporting per scenario
- ⏳ No crashes (only expected test failures)
- ⏳ Can identify which Cypher features need implementation

## Dependencies

- `cucumber` - BDD test framework
- `nom` - Parser combinator library
- `uni-db` - Uni database
- `uni-query` - Query types and execution
- `uni-common` - Common error types

## License

Apache-2.0
