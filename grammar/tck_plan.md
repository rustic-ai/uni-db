# openCypher TCK Test Harness Implementation Plan

## Goal
Implement a complete TCK (Technology Compatibility Kit) test harness for the uni graph database to validate full openCypher compliance against all 1,339 scenarios from the M23 specification.

## User Requirements
- **Full compliance**: Run all TCK tests, fail fast on unsupported features
- **Location**: New crate `crates/uni-tck/`
- **Feature files**: Copy from `grammar/tck-M23/tck/features/` to `crates/uni-tck/features/`
- **CI**: Later phase - get working locally first

## Architecture Overview

```
crates/uni-tck/
├── Cargo.toml              # Dependencies: cucumber, nom, uni-db
├── src/
│   ├── lib.rs              # Re-exports
│   ├── world.rs            # UniWorld state (db, results, errors)
│   ├── steps/              # Given/When/Then/And implementations
│   ├── parser/             # TCK syntax → uni::Value
│   ├── matcher/            # Result/error comparison
│   ├── fixtures/           # Named graph loaders
│   └── reporting.rs        # Metrics collection
├── features/               # Copied TCK .feature files
└── tests/
    └── cucumber.rs         # Test runner
```

## Implementation Steps

### 1. Create Crate Structure

**Files to create:**
- `crates/uni-tck/Cargo.toml`
- `crates/uni-tck/src/lib.rs`
- `crates/uni-tck/src/world.rs`
- `crates/uni-tck/tests/cucumber.rs`

**Workspace update:**
- Add `"crates/uni-tck"` to workspace members in root `Cargo.toml`

**Dependencies to add:**
```toml
[dependencies]
uni-db = { workspace = true }
uni-query = { workspace = true }
uni-common = { workspace = true }
cucumber = "0.21"
async-trait = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }
anyhow = { workspace = true }
thiserror = { workspace = true }
regex = { workspace = true }
indexmap = "2.0"
nom = "7.1"

[[test]]
name = "cucumber"
harness = false
```

### 2. Implement UniWorld State Management

**File**: `crates/uni-tck/src/world.rs`

**Key types:**
```rust
#[derive(Debug, World)]
#[world(init = Self::new)]
pub struct UniWorld {
    db: Arc<Uni>,                      // In-memory database
    last_result: Option<QueryResult>,  // Last query result
    last_error: Option<UniError>,      // Last error
    side_effects: SideEffects,         // Before/after counts
    params: HashMap<String, Value>,    // Query parameters
}

pub struct SideEffects {
    nodes_before: usize,
    nodes_after: usize,
    edges_before: usize,
    edges_after: usize,
    properties_before: usize,
    properties_after: usize,
    labels_before: HashSet<String>,
    labels_after: HashSet<String>,
}
```

**API needed from uni:**
- `Uni::in_memory().build()` - Create temp database
- `db.query(cypher)` - Execute queries
- `db.execute(cypher)` - Mutations
- `db.list_labels()` - Get labels for side effects

**Responsibilities:**
- Create fresh database per scenario
- Track graph state before/after mutations
- Store last result OR error (mutually exclusive)
- Implement helper methods: `count_nodes()`, `count_edges()`, `count_properties()`

### 3. Implement Step Definitions

**Module structure:**
```
src/steps/
├── mod.rs      # Re-exports
├── given.rs    # Setup steps
├── when.rs     # Execution steps
├── then.rs     # Assertion steps
└── and.rs      # Side effect steps
```

**Given steps** (`src/steps/given.rs`):
- `#[given("an empty graph")]` - Default state (already empty)
- `#[given("any graph")]` - No setup needed
- `#[given(regex = r"^the (.+) graph$")]` - Load named fixture
- `#[given("having executed:")]` - Run setup query from docstring

**When steps** (`src/steps/when.rs`):
- `#[when("executing query:")]` - Execute query, capture result/error
- `#[when(regex = r"^executing query with parameters (.+):$")]` - Parameterized query
- Must call `world.capture_state_before()` before execution
- Must call `world.capture_state_after()` after successful execution

**Then steps** (`src/steps/then.rs`):
- `#[then("the result should be empty")]` - Assert zero rows
- `#[then(regex = r"^the result should be, in any order:$")]` - Order-agnostic match
- `#[then(regex = r"^the result should be, in order:$")]` - Order-sensitive match
- `#[then(regex = r"^a (\w+) should be raised at (compile time|runtime): (\w+)$")]` - Error match

**And steps** (`src/steps/and.rs`):
- `#[then("no side effects")]` - Assert no changes
- `#[then(regex = r"^the side effects should be:$")]` - Verify counts (+nodes, -edges, etc.)

### 4. Implement Value Parser

**Module structure:**
```
src/parser/
├── mod.rs      # Re-exports
├── value.rs    # Main parser entry point
├── node.rs     # Node pattern parser
├── edge.rs     # Edge pattern parser
├── path.rs     # Path pattern parser
└── table.rs    # Gherkin table parser
```

**Parser strategy**: Use `nom` combinator library

**TCK syntax → uni types:**
- `null` → `Value::Null`
- `true`/`false` → `Value::Bool(bool)`
- `123` → `Value::Int(i64)`
- `3.14` → `Value::Float(f64)`
- `'string'` → `Value::String(String)`
- `[1, 2, 3]` → `Value::List(Vec<Value>)`
- `{key: val}` → `Value::Map(HashMap<String, Value>)`
- `(:Label {prop: val})` → `Value::Node(Node)`
- `[:TYPE {prop: val}]` → `Value::Edge(Edge)`
- `<n0-[r1]->n1>` → `Value::Path(Path)`

**Node structure** (from `uni-query/src/types.rs`):
```rust
pub struct Node {
    pub vid: Vid,
    pub label: String,
    pub properties: HashMap<String, Value>,
}
```

**Edge structure**:
```rust
pub struct Edge {
    pub eid: Eid,
    pub edge_type: String,
    pub src: Vid,
    pub dst: Vid,
    pub properties: HashMap<String, Value>,
}
```

**Path structure**:
```rust
pub struct Path {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}
```

### 5. Implement Result Matcher

**File**: `src/matcher/result.rs`

**Algorithm**:
1. Check row count matches
2. Check column names match
3. If `order_sensitive`:
   - Compare row-by-row in order
4. Else (order-agnostic):
   - Build multiset of actual rows
   - For each expected row, find and remove match
   - Verify no unmatched rows remain

**Value comparison**:
- Null: exact match
- Bool: exact match
- Int: exact match
- Float: epsilon comparison (1e-10)
- String: exact match
- List: recursive comparison (order-sensitive)
- Map: recursive comparison (order-agnostic on keys)
- Node: compare labels (order-agnostic) + properties
- Edge: compare type + properties
- Path: compare nodes and edges arrays

**Special cases**:
- NaN equals NaN
- Empty results must both be empty
- Column order doesn't matter (only values)

### 6. Implement Error Matcher

**File**: `src/matcher/error.rs`

**Error classification** (from `uni-common/src/api/error.rs`):

**Compile-time errors:**
- `UniError::Parse { ... }` - Syntax errors
- `UniError::Query { ... }` - Semantic/planning errors
- `UniError::LabelNotFound { ... }` - Schema errors
- `UniError::EdgeTypeNotFound { ... }` - Schema errors

**Runtime errors:**
- `UniError::Type { ... }` - Type mismatches
- `UniError::Constraint { ... }` - Constraint violations
- `UniError::PropertyNotFound { ... }` - Missing properties
- `UniError::Query { ... }` - Execution errors

**TCK error types → UniError mapping:**
- `SyntaxError` → `UniError::Parse`
- `TypeError` → `UniError::Type`
- `SemanticError` → `UniError::Query`
- `ConstraintValidationFailed` → `UniError::Constraint`
- `EntityNotFound` → `UniError::LabelNotFound | EdgeTypeNotFound`
- `PropertyNotFound` → `UniError::PropertyNotFound`

**Matcher logic**:
1. Verify phase (compile vs runtime)
2. Match error type to UniError variant
3. Check detail code (message substring match)

### 7. Implement Named Graph Fixtures

**File**: `src/fixtures/graphs.rs`

**Registry pattern**:
```rust
pub async fn load_graph(db: &Uni, name: &str) -> Result<()> {
    match name {
        "binary-tree-1" => binary_tree::load_binary_tree_1(db).await,
        "binary-tree-2" => binary_tree::load_binary_tree_2(db).await,
        _ => Err(anyhow::anyhow!("Unknown graph: {}", name)),
    }
}
```

**Implementation strategy**:
- Phase 1: Stub out with error
- Phase 2: Implement graphs as tests fail
- Discovery: Grep feature files for `Given the (.+) graph`

**Binary tree example** (`src/fixtures/binary_tree.rs`):
```rust
pub async fn load_binary_tree_1(db: &Uni) -> Result<()> {
    // Create schema
    db.execute("CREATE LABEL A").await?;
    db.execute("CREATE LABEL B").await?;
    db.execute("CREATE LABEL C").await?;
    db.execute("CREATE EDGE_TYPE KNOWS").await?;
    db.execute("CREATE EDGE_TYPE FOLLOWS").await?;

    // Create nodes and edges
    db.execute("CREATE (a:A {name: 'a'})").await?;
    db.execute("CREATE (b2:B {name: 'b2'})").await?;
    // ... etc

    Ok(())
}
```

### 8. Copy Feature Files

**Command**:
```bash
cp -r grammar/tck-M23/tck/features crates/uni-tck/
```

**Structure preserved**:
```
crates/uni-tck/features/
├── clauses/
│   ├── call/
│   ├── create/
│   ├── match/
│   └── ... (17 total)
├── expressions/
│   ├── aggregation/
│   ├── boolean/
│   └── ... (18 total)
└── useCases/
```

### 9. Implement Test Runner

**File**: `crates/uni-tck/tests/cucumber.rs`

```rust
use cucumber::{World, writer};
use uni_tck::UniWorld;

#[tokio::main]
async fn main() {
    UniWorld::cucumber()
        .with_writer(
            writer::Libtest::or_basic()
                .summarized()
                .assert_normalized(),
        )
        .before(|_feature, _rule, _scenario, _world| {
            Box::pin(async move {
                tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::WARN)
                    .init();
            })
        })
        .run("features/")
        .await;
}
```

**Execution**:
```bash
cargo test -p uni-tck --test cucumber
```

### 10. Implement Reporting

**File**: `src/reporting.rs`

**Metrics to collect**:
- Total scenarios: passed/failed/skipped
- Per-feature breakdown
- Error type distribution
- Execution duration

**Output formats**:
- Console summary (via cucumber default output)
- JSON report for CI integration

## Critical Files Reference

**uni API:**
- `crates/uni/src/api/impl_query.rs` - Query execution entry points
- `crates/uni/src/api/mod.rs` - Uni struct and builder

**Types:**
- `crates/uni-query/src/types.rs` - Value, Node, Edge, Path definitions
- `crates/uni-common/src/api/error.rs` - UniError variants

**TCK source:**
- `grammar/tck-M23/tck/features/` - Feature files to copy
- `grammar/tck-M23/tck/index.adoc` - TCK documentation

## Implementation Phases

**Phase 1: Foundation** (First session)
- Create crate structure
- Implement UniWorld with basic state
- Stub step definitions (all return unimplemented!())
- Get cucumber runner compiling
- Copy feature files
- **Deliverable**: `cargo test -p uni-tck` runs (but fails scenarios)

**Phase 2: Core Steps** (Next session)
- Implement Given/When/Then steps for simple scenarios
- Basic value parser (scalars only)
- Simple result matcher (scalar comparisons)
- **Target**: 10+ scenarios passing (simple RETURN tests)

**Phase 3: Graph Types** (Next session)
- Implement Node/Edge/Path parsers
- Graph value comparison in matcher
- Side effect tracking
- **Target**: 50+ scenarios passing (basic MATCH/CREATE)

**Phase 4: Fixtures & Errors** (Next session)
- Named graph fixtures
- Error matcher implementation
- Parameterized query support
- **Target**: 200+ scenarios passing

**Phase 5: Full Coverage** (Ongoing)
- Fix edge cases
- Handle all TCK syntax variations
- Optimize performance
- **Target**: 1000+ scenarios passing (75%+ compliance)

## Verification Steps

After implementation:

1. **Smoke test**: `cargo test -p uni-tck --test cucumber -- features/expressions/literals/Literals1.feature`
2. **Full run**: `cargo test -p uni-tck --test cucumber`
3. **Check output**: Should see pass/fail counts per feature
4. **Validate parsing**: Ensure no "unimplemented" panics, only real failures
5. **Review failures**: Categorize by missing features vs bugs

## Success Criteria

- ✅ All 220 feature files load without parse errors
- ✅ Test runner executes all 1,339 scenarios
- ✅ Clear pass/fail reporting per scenario
- ✅ No crashes or panics (only expected test failures)
- ✅ Can identify which Cypher features need implementation
- ✅ Can run subsets: `cargo test -p uni-tck --test cucumber -- features/clauses/match/`

## Known Challenges

1. **TCK syntax variations**: May need parser adjustments as edge cases discovered
2. **Named graphs**: Need to discover all fixture types used
3. **Temporal types**: May not be fully supported in uni yet
4. **Procedures**: CALL clause support may be limited
5. **Float precision**: Need consistent epsilon comparison

## Next Steps After Initial Implementation

1. Generate compliance report (% passing by feature)
2. Prioritize failing features for uni development
3. Add CI workflow (`.github/workflows/tck.yml`)
4. Track compliance over time
5. Compare with other openCypher implementations
