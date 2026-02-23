# Locy API Design for UniDB

**How UniDB exposes Locy reasoning to users**

Version 0.2 — February 2026

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [API Surface](#2-api-surface)
3. [The evaluate() Method](#3-the-evaluate-method)
4. [LocyResult](#4-locyresult)
5. [Catalog Introspection](#5-catalog-introspection)
6. [Configuration](#6-configuration)
7. [Engine Internals](#7-engine-internals)
8. [Cypher Procedures](#8-cypher-procedures)
9. [Python Bindings](#9-python-bindings)
10. [Usage Examples](#10-usage-examples)

---

## 1 Design Principles

### 1.1 The Language IS the API

Locy extends OpenCypher with new language constructs: `CREATE RULE`, `DERIVE`, `QUERY`, `ASSUME`, `ABDUCE`, `EXPLAIN RULE`, `USE`. These are **language-level** constructs, not API methods. They flow through a single entry point — the user writes a Locy program and submits it for evaluation. UniDB does not duplicate language constructs as separate Rust methods or Cypher procedures.

### 1.2 Why Not db.query() / db.execute()?

UniDB's existing Cypher surface splits into two methods:

| Method | Returns | Purpose |
|---|---|---|
| `db.query(cypher)` | `QueryResult` (rows) | Read — `MATCH ... RETURN` |
| `db.execute(cypher)` | `ExecuteResult` (affected count) | Write — `CREATE`, `DELETE`, `SET` |

This split doesn't fit Locy programs, which routinely mix both:

- A single program can `CREATE RULE` (registration), `DERIVE` (materialization), and `QUERY ... RETURN` (read results).
- `ASSUME { ... } THEN ... RETURN` looks like a read but involves mutations (that get rolled back).
- `ABDUCE` returns modification suggestions — neither a traditional read nor a write.
- `DERIVE` returns an affected count, but the user also wants iterations, convergence, and duration.

Forcing users to classify Locy programs as "query" or "execute" is the wrong cognitive burden. Locy programs are **evaluations** that may produce results, side effects, or both.

### 1.3 The Parallel with db.schema()

UniDB already has a pattern for namespaced functionality that doesn't fit query/execute:

```rust
db.schema()       // → SchemaBuilder  — define and inspect structure
db.query(cypher)  // → QueryResult    — read data
db.execute(cypher)// → ExecuteResult  — write data
```

Locy follows the same pattern:

```rust
db.locy()         // → LocyEngine     — reason over data
```

### 1.4 Naming: Why "locy()"

| Candidate | Problem |
|---|---|
| `db.reason()` | Too generic — could conflict with future probabilistic reasoning, LLM reasoning, etc. |
| `db.logic()` | Too generic — logic programming is a family, Locy is a specific language. |
| `db.rules()` | Collides with the concept of listing rules (the noun). |
| `db.locy()` | The language name. Short, unique, unambiguous. Same as saying "the Locy engine." |

---

## 2 API Surface

### 2.1 Complete Public Interface

```rust
impl Uni {
    // ── Existing (unchanged) ──
    pub fn schema(&self) -> SchemaBuilder<'_>;
    pub async fn query(&self, cypher: &str) -> Result<QueryResult>;
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;
    pub async fn begin(&self) -> Result<Transaction<'_>>;

    // ── New ──
    pub fn locy(&self) -> LocyEngine<'_>;
}
```

### 2.2 LocyEngine

```rust
pub struct LocyEngine<'a> {
    db: &'a Uni,
}

impl<'a> LocyEngine<'a> {
    // ── Primary entry point ──
    pub async fn evaluate(&self, program: &str) -> Result<LocyResult>;

    // ── Catalog introspection (read-only) ──
    pub fn rules(&self) -> Vec<RuleInfo>;
    pub fn rule(&self, name: &str) -> Option<RuleInfo>;
    pub fn stratification(&self) -> Vec<Stratum>;
    pub fn modules(&self) -> Vec<ModuleInfo>;
    pub fn last_eval_stats(&self) -> Option<EvalStats>;

    // ── Configuration ──
    pub fn config(&mut self) -> LocyConfigBuilder<'_>;
}
```

That's the entire public surface. Everything else is internal.

---

## 3 The evaluate() Method

### 3.1 Signature

```rust
impl<'a> LocyEngine<'a> {
    /// Evaluate a Locy program.
    ///
    /// Parses, compiles (stratification, wardedness, schema checks),
    /// and evaluates the program. Returns a rich result containing
    /// any query rows, materialization stats, and compile warnings.
    ///
    /// A Locy program may contain any combination of:
    /// - Rule definitions (CREATE RULE)
    /// - Module declarations (MODULE, USE)
    /// - Materialization commands (DERIVE)
    /// - Goal-directed queries (QUERY ... RETURN)
    /// - Hypothetical reasoning (ASSUME ... THEN)
    /// - Abductive reasoning (ABDUCE)
    /// - Proof traces (EXPLAIN RULE)
    /// - Standard Cypher statements
    pub async fn evaluate(&self, program: &str) -> Result<LocyResult>;
}
```

### 3.2 What It Does Internally

```
program (text)
  │
  ├─ 1. Parse (LocyParser)
  │     Detects Locy constructs vs pure Cypher.
  │     Produces LocyAST (superset of CypherAST).
  │
  ├─ 2. Compile (LocyCompiler)
  │     - Register rules in catalog
  │     - Compute dependency graph
  │     - Stratification
  │     - Wardedness analysis (for NEW)
  │     - Schema consistency checks (YIELD matching)
  │     - Type constraint validation
  │     - Emit compile warnings
  │
  ├─ 3. Evaluate (Orchestrator)
  │     For each statement in program order:
  │       CREATE RULE  → register in catalog (already done in compile)
  │       DERIVE       → bottom-up fixpoint via db.query_with() loop
  │       QUERY        → goal-directed via parameterized db.query_with()
  │       ASSUME       → savepoint + mutations + evaluate + rollback
  │       ABDUCE       → EXPLAIN + derivation tree cut analysis
  │       EXPLAIN RULE → instrumented fixpoint, build derivation tree
  │       Cypher stmt  → pass through to db.query() / db.execute()
  │
  └─ 4. Collect results into LocyResult
```

Every evaluation step internally calls `db.query_with()` and `db.execute_with()` with parameterized Cypher — exactly as the spec prescribes (§19.6). The orchestrator is a thin loop; the host engine does all the real work.

### 3.3 Parameterized Evaluation

```rust
impl<'a> LocyEngine<'a> {
    /// Evaluate with parameters (like db.query_with() for Cypher).
    pub fn evaluate_with(&self, program: &str) -> LocyEvalBuilder<'_>;
}

pub struct LocyEvalBuilder<'a> {
    engine: &'a LocyEngine<'a>,
    program: String,
    params: HashMap<String, Value>,
}

impl<'a> LocyEvalBuilder<'a> {
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.params.insert(name.to_string(), value.into());
        self
    }

    pub async fn run(self) -> Result<LocyResult> {
        // parameters are available to Cypher expressions within rules
        todo!()
    }
}
```

---

## 4 LocyResult

### 4.1 Definition

A Locy program may register rules, materialize facts, and return query rows — all in one submission. `LocyResult` captures everything that happened:

```rust
pub struct LocyResult {
    // ── Query output (from the terminal QUERY/RETURN/ABDUCE/EXPLAIN) ──

    /// Rows returned by the final result-producing statement.
    /// Empty if the program only registers rules or materializes.
    pub query_result: QueryResult,

    // ── Rule registration ──

    /// Rules that were created or updated by this evaluation.
    pub rules_created: Vec<String>,

    /// Rules that were dropped by this evaluation.
    pub rules_dropped: Vec<String>,

    // ── Materialization (DERIVE) ──

    /// Stats for each DERIVE command executed.
    pub derive_stats: Vec<DeriveStats>,

    // ── Hypothetical (ASSUME) ──

    /// Confirms that ASSUME blocks rolled back cleanly.
    pub assume_rollback_clean: bool,

    // ── Abduction (ABDUCE) ──

    /// Suggested modifications from ABDUCE.
    /// Also available as rows in query_result.
    pub abduce_modifications: Vec<Modification>,

    // ── Proof trace (EXPLAIN RULE) ──

    /// Derivation tree from EXPLAIN RULE.
    /// Also available as rows in query_result.
    pub derivation_tree: Option<DerivationTree>,

    // ── Compilation metadata ──

    /// Warnings from the compile phase.
    pub warnings: Vec<CompileWarning>,

    // ── Modules ──

    /// Modules loaded by this evaluation.
    pub modules_loaded: Vec<String>,
}
```

### 4.2 Supporting Types

```rust
/// Stats from a single DERIVE command
pub struct DeriveStats {
    pub rule_name: String,
    pub iterations: usize,
    pub facts_derived: usize,
    pub edges_created: usize,
    pub nodes_created: usize,       // from NEW
    pub nodes_merged: usize,        // from DERIVE MERGE
    pub converged: bool,
    pub duration: Duration,
}

/// A suggested graph modification from ABDUCE
pub struct Modification {
    /// The Cypher mutation statement
    /// e.g. "DELETE (a)-[:OWNS]->(b)" or "SET r.stake = 0.15"
    pub cypher: String,

    /// Number of atomic changes
    pub cost: usize,

    /// Human-readable description
    pub description: String,
}

/// Derivation tree from EXPLAIN RULE
pub struct DerivationTree {
    pub root: DerivationNode,
}

pub struct DerivationNode {
    pub rule_name: String,
    pub clause_index: usize,
    pub priority: Option<u32>,
    pub bindings: HashMap<String, Value>,
    pub along_values: HashMap<String, Value>,
    pub is_base_fact: bool,
    pub children: Vec<DerivationNode>,
}

/// Compile-time warning
pub struct CompileWarning {
    pub code: String,              // e.g. "MSUM_NON_NEGATIVITY"
    pub message: String,
    pub rule_name: Option<String>,
    pub line: Option<usize>,
}
```

### 4.3 Convenience Accessors

```rust
impl LocyResult {
    /// Shorthand: get the query rows (delegates to query_result).
    pub fn rows(&self) -> &[Row] {
        self.query_result.rows()
    }

    /// Shorthand: total facts derived across all DERIVE steps.
    pub fn total_facts_derived(&self) -> usize {
        self.derive_stats.iter().map(|s| s.facts_derived).sum()
    }

    /// Shorthand: total iterations across all DERIVE steps.
    pub fn total_iterations(&self) -> usize {
        self.derive_stats.iter().map(|s| s.iterations).sum()
    }

    /// Did all fixpoints converge?
    pub fn all_converged(&self) -> bool {
        self.derive_stats.iter().all(|s| s.converged)
    }

    /// Were there any compile warnings?
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}
```

---

## 5 Catalog Introspection

These are read-only queries about the current state of the rule catalog. They have no Locy language equivalent — there is no `LIST RULES` syntax, just as there is no `LIST TABLES` in SQL (you use `INFORMATION_SCHEMA` or procedures).

### 5.1 Methods

```rust
impl<'a> LocyEngine<'a> {
    /// List all registered rules.
    pub fn rules(&self) -> Vec<RuleInfo>;

    /// Get metadata for a specific rule.
    pub fn rule(&self, name: &str) -> Option<RuleInfo>;

    /// Get the computed stratification of all rules.
    pub fn stratification(&self) -> Vec<Stratum>;

    /// List loaded modules.
    pub fn modules(&self) -> Vec<ModuleInfo>;

    /// Get stats from the last evaluate() call.
    pub fn last_eval_stats(&self) -> Option<EvalStats>;
}
```

### 5.2 Types

```rust
/// Metadata about a registered rule
pub struct RuleInfo {
    pub name: String,
    pub qualified_name: Option<String>,
    pub module: Option<String>,
    pub clause_count: usize,
    pub is_recursive: bool,
    pub has_priority: bool,
    pub has_along: bool,
    pub has_fold: bool,
    pub has_monotonic_fold: bool,
    pub has_derive: bool,
    pub has_best_by: bool,
    pub yield_schema: Vec<YieldColumn>,
    pub stratum: usize,
}

pub struct YieldColumn {
    pub name: String,
    pub is_key: bool,
    pub data_type: Option<String>,
}

/// A stratum in the dependency graph
pub struct Stratum {
    pub index: usize,
    pub rules: Vec<String>,
    pub has_negation_dependency: bool,
    pub has_aggregation_dependency: bool,
}

/// Metadata about a loaded module
pub struct ModuleInfo {
    pub name: String,
    pub version: Option<String>,
    pub exported_rules: Vec<String>,
    pub total_rules: usize,
    pub source_path: Option<String>,
}

/// Aggregate evaluation statistics
pub struct EvalStats {
    pub total_iterations: usize,
    pub total_facts: usize,
    pub total_cypher_queries_emitted: usize,
    pub total_duration: Duration,
    pub per_stratum: Vec<StratumStats>,
}

pub struct StratumStats {
    pub stratum: usize,
    pub rules: Vec<String>,
    pub iterations: usize,
    pub facts_derived: usize,
    pub duration: Duration,
}
```

---

## 6 Configuration

Operational parameters that don't have Locy syntax:

```rust
impl<'a> LocyEngine<'a> {
    pub fn config(&mut self) -> LocyConfigBuilder<'_>;
}

pub struct LocyConfigBuilder<'a> { /* ... */ }

impl<'a> LocyConfigBuilder<'a> {
    /// Maximum fixpoint iterations before forced termination (default: 1000).
    pub fn max_iterations(self, n: usize) -> Self;

    /// Convergence tolerance for MSUM floating-point comparisons (default: 1e-10).
    pub fn msum_tolerance(self, epsilon: f64) -> Self;

    /// Time limit for ABDUCE search (default: 30s).
    pub fn abduce_timeout(self, duration: Duration) -> Self;

    /// Maximum tabling cache size for QUERY (default: 100_000 entries).
    pub fn tabling_cache_size(self, n: usize) -> Self;

    /// Whether to emit EXPLAIN-style profiling on every evaluate() (default: false).
    pub fn profile(self, enabled: bool) -> Self;

    /// Apply the configuration.
    pub fn apply(self) -> Result<()>;
}
```

---

## 7 Engine Internals

These are **not public API** — they are internal to the orchestrator. Documented here for implementors.

### 7.1 Transaction Savepoints

`ASSUME` blocks need savepoints for nested hypothetical reasoning. This is an internal engine primitive:

```rust
// pub(crate) — used by the orchestrator, not exposed to users
impl<'a> Transaction<'a> {
    pub(crate) async fn savepoint(&self, name: &str) -> Result<Savepoint<'_>>;
}

pub(crate) struct Savepoint<'a> {
    tx: &'a Transaction<'a>,
    name: String,
}

impl<'a> Savepoint<'a> {
    pub(crate) async fn query(&self, cypher: &str) -> Result<QueryResult>;
    pub(crate) async fn execute(&self, cypher: &str) -> Result<ExecuteResult>;
    pub(crate) async fn release(self) -> Result<()>;
    pub(crate) async fn rollback(self) -> Result<()>;
}
```

### 7.2 Orchestrator Loop

The orchestrator is internal to `evaluate()`. It calls existing UniDB APIs:

```rust
// Pseudocode for fixpoint evaluation inside evaluate()
async fn evaluate_derive(
    &self,
    rule: &CompiledRule,
    db: &Uni,
) -> Result<DeriveStats> {
    let mut accumulated = ResultSet::new();
    let mut delta = ResultSet::new();
    let mut iterations = 0;

    // Base case
    let base_cypher = rule.compile_base_case();
    let base_result = db.query_with(&base_cypher)
        .param("params", &rule.params)
        .fetch_all().await?;
    accumulated.extend(&base_result);
    delta = base_result;

    // Fixpoint loop
    loop {
        iterations += 1;
        let recursive_cypher = rule.compile_recursive_case();
        let new_facts = db.query_with(&recursive_cypher)
            .param("delta_targets", &delta.targets())
            .param("accumulated", &accumulated.pairs())
            .fetch_all().await?;

        let new_delta = new_facts.minus(&accumulated);
        if new_delta.is_empty() && !monotonic_aggregates_changed() {
            break; // converged
        }

        accumulated.extend(&new_delta);
        delta = new_delta;
    }

    Ok(DeriveStats { iterations, facts_derived: accumulated.len(), .. })
}
```

Every call in this loop is `db.query_with()` — standard parameterized Cypher. The host engine doesn't know it's participating in fixpoint computation.

---

## 8 Cypher Procedures

For users in the REPL, notebooks, or language bindings that only speak Cypher, catalog introspection is exposed as procedures. These mirror the Rust API on `LocyEngine` — they are **read-only introspection**, not duplicating language constructs.

```cypher
-- List all registered rules
CALL uni.locy.rules()
YIELD name, clause_count, is_recursive, has_priority, stratum, yield_schema

-- Inspect a specific rule
CALL uni.locy.rule('reachable')
YIELD clause_index, priority, has_along, has_fold, has_derive, source

-- View stratification
CALL uni.locy.stratification()
YIELD stratum, rules, has_negation_dependency

-- List loaded modules
CALL uni.locy.modules()
YIELD name, version, exported_rules, rule_count

-- Stats from the last evaluation
CALL uni.locy.evalStats()
YIELD rule_name, iterations, facts_derived, duration_ms, converged
```

Note: `CREATE RULE`, `DERIVE`, `QUERY`, `ASSUME`, `ABDUCE`, and `EXPLAIN RULE` are **not** procedures — they are Locy language constructs evaluated via `db.locy().evaluate()`.

---

## 9 Python Bindings

```python
import uni_db

db = uni_db.Database("/path/to/db")

# Evaluate a Locy program
result = db.locy().evaluate("""
    CREATE RULE reachable AS
      MATCH (a:Person)-[:KNOWS]->(b:Person)
      YIELD a, b

    CREATE RULE reachable AS
      MATCH (a:Person)-[:KNOWS]->(mid:Person)
      WHERE mid IS reachable TO b
      YIELD a, b

    DERIVE reachable

    QUERY reachable
      WHERE a.name = 'Alice'
      RETURN b.name AS person
""")

# Query rows
for row in result.rows():
    print(row["person"])

# Materialization stats
for stat in result.derive_stats:
    print(f"{stat.rule_name}: {stat.facts_derived} facts in {stat.iterations} iterations")

# Catalog introspection
rules = db.locy().rules()
for r in rules:
    print(f"{r.name}: {r.clause_count} clauses, stratum {r.stratum}")
```

---

## 10 Usage Examples

### 10.1 Basic: Define and Query Rules

```rust
let db = Uni::in_memory().build().await?;

// Setup graph
db.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})-[:KNOWS]->(c:Person {name:'Carol'})").await?;

// Evaluate Locy program
let result = db.locy().evaluate("
    CREATE RULE reachable AS
      MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD a, b
    CREATE RULE reachable AS
      MATCH (a:Person)-[:KNOWS]->(mid:Person)
      WHERE mid IS reachable TO b YIELD a, b

    DERIVE reachable

    QUERY reachable WHERE a.name = 'Alice' RETURN b.name AS person
").await?;

for row in result.rows() {
    println!("{}", row.get::<String>("person")?);
}
// Bob
// Carol

println!("Derived {} facts in {} iterations",
    result.total_facts_derived(),
    result.total_iterations());
```

### 10.2 Hypothetical: What-If Analysis

```rust
let result = db.locy().evaluate("
    CREATE RULE cascade AS
      MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
      WHERE b.status = 'DOWN' YIELD a, b
    CREATE RULE cascade AS
      MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
      WHERE b IS cascade YIELD a, b

    ASSUME {
      MATCH (s:Service {name: 'postgres-primary'})
      SET s.status = 'DOWN'
    }
    THEN
      MATCH (svc:Service) WHERE svc IS cascade
      RETURN svc.name AS affected, svc.tier
      ORDER BY svc.tier
").await?;

// Results show cascading impact — but the graph is unchanged
for row in result.rows() {
    println!("{}: {}", row.get::<String>("affected")?, row.get::<String>("svc.tier")?);
}
assert!(result.assume_rollback_clean);
```

### 10.3 Incremental: Rules Persist Across Evaluations

```rust
// First evaluation: define rules
db.locy().evaluate("
    CREATE RULE reachable AS
      MATCH (a)-[:KNOWS]->(b) YIELD a, b
    CREATE RULE reachable AS
      MATCH (a)-[:KNOWS]->(mid) WHERE mid IS reachable TO b YIELD a, b
").await?;

// Rules are now in the catalog
assert_eq!(db.locy().rules().len(), 1); // "reachable" (2 clauses)

// Second evaluation: use previously defined rules
let result = db.locy().evaluate("
    DERIVE reachable
    QUERY reachable WHERE a.name = 'Alice' RETURN b.name AS person
").await?;
```

### 10.4 Configuration

```rust
db.locy().config()
    .max_iterations(500)
    .msum_tolerance(1e-8)
    .abduce_timeout(Duration::from_secs(10))
    .apply()?;

let result = db.locy().evaluate("...").await?;
```

### 10.5 Introspection After Evaluation

```rust
db.locy().evaluate("
    CREATE RULE control AS ...
    CREATE RULE sanctioned_exposure AS ...
    DERIVE control
").await?;

// Inspect the rule catalog
for rule in db.locy().rules() {
    println!("{}: {} clauses, recursive={}, stratum={}",
        rule.name, rule.clause_count, rule.is_recursive, rule.stratum);
}

// Inspect stratification
for stratum in db.locy().stratification() {
    println!("Stratum {}: {:?}", stratum.index, stratum.rules);
}

// Check last evaluation performance
if let Some(stats) = db.locy().last_eval_stats() {
    println!("Emitted {} Cypher queries in {:?}",
        stats.total_cypher_queries_emitted, stats.total_duration);
}
```

---

## Appendix: API Summary

| Surface | Method | Purpose |
|---|---|---|
| **Entry point** | `db.locy()` | Access the Locy engine |
| **Evaluation** | `.evaluate(program)` | Parse, compile, evaluate a Locy program |
| **Evaluation** | `.evaluate_with(program).param().run()` | Parameterized evaluation |
| **Introspection** | `.rules()` | List registered rules |
| **Introspection** | `.rule(name)` | Get specific rule metadata |
| **Introspection** | `.stratification()` | View dependency strata |
| **Introspection** | `.modules()` | List loaded modules |
| **Introspection** | `.last_eval_stats()` | Stats from last evaluate() |
| **Configuration** | `.config()` | Set operational parameters |
| **Procedures** | `CALL uni.locy.rules()` | Cypher access to introspection |
| **Procedures** | `CALL uni.locy.stratification()` | Cypher access to strata |
| **Procedures** | `CALL uni.locy.evalStats()` | Cypher access to eval stats |
| **Internal** | `tx.savepoint()` | Engine primitive for ASSUME (pub(crate)) |
