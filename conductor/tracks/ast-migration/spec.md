# Plan: True AST Migration - Eliminate Legacy AST Completely

## Context

**Current State (After Lazy Refactor):**
```
Parser → uni_cypher::ast::Query
    ↓
ast_convert::convert_*() [conversion layer]
    ↓
Legacy AST (Expr, Clause, etc.)
    ↓
LogicalPlan (contains legacy Expr)
    ↓
Executor (evaluates legacy Expr)
```

**Problem:** We just renamed the adapter to `ast_convert` but didn't actually eliminate the conversion. The planner still immediately converts to legacy AST and everything downstream uses legacy types.

**Target State (True Migration):**
```
Parser → uni_cypher::ast::Query
    ↓
Planner → LogicalPlan (contains uni_cypher::ast::Expr)
    ↓
Executor → evaluate(uni_cypher::ast::Expr)
    ↓
No conversion layer, no legacy AST
```

## Principle

**Only use `uni_cypher::ast` types throughout the system.** The legacy AST should be completely removed.

## Scope Analysis

### Files Affected

| File | Lines | Changes Required | Risk |
|------|-------|------------------|------|
| `planner.rs` | 3,971 | Update LogicalPlan definition, 91 Expr usage sites | **HIGH** |
| `executor/read.rs` | 4,988 | Update evaluate_expr (21 variants) | **HIGH** |
| `executor/write.rs` | 1,182 | Update pattern matching | **MEDIUM** |
| `expr_eval.rs` | ~500 | Update operator enums | **MEDIUM** |
| `df_expr.rs` | ~500 | Update DataFusion translation | **MEDIUM** |
| `ast.rs` (legacy) | ~930 | **DELETE** entire file | **MEDIUM** |
| `ast_adapter.rs` | 481 | **DELETE** entire file | **LOW** |
| `ast_convert.rs` | 342 | **DELETE** entire file | **LOW** |

**Total Estimated Changes:** ~12,000 lines across 8 files

### LogicalPlan Variants Using Expr (14 variants)

From exploration, these LogicalPlan variants contain Expr fields that must change:

1. `Scan { filter: Option<Expr> }`
2. `ExtIdLookup { filter: Option<Expr> }`
3. `Unwind { expr: Expr }`
4. `Traverse { target_filter: Option<Expr> }`
5. `Filter { predicate: Expr }`
6. `Delete { items: Vec<Expr> }`
7. `Foreach { list: Expr }`
8. `Aggregate { group_by: Vec<Expr>, aggregates: Vec<Expr> }`
9. `Window { window_exprs: Vec<Expr> }`
10. `Project { projections: Vec<(Expr, Option<String>)> }`
11. `Apply { input_filter: Option<Expr> }`
12. `ProcedureCall { arguments: Vec<Expr> }`
13. `VectorKnn { query: Expr }`
14. `InvertedIndexLookup { terms: Expr }`

**All 14 must change:** `use crate::query::ast::Expr` → `use uni_cypher::ast::Expr`

### Key Structural Differences (From AST Comparison)

| Aspect | Legacy AST | New AST | Migration Action |
|--------|-----------|---------|------------------|
| Variable reference | `Expr::Identifier(String)` | `Expr::Variable(String)` | Rename all pattern matches |
| Array indexing | `ArrayIndex(Box, Box)` | `ArrayIndex { array, index }` | Change to named fields |
| IN operator | `BinaryOp { op: Operator::In }` | `Expr::In { expr, list }` | Handle as separate variant |
| Window functions | `Expr::WindowFunction { ... }` | **Missing** | Keep in LogicalPlan::Window operator |
| Scalar subquery | `ScalarSubquery(usize)` | `CountSubquery(Box<Query>)` | Replace with new variant |
| Unary minus | `UnaryOp { op: UnaryOperator::Minus }` | `UnaryOp { op: UnaryOp::Neg }` | Update enum reference |
| Operators | `Operator` enum (34 variants) | `BinaryOp` enum (24 variants) | Update all operator handling |

### Critical Breaking Changes

**1. WindowFunction Removal**
- Legacy has `Expr::WindowFunction { function, partition_by, order_by }`
- New AST doesn't have this (window functions handled by LogicalPlan::Window operator)
- **Solution:** During planning, detect window function calls and create Window operator, don't embed in Expr

**2. Missing Variants in New AST**
- `ScalarSubquery(usize)` → Use `CountSubquery(Box<Query>)` instead
- Window function logic moves to planner operator creation

**3. New Variants Not in Legacy**
- `Expr::In { expr, list }` - need new handler in executor
- `CountSubquery(Box<Query>)` - need new handler in executor
- Bitwise shift operators - return unsupported error

## Implementation Plan

### Phase 1: Update Type Definitions (1-2 hours)

**Goal:** Change all type references from legacy to new AST

**Changes:**

1. **In `planner.rs`:**
   ```rust
   // OLD
   use crate::query::expr::Expr;

   // NEW
   use uni_cypher::ast::Expr;
   ```

2. **Update LogicalPlan enum** (line 25):
   ```rust
   pub enum LogicalPlan {
       Scan {
           filter: Option<uni_cypher::ast::Expr>,  // Changed
       },
       // ... update all 14 variants
   }
   ```

3. **Delete legacy imports** in planner.rs:
   - Remove `use crate::query::expr::{Expr, Operator};`
   - Add `use uni_cypher::ast::{Expr, BinaryOp, UnaryOp, Quantifier};`

4. **Update helper structs** (lines 257-360):
   ```rust
   struct AnyInPredicate {
       variable: String,
       property: String,
       terms: uni_cypher::ast::Expr,  // Changed
   }
   // ... etc
   ```

**Verification:**
```bash
cargo check -p uni-query 2>&1 | tee phase1-errors.txt
# Expected: ~1000+ compilation errors showing all places needing updates
```

### Phase 2: Update Planner Expression Handling (8-12 hours)

**Goal:** Fix all planner code to work with new Expr structure

**Critical Changes:**

#### 2.1 Rename Pattern Matches (91 sites)
```rust
// OLD
match expr {
    Expr::Identifier(name) => { ... }
}

// NEW
match expr {
    Expr::Variable(name) => { ... }
}
```

**File:** `planner.rs`
**Locations:** All pattern matches (lines 268-3525)
**Strategy:** Use find-replace for `Expr::Identifier` → `Expr::Variable`

#### 2.2 Update ArrayIndex Patterns
```rust
// OLD
Expr::ArrayIndex(arr, idx) => {
    // handle
}

// NEW
Expr::ArrayIndex { array, index } => {
    // handle
}
```

**Files:** `planner.rs` (2 occurrences)

#### 2.3 Handle IN Operator Split
```rust
// OLD
Expr::BinaryOp {
    op: Operator::In,
    left,
    right
} => { ... }

// NEW - two cases:
match expr {
    Expr::In { expr, list } => { ... }  // Direct IN expression
    Expr::BinaryOp { op: BinaryOp::???, ... } => { ... }  // Other operators
}
```

**Note:** `Operator::In` no longer exists in BinaryOp, need to check if any code relies on it

#### 2.4 Remove WindowFunction Handling

Window functions are no longer expressions. Instead, detect them during planning:

```rust
// In plan_with_clause or plan_return_clause:
fn detect_window_functions(exprs: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
    let mut windows = vec![];
    let mut regular = vec![];

    for expr in exprs {
        if is_window_function_call(expr) {
            windows.push(expr.clone());
        } else {
            regular.push(expr.clone());
        }
    }

    (windows, regular)
}

fn is_window_function_call(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } => {
            matches!(name.to_uppercase().as_str(),
                "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "LAG" | "LEAD")
        }
        _ => false
    }
}
```

Then create `LogicalPlan::Window { window_exprs }` operator when detected.

**Current window handling:** Check existing code around line 1010-1079 (`collect_window_functions`)

#### 2.5 Update Operator Enum References

```rust
// OLD
use crate::query::expr::Operator;
match op {
    Operator::Add => ...
    Operator::In => ...  // No longer exists!
}

// NEW
use uni_cypher::ast::BinaryOp;
match op {
    BinaryOp::Add => ...
    // In is now Expr::In, not BinaryOp
}
```

**Files:** `planner.rs`, `expr_eval.rs`
**Strategy:** Update all match statements on operators

#### 2.6 Update Pattern Construction

When building patterns during planning, use new AST structure:

```rust
// NodePattern properties changed:
// OLD: properties: Vec<(String, Expr)>
// NEW: properties: Option<Expr>  (Map literal)

// OLD
NodePattern {
    variable: Some("n".to_string()),
    labels: vec!["Person".to_string()],
    properties: vec![("name".to_string(), Expr::Literal(...))],
}

// NEW
NodePattern {
    variable: Some("n".to_string()),
    labels: vec!["Person".to_string()],
    properties: Some(Expr::Map(vec![
        ("name".to_string(), Expr::Literal(...))
    ])),
}
```

**Verification:**
```bash
cargo check -p uni-query
# Should compile with planner changes complete
```

### Phase 3: Update Executor Expression Evaluation (10-15 hours)

**Goal:** Make evaluate_expr work with `uni_cypher::ast::Expr`

**File:** `executor/read.rs`

#### 3.1 Update Function Signature

```rust
// OLD (line 462)
pub(crate) fn evaluate_expr<'a>(
    &'a self,
    expr: &'a crate::query::expr::Expr,  // Legacy
    ...
) -> BoxFuture<'a, Result<Value>>

// NEW
pub(crate) fn evaluate_expr<'a>(
    &'a self,
    expr: &'a uni_cypher::ast::Expr,  // New AST
    ...
) -> BoxFuture<'a, Result<Value>>
```

#### 3.2 Update All Variant Handlers (21 variants)

**Direct renames:**
```rust
// Line 479
Expr::Identifier(name) => { ... }
// BECOMES
Expr::Variable(name) => { ... }
```

**Structural changes:**
```rust
// ArrayIndex - line 534
// OLD
Expr::ArrayIndex(arr_expr, idx_expr) => {
    let arr_val = this.evaluate_expr(arr_expr, ...).await?;
    let idx_val = this.evaluate_expr(idx_expr, ...).await?;
}

// NEW
Expr::ArrayIndex { array, index } => {
    let arr_val = this.evaluate_expr(array, ...).await?;
    let idx_val = this.evaluate_expr(index, ...).await?;
}
```

**New variant - Expr::In:**
```rust
// Add new handler around line 750
Expr::In { expr, list } => {
    let val = this.evaluate_expr(expr, row, prop_manager, params, ctx).await?;
    let list_val = this.evaluate_expr(list, row, prop_manager, params, ctx).await?;

    if let Value::Array(items) = list_val {
        Ok(Value::Bool(items.contains(&val)))
    } else {
        Err(anyhow!("IN requires a list, got: {:?}", list_val))
    }
}
```

**Update BinaryOp handling:**
```rust
// Line 731
Expr::BinaryOp { left, op, right } => {
    let l = this.evaluate_expr(left, ...).await?;
    let r = this.evaluate_expr(right, ...).await?;

    // OLD: eval_binary_op(&l, op, &r)
    // NEW: eval_binary_op_new(&l, op, &r)  // Updated function
}
```

**Update UnaryOp handling:**
```rust
// Line 740
Expr::UnaryOp { op, expr } => {
    let val = this.evaluate_expr(expr, ...).await?;
    match op {
        UnaryOp::Not => { ... }
        UnaryOp::Neg => { ... }  // Was UnaryOperator::Minus
        UnaryOp::BitwiseNot => {
            Err(anyhow!("Bitwise NOT not yet supported"))
        }
    }
}
```

**Remove WindowFunction:**
```rust
// DELETE this entire match arm (line 1378):
// Expr::WindowFunction { ... } => { ... }

// Window functions are now handled by Window operator pre-computation
// If we see one here, it's an error:
// (Should never reach evaluate_expr - caught during planning)
```

**Replace ScalarSubquery:**
```rust
// OLD (line 651)
Expr::ScalarSubquery(usize) => {
    Err(anyhow!("ScalarSubquery not supported"))
}

// NEW - Handle CountSubquery
Expr::CountSubquery(query) => {
    // Plan and execute subquery
    let planner = QueryPlanner::new(self.schema.clone());
    let plan = planner.plan_new(*query)?;

    // Execute subquery
    let merged_params = /* merge row into params */;
    let results = self.execute(plan, prop_manager, &merged_params).await?;

    Ok(Value::from(results.len() as i64))
}
```

**Update Quantifier:**
```rust
// Line 664 - enum type change
Expr::Quantifier { quantifier, ... } => {
    // quantifier is now uni_cypher::ast::Quantifier instead of QuantifierType
    match quantifier {
        Quantifier::All => { ... }    // Was QuantifierType::All
        Quantifier::Any => { ... }
        Quantifier::Single => { ... }
        Quantifier::None => { ... }
    }
}
```

#### 3.3 Update Helper Functions

**File:** `expr_eval.rs`

Update operator handling:
```rust
// OLD
pub fn eval_binary_op(
    left: &Value,
    op: &crate::query::expr::Operator,
    right: &Value
) -> Result<Value>

// NEW
pub fn eval_binary_op(
    left: &Value,
    op: &uni_cypher::ast::BinaryOp,
    right: &Value
) -> Result<Value>
```

Update all match arms:
```rust
match op {
    BinaryOp::Eq => { ... }      // Was Operator::Eq
    BinaryOp::Add => { ... }     // Was Operator::Add
    // ... all operators
    // Remove Operator::In case (now handled by Expr::In)
}
```

**Verification:**
```bash
cargo test -p uni-query executor::tests
# Should pass all executor tests
```

### Phase 4: Update DataFusion Integration (3-5 hours)

**Goal:** Update DataFusion expression translation

**File:** `df_expr.rs`

#### 4.1 Update Function Signature

```rust
// OLD
pub fn cypher_expr_to_df(
    expr: &crate::query::expr::Expr,
    ...
) -> Result<df::Expr>

// NEW
pub fn cypher_expr_to_df(
    expr: &uni_cypher::ast::Expr,
    ...
) -> Result<df::Expr>
```

#### 4.2 Update Pattern Matches

Similar changes as executor:
- `Expr::Identifier` → `Expr::Variable`
- `ArrayIndex(a, b)` → `ArrayIndex { array, index }`
- Handle `Expr::In` separately
- Update operator enum references

**Verification:**
```bash
cargo test df_expr::tests
```

### Phase 5: Update Write Executor (2-4 hours)

**Goal:** Update pattern construction and evaluation in write operations

**File:** `executor/write.rs`

Look for:
- Pattern creation in CREATE/MERGE
- Expression evaluation calls
- Property map construction

Update:
- NodePattern property format
- Expression type references
- Pattern matching

**Verification:**
```bash
cargo test -p uni-query executor::write::tests
```

### Phase 6: Delete Legacy Code (1 hour)

**Goal:** Remove all legacy AST files

**Files to DELETE:**
1. `crates/uni-query/src/query/ast.rs` (930 lines)
2. `crates/uni-query/src/query/ast_adapter.rs` (481 lines)
3. `crates/uni-query/src/query/ast_convert.rs` (342 lines)
4. `crates/uni-query/src/query/expr.rs` (6 lines - just re-exports)

**Update module declarations:**

`crates/uni-query/src/query/mod.rs`:
```rust
// DELETE these lines:
pub mod ast;
pub mod ast_adapter;
pub mod ast_convert;
pub mod expr;
```

**Verification:**
```bash
cargo build --all
cargo test --all
# Should compile and pass with no legacy AST references
```

### Phase 7: Integration Testing (2-3 hours)

**Goal:** Verify all tests pass

**Test Strategy:**

```bash
# Unit tests
cargo test -p uni-query

# Integration tests
cargo test -p uni-db --tests

# Specific test suites
cargo test --test cypher_match
cargo test --test cypher_create
cargo test --test cypher_where
cargo test --test e2e_quantifier_tests

# Window functions (verify still work via operator)
cargo test window_execution_test
```
