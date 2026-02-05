# Analysis: Stack Overflow in TCK Create Tests

## Root Cause

The stack overflow in `Create4.feature` is caused by **deeply nested recursive async calls** when processing 971 CREATE statements.

## Mechanism

### 1. Planner Creates Deeply Nested Structure

**File**: `crates/uni-query/src/query/planner.rs` (line 896)

Each `CREATE` clause wraps the previous plan:

```rust
Clause::Create(create_clause) => {
    plan = LogicalPlan::Create {
        input: Box::new(plan),  // Previous plan becomes nested input
        pattern: create_clause.pattern.clone(),
    };
}
```

With 971 CREATE statements, this creates:
```
Create { input: 
  Create { input: 
    Create { input: 
      ... 968 more levels ...
        Empty
    }
  }
}
```

### 2. Executor Recurses Through Nested Plans

**File**: `crates/uni-query/src/query/executor/read.rs` (lines 2296-2316)

```rust
LogicalPlan::Create { input, pattern } => {
    let mut rows = self
        .execute_subplan(*input, prop_manager, params, ctx)  // ← Recursive call
        .await?;
    // ... execute pattern ...
}
```

The `execute_subplan` function (lines 1667-2596, ~930 lines) is a massive async function that:
- Uses `BoxFuture` and `Box::pin(async move { ... })`
- Recursively calls itself for each nested plan level

### 3. Async State Machines Consume Stack

Each async function call creates a Future state machine that lives on the stack until `.await` completes. In debug builds:
- Each Future can be 1000+ bytes due to debug info and unoptimized layouts
- 971 recursive calls × 1000+ bytes = **~1MB+ of stack**
- The actual size is often much larger due to the massive `execute_subplan` function

Even with 8MB stack (`RUST_MIN_STACK=8388608`), the deeply nested async state machines overflow.

## The Problematic Query

**File**: `crates/uni-tck/features/clauses/create/Create4.feature`

Contains the entire Neo4j movie database in a single query:
- **971 CREATE statements** chained together
- **1374 lines** total
- Creates ~171 Person nodes, ~38 Movie nodes, and hundreds of relationships

Example structure:
```cypher
CREATE (theMatrix:Movie {title: 'The Matrix', released: 1999, ...})
CREATE (keanu:Person {name: 'Keanu Reeves', born: 1964})
CREATE (carrie:Person {name: 'Carrie-Anne Moss', born: 1967})
...
CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(theMatrix),
       (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(theMatrix),
...
```

## Fix Options

### Option 1: Flatten Plan Structure (Recommended)

**Complexity**: Medium | **Impact**: High | **Risk**: Low

Modify planner to detect consecutive CREATE clauses and flatten them into a single plan node:

```rust
// New LogicalPlan variant
CreateBatch {
    input: Box<LogicalPlan>,
    patterns: Vec<Pattern>,  // All CREATE patterns in sequence
}
```

**Changes**:
- Add `LogicalPlan::CreateBatch` variant to `planner.rs`
- Modify planner to accumulate consecutive CREATE patterns
- Add executor branch for `CreateBatch` that iterates instead of recurses

### Option 2: Trampoline Pattern

**Complexity**: High | **Impact**: High | **Risk**: Medium

Convert recursive execution to iterative with manual stack:

```rust
fn execute_subplan(&self, plan: LogicalPlan, ...) -> Result<Vec<Row>> {
    let mut stack = vec![plan];
    let mut results = HashMap::new();
    
    while let Some(current) = stack.pop() {
        match current {
            LogicalPlan::Create { input, pattern } => {
                if !results.contains_key(&input_id) {
                    stack.push(current);  // Re-push current
                    stack.push(*input);   // Process input first
                } else {
                    // Process pattern with input results
                }
            }
            // ... other variants
        }
    }
}
```

### Option 3: Spawn on Larger Stack (Quick Fix)

**Complexity**: Low | **Impact**: Medium | **Risk**: Low

Use `tokio::task::spawn_blocking` with a custom thread stack size for deep queries:

```rust
// Detect deep nesting
fn plan_depth(plan: &LogicalPlan) -> usize { ... }

// If depth > threshold, spawn on larger stack
if plan_depth(&plan) > 100 {
    let result = tokio::task::spawn_blocking(move || {
        // Execute with 64MB stack
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| { ... })
    }).await??;
}
```

### Option 4: Increase Default Stack (Workaround)

**Complexity**: Trivial | **Impact**: Low | **Risk**: Medium

Increase `RUST_MIN_STACK` in `.cargo/config.toml`:

```toml
[env]
RUST_MIN_STACK = "33554432"  # 32MB instead of 8MB
```

**Problems**:
- Wastes memory for simple queries
- Doesn't fix the underlying issue
- May still overflow with larger queries

## Recommendation

**Implement Option 1 (Flatten Plan Structure)** as the proper fix:

1. It addresses the root cause
2. Changes are isolated to planner and executor
3. Improves performance for batch inserts
4. No risk to existing functionality

For immediate TCK testing, **Option 4** (increase stack) can be used as a temporary workaround.

## Implementation Plan for Option 1

### Step 1: Add CreateBatch variant

**File**: `crates/uni-query/src/query/planner.rs`

```rust
pub enum LogicalPlan {
    // ... existing variants ...
    
    /// Batch CREATE for multiple patterns (avoids deep nesting)
    CreateBatch {
        input: Box<LogicalPlan>,
        patterns: Vec<Pattern>,
    },
}
```

### Step 2: Modify planner to accumulate CREATEs

**File**: `crates/uni-query/src/query/planner.rs`

Replace the simple CREATE handling:

```rust
// Accumulate consecutive CREATE clauses
let mut create_patterns = Vec::new();
for clause in clauses {
    match clause {
        Clause::Create(c) => {
            create_patterns.push(c.pattern.clone());
            // Continue accumulating
        }
        other => {
            // Flush accumulated CREATEs as batch
            if !create_patterns.is_empty() {
                plan = LogicalPlan::CreateBatch {
                    input: Box::new(plan),
                    patterns: std::mem::take(&mut create_patterns),
                };
            }
            // Handle other clause
            // ...
        }
    }
}
// Final flush
if !create_patterns.is_empty() {
    plan = LogicalPlan::CreateBatch {
        input: Box::new(plan),
        patterns: create_patterns,
    };
}
```

### Step 3: Add executor for CreateBatch

**File**: `crates/uni-query/src/query/executor/read.rs`

```rust
LogicalPlan::CreateBatch { input, patterns } => {
    let mut rows = self
        .execute_subplan(*input, prop_manager, params, ctx)
        .await?;
    
    if let Some(writer_lock) = &self.writer {
        let mut writer = writer_lock.write().await;
        for pattern in patterns {
            for row in &mut rows {
                self.execute_create_pattern(
                    &pattern, row, &mut writer, prop_manager, params, ctx
                ).await?;
            }
        }
    }
    Ok(rows)
}
```

### Step 4: Update df_planner.rs

Add `CreateBatch` to all the match exhaustiveness patterns.

## Verification

```bash
# After fix, this should pass without stack overflow
cargo test -p uni-tck --test cucumber -- -i "features/clauses/create/Create4.feature"
```

## Risk Assessment

**Low Risk**: The change preserves semantics - CREATE operations still execute in order, just without recursion.
