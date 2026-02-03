# Query Rewriting Framework

Uni includes a powerful query rewriting framework that transforms function calls into equivalent predicate expressions at compile time. This enables full predicate pushdown to storage, index utilization, and eliminates runtime function evaluation overhead.

## Overview

The query rewriting framework is a general-purpose transformation system that operates on the Cypher AST before logical planning. It applies registered rewrite rules to function calls, converting them into simpler expressions that the storage layer can optimize.

**Key Insight**: Many functions can be expressed as simple predicate expressions. By rewriting at compile time, we eliminate function evaluation overhead and enable the storage layer (Lance/DataFusion) to filter data directly.

### Example

```cypher
// Original query with temporal function
MATCH (p:Person)-[e:EMPLOYED_BY]->(c:Company)
WHERE uni.temporal.validAt(e, 'start', 'end', datetime('2021-06-15'))
RETURN c.name

// Automatically rewritten to
MATCH (p:Person)-[e:EMPLOYED_BY]->(c:Company)
WHERE e.start <= datetime('2021-06-15')
  AND (e.end IS NULL OR e.end >= datetime('2021-06-15'))
RETURN c.name
```

The rewritten form enables:
- **Predicate pushdown** to Lance/DataFusion
- **Index utilization** on `start` and `end` columns
- **Native storage filtering** instead of row-by-row evaluation

## Architecture

### Components

The framework consists of several key components located in `crates/uni-query/src/query/rewrite/`:

```
query/rewrite/
├── mod.rs          # Public API (rewrite_query, rewrite_statement)
├── rule.rs         # RewriteRule trait and constraint types
├── registry.rs     # Global rule registry
├── walker.rs       # Expression tree walker
├── context.rs      # Rewrite context and configuration
├── error.rs        # Error types
└── rules/          # Built-in rule implementations
    ├── mod.rs      # Rule registration
    ├── temporal.rs # Temporal function rewrites
    └── README.md   # Developer guide for adding rules
```

### RewriteRule Trait

All rewrite rules implement the `RewriteRule` trait:

```rust
pub trait RewriteRule: Send + Sync {
    /// The fully-qualified function name to match
    fn function_name(&self) -> &str;

    /// Validate arguments before attempting rewrite
    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError>;

    /// Perform the transformation
    fn rewrite(&self, args: Vec<Expr>, ctx: &RewriteContext)
        -> Result<Expr, RewriteError>;

    /// Check if rule is applicable in current context (optional)
    fn is_applicable(&self, ctx: &RewriteContext) -> bool {
        true
    }
}
```

### Integration Point

The rewriting framework integrates into the query pipeline at the planning stage:

```rust
// In planner.rs
pub fn plan_with_scope(&self, query: Query, vars: Vec<String>) -> Result<LogicalPlan> {
    // Apply query rewrites before planning
    let rewritten_query = crate::query::rewrite::rewrite_query(query)?;

    match rewritten_query {
        Query::Single(stmt) => self.plan_single(stmt, vars),
        // ... rest of planning
    }
}
```

This ensures rewrites happen **before** logical planning, enabling all downstream optimizations.

## Built-in Rewrites

### Temporal Functions

Uni includes several temporal function rewrites that enable efficient time-based queries:

| Function | Transformation |
|----------|---------------|
| `uni.temporal.validAt(e, 'start', 'end', ts)` | `e.start <= ts AND (e.end IS NULL OR e.end >= ts)` |
| `uni.temporal.overlaps(e, 'start', 'end', rs, re)` | `e.start <= re AND (e.end IS NULL OR e.end >= rs)` |
| `uni.temporal.precedes(e, 'end', ts)` | `e.end < ts` |
| `uni.temporal.succeeds(e, 'start', ts)` | `e.start > ts` |
| `uni.temporal.isOngoing(e, 'end')` | `e.end IS NULL` |
| `uni.temporal.hasClosed(e, 'end')` | `e.end IS NOT NULL` |

All temporal rewrites preserve three-valued logic and handle null values correctly (treating null end dates as "ongoing").

## Adding New Rewrite Rules

### Step 1: Implement the RewriteRule Trait

Create a new struct implementing `RewriteRule`:

```rust
use crate::query::rewrite::{RewriteRule, RewriteContext, RewriteError};
use crate::query::rewrite::rule::{Arity, ArgConstraints};
use uni_cypher::ast::{BinaryOp, Expr};
use serde_json::Value;

pub struct InRangeRule;

impl RewriteRule for InRangeRule {
    fn function_name(&self) -> &str {
        "uni.util.inRange"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        // Validate: inRange(entity, 'property', min, max)
        let constraints = ArgConstraints {
            arity: Arity::Exact(4),
            literal_args: vec![1],  // Property name must be literal
            entity_arg: Some(0),    // First arg is entity
        };
        constraints.validate(args)
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext)
        -> Result<Expr, RewriteError>
    {
        let entity = args[0].clone();
        let prop = extract_string_literal(&args[1])?;
        let min = args[2].clone();
        let max = args[3].clone();

        // Rewrite to: entity.prop >= min AND entity.prop <= max
        Ok(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Property(Box::new(entity.clone()), prop.clone())),
                op: BinaryOp::GtEq,
                right: Box::new(min),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Property(Box::new(entity), prop)),
                op: BinaryOp::LtEq,
                right: Box::new(max),
            }),
        })
    }
}
```

### Step 2: Register the Rule

Add to `rules/mod.rs`:

```rust
pub fn register_builtin_rules(registry: &mut RewriteRegistry) {
    // Existing rules
    registry.register(Arc::new(temporal::ValidAtRule));

    // Your new rule
    registry.register(Arc::new(util::InRangeRule));
}
```

### Step 3: Write Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_range_rewrite() {
        let rule = InRangeRule;
        let ctx = RewriteContext::default();

        let args = vec![
            Expr::Variable("p".into()),
            Expr::Literal(Value::String("age".into())),
            Expr::Literal(Value::Number(18.into())),
            Expr::Literal(Value::Number(65.into())),
        ];

        let result = rule.rewrite(args, &ctx).unwrap();

        // Should produce AND expression
        assert!(matches!(result, Expr::BinaryOp { op: BinaryOp::And, .. }));
    }
}
```

## Design Principles

### 1. Semantic Preservation

Rewrites **must** preserve the exact semantics of the original function:

- **Three-valued logic**: SQL/Cypher uses true/false/null
- **Null handling**: Must match original function behavior
- **Type coercion**: Preserve type semantics

Example of correct null handling:

```rust
// WRONG: Breaks when end is null
// e.start <= ts AND e.end >= ts

// CORRECT: Treats null end as "ongoing"
// e.start <= ts AND (e.end IS NULL OR e.end >= ts)
```

### 2. Declarative Validation

Use `ArgConstraints` for declarative argument validation:

```rust
let constraints = ArgConstraints {
    arity: Arity::Exact(4),           // Exact argument count
    literal_args: vec![1, 2],         // Indices that must be literals
    entity_arg: Some(0),              // Index of entity reference
};

constraints.validate(args)?;
```

### 3. Graceful Fallback

When rewriting cannot be applied, the framework falls back to scalar execution:

- Dynamic property names (e.g., parameterized: `$propName`)
- Complex expressions that can't be analyzed statically
- Missing context information

The framework automatically handles fallback without manual intervention.

### 4. Observability

The framework tracks detailed statistics:

```rust
pub struct RewriteStats {
    pub functions_visited: usize,
    pub functions_rewritten: usize,
    pub functions_skipped: usize,
    pub errors: Vec<RewriteError>,
    pub rule_stats: HashMap<String, RuleStats>,
}
```

Enable verbose logging to see rewrite operations:

```bash
RUST_LOG=uni_query::rewrite=debug cargo test
```

## Performance Impact

Rewriting provides significant performance benefits:

### Before Rewriting

```cypher
MATCH (p)-[e:EMPLOYED_BY]->()
WHERE uni.temporal.validAt(e, 'start', 'end', datetime('2021-06-15'))
RETURN count(*)
```

- Function evaluated for **every edge** in the result set
- No predicate pushdown to storage
- Full table scan required

### After Rewriting

```cypher
MATCH (p)-[e:EMPLOYED_BY]->()
WHERE e.start <= datetime('2021-06-15')
  AND (e.end IS NULL OR e.end >= datetime('2021-06-15'))
RETURN count(*)
```

- Predicates pushed down to Lance/DataFusion
- Storage-level filtering before materialization
- Can use indexes on `start` and `end` columns
- Significantly reduced data transfer

## Testing Strategy

### Unit Tests

Test rules in isolation:

```rust
#[test]
fn test_rule_validation() {
    let rule = MyRule;

    // Valid arguments
    assert!(rule.validate_args(&valid_args).is_ok());

    // Invalid arguments
    assert!(rule.validate_args(&invalid_args).is_err());
}
```

### Integration Tests

Test semantic equivalence:

```rust
#[tokio::test]
async fn test_semantic_equivalence() {
    let db = setup_test_db().await?;

    // Query with function (will be rewritten)
    let result_function = db.query(
        "MATCH (n) WHERE myFunc(n, 'prop', 42) RETURN count(*)"
    ).await?;

    // Query with explicit predicate (baseline)
    let result_predicate = db.query(
        "MATCH (n) WHERE n.prop >= 42 RETURN count(*)"
    ).await?;

    // Results must match
    assert_eq!(result_function, result_predicate);
}
```

## Common Patterns

### Pattern 1: Property Comparison

```rust
// Transform: hasValue(e, 'prop', val)
// Into: e.prop = val

let entity = args[0].clone();
let prop = extract_string_literal(&args[1])?;
let value = args[2].clone();

Ok(Expr::BinaryOp {
    left: Box::new(Expr::Property(Box::new(entity), prop)),
    op: BinaryOp::Eq,
    right: Box::new(value),
})
```

### Pattern 2: Range Check

```rust
// Transform: inRange(e, 'prop', min, max)
// Into: e.prop >= min AND e.prop <= max

Ok(Expr::BinaryOp {
    left: Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Property(Box::new(entity.clone()), prop.clone())),
        op: BinaryOp::GtEq,
        right: Box::new(min),
    }),
    op: BinaryOp::And,
    right: Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Property(Box::new(entity), prop)),
        op: BinaryOp::LtEq,
        right: Box::new(max),
    }),
})
```

### Pattern 3: Null Check

```rust
// Transform: hasProperty(e, 'prop')
// Into: e.prop IS NOT NULL

Ok(Expr::IsNotNull(Box::new(Expr::Property(
    Box::new(entity),
    prop,
))))
```

### Pattern 4: OR with Null

```rust
// Transform: validOrNull(e, 'prop', val)
// Into: e.prop IS NULL OR e.prop = val

Ok(Expr::BinaryOp {
    left: Box::new(Expr::IsNull(Box::new(Expr::Property(
        Box::new(entity.clone()),
        prop.clone(),
    )))),
    op: BinaryOp::Or,
    right: Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Property(Box::new(entity), prop)),
        op: BinaryOp::Eq,
        right: Box::new(value),
    }),
})
```

## Best Practices

### 1. Always Validate Arguments

Use `ArgConstraints` to ensure function calls can be safely rewritten:

```rust
fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
    let constraints = ArgConstraints {
        arity: Arity::Exact(3),
        literal_args: vec![1],
        entity_arg: Some(0),
    };
    constraints.validate(args)
}
```

### 2. Preserve Null Semantics

Always consider how nulls should be handled:

```rust
// For temporal "valid at" queries:
// null end date means "ongoing" - should match!
e.start <= ts AND (e.end IS NULL OR e.end >= ts)
```

### 3. Keep Rewrites Simple

Rewritten expressions should be simple enough for storage layer optimization:

```rust
// Good: Simple comparisons
entity.start_date >= datetime('2021-01-01')

// Bad: Complex expressions that block pushdown
year(entity.start_date) >= 2021
```

### 4. Write Comprehensive Tests

Test all edge cases:

- Null values
- Boundary conditions
- Type mismatches
- Dynamic arguments (should fallback gracefully)

### 5. Document Transformations

Clearly document what each rule does:

```rust
/// Rewrite rule for uni.temporal.validAt
///
/// Transforms: uni.temporal.validAt(e, 'start', 'end', ts)
/// Into: e.start <= ts AND (e.end IS NULL OR e.end >= ts)
///
/// This preserves the semantics where a null end date means "ongoing".
pub struct ValidAtRule;
```

## Future Extensions

The framework is designed to be extensible. Future rewrite rules could include:

### Spatial Rewrites

```rust
// POINT.WITHINBBOX(p, ll, ur)
// → p.x >= ll.x AND p.x <= ur.x AND p.y >= ll.y AND p.y <= ur.y
```

### Datetime Range Rewrites

```rust
// YEAR(e.created_at) = 2021
// → e.created_at >= datetime('2021-01-01')
//   AND e.created_at < datetime('2022-01-01')
```

### Property Pattern Rewrites

```rust
// hasProperty(e, 'x')
// → e.x IS NOT NULL
```

## References

- Source code: `crates/uni-query/src/query/rewrite/`
- Developer guide: `crates/uni-query/src/query/rewrite/rules/README.md`
- Architecture document: `docs/ARCH_QUERY_REWRITE.md`
- Temporal rules: `crates/uni-query/src/query/rewrite/rules/temporal.rs`

## Summary

The query rewriting framework is a powerful optimization tool that:

- ✅ Transforms function calls into predicate expressions at compile time
- ✅ Enables full predicate pushdown to storage layer
- ✅ Provides extensible plugin architecture for new rules
- ✅ Preserves semantic correctness with graceful fallback
- ✅ Includes comprehensive temporal function rewrites

The framework significantly improves query performance while maintaining a clean separation from the core query engine.
