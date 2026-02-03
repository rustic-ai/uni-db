# Adding New Rewrite Rules

This guide explains how to add custom rewrite rules to the query rewriting framework.

## Overview

A rewrite rule transforms function calls into equivalent predicate expressions at compile time. Rules enable:

- Predicate pushdown to storage
- Index utilization
- Elimination of runtime function evaluation overhead

## Step 1: Implement the RewriteRule Trait

Create a new struct that implements the `RewriteRule` trait:

```rust
use crate::query::rewrite::context::RewriteContext;
use crate::query::rewrite::error::RewriteError;
use crate::query::rewrite::rule::{Arity, ArgConstraints, RewriteRule};
use uni_cypher::ast::{BinaryOp, Expr, Value};

pub struct MyCustomRule;

impl RewriteRule for MyCustomRule {
    fn function_name(&self) -> &str {
        "my.custom.function"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        // Define constraints
        let constraints = ArgConstraints {
            arity: Arity::Exact(3),    // Expect exactly 3 arguments
            literal_args: vec![1],     // Second arg must be a string literal
            entity_arg: Some(0),       // First arg must be an entity reference
        };

        // Validate arguments against constraints
        constraints.validate(args)
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
        // Extract arguments
        let entity = args[0].clone();
        let property_name = extract_string_literal(&args[1])?;
        let value = args[2].clone();

        // Build rewritten expression
        // Example: entity.property >= value
        Ok(Expr::BinaryOp {
            left: Box::new(Expr::Property(Box::new(entity), property_name)),
            op: BinaryOp::GreaterThanOrEqual,
            right: Box::new(value),
        })
    }
}

// Helper to extract string literals
fn extract_string_literal(expr: &Expr) -> Result<String, RewriteError> {
    match expr {
        Expr::Literal(Value::String(s)) => Ok(s.to_string()),
        _ => Err(RewriteError::TransformError {
            message: "Expected string literal".to_string(),
        }),
    }
}
```

## Step 2: Register the Rule

Add your rule to the registration function in `mod.rs`:

```rust
pub fn register_builtin_rules(registry: &mut RewriteRegistry) {
    // Existing rules
    registry.register(Arc::new(temporal::ValidAtRule));
    registry.register(Arc::new(temporal::OverlapsRule));
    // ... other rules

    // Your new rule
    registry.register(Arc::new(my_module::MyCustomRule));
}
```

## Step 3: Write Tests

Add comprehensive tests for your rule:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_my_rule_validation() {
        let rule = MyCustomRule;

        // Valid arguments
        let valid_args = vec![
            Expr::Variable("e".into()),
            Expr::Literal(Value::String("property".into())),
            Expr::Literal(Value::Integer(42)),
        ];
        assert!(rule.validate_args(&valid_args).is_ok());

        // Invalid: wrong arity
        let wrong_arity = vec![Expr::Variable("e".into())];
        assert!(rule.validate_args(&wrong_arity).is_err());

        // Invalid: non-literal property name
        let non_literal = vec![
            Expr::Variable("e".into()),
            Expr::Variable("prop".into()),  // Should be literal
            Expr::Literal(Value::Integer(42)),
        ];
        assert!(rule.validate_args(&non_literal).is_err());
    }

    #[test]
    fn test_my_rule_rewrite() {
        let rule = MyCustomRule;
        let ctx = RewriteContext::default();

        let args = vec![
            Expr::Variable("e".into()),
            Expr::Literal(Value::String("age".into())),
            Expr::Literal(Value::Integer(18)),
        ];

        let result = rule.rewrite(args, &ctx).unwrap();

        // Verify structure
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOp::GreaterThanOrEqual,
                ..
            }
        ));
    }

    #[test]
    fn test_semantic_equivalence() {
        // Test that rewritten expression produces same results as original function
        // This requires integration testing with a database
        // See tests/ directory for examples
    }
}
```

## Best Practices

### 1. Semantic Preservation

**Always preserve the semantics of the original function:**

- Handle null values correctly (three-valued logic)
- Preserve type coercion behavior
- Match edge cases (empty strings, zero, etc.)

Example of correct null handling:

```rust
// WRONG: Breaks when end is null
// e.start <= ts AND e.end >= ts

// CORRECT: Treats null end as "ongoing"
// e.start <= ts AND (e.end IS NULL OR e.end >= ts)
```

### 2. Argument Validation

**Use ArgConstraints for declarative validation:**

```rust
let constraints = ArgConstraints {
    arity: Arity::Exact(4),           // Exact count
    // OR: Arity::Range(2, 4),        // Range
    // OR: Arity::VarArgs(2),         // At least 2

    literal_args: vec![1, 2],         // Indices that must be literals
    entity_arg: Some(0),              // Index of entity reference
};

constraints.validate(args)?;
```

### 3. Fallback Strategy

**Your rule should gracefully handle cases where rewriting isn't possible:**

- Dynamic property names (e.g., parameterized: `$propName`)
- Complex expressions that can't be analyzed statically
- Missing context information

The framework will automatically fall back to scalar execution when validation fails.

### 4. Performance Considerations

**Rewrites should enable better performance:**

- Use simple comparisons (enable predicate pushdown)
- Avoid complex nested expressions
- Consider index utilization

Good example:
```rust
// Simple comparison - can use index on `start_date`
entity.start_date >= datetime('2021-01-01')
```

Bad example:
```rust
// Complex expression - may not push down
year(entity.start_date) >= 2021
```

### 5. Documentation

**Document your rule thoroughly:**

- What function it rewrites
- The transformation performed
- Any special null handling
- Examples of before/after

## Testing Strategy

### Unit Tests

Test your rule in isolation:

```rust
#[test]
fn test_my_rule() {
    let rule = MyCustomRule;
    let ctx = RewriteContext::default();

    let args = vec![/* ... */];
    let result = rule.rewrite(args, &ctx).unwrap();

    // Assert structure
    assert!(matches!(result, Expr::BinaryOp { .. }));
}
```

### Integration Tests

Test semantic equivalence (see `tests/` directory):

```rust
#[tokio::test]
async fn test_semantic_equivalence() {
    let db = setup_test_db().await?;

    // Query with function (will be rewritten)
    let result_with_function = db.query(
        "MATCH (n) WHERE my.custom.function(n, 'prop', 42) RETURN count(*)"
    ).await?;

    // Query with explicit predicate (baseline)
    let result_with_predicate = db.query(
        "MATCH (n) WHERE n.prop >= 42 RETURN count(*)"
    ).await?;

    // Results must match
    assert_eq!(
        result_with_function[0].get::<i64>("count")?,
        result_with_predicate[0].get::<i64>("count")?
    );
}
```

## Examples

See `temporal.rs` for complete examples of rewrite rules:

- `ValidAtRule` - Complex rule with null handling
- `OverlapsRule` - Multiple property accesses
- `IsOngoingRule` - Simple IS NULL check
- `PrecedesRule` - Single comparison

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
    op: BinaryOp::Equal,
    right: Box::new(value),
})
```

### Pattern 2: Range Check

```rust
// Transform: inRange(e, 'prop', min, max)
// Into: e.prop >= min AND e.prop <= max

let entity = args[0].clone();
let prop = extract_string_literal(&args[1])?;
let min = args[2].clone();
let max = args[3].clone();

Ok(Expr::BinaryOp {
    left: Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Property(Box::new(entity.clone()), prop.clone())),
        op: BinaryOp::GreaterThanOrEqual,
        right: Box::new(min),
    }),
    op: BinaryOp::And,
    right: Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Property(Box::new(entity), prop)),
        op: BinaryOp::LessThanOrEqual,
        right: Box::new(max),
    }),
})
```

### Pattern 3: Null Check

```rust
// Transform: hasProperty(e, 'prop')
// Into: e.prop IS NOT NULL

let entity = args[0].clone();
let prop = extract_string_literal(&args[1])?;

Ok(Expr::IsNotNull(Box::new(Expr::Property(
    Box::new(entity),
    prop,
))))
```

### Pattern 4: OR with Null

```rust
// Transform: validOrNull(e, 'prop', val)
// Into: e.prop IS NULL OR e.prop = val

let entity = args[0].clone();
let prop = extract_string_literal(&args[1])?;
let value = args[2].clone();

Ok(Expr::BinaryOp {
    left: Box::new(Expr::IsNull(Box::new(Expr::Property(
        Box::new(entity.clone()),
        prop.clone(),
    )))),
    op: BinaryOp::Or,
    right: Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Property(Box::new(entity), prop)),
        op: BinaryOp::Equal,
        right: Box::new(value),
    }),
})
```

## Debugging

Enable verbose logging to see rewrite operations:

```rust
use crate::query::rewrite::context::RewriteConfig;

let config = RewriteConfig::default().with_verbose_logging();
let context = RewriteContext::with_config(config);
```

Or via environment variable:

```bash
RUST_LOG=uni_query::rewrite=debug cargo test
```

## Questions?

See the framework documentation in `mod.rs` or refer to the implementation plan in `docs/QUERY_REWRITE_FRAMEWORK.md`.
