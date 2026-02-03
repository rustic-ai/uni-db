# Schema & Indexing

Uni lets you define schema, constraints, and indexes to enforce correctness and speed up queries. You can start schemaless and evolve toward typed properties.

## What It Provides

- Typed properties and constraints (unique, exists, check).
- Scalar indexes, vector indexes, and full-text indexes.
- Fast predicate pushdown and index usage in the planner.

## Example

=== "Rust"
    ```rust
    use uni_db::{DataType, IndexType, ScalarType, Uni};

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    db.schema()
        .label("User")
            .property("email", DataType::String)
            .property("age", DataType::Int64)
            .index("email", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;

    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")

    db.schema() \
        .label("User") \
            .property("email", "string") \
            .property("age", "int64") \
            .index("email", "btree") \
            .done() \
        .apply()
    ```

## Use Cases

- Enforcing data consistency in production.
- Speeding up high-selectivity filters and joins.
- Adding indexes incrementally as query patterns stabilize.

## When To Use

Start with a minimal schema for agility, then add indexes and constraints once your access patterns are clear.
