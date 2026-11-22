# Schema & Indexing

Uni lets you define schema, constraints, and indexes to enforce correctness and speed up queries. You can start schemaless and evolve toward typed properties.

## What It Provides

- Typed properties and constraints (unique, exists, check).
- Scalar indexes (BTree, Bitmap, LabelList, and Hash, which is stored as a BTree), 8 single-vector ANN algorithms (with quantization options) plus MUVERA for multi-vector (ColBERT) columns, and full-text indexes.
- Fast predicate pushdown and index usage in the planner.

## Example

=== "Rust"
    ```rust
    use uni_db::{DataType, IndexType, ScalarType, Uni};

    let db = Uni::open("./my_db").build().await?;

    db.schema()
        .label("User")
            .property("email", DataType::String)
            .property("age", DataType::Int64)
            .index("email", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;

    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")

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
