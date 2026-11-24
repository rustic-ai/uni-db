# Schema & Indexing

Uni lets you define schema, constraints, and indexes to enforce correctness and speed up queries. You can start schemaless and evolve toward typed properties.

## What It Provides

- Typed properties and constraints (unique, exists, check).
- Scalar indexes (BTree, Bitmap, LabelList, and Hash, which is stored as a BTree), on flat column types. A BTree, Hash or Bitmap index on a `DATETIME` property is built on a hidden key column holding each value's instant, and serves `=`, `<`, `<=`, `>`, `>=`, `IN` and pattern-map equality against a constant DATETIME (a literal, `datetime(...)`, `datetime()` or a parameter); `<>` and comparisons with other types scan. A BTree, Hash or Bitmap index on a `BTIC` property is built on two hidden key columns holding each interval's bounds, and serves `btic_overlaps`, `btic_contains`, `btic_during`, `btic_before`, `btic_after`, `btic_meets`, `btic_starts`, `btic_finishes`, `btic_equals`, `btic_disjoint`, `btic_adjacent` (with the property as either argument and a constant interval — `btic('…')` or a parameter — as the other), `btic_contains_point(p, t)` with a constant point (integer ms, datetime or date), `=` / `IN` against constant intervals, and `btic_lo(p)` / `btic_hi(p)` compared with a constant datetime. A BTree, Hash or Bitmap index on a `TIME`, point, list, map or vector property is refused when declared, since those are stored as nested columns that the index cannot cover. 8 single-vector ANN algorithms (with quantization options) plus MUVERA for multi-vector (ColBERT) columns, and full-text indexes.
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
