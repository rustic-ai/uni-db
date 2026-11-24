# Schema & Indexing

Uni lets you define schema, constraints, and indexes to enforce correctness and speed up queries. You can start schemaless and evolve toward typed properties.

## What It Provides

- Typed properties and constraints (unique, exists, check).
- Scalar indexes (BTree, Bitmap, LabelList, and Hash, which is stored as a BTree), on flat column types. A BTree, Hash or Bitmap index on a `DATETIME` property is built on a hidden key column holding each value's instant, and serves `=`, `<`, `<=`, `>`, `>=`, `IN` and pattern-map equality against a constant DATETIME (a literal, `datetime(...)`, `datetime()` or a parameter); `<>` and comparisons with other types scan. A BTree, Hash or Bitmap index on a `BTIC` property is built on two hidden key columns holding each interval's bounds, and serves `btic_overlaps`, `btic_contains`, `btic_during`, `btic_before`, `btic_after`, `btic_meets`, `btic_starts`, `btic_finishes`, `btic_equals`, `btic_disjoint`, `btic_adjacent` (with the property as either argument and a constant interval — `btic('…')` or a parameter — as the other), `btic_contains_point(p, t)` with a constant point (integer ms, datetime or date), `=` / `IN` against constant intervals, and `btic_lo(p)` / `btic_hi(p)` compared with a constant datetime. A BTree, Hash or Bitmap index on a `TIME` property is built on a hidden key column holding each value's UTC time (`time('23:00-05:00')` is after `time('04:00Z')`: times are not wrapped at midnight), and serves `=`, `<`, `<=`, `>`, `>=`, `IN` and pattern-map equality against a constant TIME (a literal, `time(...)`, `time()` or a parameter); `<>`, `localtime` and other types scan, and a `localtime` stored in a `TIME` column matches no comparison with a TIME, indexed or not. An index on a key path of a typed map, `CREATE INDEX i FOR (n:L) ON (n.m.k)` (or `uni.schema.createIndex('L', 'm.k', …)`, or `.index("m.k", …)` in the schema builder) on a `MAP<STRING, T>` property with `T` a string, integer or float type, is built on a hidden column holding the value at `k` (NULL where the map has no `k`), and serves `n.m.k =`, `<`, `<=`, `>`, `>=` and `IN` against constants of `T` (an integer against a float map too); several paths of one map can each be indexed. A BTree, Hash or Bitmap index on a point property (`POINT`, `POINT3D`, `GEOPOINT`) is built on two hidden key columns holding each point's x and y (longitude and latitude; a 3-D point's z is not indexed), and serves `point.withinBBox(p, lowerLeft, upperRight)` (a geographic box may cross the antimeridian) and `distance(p, c) < r` / `<= r` (either argument order, or `r > distance(...)`) with constant corners, center and radius (`point({...})` literals or parameters) of the column's CRS; a constant of another CRS raises the same error as without the index, and point `=` and ordering scan. A BTree, Hash or Bitmap index on a type no scalar index serves is refused with the index kind that fits: a `LIST<STRING>` takes `CREATE INVERTED INDEX`, a `VECTOR` or list of vectors `CREATE VECTOR INDEX`, a whole `JSON` column `CREATE JSON FULLTEXT INDEX` (or declare a typed MAP and index a key path), a whole `MAP` an index on `n.m.<key>`; other lists and `DURATION` (durations are not ordered) take none, and queries on them scan. A `DURATION` or `JSON` BTree created by an earlier release keeps working as before. 8 single-vector ANN algorithms (with quantization options) plus MUVERA for multi-vector (ColBERT) columns, and full-text indexes.
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
