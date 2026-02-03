# Window Functions

Uni supports Cypher window queries with `OVER (...)` clauses for both manual window functions (like `ROW_NUMBER`) and aggregate windows (like `SUM OVER`). These are useful for ranking, running totals, and partitioned analytics directly in graph queries.

## What It Provides

- Window specifications with `PARTITION BY` and `ORDER BY`.
- Manual window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`.
- Aggregate window functions: `SUM`, `AVG`, `MIN`, `MAX`, `COUNT` with `OVER`.

## Example: Manual Window Function

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let rows = db.query(
        "MATCH (p:Purchase) RETURN p.userId, p.amount, ROW_NUMBER() OVER (PARTITION BY p.userId ORDER BY p.timestamp) AS rn"
    ).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    rows = db.query(
        "MATCH (p:Purchase) RETURN p.userId, p.amount, ROW_NUMBER() OVER (PARTITION BY p.userId ORDER BY p.timestamp) AS rn"
    )
    print(rows)
    ```

## Example: LAG/LEAD for Change Tracking

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let rows = db.query(
        "MATCH (p:Price) RETURN p.symbol, p.ts, p.value, LAG(p.value) OVER (PARTITION BY p.symbol ORDER BY p.ts) AS prev_value"
    ).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    rows = db.query(
        "MATCH (p:Price) RETURN p.symbol, p.ts, p.value, LAG(p.value) OVER (PARTITION BY p.symbol ORDER BY p.ts) AS prev_value"
    )
    print(rows)
    ```

## Example: Aggregate Window Function

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let rows = db.query(
        "MATCH (p:Purchase) RETURN p.userId, p.amount, SUM(p.amount) OVER (PARTITION BY p.userId ORDER BY p.timestamp) AS running_total"
    ).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    rows = db.query(
        "MATCH (p:Purchase) RETURN p.userId, p.amount, SUM(p.amount) OVER (PARTITION BY p.userId ORDER BY p.timestamp) AS running_total"
    )
    print(rows)
    ```

## Example: Partition Totals Without Ordering

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let rows = db.query(
        "MATCH (p:Purchase) RETURN p.userId, p.amount, COUNT(*) OVER (PARTITION BY p.userId) AS user_count"
    ).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    rows = db.query(
        "MATCH (p:Purchase) RETURN p.userId, p.amount, COUNT(*) OVER (PARTITION BY p.userId) AS user_count"
    )
    print(rows)
    ```

## Use Cases

- Per-user ranking or top-N within partitions.
- Running totals and cumulative metrics.
- Sessionization or time-ordered analytics.

## When To Use

Use window functions when you need per-partition analytics within the same query result.

## Current Limitation

!!! warning
    Queries that mix manual window functions (like `ROW_NUMBER`) and aggregate window functions (like `SUM OVER`) in the same query are not yet supported.
