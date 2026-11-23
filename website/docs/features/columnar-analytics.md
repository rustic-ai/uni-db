# Columnar Analytics

Uni executes filters, projections, and aggregations using columnar, vectorized execution (Apache Arrow + DataFusion). This gives fast analytical queries without a separate warehouse.

## What It Provides

- Vectorized scans and predicate pushdown.
- Aggregations like `COUNT`, `SUM`, `AVG`, `COLLECT`.
- Columnar storage via Lance.
- Windowed analytics via `OVER (...)` clauses (see [Window Functions](window-functions.md)).

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let results = session.query(
        "MATCH (p:Purchase) RETURN p.category, SUM(p.amount) AS revenue"
    ).await?;

    println!("{:?}", results);
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()
    rows = session.query(
        "MATCH (p:Purchase) RETURN p.category, SUM(p.amount) AS revenue"
    )
    print(rows)
    ```

## Use Cases

- Product or sales analytics in an application.
- Aggregations over large property sets.
- Fast filters and grouping without a separate analytics system.

## When To Use

Choose columnar analytics when you need fast aggregations and scans over large datasets, but still want graph and vector queries in the same system.
