# OpenCypher Querying

Uni uses OpenCypher as its primary query language, with extensions for vector search, time travel, and admin procedures. This keeps graph queries expressive and readable.

## What It Provides

- Pattern matching for nodes and relationships.
- Aggregations, ordering, and filtering.
- Procedure calls for algorithms, snapshots, and indexes.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let results = db
        .query_with("MATCH (p:Person) WHERE p.age > $min RETURN p.name")
        .param("min", 30)
        .fetch_all()
        .await?;

    println!("{:?}", results);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    results = db.query_with(
        "MATCH (p:Person) WHERE p.age > $min RETURN p.name"
    ).param("min", 30).fetch_all()

    print(results)
    ```

## Use Cases

- Relationship queries and graph traversals.
- Pattern matching with filters and aggregations.
- Admin and metadata procedures.

## When To Use

OpenCypher is the best choice when your data model is graph-shaped and you need expressive relationship queries, not just key-value lookups or SQL tables.
