# Multi-Model Data Model

Uni combines a property graph with vectors and JSON documents. You can model entities and relationships, attach embeddings, and query structured or semi-structured data in one place.

## What It Provides

- Labeled vertices and typed, directed edges.
- Typed properties plus flexible JSON overflow for ad-hoc fields.
- Vector properties for similarity search.

## Example

=== "Rust"
    ```rust
    use uni_db::{DataType, Uni};

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    db.schema()
        .label("Person")
            .property("name", DataType::String)
            .property("age", DataType::Int64)
            .vector("embedding", 384)
        .edge_type("KNOWS", &["Person"], &["Person"])
            .property("since", DataType::Date)
        .apply()
        .await?;

    db.execute("CREATE (:Person {name: 'Ada', age: 31, city: 'London'})")
        .await?;
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")

    db.schema() \
        .label("Person") \
            .property("name", "string") \
            .property("age", "int64") \
            .vector("embedding", 384) \
            .done() \
        .edge_type("KNOWS", ["Person"], ["Person"]) \
            .property("since", "date") \
            .done() \
        .apply()

    db.execute("CREATE (:Person {name: 'Ada', age: 31, city: 'London'})")
    ```

## Use Cases

- Knowledge graphs with semantic search.
- Product graphs with text metadata and embeddings.
- Mixed graph + document workloads in one engine.

## When To Use

Use Uni's multi-model design when your application needs relationships, embeddings, and document-style properties in one consistent database.
