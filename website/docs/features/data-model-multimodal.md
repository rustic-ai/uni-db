# Multi-Model Data Model

Uni combines a property graph with vectors and JSON documents. You can model entities and relationships, attach embeddings, and query structured or semi-structured data in one place.

## What It Provides

- Labeled vertices and typed, directed edges.
- Typed properties plus flexible JSON overflow for ad-hoc fields.
- Vector properties for similarity search — including multi-vector (`List<Vector>`) for ColBERT / late-interaction MaxSim retrieval.
- Rich complex types: typed `List<T>`, string-keyed `Map<K, V>`, `Bytes`, `Point`, and CRDTs.

## Example

=== "Rust"
    ```rust
    use uni_db::{DataType, Uni};

    let db = Uni::open("./my_db").build().await?;

    db.schema()
        .label("Person")
            .property("name", DataType::String)
            .property("age", DataType::Int64)
            .property_nullable("embedding", DataType::Vector { dimensions: 384 })
        .edge_type("KNOWS", &["Person"], &["Person"])
            .property("since", DataType::Date)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Ada', age: 31, city: 'London'})")
        .await?;
    tx.commit().await?;
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")

    db.schema() \
        .label("Person") \
            .property("name", "string") \
            .property("age", "int64") \
            .property_nullable("embedding", "vector:384") \
            .done() \
        .edge_type("KNOWS", ["Person"], ["Person"]) \
            .property("since", "date") \
            .done() \
        .apply()

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Person {name: 'Ada', age: 31, city: 'London'})")
    tx.commit()
    ```

## Use Cases

- Knowledge graphs with semantic search.
- Product graphs with text metadata and embeddings.
- Mixed graph + document workloads in one engine.

## When To Use

Use Uni's multi-model design when your application needs relationships, embeddings, and document-style properties in one consistent database.
