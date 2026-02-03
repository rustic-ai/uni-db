# Programming Guide

This guide walks you through building applications with Uni using the Rust or Python API. Select your preferred language - all code examples will switch to match your choice.

## Prerequisites

Before starting, ensure you have Uni installed for your language:

=== "Rust"

    Add Uni to your `Cargo.toml`:

    ```toml
    [dependencies]
    uni-db = { git = "https://github.com/rustic-ai/uni" }
    tokio = { version = "1", features = ["full"] }
    ```

=== "Python"

    Install from PyPI or build from source:

    ```bash
    pip install uni-db

    # Or build from source
    cd bindings/uni-db
    pip install maturin
    maturin develop --release
    ```

---

## Opening a Database

The first step is opening or creating a database. Uni uses a builder pattern for configuration.

=== "Rust"

    ```rust
    use uni_db::*;

    #[tokio::main]
    async fn main() -> Result<()> {
        // Open or create a database (creates if doesn't exist)
        let db = Uni::open("./my-graph").build().await?;

        // Open existing database (fails if doesn't exist)
        let db = Uni::open_existing("./my-graph").build().await?;

        // Create new database (fails if already exists)
        let db = Uni::create("./my-graph").build().await?;

        // In-memory database (for testing)
        let db = Uni::in_memory().build().await?;

        Ok(())
    }
    ```

=== "Python"

    ```python
    import uni_db

    # Open or create a database (creates if doesn't exist)
    db = uni_db.DatabaseBuilder.open("./my-graph").build()

    # Open existing database (fails if doesn't exist)
    db = uni_db.DatabaseBuilder.open_existing("./my-graph").build()

    # Create new database (fails if already exists)
    db = uni_db.DatabaseBuilder.create("./my-graph").build()

    # Temporary in-memory database
    db = uni_db.DatabaseBuilder.temporary().build()

    # Simple shorthand (open or create)
    db = uni_db.Database("./my-graph")
    ```

### Configuration Options

Configure cache size, parallelism, and other options:

=== "Rust"

    ```rust
    let db = Uni::open("./my-graph")
        .cache_size(2 * 1024 * 1024 * 1024)  // 2 GB cache
        .parallelism(8)                       // 8 worker threads
        .build()
        .await?;
    ```

=== "Python"

    ```python
    db = (
        uni_db.DatabaseBuilder.open("./my-graph")
        .cache_size(2 * 1024 * 1024 * 1024)  # 2 GB cache
        .parallelism(8)                       # 8 worker threads
        .build()
    )
    ```

### Cloud Storage

Open databases directly from cloud object stores:

=== "Rust"

    ```rust
    // Amazon S3
    let db = Uni::open("s3://my-bucket/graph-data").build().await?;

    // Google Cloud Storage
    let db = Uni::open("gs://my-bucket/graph-data").build().await?;

    // Azure Blob Storage
    let db = Uni::open("az://my-container/graph-data").build().await?;
    ```

=== "Python"

    ```python
    # Amazon S3
    db = uni_db.DatabaseBuilder.open("s3://my-bucket/graph-data").build()

    # Google Cloud Storage
    db = uni_db.DatabaseBuilder.open("gs://my-bucket/graph-data").build()

    # Azure Blob Storage
    db = uni_db.DatabaseBuilder.open("az://my-container/graph-data").build()
    ```

Credentials are resolved automatically from environment variables or standard config files (AWS credentials, GCP Application Default Credentials, Azure CLI).

### Hybrid Mode (Local + Cloud)

For optimal write performance with cloud durability, use hybrid mode:

=== "Rust"

    ```rust
    use uni_common::CloudStorageConfig;

    // Local cache with S3 backend
    let db = Uni::open("./local-cache")
        .hybrid("./local-cache", "s3://my-bucket/graph-data")
        .cloud_config(CloudStorageConfig::S3 {
            bucket: "my-bucket".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_hosted_style: false,
        })
        .build()
        .await?;
    ```

=== "Python"

    ```python
    # Local cache with S3 backend
    db = (
        uni_db.DatabaseBuilder.open("./local-cache")
        .hybrid("./local-cache", "s3://my-bucket/graph-data")
        .build()
    )
    ```

In hybrid mode:

- **Writes** go to local WAL + L0 buffer (low latency)
- **Flushes** persist data to cloud storage (configurable interval, default 5 seconds)
- **Reads** merge local L0 with cloud storage data

### Auto-Flush Configuration

Control when data is flushed to storage:

=== "Rust"

    ```rust
    use std::time::Duration;
    use uni_db::UniConfig;

    let mut config = UniConfig::default();
    config.auto_flush_threshold = 10_000; // Flush at 10K mutations
    config.auto_flush_interval = Some(Duration::from_secs(5)); // Or every 5 seconds
    config.auto_flush_min_mutations = 1; // With at least 1 mutation

    let db = Uni::open("./my-graph").config(config).build().await?;

    // Disable time-based flush (mutation threshold only)
    let mut config = UniConfig::default();
    config.auto_flush_interval = None;
    let db = Uni::open("./my-graph").config(config).build().await?;
    ```

=== "Python"

    Auto-flush tuning is not yet exposed in the Python bindings. Use the Rust API for now.

---

## Defining Schema

Before inserting data, define your graph schema with vertex labels, edge types, and properties.

### Using the Schema Builder

=== "Rust"

    ```rust
    db.schema()
        // Define Person vertex type
        .label("Person")
            .property("name", DataType::String)
            .property("age", DataType::Int32)
            .property_nullable("email", DataType::String)
            .index("name", IndexType::Scalar(ScalarType::BTree))
        // Define Company vertex type
        .label("Company")
            .property("name", DataType::String)
            .property("founded", DataType::Int32)
        // Define WORKS_AT edge type (Person -> Company)
        .edge_type("WORKS_AT", &["Person"], &["Company"])
            .property("since", DataType::Int32)
            .property_nullable("role", DataType::String)
        // Define KNOWS edge type (Person -> Person)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    ```

=== "Python"

    ```python
    schema = db.schema()

    # Define Person vertex type
    schema = (
        schema.label("Person")
        .property("name", "string")
        .property("age", "int")
        .property_nullable("email", "string")
        .index("name", "btree")
        .done()
    )

    # Define Company vertex type
    schema = (
        schema.label("Company")
        .property("name", "string")
        .property("founded", "int")
        .done()
    )

    # Define edge types
    schema = (
        schema.edge_type("WORKS_AT", ["Person"], ["Company"])
        .property("since", "int")
        .property_nullable("role", "string")
        .done()
    )

    schema = (
        schema.edge_type("KNOWS", ["Person"], ["Person"])
        .done()
    )

    # Apply all schema changes
    schema.apply()
    ```

### Quick Schema Methods

For simple cases, use the schema builder (Rust) or direct schema calls (Python):

=== "Rust"

    ```rust
    db.schema()
        .label("Person")
            .property("name", DataType::String)
            .property_nullable("email", DataType::String)
            .index("name", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;
    ```

=== "Python"

    ```python
    # Create label
    db.create_label("Person")

    # Add property (label, name, type, nullable)
    db.add_property("Person", "name", "string", False)  # required
    db.add_property("Person", "email", "string", True)  # nullable

    # Create index
    db.create_scalar_index("Person", "name", "btree")
    ```

### Schemaless Labels

!!! tip "Flexible Schema Development"
    Labels without property definitions support flexible, schemaless properties. Properties not in the schema are automatically stored in `overflow_json` and are queryable via automatic query rewriting.

=== "Rust"

    ```rust
    // Create a label with NO properties defined
    db.schema().label("Document").apply().await?;

    // Insert with arbitrary properties
    db.execute("CREATE (:Document {
        title: 'Research Paper',
        author: 'Alice',
        tags: ['ml', 'nlp'],
        year: 2024
    })").await?;

    // Query works normally (automatic rewriting)
    let results = db.query("
        MATCH (d:Document)
        WHERE d.author = 'Alice' AND d.year > 2023
        RETURN d.title, d.tags
    ").await?;

    // Later, promote frequently-queried properties to schema
    db.schema()
        .label("Document")
        .property("title", DataType::String)  // Now typed column
        .property("author", DataType::String) // Now typed column
        .apply().await?;
    // 'tags' and 'year' remain in overflow_json
    ```

=== "Python"

    ```python
    # Create a label with NO properties defined
    db.schema().label("Document").apply()

    # Insert with arbitrary properties
    db.execute("""CREATE (:Document {
        title: 'Research Paper',
        author: 'Alice',
        tags: ['ml', 'nlp'],
        year: 2024
    })""")

    # Query works normally (automatic rewriting)
    results = db.query("""
        MATCH (d:Document)
        WHERE d.author = 'Alice' AND d.year > 2023
        RETURN d.title, d.tags
    """)

    # Later, promote frequently-queried properties to schema
    (
        db.schema()
        .label("Document")
        .property("title", "string")  # Now typed column
        .property("author", "string") # Now typed column
        .apply()
    )
    # 'tags' and 'year' remain in overflow_json
    ```

**Use Cases:**
- 🚀 Rapid prototyping without predefined schema
- 📝 User-defined metadata fields
- 🔄 Evolving schemas without migrations
- 🎯 Optional/rare properties

**Performance:**
- Schema properties: Fast filtering/sorting (columnar)
- Overflow properties: Slower but flexible (JSONB parsing)

[Learn more about schemaless properties →](../guides/schema-design.md#schemaless-properties-overflow)

---

## Basic Queries

Execute Cypher queries to read and write data.

### Creating Data

=== "Rust"

    ```rust
    // Create a single vertex
    db.execute("CREATE (p:Person {name: 'Alice', age: 30})").await?;

    // Create multiple vertices and an edge
    db.execute(r#"
        CREATE (alice:Person {name: 'Alice', age: 30})
        CREATE (bob:Person {name: 'Bob', age: 25})
        CREATE (alice)-[:KNOWS {since: 2020}]->(bob)
    "#).await?;

    // Create edge between existing vertices
    db.execute(r#"
        MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'})
        CREATE (a)-[:WORKS_AT {since: 2022, role: 'Engineer'}]->(c)
    "#).await?;
    ```

=== "Python"

    ```python
    # Create a single vertex
    db.execute("CREATE (p:Person {name: 'Alice', age: 30})")

    # Create multiple vertices and an edge
    db.execute("""
        CREATE (alice:Person {name: 'Alice', age: 30})
        CREATE (bob:Person {name: 'Bob', age: 25})
        CREATE (alice)-[:KNOWS {since: 2020}]->(bob)
    """)

    # Create edge between existing vertices
    db.execute("""
        MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'})
        CREATE (a)-[:WORKS_AT {since: 2022, role: 'Engineer'}]->(c)
    """)
    ```

### Reading Data

=== "Rust"

    ```rust
    // Simple query
    let results = db.query("MATCH (p:Person) RETURN p.name, p.age").await?;

    for row in &results {
        let name: String = row.get("p.name")?;
        let age: i32 = row.get("p.age")?;
        println!("{} is {} years old", name, age);
    }

    // Query with filtering and ordering
    let results = db.query(r#"
        MATCH (p:Person)
        WHERE p.age >= 25
        RETURN p.name AS name, p.age AS age
        ORDER BY p.age DESC
        LIMIT 10
    "#).await?;

    for row in &results {
        println!("{}: {}", row.get::<String>("name")?, row.get::<i32>("age")?);
    }
    ```

=== "Python"

    ```python
    # Simple query
    results = db.query("MATCH (p:Person) RETURN p.name AS name, p.age AS age")

    for row in results:
        print(f"{row['name']} is {row['age']} years old")

    # Query with filtering and ordering
    results = db.query("""
        MATCH (p:Person)
        WHERE p.age >= 25
        RETURN p.name AS name, p.age AS age
        ORDER BY p.age DESC
        LIMIT 10
    """)

    for row in results:
        print(f"{row['name']}: {row['age']}")
    ```

### Parameterized Queries

Always use parameters for user-provided values to prevent injection attacks:

=== "Rust"

    ```rust
    // Single parameter
    let results = db.query_with("MATCH (p:Person) WHERE p.name = $name RETURN p")
        .param("name", "Alice")
        .fetch_all()
        .await?;

    // Multiple parameters
    let results = db.query_with(r#"
        MATCH (p:Person)
        WHERE p.age >= $min_age AND p.age <= $max_age
        RETURN p.name AS name, p.age AS age
    "#)
        .param("min_age", 20)
        .param("max_age", 40)
        .fetch_all()
        .await?;

    // Parameters from HashMap
    let params = hashmap! {
        "name" => "Alice".into(),
        "company" => "TechCorp".into(),
    };
    let results = db.query_with(
        "MATCH (p:Person {name: $name})-[:WORKS_AT]->(c:Company {name: $company}) RETURN p, c"
    )
        .params(params)
        .fetch_all()
        .await?;
    ```

=== "Python"

    ```python
    # Single parameter
    results = (
        db.query_with("MATCH (p:Person) WHERE p.name = $name RETURN p.name AS name")
        .param("name", "Alice")
        .fetch_all()
    )

    # Multiple parameters
    results = (
        db.query_with("""
            MATCH (p:Person)
            WHERE p.age >= $min_age AND p.age <= $max_age
            RETURN p.name AS name, p.age AS age
        """)
        .param("min_age", 20)
        .param("max_age", 40)
        .fetch_all()
    )

    # Parameters from dict
    params = {"name": "Alice", "company": "TechCorp"}
    results = (
        db.query_with("""
            MATCH (p:Person {name: $name})-[:WORKS_AT]->(c:Company {name: $company})
            RETURN p.name AS person, c.name AS company
        """)
        .params(params)
        .fetch_all()
    )
    ```

---

## Graph Traversals

Traverse relationships to explore connected data.

### Basic Traversal

=== "Rust"

    ```rust
    // Find all people that Alice knows
    let results = db.query(r#"
        MATCH (alice:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
        RETURN friend.name AS name
    "#).await?;

    // Find friends of friends
    let results = db.query(r#"
        MATCH (alice:Person {name: 'Alice'})-[:KNOWS*2]->(fof:Person)
        WHERE fof.name <> 'Alice'
        RETURN DISTINCT fof.name AS name
    "#).await?;

    // Variable-length paths (1 to 3 hops)
    let results = db.query(r#"
        MATCH path = (alice:Person {name: 'Alice'})-[:KNOWS*1..3]->(other:Person)
        RETURN other.name AS name, length(path) AS distance
    "#).await?;
    ```

=== "Python"

    ```python
    # Find all people that Alice knows
    results = db.query("""
        MATCH (alice:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
        RETURN friend.name AS name
    """)

    # Find friends of friends
    results = db.query("""
        MATCH (alice:Person {name: 'Alice'})-[:KNOWS*2]->(fof:Person)
        WHERE fof.name <> 'Alice'
        RETURN DISTINCT fof.name AS name
    """)

    # Variable-length paths (1 to 3 hops)
    results = db.query("""
        MATCH path = (alice:Person {name: 'Alice'})-[:KNOWS*1..3]->(other:Person)
        RETURN other.name AS name, length(path) AS distance
    """)
    ```

### Aggregations

=== "Rust"

    ```rust
    // Count friends per person
    let results = db.query(r#"
        MATCH (p:Person)-[:KNOWS]->(friend:Person)
        RETURN p.name AS person, COUNT(friend) AS friend_count
        ORDER BY friend_count DESC
    "#).await?;

    // Average age by company
    let results = db.query(r#"
        MATCH (p:Person)-[:WORKS_AT]->(c:Company)
        RETURN c.name AS company, AVG(p.age) AS avg_age, COUNT(p) AS employees
    "#).await?;
    ```

=== "Python"

    ```python
    # Count friends per person
    results = db.query("""
        MATCH (p:Person)-[:KNOWS]->(friend:Person)
        RETURN p.name AS person, COUNT(friend) AS friend_count
        ORDER BY friend_count DESC
    """)

    # Average age by company
    results = db.query("""
        MATCH (p:Person)-[:WORKS_AT]->(c:Company)
        RETURN c.name AS company, AVG(p.age) AS avg_age, COUNT(p) AS employees
    """)
    ```

---

## Transactions

Group multiple operations into atomic transactions.

### Explicit Transactions

=== "Rust"

    ```rust
    // Begin transaction
    let tx = db.begin().await?;

    // Execute operations
    tx.execute("CREATE (p:Person {name: 'Carol', age: 28})").await?;
    tx.execute("CREATE (p:Person {name: 'Dave', age: 32})").await?;
    tx.execute(r#"
        MATCH (c:Person {name: 'Carol'}), (d:Person {name: 'Dave'})
        CREATE (c)-[:KNOWS]->(d)
    "#).await?;

    // Commit (or rollback on error)
    tx.commit().await?;
    ```

=== "Python"

    ```python
    # Begin transaction
    tx = db.begin()

    try:
        # Execute operations
        tx.query("CREATE (p:Person {name: 'Carol', age: 28})")
        tx.query("CREATE (p:Person {name: 'Dave', age: 32})")
        tx.query("""
            MATCH (c:Person {name: 'Carol'}), (d:Person {name: 'Dave'})
            CREATE (c)-[:KNOWS]->(d)
        """)

        # Commit
        tx.commit()
    except Exception as e:
        # Rollback on error
        tx.rollback()
        raise e
    ```

### Transaction Closure (Rust)

=== "Rust"

    ```rust
    // Auto-commit on success, auto-rollback on error
    db.transaction(|tx| async move {
        tx.execute("CREATE (a:Person {name: 'Eve', age: 26})").await?;
        tx.execute("CREATE (b:Person {name: 'Frank', age: 29})").await?;
        tx.execute(r#"
            MATCH (e:Person {name: 'Eve'}), (f:Person {name: 'Frank'})
            CREATE (e)-[:KNOWS]->(f)
        "#).await?;
        Ok(())
    }).await?;
    ```

=== "Python"

    ```python
    # Python uses explicit try/except pattern shown above
    # No closure-based API available
    ```

---

## Vector Search

Store and search vector embeddings for semantic similarity.

### Setting Up Vector Properties

=== "Rust"

    ```rust
    // Add vector property to schema
    db.schema()
        .label("Document")
            .property("title", DataType::String)
            .property("content", DataType::String)
            .vector("embedding", 384)  // 384 dimensions
            .index("embedding", IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Hnsw { m: 16, ef_construction: 200 },
                metric: VectorMetric::Cosine,
            }))
        .apply()
        .await?;
    ```

=== "Python"

    ```python
    # Add vector property (vector:N syntax for N dimensions)
    db.add_property("Document", "embedding", "vector:384", False)

    # Create vector index (label, property, metric)
    db.create_vector_index("Document", "embedding", "cosine")
    ```

### Inserting Vectors

=== "Rust"

    ```rust
    // Insert document with embedding
    let embedding: Vec<f32> = compute_embedding("Machine learning fundamentals");

    db.query_with(r#"
        CREATE (d:Document {
            title: $title,
            content: $content,
            embedding: $embedding
        })
    "#)
        .param("title", "ML Basics")
        .param("content", "Machine learning fundamentals...")
        .param("embedding", embedding)
        .fetch_all()
        .await?;
    ```

=== "Python"

    ```python
    # Insert document with embedding
    embedding = compute_embedding("Machine learning fundamentals")

    db.query_with("""
        CREATE (d:Document {
            title: $title,
            content: $content,
            embedding: $embedding
        })
    """).param("title", "ML Basics") \
       .param("content", "Machine learning fundamentals...") \
       .param("embedding", embedding) \
       .fetch_all()
    ```

### Searching Vectors

=== "Rust"

    ```rust
    let query_vec = compute_embedding("deep learning neural networks");

    // Vector search via Cypher (procedure)
    let results = db.query_with(r#"
        CALL uni.vector.query('Document', 'embedding', $vec, 10)
        YIELD node, distance
        RETURN node.title AS title, distance
        ORDER BY distance
    "#)
        .param("vec", query_vec.clone())
        .fetch_all()
        .await?;

    // Vector search via Cypher (operator)
    let results = db.query_with(r#"
        MATCH (d:Document)
        WHERE d.embedding ~= $vec
        RETURN d.title AS title, d._score AS score
        ORDER BY score DESC
        LIMIT 10
    "#)
        .param("vec", query_vec)
        .fetch_all()
        .await?;
    ```

=== "Python"

    ```python
    query_vec = compute_embedding("deep learning neural networks")

    # Vector search via Cypher (procedure)
    results = db.query_with("""
        CALL uni.vector.query('Document', 'embedding', $vec, 10)
        YIELD node, distance
        RETURN node.title AS title, distance
        ORDER BY distance
    """).param("vec", query_vec).fetch_all()

    for row in results:
        print(f"{row['title']}: {row['distance']:.4f}")

    # Vector search via Cypher (operator)
    results = db.query_with("""
        MATCH (d:Document)
        WHERE d.embedding ~= $vec
        RETURN d.title AS title, d._score AS score
        ORDER BY score DESC
        LIMIT 10
    """).param("vec", query_vec).fetch_all()
    ```

---

## Bulk Loading

For large datasets, use the bulk writer for efficient loading.

=== "Rust"

    ```rust
    // Create bulk writer with deferred indexing
    let mut bulk = db.bulk_writer()
        .defer_vector_indexes(true)
        .defer_scalar_indexes(true)
        .batch_size(50_000)
        .on_progress(|p| {
            println!("{:?}: {} rows processed", p.phase, p.rows_processed);
        })
        .build()?;

    // Insert vertices in bulk
    let people: Vec<HashMap<String, Value>> = (0..100_000)
        .map(|i| hashmap! {
            "name" => format!("Person-{}", i).into(),
            "age" => (20 + i % 50).into(),
        })
        .collect();

    let vids = bulk.insert_vertices("Person", people).await?;

    // Insert edges in bulk (src_vid, dst_vid, properties)
    let edges: Vec<EdgeData> = vids.windows(2)
        .map(|pair| EdgeData {
            src: pair[0],
            dst: pair[1],
            properties: hashmap! { "since" => 2024.into() },
        })
        .collect();

    bulk.insert_edges("KNOWS", edges).await?;

    // Commit and rebuild indexes
    let stats = bulk.commit().await?;
    println!(
        "Loaded {} vertices, {} edges in {:?}",
        stats.vertices_inserted, stats.edges_inserted, stats.duration
    );
    ```

=== "Python"

    ```python
    # Create bulk writer with configuration
    writer = (
        db.bulk_writer()
        .batch_size(50_000)
        .build()
    )

    # Insert vertices in bulk
    people = [
        {"name": f"Person-{i}", "age": 20 + i % 50}
        for i in range(100_000)
    ]

    vids = writer.insert_vertices("Person", people)

    # Insert edges in bulk (src_vid, dst_vid, properties)
    edges = [
        (vids[i], vids[i + 1], {"since": 2024})
        for i in range(len(vids) - 1)
    ]

    writer.insert_edges("KNOWS", edges)

    # Commit and rebuild indexes
    stats = writer.commit()
    print(f"Loaded {stats.vertices_inserted} vertices, {stats.edges_inserted} edges")
    ```

---

## Sessions

Sessions provide scoped context for multi-tenant queries.

=== "Rust"

    ```rust
    // Create session with tenant context
    let session = db.session()
        .set("tenant_id", "acme-corp")
        .set("user_id", "user-123")
        .build();

    // All queries have access to $session.* variables
    let results = session.query(r#"
        MATCH (d:Document)
        WHERE d.tenant_id = $session.tenant_id
        RETURN d.title AS title
    "#).await?;

    // Query with additional parameters
    let results = session.query_with(r#"
        MATCH (d:Document)
        WHERE d.tenant_id = $session.tenant_id
          AND d.status = $status
        RETURN d.title AS title
    "#)
        .param("status", "published")
        .execute()
        .await?;

    // Read session variable
    let tenant: &str = session.get("tenant_id").unwrap().as_str().unwrap();
    ```

=== "Python"

    ```python
    # Create session with tenant context
    builder = db.session()
    builder.set("tenant_id", "acme-corp")
    builder.set("user_id", "user-123")
    session = builder.build()

    # Execute queries with session context
    results = session.query("""
        MATCH (d:Document)
        WHERE d.tenant_id = $session.tenant_id
        RETURN d.title AS title
    """)

    # Read session variable
    tenant = session.get("tenant_id")
    print(f"Tenant: {tenant}")
    ```

---

## EXPLAIN and PROFILE

Analyze query execution plans.

=== "Rust"

    ```rust
    // Get query plan without executing
    let plan = db.explain("MATCH (p:Person) WHERE p.age > 25 RETURN p.name").await?;
    println!("Plan:\n{}", plan.plan_text);
    println!("Estimated cost: {}", plan.cost_estimates.estimated_cost);

    // Execute with profiling
    let (results, profile) = db.profile("MATCH (p:Person) WHERE p.age > 25 RETURN p.name").await?;
    println!("Total time: {}ms", profile.total_time_ms);
    println!("Peak memory: {} bytes", profile.peak_memory_bytes);
    ```

=== "Python"

    ```python
    # Get query plan without executing
    plan = db.explain("MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name")
    print(f"Plan:\n{plan['plan_text']}")
    print(f"Estimated cost: {plan['cost_estimates']}")

    # Execute with profiling
    results, profile = db.profile("MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name")
    print(f"Total time: {profile['total_time_ms']}ms")
    print(f"Peak memory: {profile['peak_memory_bytes']} bytes")
    ```

---

## Time Travel Queries

Access historical database states using Cypher clauses.

=== "Rust"

    ```rust
    // Query a specific snapshot by ID
    let results = db.query(r#"
        MATCH (n:Person)
        RETURN n.name AS name
        VERSION AS OF 'snap_123'
    "#).await?;

    // Query the snapshot that was current at a timestamp
    let results = db.query(r#"
        MATCH (n:Person)
        RETURN n.name AS name
        TIMESTAMP AS OF '2025-02-01T12:00:00Z'
    "#).await?;
    ```

=== "Python"

    ```python
    # Query a specific snapshot by ID
    results = db.query("""
        MATCH (n:Person)
        RETURN n.name AS name
        VERSION AS OF 'snap_123'
    """)

    # Query the snapshot that was current at a timestamp
    results = db.query("""
        MATCH (n:Person)
        RETURN n.name AS name
        TIMESTAMP AS OF '2025-02-01T12:00:00Z'
    """)
    ```

---

## Graph Algorithms

Run built-in graph algorithms.

### PageRank

=== "Rust"

    ```rust
    // Using the algorithm builder
    let rankings = db.algo()
        .pagerank()
        .labels(&["Person"])
        .edge_types(&["KNOWS"])
        .damping(0.85)
        .max_iterations(50)
        .run()
        .await?;

    for (vid, score) in rankings.iter().take(10) {
        println!("VID: {}, Score: {:.6}", vid, score);
    }

    // Via Cypher
    let results = db.query(r#"
        CALL algo.pageRank(['Person'], ['KNOWS'])
        YIELD nodeId, score
        RETURN nodeId, score
        ORDER BY score DESC
        LIMIT 10
    "#).await?;
    ```

=== "Python"

    ```python
    # PageRank via Cypher
    results = db.query("""
        CALL algo.pageRank(['Person'], ['KNOWS'])
        YIELD nodeId, score
        RETURN nodeId, score
        ORDER BY score DESC
        LIMIT 10
    """)

    for row in results:
        print(f"Node: {row['nodeId']}, Score: {row['score']:.6f}")
    ```

### Weakly Connected Components

=== "Rust"

    ```rust
    // Find connected components
    let components = db.algo()
        .wcc()
        .labels(&["Person"])
        .edge_types(&["KNOWS"])
        .run()
        .await?;

    // Count component sizes
    let mut sizes: HashMap<i64, usize> = HashMap::new();
    for (_, component_id) in &components {
        *sizes.entry(*component_id).or_default() += 1;
    }
    println!("Found {} components", sizes.len());
    ```

=== "Python"

    ```python
    # WCC via Cypher
    results = db.query("""
        CALL algo.wcc(['Person'], ['KNOWS'])
        YIELD nodeId, componentId
        RETURN componentId, COUNT(*) AS size
        ORDER BY size DESC
    """)

    print(f"Found {len(results)} components")
    for row in results[:5]:
        print(f"Component {row['componentId']}: {row['size']} nodes")
    ```

### Community Detection

=== "Rust"

    ```rust
    // Louvain community detection via Cypher
    let results = db.query(r#"
        CALL algo.louvain(['Person'], ['KNOWS'])
        YIELD nodeId, communityId
        RETURN communityId, COUNT(*) AS size
        ORDER BY size DESC
        LIMIT 10
    "#).await?;
    ```

=== "Python"

    ```python
    # Louvain community detection
    results = db.query("""
        CALL algo.louvain(['Person'], ['KNOWS'])
        YIELD nodeId, communityId
        RETURN communityId, COUNT(*) AS size
        ORDER BY size DESC
        LIMIT 10
    """)

    for row in results:
        print(f"Community {row['communityId']}: {row['size']} members")
    ```

---

## Error Handling

Handle errors appropriately in your application.

=== "Rust"

    ```rust
    use uni_db::*;

    match db.query("INVALID CYPHER").await {
        Ok(results) => {
            // Process results
        }
        Err(UniError::Parse { message, line, column, .. }) => {
            eprintln!(
                "Syntax error at {}:{}: {}",
                line.unwrap_or(0),
                column.unwrap_or(0),
                message
            );
        }
        Err(UniError::Query { message, .. }) => {
            eprintln!("Query execution error: {}", message);
        }
        Err(UniError::Timeout { timeout_ms }) => {
            eprintln!("Query timed out after {}ms", timeout_ms);
        }
        Err(e) => {
            eprintln!("Error: {}", e);
        }
    }
    ```

=== "Python"

    ```python
    try:
        results = db.query("INVALID CYPHER")
    except RuntimeError as e:
        print(f"Query error: {e}")
    except ValueError as e:
        print(f"Invalid parameter: {e}")
    except OSError as e:
        print(f"Database I/O error: {e}")
    ```

---

## Complete Example: Social Network

Here's a complete example building a simple social network application.

=== "Rust"

    ```rust
    use uni_db::*;
    use std::collections::HashMap;

    #[tokio::main]
    async fn main() -> Result<()> {
        // Create database
        let db = Uni::open("./social-network")
            .cache_size(512 * 1024 * 1024)
            .build()
            .await?;

        // Define schema
        db.schema()
            .label("User")
                .property("username", DataType::String)
                .property("email", DataType::String)
                .property("joined", DataType::Int32)
                .index("username", IndexType::Scalar(ScalarType::BTree))
            .label("Post")
                .property("content", DataType::String)
                .property("timestamp", DataType::Int64)
            .edge_type("FOLLOWS", &["User"], &["User"])
            .edge_type("POSTED", &["User"], &["Post"])
            .edge_type("LIKES", &["User"], &["Post"])
            .apply()
            .await?;

        // Create users
        db.transaction(|tx| async move {
            tx.execute("CREATE (u:User {username: 'alice', email: 'alice@example.com', joined: 2023})").await?;
            tx.execute("CREATE (u:User {username: 'bob', email: 'bob@example.com', joined: 2023})").await?;
            tx.execute("CREATE (u:User {username: 'carol', email: 'carol@example.com', joined: 2024})").await?;

            // Create follow relationships
            tx.execute(r#"
                MATCH (a:User {username: 'alice'}), (b:User {username: 'bob'})
                CREATE (a)-[:FOLLOWS]->(b)
            "#).await?;
            tx.execute(r#"
                MATCH (b:User {username: 'bob'}), (c:User {username: 'carol'})
                CREATE (b)-[:FOLLOWS]->(c)
            "#).await?;

            Ok(())
        }).await?;

        // Create posts
        db.execute(r#"
            MATCH (a:User {username: 'alice'})
            CREATE (a)-[:POSTED]->(p:Post {content: 'Hello world!', timestamp: 1704067200000})
        "#).await?;

        // Query: Get user's feed (posts from people they follow)
        let feed = db.query_with(r#"
            MATCH (me:User {username: $username})-[:FOLLOWS]->(friend:User)-[:POSTED]->(post:Post)
            RETURN friend.username AS author, post.content AS content, post.timestamp AS ts
            ORDER BY post.timestamp DESC
            LIMIT 20
        "#)
            .param("username", "alice")
            .fetch_all()
            .await?;

        println!("Alice's Feed:");
        for row in &feed {
            println!("  @{}: {}",
                row.get::<String>("author")?,
                row.get::<String>("content")?
            );
        }

        // Query: Suggest friends (friends of friends not already following)
        let suggestions = db.query_with(r#"
            MATCH (me:User {username: $username})-[:FOLLOWS*2]->(suggestion:User)
            WHERE NOT (me)-[:FOLLOWS]->(suggestion)
              AND suggestion.username <> $username
            RETURN DISTINCT suggestion.username AS username, COUNT(*) AS mutual
            ORDER BY mutual DESC
            LIMIT 5
        "#)
            .param("username", "alice")
            .fetch_all()
            .await?;

        println!("\nFriend Suggestions:");
        for row in &suggestions {
            println!("  @{} ({} mutual)",
                row.get::<String>("username")?,
                row.get::<i64>("mutual")?
            );
        }

        // Run PageRank to find influential users
        let influential = db.query(r#"
            CALL algo.pageRank(['User'], ['FOLLOWS'])
            YIELD nodeId, score
            MATCH (u:User) WHERE id(u) = nodeId
            RETURN u.username AS username, score
            ORDER BY score DESC
            LIMIT 5
        "#).await?;

        println!("\nMost Influential Users:");
        for row in &influential {
            println!("  @{}: {:.4}",
                row.get::<String>("username")?,
                row.get::<f64>("score")?
            );
        }

        Ok(())
    }
    ```

=== "Python"

    ```python
    import uni_db

    def main():
        # Create database
        db = uni_db.DatabaseBuilder.open("./social-network") \
            .cache_size(512 * 1024 * 1024) \
            .build()

        # Define schema
        db.create_label("User")
        db.add_property("User", "username", "string", False)
        db.add_property("User", "email", "string", False)
        db.add_property("User", "joined", "int", False)
        db.create_scalar_index("User", "username", "btree")

        db.create_label("Post")
        db.add_property("Post", "content", "string", False)
        db.add_property("Post", "timestamp", "int", False)

        db.create_edge_type("FOLLOWS", ["User"], ["User"])
        db.create_edge_type("POSTED", ["User"], ["Post"])
        db.create_edge_type("LIKES", ["User"], ["Post"])

        # Create users in transaction
        tx = db.begin()
        try:
            tx.query("CREATE (u:User {username: 'alice', email: 'alice@example.com', joined: 2023})")
            tx.query("CREATE (u:User {username: 'bob', email: 'bob@example.com', joined: 2023})")
            tx.query("CREATE (u:User {username: 'carol', email: 'carol@example.com', joined: 2024})")

            # Create follow relationships
            tx.query("""
                MATCH (a:User {username: 'alice'}), (b:User {username: 'bob'})
                CREATE (a)-[:FOLLOWS]->(b)
            """)
            tx.query("""
                MATCH (b:User {username: 'bob'}), (c:User {username: 'carol'})
                CREATE (b)-[:FOLLOWS]->(c)
            """)

            tx.commit()
        except Exception as e:
            tx.rollback()
            raise e

        # Create posts
        db.execute("""
            MATCH (a:User {username: 'alice'})
            CREATE (a)-[:POSTED]->(p:Post {content: 'Hello world!', timestamp: 1704067200000})
        """)

        # Query: Get user's feed
        feed = db.query_with("""
            MATCH (me:User {username: $username})-[:FOLLOWS]->(friend:User)-[:POSTED]->(post:Post)
            RETURN friend.username AS author, post.content AS content, post.timestamp AS ts
            ORDER BY post.timestamp DESC
            LIMIT 20
        """).param("username", "alice").fetch_all()

        print("Alice's Feed:")
        for row in feed:
            print(f"  @{row['author']}: {row['content']}")

        # Query: Suggest friends
        suggestions = db.query_with("""
            MATCH (me:User {username: $username})-[:FOLLOWS*2]->(suggestion:User)
            WHERE NOT (me)-[:FOLLOWS]->(suggestion)
              AND suggestion.username <> $username
            RETURN DISTINCT suggestion.username AS username, COUNT(*) AS mutual
            ORDER BY mutual DESC
            LIMIT 5
        """).param("username", "alice").fetch_all()

        print("\nFriend Suggestions:")
        for row in suggestions:
            print(f"  @{row['username']} ({row['mutual']} mutual)")

        # Run PageRank
        influential = db.query("""
            CALL algo.pageRank(['User'], ['FOLLOWS'])
            YIELD nodeId, score
            MATCH (u:User) WHERE id(u) = nodeId
            RETURN u.username AS username, score
            ORDER BY score DESC
            LIMIT 5
        """)

        print("\nMost Influential Users:")
        for row in influential:
            print(f"  @{row['username']}: {row['score']:.4f}")

    if __name__ == "__main__":
        main()
    ```

---

## Next Steps

You now have the foundation to build applications with Uni. Continue learning:

| Topic | Description |
|-------|-------------|
| [Cypher Querying](../guides/cypher-querying.md) | Complete query language reference |
| [Vector Search](../guides/vector-search.md) | Semantic search patterns |
| [Schema Design](../guides/schema-design.md) | Best practices for graph modeling |
| [Performance Tuning](../guides/performance-tuning.md) | Optimization techniques |
| [Rust API Reference](../reference/rust-api.md) | Complete Rust API docs |
| [Python API Reference](../reference/python-api.md) | Complete Python API docs |
