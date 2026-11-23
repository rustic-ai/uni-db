# Supply Chain & BOM Analysis

Supply chains are deeply nested graphs (Part A -> Part B -> Part C). Uni's specialized support for **recursive traversals** and **flexible schema** makes it an excellent fit for Bill of Materials (BOM) management and impact analysis.

## Why Uni for Supply Chain?

| Challenge | Traditional Approach | Uni Approach |
|-----------|----------------------|--------------|
| **Recursive Depth** | SQL `WITH RECURSIVE` is slow and hard to optimize. | **Vectorized Recursion**: `MATCH (n)-[:PART_OF*]->(m)` is optimized for deep paths. |
| **Variable Data** | Parts have widely varying specs (resistors vs. CPUs). Relational schemas explode. | **Flexible Schema**: Store distinct part specs as JSON properties in the same `Part` label. |
| **Analytics** | Hard to aggregage cost/weight up the tree. | **Aggregation Pushdown**: Fast summation over traversal paths. |

---

## Scenario: Product Impact Analysis

A supplier notifies you that "Part X" has a defect. You need to find:
1.  All finished products that contain Part X (recursively).
2.  The total revenue at risk.

### 1. Schema Definition

We store variable specs in a JSON property on `Part`.

**Schema (Rust example):**
```rust
use uni_db::{DataType, IndexType, ScalarType};

db.schema()
    .label("Part")
        .property("sku", DataType::String)
        .property("cost", DataType::Float64)
        .property_nullable("spec", DataType::CypherValue)  // JSON-like value (DDL `JSON`)
        .index("sku", IndexType::Scalar(ScalarType::BTree))
        .done()
    .label("Supplier")
        .property("name", DataType::String)
        .done()
    .label("Product")
        .property("name", DataType::String)
        .property("price", DataType::Float64)
        .done()
    .edge_type("ASSEMBLED_FROM", &["Part", "Product"], &["Part"])
        .done()
    .edge_type("SUPPLIED_BY", &["Part"], &["Supplier"])
        .done()
    .apply()
    .await?;
```

### 2. Ingestion (With Documents)

When inserting parts, store variable specs as JSON in the `spec` property.

```rust
use serde_json::json;

// Rust Example
// CREATE is a mutation, so run it inside a transaction; the read-only
// `query_with` path rejects writes with `UniError::ReadOnly`.
let session = db.session();
let tx = session.tx().await?;
tx.query_with(
    "CREATE (p:Part {sku: $sku, cost: $cost, spec: $spec})"
)
    .param("sku", "RES-10K")
    .param("cost", 0.05)
    .param("spec", json!({
        "type": "resistor",
        "specs": { "resistance": "10k", "tolerance": "5%" },
        "compliance": ["RoHS"]
    }))
    .fetch_all()
    .await?;
tx.commit().await?;
```

### 3. Query: BOM Explosion

Find all products affected by the defective part.

```cypher
// Start at the defective part
MATCH (defective:Part {sku: 'RES-10K'})

// Traverse UP the assembly tree (incoming ASSEMBLED_FROM edges)
// No written upper bound — the planner caps this at 100 hops
MATCH (product:Product)-[:ASSEMBLED_FROM*]->(defective)

// Return unique affected products and their price
RETURN DISTINCT
    product.name,
    product.price
ORDER BY product.price DESC
```

!!! tip "Bounded vs Unbounded Paths"
    `[*]` writes no upper bound, so the planner supplies one: **100 hops**. For
    most BOM trees that is ample — assembly hierarchies are rarely deeper than
    ~20 levels — and `[*1..20]` states the intent explicitly. If a hierarchy
    ever does exceed 100 levels the query warns that its results are incomplete,
    so write an explicit bound rather than relying on the default.

    These queries return *endpoints* (`part`, `affected`) rather than path
    variables, which keeps them cheap: Uni only expands full paths when a query
    actually asks for one. See
    [Variable-Length Paths](../guides/cypher-querying.md#variable-length-paths).

### 4. Query: Cost Rollup

Calculate the total cost of a product by summing the cost of all its constituent parts.

```cypher
// No written upper bound — the planner caps this at 100 hops
MATCH (p:Product {name: 'Smartphone X'})
MATCH (p)-[:ASSEMBLED_FROM*]->(part:Part)
RETURN p.name, SUM(part.cost) AS total_bom_cost
```

### Key Advantages

*   **Deep Traversal Speed**: Uni's adjacency cache ensures that each hop is a simple memory lookup, not a disk seek. BOM explosions that take seconds in SQL take milliseconds in Uni.
*   **Schema Flexibility**: You can store resistors, capacitors, screens, and batteries in the same `Part` dataset without a sparse column mess.
*   **Vector Potential**: You could even add embeddings to parts (e.g., image of the component) to find visual duplicates in your inventory.
