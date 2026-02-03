# Real-Time Fraud Detection

Fraud detection requires analyzing complex relationships (graph) and patterns in real-time, often while ingesting high volumes of transaction data. Uni's **single-writer architecture** with **L0 memory buffering** makes it ideal for high-velocity ingest combined with instant "read-your-writes" queries.

## Why Uni for Fraud Detection?

| Challenge | Traditional Approach | Uni Approach |
|-----------|----------------------|--------------|
| **Ingest Rate** | Graph DBs struggle with high write loads; Relational DBs can't do hops. | **L0 Buffer**: Writes hit memory first (WAL-backed), amortized flush to disk. |
| **Freshness** | "Eventual consistency" or ETL lag (minutes/hours). | **Snapshot Isolation**: Readers see writes immediately (if configured). |
| **Deep Links** | Detecting "Payment Rings" requires 3+ hops. SQL joins choke. | **CSR Cache**: O(1) adjacency lookups make multi-hop checks fast. |

---

## Scenario: Payment Ring Detection

We want to detect a circular payment pattern: `User A -> User B -> User C -> User A` happening within a short time window.

### 1. Schema Definition

We model `Users`, `Devices`, and `IPs` as nodes to detect shared infrastructure usage. `Transactions` are edges with timestamps.

**Conceptual schema (illustrative):**
```json
{
  "labels": {
    "User": { "id": 1 },
    "Device": { "id": 2 },
    "IP": { "id": 3 }
  },
  "edge_types": {
    "SENT_MONEY": { "id": 1, "src_labels": ["User"], "dst_labels": ["User"] },
    "USED_DEVICE": { "id": 2, "src_labels": ["User"], "dst_labels": ["Device"] },
    "USED_IP": { "id": 3, "src_labels": ["User"], "dst_labels": ["IP"] }
  },
  "properties": {
    "SENT_MONEY": {
      "amount": { "type": "Float64", "nullable": false },
      "ts": { "type": "Int64", "nullable": false }
    },
    "User": {
      "risk_score": { "type": "Float32", "nullable": true }
    }
  },
  "indexes": []
}
```

**Schema (Rust example):**
```rust
use uni_db::DataType;

db.schema()
    .label("User")
        .property_nullable("risk_score", DataType::Float32)
        .done()
    .label("Device")
        .done()
    .label("IP")
        .done()
    .edge_type("SENT_MONEY", &["User"], &["User"])
        .property("amount", DataType::Float64)
        .property("ts", DataType::Int64)
        .done()
    .edge_type("USED_DEVICE", &["User"], &["Device"])
        .done()
    .edge_type("USED_IP", &["User"], &["IP"])
        .done()
    .apply()
    .await?;
```

### 2. Configuration

For high write throughput, we tune the **WAL** and **L0 Buffer**.

**Rust config example:**
```rust
use std::time::Duration;
use uni_db::UniConfig;

let mut config = UniConfig::default();
config.auto_flush_threshold = 100_000; // Larger in-memory buffer before flushing
config.auto_flush_interval = Some(Duration::from_millis(50));
config.auto_flush_min_mutations = 1;

let db = Uni::open("./fraud_graph")
    .config(config)
    .build()
    .await?;
```

### 3. Streaming Ingestion

Use the **embedded Rust API** to ingest transactions as they happen.

```rust
// Rust API Example (parameterized Cypher)
db.query_with(
    "MATCH (a:User {id: $src}), (b:User {id: $dst})
     CREATE (a)-[:SENT_MONEY {amount: $amount, ts: $ts}]->(b)"
)
    .param("src", user_a_id)
    .param("dst", user_b_id)
    .param("amount", 5000.00)
    .param("ts", current_time)
    .fetch_all()
    .await?;
```

### 4. Real-time Detection Query

Run this query synchronously when a transaction is attempted. It checks for a cycle of length 3-4 involving the sender.

```cypher
// Check for cycles starting from User A
MATCH (a:User)-[t1:SENT_MONEY]->(b:User)-[t2:SENT_MONEY]->(c:User)-[t3:SENT_MONEY]->(a)

// Filter recent transactions only (e.g., last 1 hour)
WHERE t1.ts > $threshold AND t2.ts > $threshold AND t3.ts > $threshold

// Return the cycle details
RETURN a.id, b.id, c.id, t1.amount, t2.amount, t3.amount
```

### 5. Identity Resolution Query

Check if the sender shares an IP or Device with known fraudsters.

```cypher
MATCH (sender:User)-[:USED_DEVICE]->(shared_resource)<-[:USED_DEVICE]-(other:User)
WHERE other.risk_score > 0.8
RETURN count(other) as suspicious_links
```

### Key Advantages

*   **Ingest Speed**: Uni's L0 buffer acts like a Write-Optimized Store (WOS), handling spikes in transaction volume without blocking readers.
*   **Latency**: Checking a 3-hop cycle takes milliseconds due to the in-memory CSR cache.
*   **Data Locality**: The graph structure and properties are co-located; no need to query a Redis cache for "risk scores" separately.
