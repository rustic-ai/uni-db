# REST API Reference

Uni provides a lightweight HTTP API for remote query execution and monitoring. The server is optional and can be started via the CLI or embedded in your application.

## Overview

- **Base URL:** `http://<host>:<port>` (Default: `http://127.0.0.1:8080`)
- **Version:** `v1`
- **Content-Type:** `application/json`

### Authentication (Optional)

If the server is started with `--api-key`, requests to `/api/v1/query` must include the `X-API-Key` header.

---

## Endpoints

### Execute Query

Execute a Cypher query against the database.

**Endpoint:** `POST /api/v1/query`

#### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | Yes | The Cypher query string to execute. |
| `params` | object | No | A map of parameter names to values (for `$param` usage). |

**Example Request:**

```json
{
  "query": "MATCH (n:Person) WHERE n.age > $min_age RETURN n.name, n.age LIMIT 5",
  "params": {
    "min_age": 30
  }
}
```

#### Response

**Success (200 OK):**

| Field | Type | Description |
|-------|------|-------------|
| `columns` | string[] | List of column names in the result. |
| `rows` | any[][] | Array of rows, where each row is an array of values corresponding to `columns`. |
| `execution_time_ms` | number | Time taken to execute the query in milliseconds. |

**Example Response:**

```json
{
  "columns": ["n.name", "n.age"],
  "rows": [
    ["Alice", 32],
    ["Bob", 45]
  ],
  "execution_time_ms": 12
}
```

**Error (400 Bad Request):**

```json
{
  "error": "Parse error: Unexpected token at line 1..."
}
```

---

### Health Check

**Endpoint:** `GET /health`

**Response (200 OK):**

```json
{
  "status": "healthy",
  "version": "0.1.0"
}
```

---

### Metrics

Retrieve Prometheus-compatible metrics for monitoring database health and performance.

**Endpoint:** `GET /api/v1/metrics`

**Response (200 OK):**
Text-based Prometheus format.

```text
# HELP uni_query_duration_seconds Query execution duration
# TYPE uni_query_duration_seconds histogram
uni_query_duration_seconds_bucket{le="0.005"} 12
...
```

*(Note: Metrics availability depends on feature flags and server configuration.)*

---

## Data Types

JSON values in the response map to Uni's internal types as follows:

| Uni Type | JSON Representation |
|----------|---------------------|
| `String` | String |
| `Integer` | Number |
| `Float` | Number |
| `Boolean` | Boolean |
| `Null` | `null` |
| `Vector` | Array of numbers `[0.1, 0.2, ...]` |
| `Node` | Object `{"vid": 123, "label": "Person", "properties": {...}}` |
| `Edge` | Object `{"eid": 456, "edge_type": "KNOWS", "src": 1, "dst": 2, "properties": {...}}` |
| `Path` | Object `{"nodes": [...], "edges": [...]}` |

---

## Errors

The API uses standard HTTP status codes:

- `200 OK`: Query executed successfully.
- `400 Bad Request`: Invalid JSON, parse error, or runtime error during query execution.
- `404 Not Found`: Endpoint not found.
- `500 Internal Server Error`: Unexpected server-side failure.
