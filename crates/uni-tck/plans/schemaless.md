# Plan: Support Unlabeled Nodes and Unknown Labels

## Problem Summary

TCK tests fail (~1100+ scenarios) because Uni rejects:
1. **Unlabeled nodes**: `CREATE ()` - nodes without any label
2. **Unknown labels**: `CREATE (n:Foo)` - labels not defined in schema

The storage layer already supports both cases via the main `vertices` table with `labels: List<Utf8>` and `props_json: Utf8` columns. The blocks are artificial validations in the query layer.

## Current Architecture

```
Write Path:
  CREATE (n:Label) → write.rs validation → L0Buffer → Flush → per-label table + main table

Read Path:
  MATCH (n:Label) → Planner → Scan(label_id) → PropertyManager → per-label table
  MATCH (n {ext_id:'x'}) → Planner → ExtIdLookup → MainVertexDataset → main table
```

## Solution: Alternate Paths for Schema-less Nodes

### Design Principles
1. Schema-defined labels → per-label typed tables (optimized path)
2. Unlabeled or unknown labels → main `vertices` table only (flexible path)
3. Both paths coexist - no breaking changes to existing behavior

### Good News: Storage Layer Already Works!

Investigation shows the storage layer already handles unlabeled/unknown labels:
- **L0Buffer** (`l0.rs:165-205`): `insert_vertex_with_labels()` works with empty labels
- **Flush** (`writer.rs:2170-2207`): ALL vertices go to main table, only known labels go to per-label tables
- **Main table**: Has `labels: List<Utf8>` and `props_json: Utf8` columns

**Only the query layer validation is blocking.**

## Files to Modify

| File | Purpose |
|------|---------|
| `crates/uni-query/src/query/executor/write.rs` | Add alternate write path |
| `crates/uni-store/src/storage/main_vertex.rs` | Add `find_props_by_vid()` method |
| `crates/uni-store/src/runtime/property_manager.rs` | Fall back to main table |
| `crates/uni-query/src/query/planner.rs` | Add `ScanAll` plan for unlabeled MATCH |
| `crates/uni-query/src/query/executor/read.rs` | Execute `ScanAll` plan |

## Implementation Steps

### Step 1: Add `find_props_by_vid()` to MainVertexDataset

**File**: `crates/uni-store/src/storage/main_vertex.rs`

Add method to read properties from the main table's `props_json` column:

```rust
/// Find properties for a vertex by VID from the main vertices table.
/// Returns parsed properties from the props_json column.
pub async fn find_props_by_vid(store: &LanceDbStore, vid: Vid) -> Result<Option<Properties>> {
    let table = Self::open_table(store).await?;

    let filter = format!("_vid = {} AND _deleted = false", vid.as_u64());
    let batches: Vec<RecordBatch> = table
        .query()
        .only_if(&filter)
        .select(Select::Columns(vec!["props_json".to_string(), "_version".to_string()]))
        .execute()
        .await?
        .try_collect()
        .await?;

    // Find latest version, parse props_json
    for batch in batches {
        if let Some(props_col) = batch.column_by_name("props_json") {
            if let Some(str_array) = props_col.as_any().downcast_ref::<StringArray>() {
                if !str_array.is_null(0) {
                    let json_str = str_array.value(0);
                    let props: Properties = serde_json::from_str(json_str)?;
                    return Ok(Some(props));
                }
            }
        }
    }
    Ok(None)
}
```

### Step 2: Update PropertyManager to fall back to main table

**File**: `crates/uni-store/src/runtime/property_manager.rs`

Modify `fetch_all_props_from_storage()` to query main table when no per-label data found:

```rust
async fn fetch_all_props_from_storage(&self, vid: Vid) -> Result<Option<Properties>> {
    // Existing: scan all schema-defined label tables
    let schema = self.schema_manager.schema();
    let mut merged_props: Option<Properties> = None;
    // ... existing per-label scan logic ...

    // NEW: Fall back to main vertices table if no props found
    if merged_props.is_none() {
        let lancedb = self.storage.lancedb_store();
        if let Some(main_props) = MainVertexDataset::find_props_by_vid(lancedb, vid).await? {
            return Ok(Some(main_props));
        }
    }

    Ok(merged_props)
}
```

### Step 3: Add alternate write path in write.rs

**File**: `crates/uni-query/src/query/executor/write.rs`

Replace the validation block (lines 1069-1083) with alternate path logic:

```rust
// Current validation (REMOVE):
// if n.labels.is_empty() { return Err(...) }
// for label in labels { if !schema.has(label) { return Err(...) } }

// NEW: Categorize labels
let (known_labels, unknown_labels): (Vec<_>, Vec<_>) = n.labels
    .iter()
    .partition(|label| schema.get_label_case_insensitive(label).is_some());

let new_vid = writer.next_vid().await?;

// Enrich properties only for known labels (generated columns, embeddings)
for label_name in &known_labels {
    self.enrich_properties_with_generated_columns(
        label_name, &mut props, prop_manager, params, ctx
    ).await?;
}

// Insert vertex - storage layer handles routing:
// - Known labels → per-label tables + main table
// - Unknown/no labels → main table only
let all_labels: Vec<String> = n.labels.clone();
let final_props = writer
    .insert_vertex_with_labels(new_vid, props, all_labels)
    .await?;
```

### Step 4: Update Writer to handle unknown labels gracefully

**File**: `crates/uni-store/src/runtime/writer.rs`

Modify `validate_vertex_constraints_for_label()` to skip validation for unknown labels:

```rust
async fn validate_vertex_constraints_for_label(
    &self,
    vid: Vid,
    properties: &Properties,
    label: &str,
) -> Result<()> {
    let schema = self.schema_manager.schema();

    // Skip constraint validation for unknown labels
    if schema.get_label_case_insensitive(label).is_none() {
        return Ok(());
    }

    // Existing constraint validation for known labels...
}
```

### Step 5: Flush logic (NO CHANGES NEEDED)

**File**: `crates/uni-store/src/runtime/writer.rs`

The flush logic already handles unknown labels correctly:
- Line 1919: Only adds to per-label tables if `schema.label_id_by_name(label)` returns `Some`
- Lines 2170-2195: ALL vertices go to `main_vertices` regardless of label status
- Unknown labels naturally skip per-label tables but still get written to main table

**No code changes required for flush.**

### Step 6: Add LogicalPlan variants for unlabeled/unknown label MATCH

**File**: `crates/uni-query/src/query/planner.rs`

Add two new plan variants after `ExtIdLookup` (around line 43):

```rust
/// Scan all vertices from main table (no label filter).
/// Used for MATCH (n) without any label.
ScanAll {
    variable: String,
    filter: Option<Expr>,
    optional: bool,
},
/// Scan main table filtering by label name (for unknown labels).
/// Used when label is not in schema.
ScanMainByLabel {
    variable: String,
    label: String,
    filter: Option<Expr>,
    optional: bool,
},
```

Update `plan_node()` (around line 1819) - replace the error:

```rust
if node.labels.is_empty() {
    // Try ext_id lookup first (existing logic)
    if let Some((_, ext_id_value)) = properties.iter().find(|(k, _)| k == "ext_id") {
        // ... existing ExtIdLookup logic ...
    }

    // NEW: Fall back to ScanAll for MATCH (n) without label or ext_id
    let prop_filter = self.properties_to_expr(variable, &node.properties);
    let scan_all = LogicalPlan::ScanAll {
        variable: variable.to_string(),
        filter: prop_filter,
        optional,
    };
    return if matches!(plan, LogicalPlan::Empty) {
        Ok(scan_all)
    } else {
        Ok(LogicalPlan::CrossJoin {
            left: Box::new(plan),
            right: Box::new(scan_all),
        })
    };
}
```

### Step 7: Execute ScanAll in read.rs

**File**: `crates/uni-query/src/query/executor/read.rs`

Add execution logic for `ScanAll`:

```rust
LogicalPlan::ScanAll { variable, filter, optional } => {
    // Scan main vertices table for all nodes
    let lancedb = self.storage.lancedb_store();
    let table = MainVertexDataset::open_table(lancedb).await?;

    let base_filter = "_deleted = false";
    let batches: Vec<RecordBatch> = table
        .query()
        .only_if(base_filter)
        .select(Select::Columns(vec![
            "_vid".to_string(),
            "labels".to_string(),
            "props_json".to_string()
        ]))
        .execute()
        .await?
        .try_collect()
        .await?;

    let mut matches = Vec::new();
    for batch in batches {
        // Extract VID, labels, props from each row
        // Apply filter if present
        // Build result rows
    }

    if matches.is_empty() && optional {
        // Return single row with null for optional match
    }

    Ok(matches)
}
```

### Step 8: Handle unknown labels in MATCH (n:UnknownLabel)

**File**: `crates/uni-query/src/query/planner.rs`

Update label lookup (around line 1823) to fall back to main table scan:

```rust
// After the node.labels.is_empty() check, replace the label lookup:
let label_name = &node.labels[0];
let label_meta = self.schema.get_label_case_insensitive(label_name);

if label_meta.is_none() {
    // Unknown label - scan main table filtering by label
    let prop_filter = self.properties_to_expr(variable, &node.properties);
    let scan_by_label = LogicalPlan::ScanMainByLabel {
        variable: variable.to_string(),
        label: label_name.clone(),
        filter: prop_filter,
        optional,
    };
    return if matches!(plan, LogicalPlan::Empty) {
        Ok(scan_by_label)
    } else {
        Ok(LogicalPlan::CrossJoin {
            left: Box::new(plan),
            right: Box::new(scan_by_label),
        })
    };
}

// Known label - continue with existing Scan logic
let label_meta = label_meta.unwrap();  // Safe now
```

### Step 9: Execute ScanMainByLabel in read.rs

**File**: `crates/uni-query/src/query/executor/read.rs`

Add execution for `ScanMainByLabel` (similar to `ScanAll` but with label filter):

```rust
LogicalPlan::ScanMainByLabel { variable, label, filter, optional } => {
    let lancedb = self.storage.lancedb_store();
    let table = MainVertexDataset::open_table(lancedb).await?;

    // Filter by label using array_contains on labels column
    let base_filter = format!(
        "_deleted = false AND array_contains(labels, '{}')",
        label
    );

    // ... similar to ScanAll but with label filter
}
```

### Step 10: Add index on `labels` column in main vertices table

**File**: `crates/uni-store/src/storage/main_vertex.rs`

Update `ensure_default_indexes_lancedb()` to include labels index:

```rust
pub async fn ensure_default_indexes_lancedb(table: &Table) -> Result<()> {
    // Existing indexes...

    // Add index on labels column for faster array_contains queries
    if !has_index(table, "labels_idx").await {
        table
            .create_index(&["labels"], Index::Auto)
            .execute()
            .await?;
    }

    Ok(())
}
```

Note: LanceDB's inverted index supports list columns for efficient `array_contains` queries.

## Verification

### Unit Tests
```bash
# Test main vertex dataset new method
cargo nextest run -p uni-store main_vertex

# Test property manager fallback
cargo nextest run -p uni-store property_manager
```

### Integration Tests
```bash
# Run TCK tests that were failing due to unlabeled nodes
cargo test -p uni-tck --test cucumber -- -t "delete" --nocapture 2>&1 | head -100

# Run TCK tests for unknown labels
cargo test -p uni-tck --test cucumber -- -t "match" --nocapture 2>&1 | head -100
```

### Manual Smoke Tests
```cypher
-- Unlabeled node
CREATE () RETURN 1
CREATE (n) RETURN n
CREATE (n {name: 'test'}) RETURN n

-- Unknown label
CREATE (n:NewLabel {name: 'test'}) RETURN n
MATCH (n:NewLabel) RETURN n

-- Mixed
CREATE (a:Person {name: 'Alice'}), (b {name: 'Bob'})
MATCH (n) RETURN n
```

## Expected Outcome

| Test Category | Before | After |
|---------------|--------|-------|
| Delete clause | 0/41 | ~35/41 |
| Set clause | 0/33 | ~28/33 |
| Match clause | 0/352 | ~200/352 |
| Remove clause | 0/33 | ~28/33 |

## Risk Assessment

**Medium Risk**:
- Changes touch write path (careful testing needed)
- Main table scans can be slow for large datasets (acceptable for TCK compliance)
- No index on `labels` column in main table (may need to add for production use)

## Implementation Order

### Phase 1: Storage Layer (Read)
1. **Step 1**: Add `find_props_by_vid()` to MainVertexDataset - standalone, no dependencies
2. **Step 2**: Update PropertyManager fallback - uses Step 1

### Phase 2: Write Path
3. **Step 3**: Update write.rs to allow unlabeled/unknown labels
4. **Step 4**: Update Writer constraint validation to skip unknown labels
5. **Step 5**: (No changes needed - flush already works)

### Phase 3: Read Path (Planner + Executor)
6. **Step 6**: Add `ScanAll` and `ScanMainByLabel` to LogicalPlan enum
7. **Step 7**: Execute `ScanAll` in read.rs
8. **Step 8**: Update planner for unknown label fallback
9. **Step 9**: Execute `ScanMainByLabel` in read.rs
10. **Step 10**: Add index on `labels` column for faster queries

### Testing Checkpoints
- After Phase 1: Test `find_props_by_vid()` and PropertyManager
- After Phase 2: Test `CREATE ()` and `CREATE (n:Unknown)`
- After Phase 3: Test `MATCH (n)` and `MATCH (n:Unknown)`

## Design Decisions

1. **Add index on `labels` column**: Yes - for faster `array_contains` queries
2. **Auto-register unknown labels**: No - keep schema explicit, unknown labels only go to main table
