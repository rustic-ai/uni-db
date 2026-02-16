use std::collections::HashMap;
use uni_query::{Edge, Node, Path, QueryResult, Value};

const FLOAT_EPSILON: f64 = 1e-10;

/// Match query result against expected rows (order-sensitive).
pub fn match_result(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<(), String> {
    if actual.len() != expected_rows.len() {
        return Err(format!(
            "Row count mismatch: expected {}, got {}",
            expected_rows.len(),
            actual.len()
        ));
    }

    if actual.is_empty() {
        return Ok(());
    }

    let expected_cols = validate_columns(actual, expected_rows)?;

    for (i, (actual_row, expected_row)) in
        actual.rows().iter().zip(expected_rows.iter()).enumerate()
    {
        compare_row(actual_row, expected_row, &expected_cols, i)?;
    }

    Ok(())
}

/// Match query result against expected rows (order-agnostic).
pub fn match_result_unordered(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<(), String> {
    if actual.len() != expected_rows.len() {
        return Err(format!(
            "Row count mismatch: expected {}, got {}",
            expected_rows.len(),
            actual.len()
        ));
    }

    if actual.is_empty() {
        return Ok(());
    }

    let expected_cols = validate_columns(actual, expected_rows)?;

    let mut unmatched: Vec<&HashMap<String, Value>> = expected_rows.iter().collect();

    for (i, actual_row) in actual.rows().iter().enumerate() {
        let match_idx = unmatched.iter().position(|expected_row| {
            expected_cols.iter().all(|col| {
                let Some(expected_val) = expected_row.get(col) else {
                    return false;
                };
                let Some(actual_val) = actual_row.value(col) else {
                    return false;
                };
                values_equal(actual_val, expected_val)
            })
        });

        match match_idx {
            Some(idx) => {
                unmatched.remove(idx);
            }
            None => {
                let actual_vals: Vec<_> = expected_cols
                    .iter()
                    .map(|col| (col.clone(), actual_row.value(col).cloned()))
                    .collect();
                return Err(format!(
                    "No match found for actual row {}. Actual values: {:?}. Expected: {:?}",
                    i, actual_vals, unmatched
                ));
            }
        }
    }

    if !unmatched.is_empty() {
        return Err(format!(
            "{} expected rows were not matched",
            unmatched.len()
        ));
    }

    Ok(())
}

/// Match query result against expected rows (order-sensitive), ignoring list element order.
pub fn match_result_ignoring_list_order(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<(), String> {
    if actual.len() != expected_rows.len() {
        return Err(format!(
            "Row count mismatch: expected {}, got {}",
            expected_rows.len(),
            actual.len()
        ));
    }

    if actual.is_empty() {
        return Ok(());
    }

    let expected_cols = validate_columns(actual, expected_rows)?;

    for (i, (actual_row, expected_row)) in
        actual.rows().iter().zip(expected_rows.iter()).enumerate()
    {
        for col in &expected_cols {
            let Some(expected_val) = expected_row.get(col) else {
                return Err(format!("Expected row {} missing column {}", i, col));
            };
            let Some(actual_val) = actual_row.value(col) else {
                return Err(format!("Actual row {} missing column {}", i, col));
            };
            if !values_equal_ignoring_list_order(actual_val, expected_val) {
                return Err(format!(
                    "Row {} column {} mismatch: expected {:?}, got {:?}",
                    i, col, expected_val, actual_val
                ));
            }
        }
    }

    Ok(())
}

/// Match query result against expected rows (order-agnostic), ignoring list element order.
pub fn match_result_unordered_ignoring_list_order(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<(), String> {
    if actual.len() != expected_rows.len() {
        return Err(format!(
            "Row count mismatch: expected {}, got {}",
            expected_rows.len(),
            actual.len()
        ));
    }

    if actual.is_empty() {
        return Ok(());
    }

    let expected_cols = validate_columns(actual, expected_rows)?;

    let mut unmatched: Vec<&HashMap<String, Value>> = expected_rows.iter().collect();

    for (i, actual_row) in actual.rows().iter().enumerate() {
        let match_idx = unmatched.iter().position(|expected_row| {
            expected_cols.iter().all(|col| {
                let Some(expected_val) = expected_row.get(col) else {
                    return false;
                };
                let Some(actual_val) = actual_row.value(col) else {
                    return false;
                };
                values_equal_ignoring_list_order(actual_val, expected_val)
            })
        });

        match match_idx {
            Some(idx) => {
                unmatched.remove(idx);
            }
            None => {
                let actual_vals: Vec<_> = expected_cols
                    .iter()
                    .map(|col| (col.clone(), actual_row.value(col).cloned()))
                    .collect();
                return Err(format!(
                    "No match found for actual row {}. Actual values: {:?}. Expected: {:?}",
                    i, actual_vals, unmatched
                ));
            }
        }
    }

    if !unmatched.is_empty() {
        return Err(format!(
            "{} expected rows were not matched",
            unmatched.len()
        ));
    }

    Ok(())
}

/// Validate that actual and expected column sets match, returning the expected column names.
fn validate_columns(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<Vec<String>, String> {
    let expected_cols: Vec<String> = expected_rows[0].keys().cloned().collect();
    let actual_cols: Vec<String> = actual.columns().to_vec();

    for col in &expected_cols {
        if !actual_cols.contains(col) {
            return Err(format!("Expected column '{}' not found in result", col));
        }
    }
    for col in &actual_cols {
        if !expected_cols.contains(col) {
            return Err(format!("Unexpected column '{}' in result", col));
        }
    }

    Ok(expected_cols)
}

/// Compare a single actual row against an expected row.
fn compare_row(
    actual_row: &uni_query::Row,
    expected_row: &HashMap<String, Value>,
    columns: &[String],
    row_index: usize,
) -> Result<(), String> {
    for col in columns {
        let expected_val = expected_row.get(col).ok_or_else(|| {
            format!(
                "Expected column '{}' not found in expected row {}",
                col, row_index
            )
        })?;
        let actual_val = actual_row
            .value(col)
            .ok_or_else(|| format!("Column '{}' not found in actual row {}", col, row_index))?;

        if !values_equal(actual_val, expected_val) {
            return Err(format!(
                "Value mismatch at row {} column '{}': expected {:?}, got {:?}",
                row_index, col, expected_val, actual_val
            ));
        }
    }
    Ok(())
}

/// Compare two values for equality with special handling for floats and graph types.
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => floats_equal(*a, *b),
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::Temporal(a), Value::Temporal(b)) => a == b,
        // Cross-type: Temporal vs String — compare via Display
        (Value::Temporal(_), Value::String(s)) => a.to_string() == *s,
        (Value::String(s), Value::Temporal(_)) => *s == b.to_string(),
        (Value::List(a), Value::List(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(av, bv)| values_equal(av, bv))
        }
        (Value::Map(a), Value::Map(b)) => maps_equal(a, b),
        (Value::Node(a), Value::Node(b)) => nodes_equal(a, b),
        (Value::Edge(a), Value::Edge(b)) => edges_equal(a, b),
        (Value::Path(a), Value::Path(b)) => paths_equal(a, b),
        (Value::Vector(a), Value::Vector(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(av, bv)| (av - bv).abs() < FLOAT_EPSILON as f32)
        }
        _ => false,
    }
}

fn floats_equal(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a.is_infinite() && b.is_infinite() {
        return a.is_sign_positive() == b.is_sign_positive();
    }
    (a - b).abs() < FLOAT_EPSILON
}

/// Generate a sort key for a value to enable deterministic list ordering.
fn value_sort_key(v: &Value) -> String {
    match v {
        Value::Null => "0:null".to_string(),
        Value::Bool(b) => format!("1:{}", b),
        Value::Int(i) => format!("2:{:020}", i),
        Value::Float(f) => format!("3:{:020.10}", f),
        Value::String(s) => format!("4:{}", s),
        Value::Bytes(b) => format!("5:{:?}", b),
        Value::List(l) => format!("6:len={}", l.len()),
        Value::Map(m) => format!("7:len={}", m.len()),
        Value::Node(_) => "8:node".to_string(),
        Value::Edge(_) => "9:edge".to_string(),
        Value::Path(_) => "A:path".to_string(),
        Value::Temporal(t) => format!("C:{}", t),
        Value::Vector(v) => format!("B:len={}", v.len()),
        _ => "Z:unknown".to_string(),
    }
}

/// Compare two values for equality with special handling for floats, graph types,
/// and ignoring element order within lists.
fn values_equal_ignoring_list_order(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => floats_equal(*a, *b),
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::List(a), Value::List(b)) => {
            if a.len() != b.len() {
                return false;
            }
            // Sort both lists before comparison
            let mut a_sorted: Vec<&Value> = a.iter().collect();
            let mut b_sorted: Vec<&Value> = b.iter().collect();
            a_sorted.sort_by_key(|v| value_sort_key(v));
            b_sorted.sort_by_key(|v| value_sort_key(v));
            a_sorted
                .iter()
                .zip(b_sorted.iter())
                .all(|(av, bv)| values_equal_ignoring_list_order(av, bv))
        }
        (Value::Map(a), Value::Map(b)) => maps_equal_ignoring_list_order(a, b),
        (Value::Node(a), Value::Node(b)) => nodes_equal_ignoring_list_order(a, b),
        (Value::Edge(a), Value::Edge(b)) => edges_equal_ignoring_list_order(a, b),
        (Value::Path(a), Value::Path(b)) => paths_equal_ignoring_list_order(a, b),
        (Value::Temporal(a), Value::Temporal(b)) => a == b,
        (Value::Temporal(_), Value::String(s)) => a.to_string() == *s,
        (Value::String(s), Value::Temporal(_)) => *s == b.to_string(),
        (Value::Vector(a), Value::Vector(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(av, bv)| (av - bv).abs() < FLOAT_EPSILON as f32)
        }
        _ => false,
    }
}

/// Compare two property maps for equality (order-agnostic), ignoring list element order.
fn maps_equal_ignoring_list_order(a: &HashMap<String, Value>, b: &HashMap<String, Value>) -> bool {
    a.len() == b.len()
        && a.iter().all(|(key, a_val)| {
            b.get(key)
                .is_some_and(|b_val| values_equal_ignoring_list_order(a_val, b_val))
        })
}

fn nodes_equal_ignoring_list_order(a: &Node, b: &Node) -> bool {
    let labels_match = if a.labels.is_empty() && b.labels.is_empty() {
        true
    } else {
        a.labels.len() == b.labels.len() && a.labels.iter().all(|l| b.labels.contains(l))
    };

    labels_match && maps_equal_ignoring_list_order(&a.properties, &b.properties)
}

fn edges_equal_ignoring_list_order(a: &Edge, b: &Edge) -> bool {
    a.edge_type == b.edge_type && maps_equal_ignoring_list_order(&a.properties, &b.properties)
}

fn paths_equal_ignoring_list_order(a: &Path, b: &Path) -> bool {
    a.nodes.len() == b.nodes.len()
        && a.edges.len() == b.edges.len()
        && a.nodes
            .iter()
            .zip(&b.nodes)
            .all(|(a, b)| nodes_equal_ignoring_list_order(a, b))
        && a.edges
            .iter()
            .zip(&b.edges)
            .all(|(a, b)| edges_equal_ignoring_list_order(a, b))
}

/// Compare two property maps for equality (order-agnostic).
fn maps_equal(a: &HashMap<String, Value>, b: &HashMap<String, Value>) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|(key, a_val)| b.get(key).is_some_and(|b_val| values_equal(a_val, b_val)))
}

fn nodes_equal(a: &Node, b: &Node) -> bool {
    let labels_match = if a.labels.is_empty() && b.labels.is_empty() {
        true
    } else {
        a.labels.len() == b.labels.len() && a.labels.iter().all(|l| b.labels.contains(l))
    };

    labels_match && maps_equal(&a.properties, &b.properties)
}

fn edges_equal(a: &Edge, b: &Edge) -> bool {
    a.edge_type == b.edge_type && maps_equal(&a.properties, &b.properties)
}

fn paths_equal(a: &Path, b: &Path) -> bool {
    a.nodes.len() == b.nodes.len()
        && a.edges.len() == b.edges.len()
        && a.nodes.iter().zip(&b.nodes).all(|(a, b)| nodes_equal(a, b))
        && a.edges.iter().zip(&b.edges).all(|(a, b)| edges_equal(a, b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_equal_scalars() {
        assert!(values_equal(&Value::Null, &Value::Null));
        assert!(values_equal(&Value::Bool(true), &Value::Bool(true)));
        assert!(!values_equal(&Value::Bool(true), &Value::Bool(false)));
        assert!(values_equal(&Value::Int(42), &Value::Int(42)));
        assert!(!values_equal(&Value::Int(42), &Value::Int(43)));
    }

    #[test]
    fn test_values_equal_floats() {
        assert!(values_equal(&Value::Float(3.15), &Value::Float(3.15)));
        assert!(values_equal(&Value::Float(1.0 + 1e-11), &Value::Float(1.0)));
        assert!(!values_equal(&Value::Float(1.0), &Value::Float(2.0)));
    }

    #[test]
    fn test_values_equal_nan() {
        assert!(values_equal(
            &Value::Float(f64::NAN),
            &Value::Float(f64::NAN)
        ));
    }

    #[test]
    fn test_values_equal_list() {
        assert!(values_equal(
            &Value::List(vec![Value::Int(1), Value::Int(2)]),
            &Value::List(vec![Value::Int(1), Value::Int(2)])
        ));
        assert!(!values_equal(
            &Value::List(vec![Value::Int(1), Value::Int(2)]),
            &Value::List(vec![Value::Int(2), Value::Int(1)])
        ));
    }

    #[test]
    fn test_nodes_equal_single_label() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec!["Person".to_string()],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec!["Person".to_string()],
            properties: HashMap::new(),
        };
        assert!(nodes_equal(&node1, &node2));
    }

    #[test]
    fn test_nodes_equal_multi_label_same_order() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            properties: HashMap::new(),
        };
        assert!(nodes_equal(&node1, &node2));
    }

    #[test]
    fn test_nodes_equal_multi_label_different_order() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec!["C".to_string(), "B".to_string(), "A".to_string()],
            properties: HashMap::new(),
        };
        assert!(nodes_equal(&node1, &node2));
    }

    #[test]
    fn test_nodes_not_equal_different_labels() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec!["A".to_string(), "B".to_string()],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec!["A".to_string(), "C".to_string()],
            properties: HashMap::new(),
        };
        assert!(!nodes_equal(&node1, &node2));
    }

    #[test]
    fn test_nodes_equal_empty_labels() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec![],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec![],
            properties: HashMap::new(),
        };
        assert!(nodes_equal(&node1, &node2));
    }
}
