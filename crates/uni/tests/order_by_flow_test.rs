// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{Uni, Value};

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Map(_) => "map",
        Value::Node(_) => "node",
        Value::Edge(_) => "edge",
        Value::List(_) => "list",
        Value::Path(_) => "path",
        Value::String(_) => "string",
        Value::Bool(_) => "bool",
        Value::Float(f) if f.is_nan() => "nan",
        Value::Float(_) | Value::Int(_) => "number",
        Value::Null => "null",
        _ => "other",
    }
}

fn node_prop_i64(value: &Value, key: &str) -> i64 {
    match value {
        Value::Node(node) => node.get::<i64>(key).unwrap(),
        other => panic!("expected node, got {other:?}"),
    }
}

fn list(values: Vec<Value>) -> Value {
    Value::List(values)
}

#[tokio::test]
async fn test_return_order_by_single_variable_primitives() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let bools = db
        .query("UNWIND [true, false] AS bools RETURN bools ORDER BY bools")
        .await?;
    let bool_values: Vec<bool> = bools
        .rows()
        .iter()
        .map(|row| row.get::<bool>("bools").unwrap())
        .collect();
    assert_eq!(bool_values, vec![false, true]);

    let strings = db
        .query("UNWIND ['.*', '', ' ', 'one'] AS strings RETURN strings ORDER BY strings DESC")
        .await?;
    let string_values: Vec<String> = strings
        .rows()
        .iter()
        .map(|row| row.get::<String>("strings").unwrap())
        .collect();
    assert_eq!(
        string_values,
        vec![
            "one".to_string(),
            ".*".to_string(),
            " ".to_string(),
            "".to_string()
        ]
    );

    let ints = db
        .query("UNWIND [1, 3, 2] AS ints RETURN ints ORDER BY ints")
        .await?;
    let int_values: Vec<i64> = ints
        .rows()
        .iter()
        .map(|row| row.get::<i64>("ints").unwrap())
        .collect();
    assert_eq!(int_values, vec![1, 2, 3]);

    let floats = db
        .query("UNWIND [1.5, 1.3, 999.99] AS floats RETURN floats ORDER BY floats DESC")
        .await?;
    let float_values: Vec<f64> = floats
        .rows()
        .iter()
        .map(|row| row.get::<f64>("floats").unwrap())
        .collect();
    assert_eq!(float_values, vec![999.99, 1.5, 1.3]);

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_lists() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let lists_asc = db
        .query(
            "UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists \
             RETURN lists ORDER BY lists",
        )
        .await?;
    let asc_values: Vec<Value> = lists_asc
        .rows()
        .iter()
        .map(|row| row.value("lists").unwrap().clone())
        .collect();
    assert_eq!(
        asc_values,
        vec![
            list(vec![]),
            list(vec![Value::String("a".to_string())]),
            list(vec![Value::String("a".to_string()), Value::Int(1)]),
            list(vec![Value::Int(1)]),
            list(vec![Value::Int(1), Value::String("a".to_string())]),
            list(vec![Value::Int(1), Value::Null]),
            list(vec![Value::Null, Value::Int(1)]),
            list(vec![Value::Null, Value::Int(2)]),
        ]
    );

    let lists_desc = db
        .query(
            "UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists \
             RETURN lists ORDER BY lists DESC",
        )
        .await?;
    let desc_values: Vec<Value> = lists_desc
        .rows()
        .iter()
        .map(|row| row.value("lists").unwrap().clone())
        .collect();
    assert_eq!(
        desc_values,
        vec![
            list(vec![Value::Null, Value::Int(2)]),
            list(vec![Value::Null, Value::Int(1)]),
            list(vec![Value::Int(1), Value::Null]),
            list(vec![Value::Int(1), Value::String("a".to_string())]),
            list(vec![Value::Int(1)]),
            list(vec![Value::String("a".to_string()), Value::Int(1)]),
            list(vec![Value::String("a".to_string())]),
            list(vec![]),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_distinct_type_precedence() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (:N)-[:REL]->()").await?;
    let mixed = db
        .query(
            "MATCH p = (n:N)-[r:REL]->() \
             UNWIND [n, r, p, 1.5, ['list'], 'text', null, false, 0.0 / 0.0, {a: 'map'}] AS types \
             RETURN types ORDER BY types",
        )
        .await?;

    let kind_order: Vec<&str> = mixed
        .rows()
        .iter()
        .map(|row| kind_of(row.value("types").unwrap()))
        .collect();

    assert_eq!(
        kind_order,
        vec![
            "map", "node", "edge", "list", "path", "string", "bool", "number", "nan", "null"
        ]
    );

    let mixed_desc = db
        .query(
            "MATCH p = (n:N)-[r:REL]->() \
             UNWIND [n, r, p, 1.5, ['list'], 'text', null, false, 0.0 / 0.0, {a: 'map'}] AS types \
             RETURN types ORDER BY types DESC",
        )
        .await?;
    let kind_order_desc: Vec<&str> = mixed_desc
        .rows()
        .iter()
        .map(|row| kind_of(row.value("types").unwrap()))
        .collect();
    assert_eq!(
        kind_order_desc,
        vec![
            "null", "nan", "number", "bool", "string", "path", "list", "edge", "node", "map"
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_aggregate_function_expression() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE ({division: 'A', age: 22}), ({division: 'B', age: 33}), ({division: 'B', age: 44}), ({division: 'C', age: 55})",
    )
    .await?;

    let aggregate_sorted = db
        .query("MATCH (n) RETURN n.division, max(n.age) ORDER BY max(n.age)")
        .await?;
    let actual: Vec<(String, i64)> = aggregate_sorted
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("n.division").unwrap(),
                row.get::<i64>("max(n.age)").unwrap(),
            )
        })
        .collect();

    assert_eq!(
        actual,
        vec![
            ("A".to_string(), 22),
            ("B".to_string(), 44),
            ("C".to_string(), 55)
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_distinct_alias_and_columns_shape() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({id: 1}), ({id: 10}), ({id: 10})")
        .await?;

    let alias_distinct = db
        .query("MATCH (n) RETURN DISTINCT n.id AS id ORDER BY id DESC")
        .await?;
    let ids: Vec<i64> = alias_distinct
        .rows()
        .iter()
        .map(|row| row.get::<i64>("id").unwrap())
        .collect();
    assert_eq!(ids, vec![10, 1]);

    let distinct_nodes = db
        .query("MATCH (n) RETURN DISTINCT n ORDER BY n.id")
        .await?;
    assert_eq!(distinct_nodes.columns(), &["n".to_string()]);

    let ordered_node_ids: Vec<i64> = distinct_nodes
        .rows()
        .iter()
        .map(|row| node_prop_i64(row.value("n").unwrap(), "id"))
        .collect();
    // DISTINCT on nodes uses node identity, not property-value equality.
    assert_eq!(ordered_node_ids, vec![1, 10, 10]);

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_distinct_node_identity_deduplicates_same_binding() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (a:Start {id: 0}), (b:Target {id: 10}), (c:Target {id: 20}), \
         (a)-[:R]->(b), (a)-[:R]->(b), (a)-[:R]->(c)",
    )
    .await?;

    let distinct_nodes = db
        .query("MATCH (:Start)-[:R]->(n:Target) RETURN DISTINCT n ORDER BY n.id")
        .await?;
    let ids: Vec<i64> = distinct_nodes
        .rows()
        .iter()
        .map(|row| node_prop_i64(row.value("n").unwrap(), "id"))
        .collect();
    assert_eq!(ids, vec![10, 20]);

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_multi_expression_with_aggregate_and_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        "CREATE ({division: 'Sweden'}), ({division: 'Germany'}), ({division: 'England'}), ({division: 'Sweden'})",
    )
    .await?;

    let multi = db
        .query(
            "MATCH (n) \
             RETURN n.division, count(*) \
             ORDER BY count(*) DESC, n.division ASC",
        )
        .await?;

    let actual: Vec<(String, i64)> = multi
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("n.division").unwrap(),
                row.get::<i64>("count(*)").unwrap(),
            )
        })
        .collect();
    assert_eq!(
        actual,
        vec![
            ("Sweden".to_string(), 2_i64),
            ("England".to_string(), 1_i64),
            ("Germany".to_string(), 1_i64),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_projection_column_from_prior_scope() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let projection_sort = db
        .query(
            "WITH [0, 1] AS prows, [[2], [3, 4]] AS qrows \
             UNWIND prows AS p \
             UNWIND qrows[p] AS q \
             WITH p, count(q) AS rng \
             RETURN p \
             ORDER BY rng",
        )
        .await?;

    let ordered_p: Vec<i64> = projection_sort
        .rows()
        .iter()
        .map(|row| row.get::<i64>("p").unwrap())
        .collect();
    assert_eq!(ordered_p, vec![0, 1]);

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_aggregation_expression_with_constant_and_param() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let with_param = db
        .query_with(
            "MATCH (person) \
             RETURN avg(person.age) AS avgAge \
             ORDER BY $age + avg(person.age) - 1000",
        )
        .param("age", 38)
        .fetch_all()
        .await?;
    assert_eq!(with_param.len(), 1);
    assert_eq!(with_param.rows()[0].value("avgAge"), Some(&Value::Null));

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_aggregation_expression_with_alias_reference() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let alias_in_order_by = db
        .query(
            "MATCH (me:Person)--(you:Person) \
             RETURN me.age AS age, count(you.age) AS cnt \
             ORDER BY age, age + count(you.age)",
        )
        .await?;
    assert_eq!(alias_in_order_by.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_return_order_by_aggregation_expression_with_property_access_reference() -> Result<()>
{
    let db = Uni::in_memory().build().await?;

    let property_access_in_order_by = db
        .query(
            "MATCH (me:Person)--(you:Person) \
             RETURN me.age AS age, count(you.age) AS cnt \
             ORDER BY me.age + count(you.age)",
        )
        .await?;
    assert_eq!(property_access_in_order_by.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_with_order_by_lists() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let with_lists = db
        .query(
            "UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists \
             WITH lists ORDER BY lists LIMIT 4 \
             RETURN lists",
        )
        .await?;
    let list_values: Vec<Value> = with_lists
        .rows()
        .iter()
        .map(|row| row.value("lists").unwrap().clone())
        .collect();
    assert_eq!(
        list_values,
        vec![
            list(vec![]),
            list(vec![Value::String("a".to_string())]),
            list(vec![Value::String("a".to_string()), Value::Int(1)]),
            list(vec![Value::Int(1)]),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_with_order_by_dates() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let with_dates = db
        .query(
            "UNWIND [date({year: 1910, month: 5, day: 6}), \
                     date({year: 1980, month: 12, day: 24}), \
                     date({year: 1984, month: 10, day: 12}), \
                     date({year: 1985, month: 5, day: 6}), \
                     date({year: 1980, month: 10, day: 24}), \
                     date({year: 1984, month: 10, day: 11})] AS dates \
             WITH dates ORDER BY dates LIMIT 2 \
             RETURN dates",
        )
        .await?;
    let date_values: Vec<String> = with_dates
        .rows()
        .iter()
        .map(|row| row.value("dates").unwrap().to_string())
        .collect();
    assert_eq!(date_values, vec!["1910-05-06", "1980-10-24"]);

    Ok(())
}

#[tokio::test]
async fn test_with_order_by_local_times_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let with_local_times = db
        .query(
            "UNWIND [localtime({hour: 10, minute: 35}), \
                     localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), \
                     localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876124}), \
                     localtime({hour: 12, minute: 35, second: 13}), \
                     localtime({hour: 12, minute: 30, second: 14, nanosecond: 645876123})] AS localtimes \
             WITH localtimes ORDER BY localtimes DESC LIMIT 3 \
             RETURN localtimes",
        )
        .await?;
    let local_time_values: Vec<String> = with_local_times
        .rows()
        .iter()
        .map(|row| row.value("localtimes").unwrap().to_string())
        .collect();
    assert_eq!(
        local_time_values,
        vec!["12:35:13", "12:31:14.645876124", "12:31:14.645876123"]
    );

    Ok(())
}

#[tokio::test]
async fn test_with_order_by_distinct_type_precedence_with_limit() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (:N)-[:REL]->()").await?;
    let with_mixed = db
        .query(
            "MATCH p = (n:N)-[r:REL]->() \
             UNWIND [n, r, p, 1.5, ['list'], 'text', null, false, 0.0 / 0.0, {a: 'map'}] AS types \
             WITH types ORDER BY types LIMIT 5 \
             RETURN types",
        )
        .await?;

    let mixed_kinds: Vec<&str> = with_mixed
        .rows()
        .iter()
        .map(|row| kind_of(row.value("types").unwrap()))
        .collect();
    assert_eq!(mixed_kinds, vec!["map", "node", "edge", "list", "path"]);

    Ok(())
}

#[tokio::test]
async fn test_with_order_by_aggregation_expression_with_constant_and_param() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let with_param = db
        .query_with(
            "MATCH (person) \
             WITH avg(person.age) AS avgAge \
             ORDER BY $age + avg(person.age) - 1000 \
             RETURN avgAge",
        )
        .param("age", 38)
        .fetch_all()
        .await?;
    assert_eq!(with_param.len(), 1);
    assert_eq!(with_param.rows()[0].value("avgAge"), Some(&Value::Null));

    Ok(())
}

#[tokio::test]
async fn test_with_order_by_aggregation_expression_with_alias_reference() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let alias_in_order_by = db
        .query(
            "MATCH (me:Person)--(you:Person) \
             WITH me.age AS age, count(you.age) AS cnt \
             ORDER BY age, age + count(you.age) \
             RETURN age",
        )
        .await?;
    assert_eq!(alias_in_order_by.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_with_order_by_aggregation_expression_with_property_access_reference() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let property_access_in_order_by = db
        .query(
            "MATCH (me:Person)--(you:Person) \
             WITH me.age AS age, count(you.age) AS cnt \
             ORDER BY me.age + count(you.age) \
             RETURN age",
        )
        .await?;
    assert_eq!(property_access_in_order_by.len(), 0);

    Ok(())
}
