// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for aggregate window functions (SUM, AVG, MIN, MAX, COUNT with OVER clause).
//!
//! These tests verify that aggregate window functions work correctly with DataFusion integration.

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_window_sum_basic() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Eng', salary: 150})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.dept AS dept,
                e.salary AS salary,
                sum(e.salary) OVER (PARTITION BY e.dept) AS dept_total
         ORDER BY e.dept, e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // Eng dept: 150
    assert_eq!(rows[0].get::<String>("dept")?, "Eng");
    assert_eq!(rows[0].get::<i64>("salary")?, 150);
    assert_eq!(rows[0].get::<i64>("dept_total")?, 150);

    // Sales dept: 100 + 200 = 300
    assert_eq!(rows[1].get::<String>("dept")?, "Sales");
    assert_eq!(rows[1].get::<i64>("salary")?, 100);
    assert_eq!(rows[1].get::<i64>("dept_total")?, 300);

    assert_eq!(rows[2].get::<String>("dept")?, "Sales");
    assert_eq!(rows[2].get::<i64>("salary")?, 200);
    assert_eq!(rows[2].get::<i64>("dept_total")?, 300);

    Ok(())
}

#[tokio::test]
async fn test_window_avg_basic() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Eng', salary: 150})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.dept AS dept,
                e.salary AS salary,
                avg(e.salary) OVER (PARTITION BY e.dept) AS dept_avg
         ORDER BY e.dept, e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // Eng dept: avg(150) = 150.0
    assert_eq!(rows[0].get::<String>("dept")?, "Eng");
    assert_eq!(rows[0].get::<f64>("dept_avg")?, 150.0);

    // Sales dept: avg(100, 200) = 150.0
    assert_eq!(rows[1].get::<String>("dept")?, "Sales");
    assert_eq!(rows[1].get::<f64>("dept_avg")?, 150.0);

    assert_eq!(rows[2].get::<String>("dept")?, "Sales");
    assert_eq!(rows[2].get::<f64>("dept_avg")?, 150.0);

    Ok(())
}

#[tokio::test]
async fn test_window_min_max() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 300})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Eng', salary: 200})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.dept AS dept,
                e.salary AS salary,
                min(e.salary) OVER (PARTITION BY e.dept) AS dept_min,
                max(e.salary) OVER (PARTITION BY e.dept) AS dept_max
         ORDER BY e.dept, e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // Eng: min=200, max=200
    assert_eq!(rows[0].get::<String>("dept")?, "Eng");
    assert_eq!(rows[0].get::<i64>("dept_min")?, 200);
    assert_eq!(rows[0].get::<i64>("dept_max")?, 200);

    // Sales: min=100, max=300
    assert_eq!(rows[1].get::<String>("dept")?, "Sales");
    assert_eq!(rows[1].get::<i64>("dept_min")?, 100);
    assert_eq!(rows[1].get::<i64>("dept_max")?, 300);

    assert_eq!(rows[2].get::<String>("dept")?, "Sales");
    assert_eq!(rows[2].get::<i64>("dept_min")?, 100);
    assert_eq!(rows[2].get::<i64>("dept_max")?, 300);

    Ok(())
}

#[tokio::test]
async fn test_window_count() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Eng', salary: 150})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.dept AS dept,
                e.salary AS salary,
                count(*) OVER (PARTITION BY e.dept) AS dept_count
         ORDER BY e.dept, e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // Eng dept: count=1
    assert_eq!(rows[0].get::<String>("dept")?, "Eng");
    assert_eq!(rows[0].get::<i64>("dept_count")?, 1);

    // Sales dept: count=2
    assert_eq!(rows[1].get::<String>("dept")?, "Sales");
    assert_eq!(rows[1].get::<i64>("dept_count")?, 2);

    assert_eq!(rows[2].get::<String>("dept")?, "Sales");
    assert_eq!(rows[2].get::<i64>("dept_count")?, 2);

    Ok(())
}

#[tokio::test]
async fn test_window_count_distinct() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (dept STRING, level INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'Sales', level: 1})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Sales', level: 1})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Sales', level: 2})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Eng', level: 1})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.dept AS dept,
                count(DISTINCT e.level) OVER (PARTITION BY e.dept) AS distinct_levels
         ORDER BY e.dept",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 4);

    // Eng: distinct levels = 1 (only level 1)
    assert_eq!(rows[0].get::<String>("dept")?, "Eng");
    assert_eq!(rows[0].get::<i64>("distinct_levels")?, 1);

    // Sales: distinct levels = 2 (levels 1 and 2)
    assert_eq!(rows[1].get::<String>("dept")?, "Sales");
    assert_eq!(rows[1].get::<i64>("distinct_levels")?, 2);

    assert_eq!(rows[2].get::<String>("dept")?, "Sales");
    assert_eq!(rows[2].get::<i64>("distinct_levels")?, 2);

    assert_eq!(rows[3].get::<String>("dept")?, "Sales");
    assert_eq!(rows[3].get::<i64>("distinct_levels")?, 2);

    Ok(())
}

#[tokio::test]
async fn test_window_with_order_by() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (name STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {name: 'Alice', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {name: 'Bob', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {name: 'Charlie', salary: 300})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.name AS name,
                e.salary AS salary,
                sum(e.salary) OVER (ORDER BY e.salary) AS running_sum
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // Running sum
    assert_eq!(rows[0].get::<String>("name")?, "Alice");
    assert_eq!(rows[0].get::<i64>("running_sum")?, 100);

    assert_eq!(rows[1].get::<String>("name")?, "Bob");
    assert_eq!(rows[1].get::<i64>("running_sum")?, 300); // 100 + 200

    assert_eq!(rows[2].get::<String>("name")?, "Charlie");
    assert_eq!(rows[2].get::<i64>("running_sum")?, 600); // 100 + 200 + 300

    Ok(())
}

#[tokio::test]
async fn test_window_no_partition_no_order() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 300})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                sum(e.salary) OVER () AS total_sum,
                avg(e.salary) OVER () AS avg_salary
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // All rows should have the same total and average
    for row in rows {
        assert_eq!(row.get::<i64>("total_sum")?, 600); // 100 + 200 + 300
        assert_eq!(row.get::<f64>("avg_salary")?, 200.0); // (100 + 200 + 300) / 3
    }

    Ok(())
}

#[tokio::test]
async fn test_window_multiple_aggregates() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'Sales', salary: 300})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.dept AS dept,
                e.salary AS salary,
                sum(e.salary) OVER (PARTITION BY e.dept) AS dept_total,
                avg(e.salary) OVER (PARTITION BY e.dept) AS dept_avg,
                min(e.salary) OVER (PARTITION BY e.dept) AS dept_min,
                max(e.salary) OVER (PARTITION BY e.dept) AS dept_max,
                count(*) OVER (PARTITION BY e.dept) AS dept_count
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // All rows should have the same department aggregates
    for row in rows {
        assert_eq!(row.get::<i64>("dept_total")?, 600); // 100 + 200 + 300
        assert_eq!(row.get::<f64>("dept_avg")?, 200.0); // (100 + 200 + 300) / 3
        assert_eq!(row.get::<i64>("dept_min")?, 100);
        assert_eq!(row.get::<i64>("dept_max")?, 300);
        assert_eq!(row.get::<i64>("dept_count")?, 3);
    }

    Ok(())
}
