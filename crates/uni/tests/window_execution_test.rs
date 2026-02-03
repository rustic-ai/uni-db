// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_window_row_number() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    // Dept A
    db.execute("CREATE (e:Employee {dept: 'A', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 300})")
        .await?;

    // Dept B
    db.execute("CREATE (e:Employee {dept: 'B', salary: 150})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 250})")
        .await?;

    // Query: ROW_NUMBER partitioned by dept, ordered by salary DESC
    // Expected:
    // A: 300 -> 1, 200 -> 2, 100 -> 3
    // B: 250 -> 1, 150 -> 2

    let result = db.query("MATCH (e:Employee) RETURN e.dept AS dept, e.salary AS salary, row_number() OVER (PARTITION BY e.dept ORDER BY e.salary DESC) AS rn ORDER BY e.dept, row_number() OVER (PARTITION BY e.dept ORDER BY e.salary DESC)").await?;

    assert_eq!(result.len(), 5);

    let rows = result.rows();

    // Row 0: Dept A, Salary 300, RN 1
    assert_eq!(rows[0].get::<String>("dept")?, "A");
    assert_eq!(rows[0].get::<i64>("salary")?, 300);
    assert_eq!(rows[0].get::<i64>("rn")?, 1);

    // Row 1: Dept A, Salary 200, RN 2
    assert_eq!(rows[1].get::<String>("dept")?, "A");
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("rn")?, 2);

    // Row 2: Dept A, Salary 100, RN 3
    assert_eq!(rows[2].get::<String>("dept")?, "A");
    assert_eq!(rows[2].get::<i64>("salary")?, 100);
    assert_eq!(rows[2].get::<i64>("rn")?, 3);

    // Row 3: Dept B, Salary 250, RN 1
    assert_eq!(rows[3].get::<String>("dept")?, "B");
    assert_eq!(rows[3].get::<i64>("salary")?, 250);
    assert_eq!(rows[3].get::<i64>("rn")?, 1);

    // Row 4: Dept B, Salary 150, RN 2
    assert_eq!(rows[4].get::<String>("dept")?, "B");
    assert_eq!(rows[4].get::<i64>("salary")?, 150);
    assert_eq!(rows[4].get::<i64>("rn")?, 2);

    Ok(())
}

#[tokio::test]
async fn test_window_lag_basic() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 300})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                lag(e.salary) OVER (ORDER BY e.salary) AS prev_salary
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // First row should have NULL for prev_salary
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<Option<i64>>("prev_salary")?, None);

    // Second row should have previous salary
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("prev_salary")?, 100);

    // Third row should have previous salary
    assert_eq!(rows[2].get::<i64>("salary")?, 300);
    assert_eq!(rows[2].get::<i64>("prev_salary")?, 200);

    Ok(())
}

#[tokio::test]
async fn test_window_lag_with_offset() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 300})").await?;
    db.execute("CREATE (e:Employee {salary: 400})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                lag(e.salary, 2) OVER (ORDER BY e.salary) AS prev_salary
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 4);

    // First two rows should have NULL
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<Option<i64>>("prev_salary")?, None);

    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<Option<i64>>("prev_salary")?, None);

    // Third row should have value from row 0
    assert_eq!(rows[2].get::<i64>("salary")?, 300);
    assert_eq!(rows[2].get::<i64>("prev_salary")?, 100);

    // Fourth row should have value from row 1
    assert_eq!(rows[3].get::<i64>("salary")?, 400);
    assert_eq!(rows[3].get::<i64>("prev_salary")?, 200);

    Ok(())
}

#[tokio::test]
async fn test_window_lag_with_default() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                lag(e.salary, 1, 0) OVER (ORDER BY e.salary) AS prev_salary
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 2);

    // First row should have default value 0
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<i64>("prev_salary")?, 0);

    // Second row should have previous salary
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("prev_salary")?, 100);

    Ok(())
}

#[tokio::test]
async fn test_window_lag_partition() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    // Dept A
    db.execute("CREATE (e:Employee {dept: 'A', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 200})")
        .await?;

    // Dept B
    db.execute("CREATE (e:Employee {dept: 'B', salary: 150})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 250})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.dept AS dept,
                e.salary AS salary,
                lag(e.salary) OVER (PARTITION BY e.dept ORDER BY e.salary) AS prev_salary
         ORDER BY e.dept, e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 4);

    // First row of each partition should have NULL
    assert_eq!(rows[0].get::<String>("dept")?, "A");
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<Option<i64>>("prev_salary")?, None);

    assert_eq!(rows[1].get::<String>("dept")?, "A");
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("prev_salary")?, 100);

    assert_eq!(rows[2].get::<String>("dept")?, "B");
    assert_eq!(rows[2].get::<i64>("salary")?, 150);
    assert_eq!(rows[2].get::<Option<i64>>("prev_salary")?, None);

    assert_eq!(rows[3].get::<String>("dept")?, "B");
    assert_eq!(rows[3].get::<i64>("salary")?, 250);
    assert_eq!(rows[3].get::<i64>("prev_salary")?, 150);

    Ok(())
}

#[tokio::test]
async fn test_window_lead_basic() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 300})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                lead(e.salary) OVER (ORDER BY e.salary) AS next_salary
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // First row should have next salary
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<i64>("next_salary")?, 200);

    // Second row should have next salary
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("next_salary")?, 300);

    // Last row should have NULL
    assert_eq!(rows[2].get::<i64>("salary")?, 300);
    assert_eq!(rows[2].get::<Option<i64>>("next_salary")?, None);

    Ok(())
}

#[tokio::test]
async fn test_window_lead_with_offset() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 300})").await?;
    db.execute("CREATE (e:Employee {salary: 400})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                lead(e.salary, 2) OVER (ORDER BY e.salary) AS next_salary
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 4);

    // First row should have value from row 2
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<i64>("next_salary")?, 300);

    // Second row should have value from row 3
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("next_salary")?, 400);

    // Last two rows should have NULL
    assert_eq!(rows[2].get::<i64>("salary")?, 300);
    assert_eq!(rows[2].get::<Option<i64>>("next_salary")?, None);

    assert_eq!(rows[3].get::<i64>("salary")?, 400);
    assert_eq!(rows[3].get::<Option<i64>>("next_salary")?, None);

    Ok(())
}

#[tokio::test]
async fn test_window_lead_with_default() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                lead(e.salary, 1, 0) OVER (ORDER BY e.salary) AS next_salary
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 2);

    // First row should have next salary
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<i64>("next_salary")?, 200);

    // Last row should have default value 0
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("next_salary")?, 0);

    Ok(())
}

#[tokio::test]
async fn test_window_lead_partition() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    // Dept A
    db.execute("CREATE (e:Employee {dept: 'A', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 200})")
        .await?;

    // Dept B
    db.execute("CREATE (e:Employee {dept: 'B', salary: 150})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 250})")
        .await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.dept AS dept,
                e.salary AS salary,
                lead(e.salary) OVER (PARTITION BY e.dept ORDER BY e.salary) AS next_salary
         ORDER BY e.dept, e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 4);

    // Check each partition
    assert_eq!(rows[0].get::<String>("dept")?, "A");
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<i64>("next_salary")?, 200);

    assert_eq!(rows[1].get::<String>("dept")?, "A");
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<Option<i64>>("next_salary")?, None);

    assert_eq!(rows[2].get::<String>("dept")?, "B");
    assert_eq!(rows[2].get::<i64>("salary")?, 150);
    assert_eq!(rows[2].get::<i64>("next_salary")?, 250);

    assert_eq!(rows[3].get::<String>("dept")?, "B");
    assert_eq!(rows[3].get::<i64>("salary")?, 250);
    assert_eq!(rows[3].get::<Option<i64>>("next_salary")?, None);

    Ok(())
}

#[tokio::test]
async fn test_window_ntile_even() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    // Create 4 rows
    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 300})").await?;
    db.execute("CREATE (e:Employee {salary: 400})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                ntile(2) OVER (ORDER BY e.salary) AS bucket
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 4);

    // 4 rows, 2 buckets → [1, 1, 2, 2]
    assert_eq!(rows[0].get::<i64>("salary")?, 100);
    assert_eq!(rows[0].get::<i64>("bucket")?, 1);

    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("bucket")?, 1);

    assert_eq!(rows[2].get::<i64>("salary")?, 300);
    assert_eq!(rows[2].get::<i64>("bucket")?, 2);

    assert_eq!(rows[3].get::<i64>("salary")?, 400);
    assert_eq!(rows[3].get::<i64>("bucket")?, 2);

    Ok(())
}

#[tokio::test]
#[allow(clippy::needless_range_loop)]
async fn test_window_ntile_uneven() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    // Create 10 rows
    for i in 1..=10 {
        db.execute(&format!("CREATE (e:Employee {{salary: {}}})", i * 100))
            .await?;
    }

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                ntile(3) OVER (ORDER BY e.salary) AS bucket
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 10);

    // 10 rows, 3 buckets → [1,1,1,1, 2,2,2, 3,3,3]
    // First bucket: 4 rows
    for i in 0..4 {
        assert_eq!(rows[i].get::<i64>("bucket")?, 1);
    }

    // Second bucket: 3 rows
    for i in 4..7 {
        assert_eq!(rows[i].get::<i64>("bucket")?, 2);
    }

    // Third bucket: 3 rows
    for i in 7..10 {
        assert_eq!(rows[i].get::<i64>("bucket")?, 3);
    }

    Ok(())
}

#[tokio::test]
#[allow(clippy::needless_range_loop)]
async fn test_window_ntile_more_buckets_than_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    // Create 5 rows
    for i in 1..=5 {
        db.execute(&format!("CREATE (e:Employee {{salary: {}}})", i * 100))
            .await?;
    }

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                ntile(10) OVER (ORDER BY e.salary) AS bucket
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 5);

    // 5 rows, 10 buckets → [1, 2, 3, 4, 5]
    for i in 0..5 {
        assert_eq!(rows[i].get::<i64>("bucket")?, (i + 1) as i64);
    }

    Ok(())
}

#[tokio::test]
#[allow(clippy::needless_range_loop)]
async fn test_window_ntile_single_bucket() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 300})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                ntile(1) OVER (ORDER BY e.salary) AS bucket
         ORDER BY e.salary",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 3);

    // All rows in bucket 1
    for i in 0..3 {
        assert_eq!(rows[i].get::<i64>("bucket")?, 1);
    }

    Ok(())
}

#[tokio::test]
async fn test_window_ntile_zero_buckets() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                ntile(0) OVER (ORDER BY e.salary) AS bucket",
        )
        .await;

    // Should return error about positive bucket count
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("must be positive"));

    Ok(())
}

#[tokio::test]
async fn test_window_rank() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?; // Tie
    db.execute("CREATE (e:Employee {salary: 300})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                rank() OVER (ORDER BY e.salary DESC) AS rank
         ORDER BY e.salary DESC",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 4);

    // First row: salary 300, rank 1
    assert_eq!(rows[0].get::<i64>("salary")?, 300);
    assert_eq!(rows[0].get::<i64>("rank")?, 1);

    // Two tied rows: salary 200, both rank 2 (order doesn't matter)
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("rank")?, 2);

    assert_eq!(rows[2].get::<i64>("salary")?, 200);
    assert_eq!(rows[2].get::<i64>("rank")?, 2);

    // Fourth row: salary 100, rank 4 (skips 3 due to tie)
    assert_eq!(rows[3].get::<i64>("salary")?, 100);
    assert_eq!(rows[3].get::<i64>("rank")?, 4);

    Ok(())
}

#[tokio::test]
async fn test_window_dense_rank() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE LABEL Employee (salary INT)").await?;

    db.execute("CREATE (e:Employee {salary: 100})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?;
    db.execute("CREATE (e:Employee {salary: 200})").await?; // Tie
    db.execute("CREATE (e:Employee {salary: 300})").await?;

    let result = db
        .query(
            "MATCH (e:Employee)
         RETURN e.salary AS salary,
                dense_rank() OVER (ORDER BY e.salary DESC) AS drank
         ORDER BY e.salary DESC",
        )
        .await?;

    let rows = result.rows();
    assert_eq!(rows.len(), 4);

    // First row: salary 300, dense_rank 1
    assert_eq!(rows[0].get::<i64>("salary")?, 300);
    assert_eq!(rows[0].get::<i64>("drank")?, 1);

    // Two tied rows: salary 200, both dense_rank 2 (order doesn't matter)
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("drank")?, 2);

    assert_eq!(rows[2].get::<i64>("salary")?, 200);
    assert_eq!(rows[2].get::<i64>("drank")?, 2);

    // Fourth row: salary 100, dense_rank 3 (no gap)
    assert_eq!(rows[3].get::<i64>("salary")?, 100);
    assert_eq!(rows[3].get::<i64>("drank")?, 3);

    Ok(())
}
// ============================================================================
// Extended Window Functions Tests
// ============================================================================

#[tokio::test]
async fn test_window_first_value() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'A', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 300})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 150})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 250})")
        .await?;

    // FIRST_VALUE returns the first value in the partition (after ordering)
    let result = db
        .query(
            "MATCH (e:Employee) 
             RETURN e.dept AS dept, e.salary AS salary, 
                    first_value(e.salary) OVER (PARTITION BY e.dept ORDER BY e.salary) AS first_sal
             ORDER BY e.dept, e.salary",
        )
        .await?;

    assert_eq!(result.len(), 5);
    let rows = result.rows();

    // Dept A: first value should be 100 for all rows
    assert_eq!(rows[0].get::<i64>("first_sal")?, 100);
    assert_eq!(rows[1].get::<i64>("first_sal")?, 100);
    assert_eq!(rows[2].get::<i64>("first_sal")?, 100);

    // Dept B: first value should be 150 for all rows
    assert_eq!(rows[3].get::<i64>("first_sal")?, 150);
    assert_eq!(rows[4].get::<i64>("first_sal")?, 150);

    Ok(())
}

#[tokio::test]
async fn test_window_last_value() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'A', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 300})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 150})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 250})")
        .await?;

    // LAST_VALUE returns the last value in the partition (after ordering)
    let result = db
        .query(
            "MATCH (e:Employee) 
             RETURN e.dept AS dept, e.salary AS salary, 
                    last_value(e.salary) OVER (PARTITION BY e.dept ORDER BY e.salary) AS last_sal
             ORDER BY e.dept, e.salary",
        )
        .await?;

    assert_eq!(result.len(), 5);
    let rows = result.rows();

    // Dept A: last value should be 300 for all rows
    assert_eq!(rows[0].get::<i64>("last_sal")?, 300);
    assert_eq!(rows[1].get::<i64>("last_sal")?, 300);
    assert_eq!(rows[2].get::<i64>("last_sal")?, 300);

    // Dept B: last value should be 250 for all rows
    assert_eq!(rows[3].get::<i64>("last_sal")?, 250);
    assert_eq!(rows[4].get::<i64>("last_sal")?, 250);

    Ok(())
}

#[tokio::test]
async fn test_window_nth_value() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    db.execute("CREATE (e:Employee {dept: 'A', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 300})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 150})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 250})")
        .await?;

    // NTH_VALUE(expr, 2) returns the 2nd value in the partition
    let result = db
        .query(
            "MATCH (e:Employee) 
             RETURN e.dept AS dept, e.salary AS salary, 
                    nth_value(e.salary, 2) OVER (PARTITION BY e.dept ORDER BY e.salary) AS second_sal
             ORDER BY e.dept, e.salary",
        )
        .await?;

    assert_eq!(result.len(), 5);
    let rows = result.rows();

    // Dept A: 2nd value should be 200 for all rows
    assert_eq!(rows[0].get::<i64>("second_sal")?, 200);
    assert_eq!(rows[1].get::<i64>("second_sal")?, 200);
    assert_eq!(rows[2].get::<i64>("second_sal")?, 200);

    // Dept B: 2nd value should be 250 for all rows
    assert_eq!(rows[3].get::<i64>("second_sal")?, 250);
    assert_eq!(rows[4].get::<i64>("second_sal")?, 250);

    Ok(())
}
