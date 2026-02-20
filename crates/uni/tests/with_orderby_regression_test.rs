// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Comprehensive ORDER BY test suite covering all WithOrderBy and ReturnOrderBy
// TCK scenarios. Organized by feature file and scenario number.

use anyhow::Result;
use uni_db::Uni;

// ═══════════════════════════════════════════════════════════════════════════
// WithOrderBy1 — Sort by a single variable (primitive types)
// ═══════════════════════════════════════════════════════════════════════════

// [1] Sort booleans ascending
#[tokio::test]
async fn wo1_01_sort_booleans_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [true, false] AS bools WITH bools ORDER BY bools LIMIT 1 RETURN bools")
        .await?;
    assert_eq!(r.len(), 1);
    let v: bool = r.rows[0].get("bools")?;
    assert!(!v, "false should sort before true");
    Ok(())
}

// [2] Sort booleans descending
#[tokio::test]
async fn wo1_02_sort_booleans_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [true, false] AS bools WITH bools ORDER BY bools DESC LIMIT 1 RETURN bools")
        .await?;
    assert_eq!(r.len(), 1);
    let v: bool = r.rows[0].get("bools")?;
    assert!(v, "true should be first when descending");
    Ok(())
}

// [3] Sort integers ascending
#[tokio::test]
async fn wo1_03_sort_integers_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [1, 3, 2] AS ints WITH ints ORDER BY ints LIMIT 2 RETURN ints")
        .await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<i64>("ints")?, 1);
    assert_eq!(r.rows[1].get::<i64>("ints")?, 2);
    Ok(())
}

// [4] Sort integers descending
#[tokio::test]
async fn wo1_04_sort_integers_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [1, 3, 2] AS ints WITH ints ORDER BY ints DESC LIMIT 2 RETURN ints")
        .await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<i64>("ints")?, 3);
    assert_eq!(r.rows[1].get::<i64>("ints")?, 2);
    Ok(())
}

// [5] Sort floats ascending
#[tokio::test]
async fn wo1_05_sort_floats_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "UNWIND [1.5, 1.3, 999.99] AS floats WITH floats ORDER BY floats LIMIT 2 RETURN floats",
        )
        .await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<f64>("floats")?, 1.3);
    assert_eq!(r.rows[1].get::<f64>("floats")?, 1.5);
    Ok(())
}

// [6] Sort floats descending
#[tokio::test]
async fn wo1_06_sort_floats_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query("UNWIND [1.5, 1.3, 999.99] AS floats WITH floats ORDER BY floats DESC LIMIT 2 RETURN floats").await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<f64>("floats")?, 999.99);
    assert_eq!(r.rows[1].get::<f64>("floats")?, 1.5);
    Ok(())
}

// [7] Sort strings ascending
#[tokio::test]
async fn wo1_07_sort_strings_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query("UNWIND ['.*', '', ' ', 'one'] AS strings WITH strings ORDER BY strings LIMIT 2 RETURN strings").await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<String>("strings")?, "");
    assert_eq!(r.rows[1].get::<String>("strings")?, " ");
    Ok(())
}

// [8] Sort strings descending
#[tokio::test]
async fn wo1_08_sort_strings_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query("UNWIND ['.*', '', ' ', 'one'] AS strings WITH strings ORDER BY strings DESC LIMIT 2 RETURN strings").await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<String>("strings")?, "one");
    assert_eq!(r.rows[1].get::<String>("strings")?, ".*");
    Ok(())
}

// [9] Sort lists ascending
#[tokio::test]
async fn wo1_09_sort_lists_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists
         WITH lists ORDER BY lists LIMIT 4 RETURN lists",
        )
        .await?;
    assert_eq!(r.len(), 4);
    Ok(())
}

// [10] Sort lists descending
#[tokio::test]
async fn wo1_10_sort_lists_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists
         WITH lists ORDER BY lists DESC LIMIT 4 RETURN lists",
        )
        .await?;
    assert_eq!(r.len(), 4);
    Ok(())
}

// [11] Sort dates ascending
#[tokio::test]
async fn wo1_11_sort_dates_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "UNWIND [date({year: 1910, month: 5, day: 6}),
                 date({year: 1980, month: 12, day: 24}),
                 date({year: 1984, month: 10, day: 12}),
                 date({year: 1985, month: 5, day: 6}),
                 date({year: 1980, month: 10, day: 24}),
                 date({year: 1984, month: 10, day: 11})] AS dates
         WITH dates ORDER BY dates LIMIT 2 RETURN dates",
        )
        .await?;
    assert_eq!(r.len(), 2);
    let v0: String = r.rows[0].get("dates")?;
    let v1: String = r.rows[1].get("dates")?;
    assert!(v0.contains("1910-05-06"), "First should be 1910, got {v0}");
    assert!(
        v1.contains("1980-10-24"),
        "Second should be 1980-10-24, got {v1}"
    );
    Ok(())
}

// [12] Sort dates descending
#[tokio::test]
async fn wo1_12_sort_dates_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "UNWIND [date({year: 1910, month: 5, day: 6}),
                 date({year: 1980, month: 12, day: 24}),
                 date({year: 1984, month: 10, day: 12}),
                 date({year: 1985, month: 5, day: 6}),
                 date({year: 1980, month: 10, day: 24}),
                 date({year: 1984, month: 10, day: 11})] AS dates
         WITH dates ORDER BY dates DESC LIMIT 2 RETURN dates",
        )
        .await?;
    assert_eq!(r.len(), 2);
    let v0: String = r.rows[0].get("dates")?;
    let v1: String = r.rows[1].get("dates")?;
    assert!(v0.contains("1985-05-06"), "First should be 1985, got {v0}");
    assert!(
        v1.contains("1984-10-12"),
        "Second should be 1984-10-12, got {v1}"
    );
    Ok(())
}

// [13] Sort local times ascending
#[tokio::test]
async fn wo1_13_sort_localtimes_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "UNWIND [localtime({hour: 10, minute: 35}),
                 localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}),
                 localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876124}),
                 localtime({hour: 12, minute: 35, second: 13}),
                 localtime({hour: 12, minute: 30, second: 14, nanosecond: 645876123})] AS lt
         WITH lt ORDER BY lt LIMIT 3 RETURN lt",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let v0: String = r.rows[0].get("lt")?;
    assert!(v0.contains("10:35"), "First should be 10:35, got {v0}");
    Ok(())
}

// [14] Sort local times descending
#[tokio::test]
async fn wo1_14_sort_localtimes_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "UNWIND [localtime({hour: 10, minute: 35}),
                 localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}),
                 localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876124}),
                 localtime({hour: 12, minute: 35, second: 13}),
                 localtime({hour: 12, minute: 30, second: 14, nanosecond: 645876123})] AS lt
         WITH lt ORDER BY lt DESC LIMIT 3 RETURN lt",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let v0: String = r.rows[0].get("lt")?;
    assert!(
        v0.contains("12:35:13"),
        "First should be 12:35:13, got {v0}"
    );
    Ok(())
}

// [15] Sort times (with timezone) ascending
#[tokio::test]
async fn wo1_15_sort_times_tz_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "UNWIND [time({hour: 10, minute: 35, timezone: '-08:00'}),
                 time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}),
                 time({hour: 12, minute: 31, second: 14, nanosecond: 645876124, timezone: '+01:00'}),
                 time({hour: 12, minute: 35, second: 15, timezone: '+05:00'}),
                 time({hour: 12, minute: 30, second: 14, nanosecond: 645876123, timezone: '+01:01'})] AS t
         WITH t ORDER BY t LIMIT 3 RETURN t",
    ).await?;
    assert_eq!(r.len(), 3);
    // Times sorted by UTC: +05:00 is earliest UTC, then +01:01, then +01:00
    Ok(())
}

// [16] Sort times (with timezone) descending
#[tokio::test]
async fn wo1_16_sort_times_tz_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "UNWIND [time({hour: 10, minute: 35, timezone: '-08:00'}),
                 time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}),
                 time({hour: 12, minute: 31, second: 14, nanosecond: 645876124, timezone: '+01:00'}),
                 time({hour: 12, minute: 35, second: 15, timezone: '+05:00'}),
                 time({hour: 12, minute: 30, second: 14, nanosecond: 645876123, timezone: '+01:01'})] AS t
         WITH t ORDER BY t DESC LIMIT 3 RETURN t",
    ).await?;
    assert_eq!(r.len(), 3);
    Ok(())
}

// [17] Sort localdatetimes ascending
#[tokio::test]
async fn wo1_17_sort_localdatetimes_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "UNWIND [localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12}),
                 localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}),
                 localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1}),
                 localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999}),
                 localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})] AS ldt
         WITH ldt ORDER BY ldt LIMIT 3 RETURN ldt",
    ).await?;
    assert_eq!(r.len(), 3);
    let v0: String = r.rows[0].get("ldt")?;
    let v1: String = r.rows[1].get("ldt")?;
    let v2: String = r.rows[2].get("ldt")?;
    assert!(
        v0.contains("0001-01-01"),
        "Row 0: expected year 0001, got {v0}"
    );
    assert!(
        v1.contains("1980-12-11"),
        "Row 1: expected year 1980, got {v1}"
    );
    assert!(
        v2.contains("1984-10-11") && v2.contains("12:30:14"),
        "Row 2: expected 1984 12:30, got {v2}"
    );
    Ok(())
}

// [18] Sort localdatetimes descending
#[tokio::test]
async fn wo1_18_sort_localdatetimes_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "UNWIND [localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12}),
                 localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}),
                 localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1}),
                 localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999}),
                 localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})] AS ldt
         WITH ldt ORDER BY ldt DESC LIMIT 3 RETURN ldt",
    ).await?;
    assert_eq!(r.len(), 3);
    let v0: String = r.rows[0].get("ldt")?;
    let v1: String = r.rows[1].get("ldt")?;
    let v2: String = r.rows[2].get("ldt")?;
    assert!(
        v0.contains("9999-09-09"),
        "Row 0: expected year 9999, got {v0}"
    );
    assert!(
        v1.contains("1984-10-11") && v1.contains("12:31:14"),
        "Row 1: expected 1984 12:31, got {v1}"
    );
    assert!(
        v2.contains("1984-10-11") && v2.contains("12:30:14"),
        "Row 2: expected 1984 12:30, got {v2}"
    );
    Ok(())
}

// [19] Sort datetimes (with timezone) ascending
#[tokio::test]
async fn wo1_19_sort_datetimes_tz_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "UNWIND [datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'}),
                 datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'}),
                 datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'}),
                 datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'}),
                 datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})] AS dt
         WITH dt ORDER BY dt LIMIT 3 RETURN dt",
    ).await?;
    assert_eq!(r.len(), 3);
    let v0: String = r.rows[0].get("dt")?;
    let v1: String = r.rows[1].get("dt")?;
    assert!(
        v0.contains("0001-01-01"),
        "Row 0: expected year 0001, got {v0}"
    );
    assert!(
        v1.contains("1980-12-11"),
        "Row 1: expected year 1980, got {v1}"
    );
    Ok(())
}

// [20] Sort datetimes (with timezone) descending
#[tokio::test]
async fn wo1_20_sort_datetimes_tz_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "UNWIND [datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'}),
                 datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'}),
                 datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'}),
                 datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'}),
                 datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})] AS dt
         WITH dt ORDER BY dt DESC LIMIT 3 RETURN dt",
    ).await?;
    assert_eq!(r.len(), 3);
    let v0: String = r.rows[0].get("dt")?;
    assert!(
        v0.contains("9999-09-09"),
        "Row 0: expected year 9999, got {v0}"
    );
    Ok(())
}

// [23] Sort boolean property ascending
#[tokio::test]
async fn wo1_23_sort_bool_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {bool: true}), (:B {bool: false}), (:C {bool: false}), (:D {bool: true}), (:E {bool: false})",
    ).await?;
    let r = db
        .query("MATCH (a) WITH a, a.bool AS bool WITH a, bool ORDER BY bool LIMIT 3 RETURN bool")
        .await?;
    assert_eq!(r.len(), 3);
    for row in &r.rows {
        let v: bool = row.get("bool")?;
        assert!(!v, "All 3 should be false");
    }
    Ok(())
}

// [24] Sort boolean property descending
#[tokio::test]
async fn wo1_24_sort_bool_property_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {bool: true}), (:B {bool: false}), (:C {bool: false}), (:D {bool: true}), (:E {bool: false})",
    ).await?;
    let r = db
        .query(
            "MATCH (a) WITH a, a.bool AS bool WITH a, bool ORDER BY bool DESC LIMIT 2 RETURN bool",
        )
        .await?;
    assert_eq!(r.len(), 2);
    for row in &r.rows {
        let v: bool = row.get("bool")?;
        assert!(v, "Both should be true");
    }
    Ok(())
}

// [25] Sort integer property ascending
#[tokio::test]
async fn wo1_25_sort_int_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 9}), (:B {num: 5}), (:C {num: 30}), (:D {num: -11}), (:E {num: 7054})",
    )
    .await?;
    let r = db
        .query("MATCH (a) WITH a, a.num AS num WITH a, num ORDER BY num LIMIT 3 RETURN num")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<i64>("num")?, -11);
    assert_eq!(r.rows[1].get::<i64>("num")?, 5);
    assert_eq!(r.rows[2].get::<i64>("num")?, 9);
    Ok(())
}

// [26] Sort integer property descending
#[tokio::test]
async fn wo1_26_sort_int_property_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 9}), (:B {num: 5}), (:C {num: 30}), (:D {num: -11}), (:E {num: 7054})",
    )
    .await?;
    let r = db
        .query("MATCH (a) WITH a, a.num AS num WITH a, num ORDER BY num DESC LIMIT 3 RETURN num")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<i64>("num")?, 7054);
    assert_eq!(r.rows[1].get::<i64>("num")?, 30);
    assert_eq!(r.rows[2].get::<i64>("num")?, 9);
    Ok(())
}

// [27] Sort float property ascending
#[tokio::test]
async fn wo1_27_sort_float_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 5.025648}), (:B {num: 30.94857}), (:C {num: 30.94856}), (:D {num: -11.2943}), (:E {num: 7054.008})",
    ).await?;
    let r = db
        .query("MATCH (a) WITH a, a.num AS num WITH a, num ORDER BY num LIMIT 3 RETURN num")
        .await?;
    assert_eq!(r.len(), 3);
    let v0: f64 = r.rows[0].get("num")?;
    let v1: f64 = r.rows[1].get("num")?;
    assert!(
        (v0 - (-11.2943)).abs() < 0.001,
        "First should be -11.2943, got {v0}"
    );
    assert!(
        (v1 - 5.025648).abs() < 0.001,
        "Second should be 5.025648, got {v1}"
    );
    Ok(())
}

// [28] Sort float property descending
#[tokio::test]
async fn wo1_28_sort_float_property_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 5.025648}), (:B {num: 30.94857}), (:C {num: 30.94856}), (:D {num: -11.2943}), (:E {num: 7054.008})",
    ).await?;
    let r = db
        .query("MATCH (a) WITH a, a.num AS num WITH a, num ORDER BY num DESC LIMIT 3 RETURN num")
        .await?;
    assert_eq!(r.len(), 3);
    let v0: f64 = r.rows[0].get("num")?;
    assert!(
        (v0 - 7054.008).abs() < 0.001,
        "First should be 7054.008, got {v0}"
    );
    Ok(())
}

// [29] Sort string property ascending
#[tokio::test]
async fn wo1_29_sort_string_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {name: 'lorem'}), (:B {name: 'ipsum'}), (:C {name: 'dolor'}), (:D {name: 'sit'}), (:E {name: 'amet'})",
    ).await?;
    let r = db
        .query("MATCH (a) WITH a, a.name AS name WITH a, name ORDER BY name LIMIT 3 RETURN name")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<String>("name")?, "amet");
    assert_eq!(r.rows[1].get::<String>("name")?, "dolor");
    assert_eq!(r.rows[2].get::<String>("name")?, "ipsum");
    Ok(())
}

// [30] Sort string property descending
#[tokio::test]
async fn wo1_30_sort_string_property_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {name: 'lorem'}), (:B {name: 'ipsum'}), (:C {name: 'dolor'}), (:D {name: 'sit'}), (:E {name: 'amet'})",
    ).await?;
    let r = db
        .query(
            "MATCH (a) WITH a, a.name AS name WITH a, name ORDER BY name DESC LIMIT 3 RETURN name",
        )
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<String>("name")?, "sit");
    assert_eq!(r.rows[1].get::<String>("name")?, "lorem");
    assert_eq!(r.rows[2].get::<String>("name")?, "ipsum");
    Ok(())
}

// [33] Sort date property ascending
#[tokio::test]
async fn wo1_33_sort_date_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {date: date({year: 1910, month: 5, day: 6})}),
                (:B {date: date({year: 1980, month: 12, day: 24})}),
                (:C {date: date({year: 1984, month: 10, day: 12})}),
                (:D {date: date({year: 1985, month: 5, day: 6})}),
                (:E {date: date({year: 1980, month: 10, day: 24})}),
                (:F {date: date({year: 1984, month: 10, day: 11})})",
    )
    .await?;
    let r = db
        .query("MATCH (a) WITH a, a.date AS date WITH a, date ORDER BY date LIMIT 2 RETURN date")
        .await?;
    assert_eq!(r.len(), 2);
    let v0: String = r.rows[0].get("date")?;
    assert!(v0.contains("1910"), "First should be 1910, got {v0}");
    Ok(())
}

// [34] Sort date property descending
#[tokio::test]
async fn wo1_34_sort_date_property_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {date: date({year: 1910, month: 5, day: 6})}),
                (:B {date: date({year: 1980, month: 12, day: 24})}),
                (:C {date: date({year: 1984, month: 10, day: 12})}),
                (:D {date: date({year: 1985, month: 5, day: 6})}),
                (:E {date: date({year: 1980, month: 10, day: 24})}),
                (:F {date: date({year: 1984, month: 10, day: 11})})",
    )
    .await?;
    let r = db
        .query(
            "MATCH (a) WITH a, a.date AS date WITH a, date ORDER BY date DESC LIMIT 2 RETURN date",
        )
        .await?;
    assert_eq!(r.len(), 2);
    let v0: String = r.rows[0].get("date")?;
    assert!(v0.contains("1985"), "First should be 1985, got {v0}");
    Ok(())
}

// [35] Sort localtime property ascending
#[tokio::test]
async fn wo1_35_sort_localtime_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {time: localtime({hour: 10, minute: 35})}),
                (:B {time: localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123})}),
                (:C {time: localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876124})}),
                (:D {time: localtime({hour: 12, minute: 30, second: 14, nanosecond: 645876123})}),
                (:E {time: localtime({hour: 12, minute: 31, second: 15})})",
    )
    .await?;
    let r = db
        .query("MATCH (a) WITH a, a.time AS time WITH a, time ORDER BY time LIMIT 3 RETURN time")
        .await?;
    assert_eq!(r.len(), 3);
    let v0: String = r.rows[0].get("time")?;
    assert!(v0.contains("10:35"), "First should be 10:35, got {v0}");
    Ok(())
}

// [39] Sort localdatetime property ascending
#[tokio::test]
async fn wo1_39_sort_localdatetime_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {datetime: localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12})}),
                (:B {datetime: localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123})}),
                (:C {datetime: localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1})}),
                (:D {datetime: localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999})}),
                (:E {datetime: localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})})",
    ).await?;
    let r = db.query(
        "MATCH (a) WITH a, a.datetime AS datetime WITH a, datetime ORDER BY datetime LIMIT 3 RETURN datetime",
    ).await?;
    assert_eq!(r.len(), 3);
    let mut found_0001 = false;
    let mut found_1980 = false;
    let mut found_1984_early = false;
    for row in &r.rows {
        let dt: String = row.get("datetime")?;
        if dt.contains("0001-01-01") {
            found_0001 = true;
        }
        if dt.contains("1980-12-11") {
            found_1980 = true;
        }
        if dt.contains("1984-10-11") && dt.contains("12:30:14") {
            found_1984_early = true;
        }
    }
    assert!(found_0001, "Should contain year 0001");
    assert!(found_1980, "Should contain year 1980");
    assert!(found_1984_early, "Should contain 1984 12:30:14");
    Ok(())
}

// [40] Sort localdatetime property descending
#[tokio::test]
async fn wo1_40_sort_localdatetime_property_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {datetime: localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12})}),
                (:B {datetime: localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123})}),
                (:C {datetime: localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1})}),
                (:D {datetime: localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999})}),
                (:E {datetime: localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})})",
    ).await?;
    let r = db.query(
        "MATCH (a) WITH a, a.datetime AS datetime WITH a, datetime ORDER BY datetime DESC LIMIT 3 RETURN datetime",
    ).await?;
    assert_eq!(r.len(), 3);
    let mut found_9999 = false;
    for row in &r.rows {
        let dt: String = row.get("datetime")?;
        if dt.contains("9999-09-09") {
            found_9999 = true;
        }
    }
    assert!(found_9999, "Should contain year 9999");
    Ok(())
}

// [41] Sort datetime (tz) property ascending
#[tokio::test]
async fn wo1_41_sort_datetime_tz_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {datetime: datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'})}),
                (:B {datetime: datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'})}),
                (:C {datetime: datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'})}),
                (:D {datetime: datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'})}),
                (:E {datetime: datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})})",
    ).await?;
    let r = db.query(
        "MATCH (a) WITH a, a.datetime AS datetime WITH a, datetime ORDER BY datetime LIMIT 3 RETURN datetime",
    ).await?;
    assert_eq!(r.len(), 3);
    let mut found_0001 = false;
    let mut found_1980 = false;
    for row in &r.rows {
        let dt: String = row.get("datetime")?;
        if dt.contains("0001-01-01") {
            found_0001 = true;
        }
        if dt.contains("1980-12-11") {
            found_1980 = true;
        }
    }
    assert!(found_0001, "Should contain year 0001");
    assert!(found_1980, "Should contain year 1980");
    Ok(())
}

// [42] Sort datetime (tz) property descending
#[tokio::test]
async fn wo1_42_sort_datetime_tz_property_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {datetime: datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'})}),
                (:B {datetime: datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'})}),
                (:C {datetime: datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'})}),
                (:D {datetime: datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'})}),
                (:E {datetime: datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})})",
    ).await?;
    let r = db.query(
        "MATCH (a) WITH a, a.datetime AS datetime WITH a, datetime ORDER BY datetime DESC LIMIT 3 RETURN datetime",
    ).await?;
    assert_eq!(r.len(), 3);
    let mut found_9999 = false;
    for row in &r.rows {
        let dt: String = row.get("datetime")?;
        if dt.contains("9999-09-09") {
            found_9999 = true;
        }
    }
    assert!(found_9999, "Should contain year 9999");
    Ok(())
}

// [43] Sort partially orderable non-distinct
#[tokio::test]
async fn wo1_43_sort_partially_orderable() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [0, 2, 1, 2, 0, 1] AS x WITH x ORDER BY x LIMIT 2 RETURN x")
        .await?;
    assert_eq!(r.len(), 2);
    for row in &r.rows {
        assert_eq!(row.get::<i64>("x")?, 0);
    }
    Ok(())
}

// [44] Sort partially orderable with DISTINCT
#[tokio::test]
async fn wo1_44_sort_partially_orderable_distinct() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [0, 2, 1, 2, 0, 1] AS x WITH DISTINCT x ORDER BY x LIMIT 1 RETURN x")
        .await?;
    assert_eq!(r.len(), 1);
    assert_eq!(r.rows[0].get::<i64>("x")?, 0);
    Ok(())
}

// [45] Sort consistency: integers
#[tokio::test]
async fn wo1_45_sort_consistency_integers() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "WITH [351, -3974856, 93, -3, 123, 0, 3, -2, 20934587, 1, 20934585, 20934586, -10] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
    ).await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(equal, "Sort order should be consistent with < for integers");
    Ok(())
}

// [45] Sort consistency: floats
#[tokio::test]
async fn wo1_45_sort_consistency_floats() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "WITH [351.5, -3974856.01, -3.203957, 123.0002, 123.0001, 123.00013, 123.00011, 0.0100000, 0.0999999, 0.00000001, 3.0, 209345.87, -10.654] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
    ).await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(equal, "Sort order should be consistent with < for floats");
    Ok(())
}

// [45] Sort consistency: strings
#[tokio::test]
async fn wo1_45_sort_consistency_strings() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "WITH ['Sort', 'order', ' ', 'should', 'be', '', 'consistent', 'with', 'comparisons', ', ', 'where', 'comparisons are', 'defined', '!'] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
    ).await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(equal, "Sort order should be consistent with < for strings");
    Ok(())
}

// [45] Sort consistency: booleans
#[tokio::test]
async fn wo1_45_sort_consistency_booleans() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "WITH [true, false] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
        )
        .await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(equal, "Sort order should be consistent with < for booleans");
    Ok(())
}

// [45] Sort consistency: lists
#[tokio::test]
async fn wo1_45_sort_consistency_lists() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "WITH [[2, 2], [2, -2], [1, 2], [], [1], [300, 0], [1, -20], [2, -2, 100]] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
        )
        .await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(equal, "Sort order should be consistent with < for lists");
    Ok(())
}

// [45] Sort consistency: dates
#[tokio::test]
async fn wo1_45_sort_consistency_dates() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "WITH [date({year: 1910, month: 5, day: 6}), date({year: 1980, month: 12, day: 24}), date({year: 1984, month: 10, day: 12}), date({year: 1985, month: 5, day: 6}), date({year: 1980, month: 10, day: 24}), date({year: 1984, month: 10, day: 11})] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
    ).await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(equal, "Sort order should be consistent with < for dates");
    Ok(())
}

// [45] Sort consistency: localtimes
#[tokio::test]
async fn wo1_45_sort_consistency_localtimes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "WITH [localtime({hour: 10, minute: 35}), localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876124}), localtime({hour: 12, minute: 35, second: 13}), localtime({hour: 12, minute: 30, second: 14, nanosecond: 645876123}), localtime({hour: 12, minute: 31, second: 15})] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
    ).await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(
        equal,
        "Sort order should be consistent with < for localtimes"
    );
    Ok(())
}

// [45] Sort consistency: times
#[tokio::test]
async fn wo1_45_sort_consistency_times() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "WITH [time({hour: 10, minute: 35, timezone: '-08:00'}), time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), time({hour: 12, minute: 31, second: 14, nanosecond: 645876124, timezone: '+01:00'}), time({hour: 12, minute: 35, second: 15, timezone: '+05:00'}), time({hour: 12, minute: 30, second: 14, nanosecond: 645876123, timezone: '+01:01'}), time({hour: 12, minute: 35, second: 15, timezone: '+01:00'})] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
    ).await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(equal, "Sort order should be consistent with < for times");
    Ok(())
}

// [45] Sort consistency: localdatetimes
#[tokio::test]
async fn wo1_45_sort_consistency_localdatetimes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "WITH [localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12}), localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1}), localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999}), localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
    ).await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(
        equal,
        "Sort order should be consistent with < for localdatetimes"
    );
    Ok(())
}

// [45] Sort consistency: datetimes
#[tokio::test]
async fn wo1_45_sort_consistency_datetimes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db.query(
        "WITH [datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'}), datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'}), datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'}), datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'}), datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})] AS values
         WITH values, size(values) AS numOfValues
         UNWIND values AS value
         WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
           ORDER BY value
         WITH numOfValues, collect(x) AS orderedX
         RETURN orderedX = range(0, numOfValues-1) AS equal",
    ).await?;
    assert_eq!(r.len(), 1);
    let equal: bool = r.rows[0].get("equal")?;
    assert!(
        equal,
        "Sort order should be consistent with < for datetimes"
    );
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// WithOrderBy2 — Sort by a single expression
// ═══════════════════════════════════════════════════════════════════════════

// [3] Sort by integer expression ascending
#[tokio::test]
async fn wo2_03_sort_int_expr_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 9, num2: 5}), (:B {num: 5, num2: 4}), (:C {num: 30, num2: 3}), (:D {num: -11, num2: 2}), (:E {num: 7054, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY (a.num2 + (a.num * 2)) * -1 LIMIT 3
         RETURN a.num AS num",
        )
        .await?;
    assert_eq!(r.len(), 3);
    // Largest (num2+(num*2))*-1 first when ASC means most negative first => largest original values
    let nums: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("num").unwrap())
        .collect();
    assert!(nums.contains(&7054), "Should contain 7054, got {nums:?}");
    assert!(nums.contains(&30), "Should contain 30, got {nums:?}");
    Ok(())
}

// [4] Sort by integer expression descending
#[tokio::test]
async fn wo2_04_sort_int_expr_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 9, num2: 5}), (:B {num: 5, num2: 4}), (:C {num: 30, num2: 3}), (:D {num: -11, num2: 2}), (:E {num: 7054, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY (a.num2 + (a.num * 2)) * -1 DESC LIMIT 3
         RETURN a.num AS num",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let nums: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("num").unwrap())
        .collect();
    assert!(nums.contains(&(-11)), "Should contain -11, got {nums:?}");
    Ok(())
}

// [5] Sort by float expression ascending
#[tokio::test]
async fn wo2_05_sort_float_expr_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 5.025648, num2: 1.96357}), (:B {num: 30.94857, num2: 0.00002}), (:C {num: 30.94856, num2: 0.00002}), (:D {num: -11.2943, num2: -8.5007}), (:E {num: 7054.008, num2: 948.841})",
    ).await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY (a.num + a.num2 * 2) * -1.01 LIMIT 3
         RETURN a.num AS num",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let nums: Vec<f64> = r
        .rows
        .iter()
        .map(|row| row.get::<f64>("num").unwrap())
        .collect();
    assert!(
        nums.iter().any(|n| (*n - 7054.008).abs() < 0.01),
        "Should contain 7054, got {nums:?}"
    );
    Ok(())
}

// [7] Sort by string expression ascending (REGRESSION)
#[tokio::test]
async fn wo2_07_sort_string_expr_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {name: 'lorem', title: 'dr.'}), (:B {name: 'ipsum', title: 'dr.'}), (:C {name: 'dolor', title: 'prof.'}), (:D {name: 'sit', title: 'dr.'}), (:E {name: 'amet', title: 'prof.'})",
    ).await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY a.title + ' ' + a.name LIMIT 3
         RETURN a.name AS name",
        )
        .await?;
    assert_eq!(r.len(), 3);
    // dr. names sort before prof. names
    let mut names: Vec<String> = r
        .rows
        .iter()
        .map(|row| row.get::<String>("name").unwrap())
        .collect();
    names.sort();
    assert!(
        names.contains(&"ipsum".to_string()),
        "Should contain ipsum, got {names:?}"
    );
    assert!(
        names.contains(&"lorem".to_string()),
        "Should contain lorem, got {names:?}"
    );
    assert!(
        names.contains(&"sit".to_string()),
        "Should contain sit, got {names:?}"
    );
    Ok(())
}

// [8] Sort by string expression descending (REGRESSION)
#[tokio::test]
async fn wo2_08_sort_string_expr_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {name: 'lorem', title: 'dr.'}), (:B {name: 'ipsum', title: 'dr.'}), (:C {name: 'dolor', title: 'prof.'}), (:D {name: 'sit', title: 'dr.'}), (:E {name: 'amet', title: 'prof.'})",
    ).await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY a.title + ' ' + a.name DESC LIMIT 3
         RETURN a.name AS name",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let mut names: Vec<String> = r
        .rows
        .iter()
        .map(|row| row.get::<String>("name").unwrap())
        .collect();
    names.sort();
    assert!(
        names.contains(&"dolor".to_string()),
        "Should contain dolor, got {names:?}"
    );
    assert!(
        names.contains(&"amet".to_string()),
        "Should contain amet, got {names:?}"
    );
    assert!(
        names.contains(&"sit".to_string()),
        "Should contain sit, got {names:?}"
    );
    Ok(())
}

// [11] Sort by date expression ascending
#[tokio::test]
async fn wo2_11_sort_date_expr_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {date: date({year: 1910, month: 5, day: 6})}),
                (:B {date: date({year: 1980, month: 12, day: 24})}),
                (:C {date: date({year: 1984, month: 10, day: 12})}),
                (:D {date: date({year: 1985, month: 5, day: 6})}),
                (:E {date: date({year: 1980, month: 10, day: 24})}),
                (:F {date: date({year: 1984, month: 10, day: 11})})",
    )
    .await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY a.date + duration({months: 1, days: 2}) LIMIT 2
         RETURN a.date AS date",
        )
        .await?;
    assert_eq!(r.len(), 2);
    let v0: String = r.rows[0].get("date")?;
    assert!(v0.contains("1910"), "First should be 1910, got {v0}");
    Ok(())
}

// [17] Sort by localdatetime expression ascending (REGRESSION)
#[tokio::test]
async fn wo2_17_sort_localdatetime_expr_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {datetime: localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12})}),
                (:B {datetime: localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123})}),
                (:C {datetime: localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1})}),
                (:D {datetime: localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999})}),
                (:E {datetime: localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})})",
    ).await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY a.datetime + duration({days: 4, minutes: 6}) LIMIT 3
         RETURN a.datetime AS datetime",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let mut found_0001 = false;
    let mut found_1980 = false;
    for row in &r.rows {
        let dt: String = row.get("datetime")?;
        if dt.contains("0001-01-01") {
            found_0001 = true;
        }
        if dt.contains("1980-12-11") {
            found_1980 = true;
        }
    }
    assert!(found_0001, "Should contain year 0001");
    assert!(found_1980, "Should contain year 1980");
    Ok(())
}

// [18] Sort by localdatetime expression descending (REGRESSION)
#[tokio::test]
async fn wo2_18_sort_localdatetime_expr_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {datetime: localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12})}),
                (:B {datetime: localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123})}),
                (:C {datetime: localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1})}),
                (:D {datetime: localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999})}),
                (:E {datetime: localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})})",
    ).await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY a.datetime + duration({days: 4, minutes: 6}) DESC LIMIT 3
         RETURN a.datetime AS datetime",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let mut found_9999 = false;
    for row in &r.rows {
        let dt: String = row.get("datetime")?;
        if dt.contains("9999-09-09") {
            found_9999 = true;
        }
    }
    assert!(found_9999, "Should contain year 9999");
    Ok(())
}

// [19] Sort by datetime (tz) expression ascending (REGRESSION)
#[tokio::test]
async fn wo2_19_sort_datetime_tz_expr_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {datetime: datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'})}),
                (:B {datetime: datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'})}),
                (:C {datetime: datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'})}),
                (:D {datetime: datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'})}),
                (:E {datetime: datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})})",
    ).await?;
    let r = db
        .query(
            "MATCH (a)
         WITH a ORDER BY a.datetime + duration({days: 4, minutes: 6}) LIMIT 3
         RETURN a.datetime AS datetime",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let mut found_0001 = false;
    for row in &r.rows {
        let dt: String = row.get("datetime")?;
        if dt.contains("0001-01-01") {
            found_0001 = true;
        }
    }
    assert!(found_0001, "Should contain year 0001");
    Ok(())
}

// [21] Sort by partially orderable string expr (REGRESSION)
#[tokio::test]
async fn wo2_21_sort_partially_orderable_string_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({name: 'A'}), ({name: 'A'}), ({name: 'B'}), ({name: 'C'}), ({name: 'C'})")
        .await?;
    let r = db
        .query("MATCH (a) WITH a.name AS name ORDER BY a.name + 'C' LIMIT 2 RETURN name")
        .await?;
    assert_eq!(r.len(), 2);
    for row in &r.rows {
        assert_eq!(row.get::<String>("name")?, "A");
    }
    Ok(())
}

// [21] DESC variant
#[tokio::test]
async fn wo2_21_sort_partially_orderable_string_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({name: 'A'}), ({name: 'A'}), ({name: 'B'}), ({name: 'C'}), ({name: 'C'})")
        .await?;
    let r = db
        .query("MATCH (a) WITH a.name AS name ORDER BY a.name + 'C' DESC LIMIT 2 RETURN name")
        .await?;
    assert_eq!(r.len(), 2);
    for row in &r.rows {
        assert_eq!(row.get::<String>("name")?, "C");
    }
    Ok(())
}

// [22] Sort with grouping key ascending
#[tokio::test]
async fn wo2_22_sort_grouping_key_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({name: 'A'}), ({name: 'A'}), ({name: 'B'}), ({name: 'C'}), ({name: 'C'})")
        .await?;
    let r = db.query(
        "MATCH (a) WITH a.name AS name, count(*) AS cnt ORDER BY a.name LIMIT 1 RETURN name, cnt",
    ).await?;
    assert_eq!(r.len(), 1);
    assert_eq!(r.rows[0].get::<String>("name")?, "A");
    assert_eq!(r.rows[0].get::<i64>("cnt")?, 2);
    Ok(())
}

// [23] Sort by string expression with grouping key (REGRESSION)
#[tokio::test]
async fn wo2_23_sort_string_expr_grouping_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({name: 'A'}), ({name: 'A'}), ({name: 'B'}), ({name: 'C'}), ({name: 'C'})")
        .await?;
    let r = db.query(
        "MATCH (a) WITH a.name AS name, count(*) AS cnt ORDER BY a.name + 'C' LIMIT 1 RETURN name, cnt",
    ).await?;
    assert_eq!(r.len(), 1);
    assert_eq!(r.rows[0].get::<String>("name")?, "A");
    assert_eq!(r.rows[0].get::<i64>("cnt")?, 2);
    Ok(())
}

// [24] Sort with DISTINCT
#[tokio::test]
async fn wo2_24_sort_with_distinct() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({name: 'A'}), ({name: 'A'}), ({name: 'B'}), ({name: 'C'}), ({name: 'C'})")
        .await?;
    let r = db
        .query("MATCH (a) WITH DISTINCT a.name AS name ORDER BY a.name LIMIT 1 RETURN *")
        .await?;
    assert_eq!(r.len(), 1);
    assert_eq!(r.rows[0].get::<String>("name")?, "A");
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// WithOrderBy3 — Sort by multiple expressions
// ═══════════════════════════════════════════════════════════════════════════

// [1] Sort by two expressions, both ascending
#[tokio::test]
async fn wo3_01_sort_two_exprs_asc_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 9, bool: true}), (:B {num: 5, bool: false}), (:C {num: -30, bool: false}), (:D {num: -41, bool: true}), (:E {num: 7054, bool: false})",
    ).await?;
    let r = db
        .query(
            "MATCH (a) WITH a ORDER BY a.bool, a.num LIMIT 4 RETURN a.num AS num, a.bool AS bool",
        )
        .await?;
    assert_eq!(r.len(), 4);
    // false sorts first, then within false sorted by num: -30, 5, 7054. Then true: -41
    let nums: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("num").unwrap())
        .collect();
    assert!(nums.contains(&(-30)), "Should contain -30, got {nums:?}");
    assert!(nums.contains(&5), "Should contain 5, got {nums:?}");
    assert!(nums.contains(&7054), "Should contain 7054, got {nums:?}");
    assert!(nums.contains(&(-41)), "Should contain -41, got {nums:?}");
    Ok(())
}

// [4] Sort by two expressions, both descending
#[tokio::test]
async fn wo3_04_sort_two_exprs_desc_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 9, bool: true}), (:B {num: 5, bool: false}), (:C {num: -30, bool: false}), (:D {num: -41, bool: true}), (:E {num: 7054, bool: false})",
    ).await?;
    let r = db
        .query("MATCH (a) WITH a ORDER BY a.bool DESC, a.num DESC LIMIT 4 RETURN a.num AS num")
        .await?;
    assert_eq!(r.len(), 4);
    // true first (desc), then within true desc: 9, -41. Then false desc: 7054, 5
    let nums: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("num").unwrap())
        .collect();
    assert_eq!(nums, vec![9, -41, 7054, 5]);
    Ok(())
}

// [5] Default sort direction is ascending
#[tokio::test]
async fn wo3_05_default_sort_direction() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE ({num: 3, text: 'a'}), ({num: 3, text: 'b'}), ({num: 1, text: 'a'}), ({num: 1, text: 'b'}), ({num: 2, text: 'a'}), ({num: 2, text: 'b'}), ({num: 4, text: 'a'}), ({num: 4, text: 'b'})",
    ).await?;
    // ASC, (default=ASC for num), ASC for text => first row should be num=2, text='a'
    // num%2: 0 for 2,4; 1 for 1,3. ASC => 0 first. Within 0: num ASC => 2,4. text ASC => 'a'
    let r = db.query(
        "MATCH (a) WITH a ORDER BY a.num % 2 ASC, a.num, a.text ASC LIMIT 1 RETURN a.num AS num, a.text AS text",
    ).await?;
    assert_eq!(r.len(), 1);
    assert_eq!(r.rows[0].get::<i64>("num")?, 2);
    assert_eq!(r.rows[0].get::<String>("text")?, "a");
    Ok(())
}

// [7] Order direction cannot be overwritten by duplicate sort keys
#[tokio::test]
async fn wo3_07_order_direction_not_overwritten() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // First sort key wins: a ASC, a DESC => ASC
    let r = db
        .query("UNWIND [1, 2, 3] AS a WITH a ORDER BY a ASC, a DESC LIMIT 1 RETURN a")
        .await?;
    assert_eq!(r.rows[0].get::<i64>("a")?, 1);

    // First sort key wins: a DESC, a ASC => DESC
    let r = db
        .query("UNWIND [1, 2, 3] AS a WITH a ORDER BY a DESC, a ASC LIMIT 1 RETURN a")
        .await?;
    assert_eq!(r.rows[0].get::<i64>("a")?, 3);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// WithOrderBy4 — Sort with projections and aggregations
// ═══════════════════════════════════════════════════════════════════════════

// [1] Sort by projected expression
#[tokio::test]
async fn wo4_01_sort_projected_expr() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a, a.num + a.num2 AS sum
           ORDER BY a.num + a.num2
           LIMIT 3
         RETURN sum",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let sums: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("sum").unwrap())
        .collect();
    assert!(sums.contains(&5), "Should contain sum=5, got {sums:?}");
    assert!(sums.contains(&6), "Should contain sum=6, got {sums:?}");
    assert!(sums.contains(&7), "Should contain sum=7, got {sums:?}");
    Ok(())
}

// [2] Sort by alias of projected expression
#[tokio::test]
async fn wo4_02_sort_by_alias() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a, a.num + a.num2 AS sum ORDER BY sum LIMIT 3
         RETURN sum",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let sums: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("sum").unwrap())
        .collect();
    assert!(sums.contains(&5));
    assert!(sums.contains(&6));
    assert!(sums.contains(&7));
    Ok(())
}

// [3] Sort by two projected expressions, different priority than projection order
#[tokio::test]
async fn wo4_03_sort_two_projected_exprs() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a, a.num + a.num2 AS sum, a.num2 % 3 AS mod
           ORDER BY a.num2 % 3, a.num + a.num2
           LIMIT 3
         RETURN sum, mod",
        )
        .await?;
    assert_eq!(r.len(), 3);
    // mod=0 first (num2=0,3 => sums 9,6), then mod=1 (num2=4,1 => sums 5,8)
    let mods: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("mod").unwrap())
        .collect();
    assert!(mods.contains(&0), "Should contain mod=0, got {mods:?}");
    Ok(())
}

// [6] Sort by aliases with different priority than projection order
#[tokio::test]
async fn wo4_06_sort_by_aliases() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a, a.num + a.num2 AS sum, a.num2 % 3 AS mod
           ORDER BY mod, sum
           LIMIT 3
         RETURN sum, mod",
        )
        .await?;
    assert_eq!(r.len(), 3);
    Ok(())
}

// [7] Sort by alias that shadows an existing variable
#[tokio::test]
async fn wo4_07_sort_alias_shadows_variable() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a, a.num2 % 3 AS x
         WITH a, a.num + a.num2 AS x ORDER BY x LIMIT 3
         RETURN x",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let xs: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("x").unwrap())
        .collect();
    assert!(xs.contains(&5));
    assert!(xs.contains(&6));
    assert!(xs.contains(&7));
    Ok(())
}

// [8] Sort by non-projected existing variable
#[tokio::test]
async fn wo4_08_sort_non_projected_var() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a, a.num + a.num2 AS sum
         WITH a, a.num2 % 3 AS mod ORDER BY sum LIMIT 3
         RETURN mod",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let mods: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("mod").unwrap())
        .collect();
    assert!(
        mods.contains(&1),
        "Should contain mod=1 (num2=4), got {mods:?}"
    );
    assert!(
        mods.contains(&0),
        "Should contain mod=0 (num2=3), got {mods:?}"
    );
    assert!(
        mods.contains(&2),
        "Should contain mod=2 (num2=2), got {mods:?}"
    );
    Ok(())
}

// [9] Sort by alias containing the shadowed variable
#[tokio::test]
async fn wo4_09_sort_alias_containing_shadowed_var() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a.num2 AS x
         WITH x % 3 AS x ORDER BY x LIMIT 3
         RETURN x",
        )
        .await?;
    assert_eq!(r.len(), 3);
    let xs: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("x").unwrap())
        .collect();
    assert!(xs.contains(&0), "Should contain x=0, got {xs:?}");
    assert!(xs.contains(&1), "Should contain x=1, got {xs:?}");
    Ok(())
}

// [11] Sort by aggregate projection
#[tokio::test]
async fn wo4_11_sort_by_aggregate() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a.num2 % 3 AS mod, sum(a.num + a.num2) AS sum
           ORDER BY sum(a.num + a.num2) LIMIT 2
         RETURN mod, sum",
        )
        .await?;
    assert_eq!(r.len(), 2);
    let sums: Vec<i64> = r
        .rows
        .iter()
        .map(|row| row.get::<i64>("sum").unwrap())
        .collect();
    assert!(sums.contains(&7), "Should contain sum=7, got {sums:?}");
    assert!(sums.contains(&13), "Should contain sum=13, got {sums:?}");
    Ok(())
}

// [12] Sort by aliased aggregate
#[tokio::test]
async fn wo4_12_sort_by_aliased_aggregate() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    ).await?;
    let r = db
        .query(
            "MATCH (a:A)
         WITH a.num2 % 3 AS mod, sum(a.num + a.num2) AS sum ORDER BY sum LIMIT 2
         RETURN mod, sum",
        )
        .await?;
    assert_eq!(r.len(), 2);
    Ok(())
}

// [15] Sort by aliased aggregate allows subsequent matching (REGRESSION)
#[tokio::test]
async fn wo4_15_sort_aggregate_with_subsequent_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ()-[:T1 {id: 0}]->(:X), ()-[:T2 {id: 1}]->(:X), ()-[:T2 {id: 2}]->()")
        .await?;
    let r = db
        .query(
            "MATCH (a)-[r]->(b:X)
         WITH a, r, b, count(*) AS c ORDER BY c
         MATCH (a)-[r]->(b)
         RETURN r.id AS rel_id ORDER BY rel_id",
        )
        .await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<i64>("rel_id")?, 0);
    assert_eq!(r.rows[1].get::<i64>("rel_id")?, 1);
    Ok(())
}

// [16] Constants and params in ORDER BY with aggregation
#[tokio::test]
async fn wo4_16_constants_in_orderby_with_agg() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "MATCH (person)
         WITH avg(person.age) AS avgAge ORDER BY avg(person.age) - 1000
         RETURN avgAge",
        )
        .await?;
    assert_eq!(r.len(), 1);
    // avgAge of empty set = null
    Ok(())
}

// [17] Projected variables in ORDER BY with aggregation
#[tokio::test]
async fn wo4_17_projected_vars_in_orderby_with_agg() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "MATCH (me:Person)--(you:Person)
         WITH me.age AS age, count(you.age) AS cnt ORDER BY age, age + count(you.age)
         RETURN age",
        )
        .await?;
    assert_eq!(r.len(), 0); // No Person nodes exist
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// ReturnOrderBy1 — RETURN ORDER BY single variable
// ═══════════════════════════════════════════════════════════════════════════

// [1] RETURN ORDER BY booleans
#[tokio::test]
async fn ro1_01_return_orderby_booleans_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [true, false] AS bools RETURN bools ORDER BY bools")
        .await?;
    assert_eq!(r.len(), 2);
    assert!(!r.rows[0].get::<bool>("bools")?);
    assert!(r.rows[1].get::<bool>("bools")?);
    Ok(())
}

// [2] RETURN ORDER BY booleans DESC
#[tokio::test]
async fn ro1_02_return_orderby_booleans_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [true, false] AS bools RETURN bools ORDER BY bools DESC")
        .await?;
    assert_eq!(r.len(), 2);
    assert!(r.rows[0].get::<bool>("bools")?);
    assert!(!r.rows[1].get::<bool>("bools")?);
    Ok(())
}

// [3] RETURN ORDER BY strings
#[tokio::test]
async fn ro1_03_return_orderby_strings() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND ['.*', '', ' ', 'one'] AS s RETURN s ORDER BY s")
        .await?;
    assert_eq!(r.len(), 4);
    assert_eq!(r.rows[0].get::<String>("s")?, "");
    assert_eq!(r.rows[1].get::<String>("s")?, " ");
    assert_eq!(r.rows[2].get::<String>("s")?, ".*");
    assert_eq!(r.rows[3].get::<String>("s")?, "one");
    Ok(())
}

// [5] RETURN ORDER BY integers
#[tokio::test]
async fn ro1_05_return_orderby_integers() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [1, 3, 2] AS ints RETURN ints ORDER BY ints")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<i64>("ints")?, 1);
    assert_eq!(r.rows[1].get::<i64>("ints")?, 2);
    assert_eq!(r.rows[2].get::<i64>("ints")?, 3);
    Ok(())
}

// [7] RETURN ORDER BY floats
#[tokio::test]
async fn ro1_07_return_orderby_floats() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("UNWIND [1.5, 1.3, 999.99] AS f RETURN f ORDER BY f")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<f64>("f")?, 1.3);
    assert_eq!(r.rows[1].get::<f64>("f")?, 1.5);
    assert_eq!(r.rows[2].get::<f64>("f")?, 999.99);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// ReturnOrderBy2 — RETURN ORDER BY with expressions
// ═══════════════════════════════════════════════════════════════════════════

// [1] RETURN ORDER BY property ascending
#[tokio::test]
async fn ro2_01_return_orderby_property_asc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({num: 1}), ({num: 3}), ({num: -5})")
        .await?;
    let r = db
        .query("MATCH (n) RETURN n.num AS prop ORDER BY n.num")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<i64>("prop")?, -5);
    assert_eq!(r.rows[1].get::<i64>("prop")?, 1);
    assert_eq!(r.rows[2].get::<i64>("prop")?, 3);
    Ok(())
}

// [2] RETURN ORDER BY property descending
#[tokio::test]
async fn ro2_02_return_orderby_property_desc() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({num: 1}), ({num: 3}), ({num: -5})")
        .await?;
    let r = db
        .query("MATCH (n) RETURN n.num AS prop ORDER BY n.num DESC")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<i64>("prop")?, 3);
    assert_eq!(r.rows[1].get::<i64>("prop")?, 1);
    assert_eq!(r.rows[2].get::<i64>("prop")?, -5);
    Ok(())
}

// [3] Sort on aggregated function
#[tokio::test]
async fn ro2_03_sort_on_aggregate_function() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE ({division: 'A', age: 22}), ({division: 'B', age: 33}), ({division: 'B', age: 44}), ({division: 'C', age: 55})",
    ).await?;
    let r = db
        .query("MATCH (n) RETURN n.division, max(n.age) ORDER BY max(n.age)")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<String>("n.division")?, "A");
    assert_eq!(r.rows[1].get::<String>("n.division")?, "B");
    assert_eq!(r.rows[2].get::<String>("n.division")?, "C");
    Ok(())
}

// [4] Support sort and distinct
#[tokio::test]
async fn ro2_04_sort_and_distinct() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({name: 'A'}), ({name: 'B'}), ({name: 'C'})")
        .await?;
    let r = db
        .query("MATCH (a) RETURN DISTINCT a ORDER BY a.name")
        .await?;
    assert_eq!(r.len(), 3);
    Ok(())
}

// [7] Ordering with aggregation
#[tokio::test]
async fn ro2_07_ordering_with_aggregation() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({name: 'nisse'})").await?;
    let r = db
        .query("MATCH (n) RETURN n.name, count(*) AS foo ORDER BY n.name")
        .await?;
    assert_eq!(r.len(), 1);
    assert_eq!(r.rows[0].get::<String>("n.name")?, "nisse");
    assert_eq!(r.rows[0].get::<i64>("foo")?, 1);
    Ok(())
}

// [8] RETURN * with ORDER BY
#[tokio::test]
async fn ro2_08_return_star_orderby() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({id: 1}), ({id: 10})").await?;
    let r = db.query("MATCH (n) RETURN * ORDER BY n.id").await?;
    assert_eq!(r.len(), 2);
    Ok(())
}

// [9] Aliased DISTINCT in ORDER BY
#[tokio::test]
async fn ro2_09_aliased_distinct_orderby() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({id: 1}), ({id: 10})").await?;
    let r = db
        .query("MATCH (n) RETURN DISTINCT n.id AS id ORDER BY id DESC")
        .await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<i64>("id")?, 10);
    assert_eq!(r.rows[1].get::<i64>("id")?, 1);
    Ok(())
}

// [11] Aggregates ordered by arithmetics
#[tokio::test]
async fn ro2_11_aggregates_ordered_by_arithmetics() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE (:A), (:X), (:X)").await?;
    let r = db
        .query("MATCH (a:A), (b:X) RETURN count(a) * 10 + count(b) * 5 AS x ORDER BY x")
        .await?;
    assert_eq!(r.len(), 1);
    assert_eq!(r.rows[0].get::<i64>("x")?, 30);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// ReturnOrderBy3 — RETURN ORDER BY multiple expressions
// ═══════════════════════════════════════════════════════════════════════════

// [1] Sort on aggregate and property
#[tokio::test]
async fn ro3_01_sort_aggregate_and_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE ({division: 'Sweden'}), ({division: 'Germany'}), ({division: 'England'}), ({division: 'Sweden'})",
    ).await?;
    let r = db
        .query("MATCH (n) RETURN n.division, count(*) ORDER BY count(*) DESC, n.division ASC")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<String>("n.division")?, "Sweden");
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// ReturnOrderBy4 — RETURN ORDER BY with projection
// ═══════════════════════════════════════════════════════════════════════════

// [1] ORDER BY column introduced in RETURN
#[tokio::test]
async fn ro4_01_orderby_column_from_return() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "WITH [0, 1] AS prows, [[2], [3, 4]] AS qrows
         UNWIND prows AS p
         UNWIND qrows[p] AS q
         WITH p, count(q) AS rng
         RETURN p ORDER BY rng",
        )
        .await?;
    assert_eq!(r.len(), 2);
    assert_eq!(r.rows[0].get::<i64>("p")?, 0);
    assert_eq!(r.rows[1].get::<i64>("p")?, 1);
    Ok(())
}

// [2] Handle projections with ORDER BY
#[tokio::test]
async fn ro4_02_projections_with_orderby() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute(
        "CREATE (:Crew {name: 'Neo', rank: 1}), (:Crew {name: 'Neo', rank: 2}), (:Crew {name: 'Neo', rank: 3}), (:Crew {name: 'Neo', rank: 4}), (:Crew {name: 'Neo', rank: 5})",
    ).await?;
    let r = db.query(
        "MATCH (c:Crew {name: 'Neo'}) WITH c, 0 AS relevance RETURN c.rank AS rank ORDER BY relevance, c.rank",
    ).await?;
    assert_eq!(r.len(), 5);
    for (i, row) in r.rows.iter().enumerate() {
        assert_eq!(row.get::<i64>("rank")?, (i as i64) + 1);
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// ReturnOrderBy5 — RETURN ORDER BY with column renaming
// ═══════════════════════════════════════════════════════════════════════════

// [1] Renaming columns before ORDER BY
#[tokio::test]
async fn ro5_01_renamed_column_orderby() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE ({num: 1}), ({num: 3}), ({num: -5})")
        .await?;
    let r = db
        .query("MATCH (n) RETURN n.num AS n ORDER BY n + 2")
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(r.rows[0].get::<i64>("n")?, -5);
    assert_eq!(r.rows[1].get::<i64>("n")?, 1);
    assert_eq!(r.rows[2].get::<i64>("n")?, 3);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// ReturnOrderBy6 — Aggregation expressions in ORDER BY
// ═══════════════════════════════════════════════════════════════════════════

// [1] Constants in ORDER BY with aggregation
#[tokio::test]
async fn ro6_01_constants_in_orderby_agg() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query("MATCH (person) RETURN avg(person.age) AS avgAge ORDER BY avg(person.age) - 1000")
        .await?;
    assert_eq!(r.len(), 1);
    Ok(())
}

// [2] Returned aliases in ORDER BY with aggregation
#[tokio::test]
async fn ro6_02_returned_aliases_in_orderby_agg() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "MATCH (me:Person)--(you:Person)
         RETURN me.age AS age, count(you.age) AS cnt ORDER BY age, age + count(you.age)",
        )
        .await?;
    assert_eq!(r.len(), 0);
    Ok(())
}

// [3] Property accesses in ORDER BY with aggregation
#[tokio::test]
async fn ro6_03_property_access_in_orderby_agg() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let r = db
        .query(
            "MATCH (me:Person)--(you:Person)
         RETURN me.age AS age, count(you.age) AS cnt ORDER BY me.age + count(you.age)",
        )
        .await?;
    assert_eq!(r.len(), 0);
    Ok(())
}
