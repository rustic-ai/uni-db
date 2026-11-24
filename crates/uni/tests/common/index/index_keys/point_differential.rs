// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T13: a POINT index answers exactly as no index does.
//!
//! `point.withinBBox` and `distance(p, c) < r` / `<= r` (both argument
//! orders, both comparison orientations), on cartesian, 3-D and WGS-84
//! points; boxes and disks across the antimeridian and near the poles;
//! `-0.0` coordinates; points exactly on a box edge and exactly at the
//! radius; literals and parameters. Constants of another CRS are not served
//! and raise the same error with and without the index, as does a point of
//! another CRS stored in the column.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(point_differential)'

// Rust guideline compliant

use super::harness::{Case, Differential, Writes, rows};
use anyhow::Result;
use uni_db::Value;

const RET: &str = "RETURN p.id AS id";

fn q(label: &str, w: &str) -> String {
    format!("MATCH (p:{label}) WHERE {w} {RET}")
}

/// The value of `expr` (a `point(...)` or a `distance(...)`), for a
/// parameter.
async fn value(diff: &Differential, expr: &str) -> Result<Value> {
    let rows = diff
        .plain
        .session()
        .query(&format!("RETURN {expr} AS v"))
        .await?;
    Ok(rows.rows()[0].value("v").expect("v").clone())
}

/// Every case's outcome, rows or error, is the same on both databases.
async fn same_outcome(diff: &Differential, stage: &str, cases: &[Case]) -> Result<()> {
    for case in cases {
        let plain = rows(&diff.plain, case).await.map_err(|e| format!("{e:#}"));
        let indexed = rows(&diff.indexed, case)
            .await
            .map_err(|e| format!("{e:#}"));
        assert_eq!(indexed, plain, "{stage}: outcomes differ for {case:?}");
        assert!(
            !diff.explained_as_used(case).await?,
            "{stage}: EXPLAIN reports the index for {case:?}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Cartesian
// ---------------------------------------------------------------------------

const XY_SCHEMA: &[&str] = &["CREATE LABEL Spot (id INT, loc POINT, n INT)"];
const XY_INDEX: &str = "CREATE INDEX idx_loc FOR (p:Spot) ON (p.loc)";

/// Points around the box `(0, 0)–(10, 10)` and the disk of radius 5 at the
/// origin: on the box's edges and corners, `-0.0` on an edge, at exactly
/// distance 5 (`(3, 4)`: `√25` is exact), just outside both.
fn xy_writes() -> Writes {
    let points: &[(i64, &str, &str)] = &[
        (1, "0.0", "0.0"),
        (2, "-0.0", "3.0"),
        (3, "10.0", "10.0"),
        (4, "3.0", "4.0"),
        (5, "-3.0", "-4.0"),
        (6, "5.0", "0.0"),
        (7, "0.0", "-5.0"),
        (8, "10.000001", "5.0"),
        (9, "5.0", "-0.000001"),
        (10, "3.5", "3.5"),
        (11, "-0.0", "-0.0"),
        (12, "100.0", "-100.0"),
        (13, "3.0000001", "4.0"),
    ];
    let creates: Vec<String> = points
        .iter()
        .map(|(id, x, y)| format!("(:Spot {{id: {id}, loc: point({{x: {x}, y: {y}}})}})"))
        .collect();
    Writes {
        initial: vec![format!("CREATE {}", creates.join(", "))],
        changes: vec![
            "MATCH (p:Spot {id: 12}) SET p.loc = point({x: 1.0, y: 1.0})".to_string(),
            "MATCH (p:Spot {id: 1}) DELETE p".to_string(),
            "MATCH (p:Spot {id: 3}) SET p.loc = point({x: 50.0, y: 50.0})".to_string(),
            "MATCH (p:Spot {id: 4}) SET p.n = 1".to_string(),
            "CREATE (:Spot {id: 20, loc: point({x: 4.0, y: 3.0})}), \
             (:Spot {id: 21, loc: point({x: 10.0, y: -0.0})})"
                .to_string(),
        ],
    }
}

async fn xy_cases(diff: &Differential) -> Result<Vec<Case>> {
    let s = |w: &str| Case::served(q("Spot", w));
    let ll = "point({x: 0.0, y: 0.0})";
    let ur = "point({x: 10.0, y: 10.0})";
    let o = "point({x: 0.0, y: 0.0})";
    let mut cases = vec![
        s(&format!("point.withinBBox(p.loc, {ll}, {ur})")),
        s("point.withinBBox(p.loc, point({x: -0.0, y: -0.0}), point({x: 3.0, y: 4.0}))"),
        s("point.withinBBox(p.loc, point({x: 3.0, y: 3.0}), point({x: 3.0, y: 4.0}))"),
        // An empty box (lower-left right of upper-right) matches nothing.
        s(&format!("point.withinBBox(p.loc, {ur}, {ll})")),
        s(&format!("distance(p.loc, {o}) < 5.0")),
        s(&format!("distance(p.loc, {o}) <= 5.0")),
        s(&format!("distance(p.loc, {o}) <= 5")),
        s(&format!("distance({o}, p.loc) <= 5.0")),
        s(&format!("5.0 > distance(p.loc, {o})")),
        s(&format!("5.0 >= distance({o}, p.loc)")),
        s(&format!("distance(p.loc, {o}) <= 0.0")),
        s(&format!("distance(p.loc, {o}) < 0.0")),
        s("distance(p.loc, point({x: 3.0, y: 4.0})) < 0.0000002"),
        s(&format!(
            "point.withinBBox(p.loc, {ll}, {ur}) AND distance(p.loc, {o}) <= 5.0"
        )),
        s(&format!("distance(p.loc, {o}) <= 5.0 AND p.n IS NULL")),
        // A 3-D constant's x/y bound a 2-D column's box (withinBBox reads
        // x and y only).
        s(
            "point.withinBBox(p.loc, point({x: 0.0, y: 0.0, z: 9.0}), point({x: 4.0, y: 4.0, z: 1.0}))",
        ),
    ];
    // Parameters: corners and centers as `point()` values, the radius as a
    // number, and a radius that is exactly the engine's distance to a row.
    let ll_v = value(diff, ll).await?;
    let ur_v = value(diff, ur).await?;
    let o_v = value(diff, o).await?;
    let d = value(
        diff,
        "distance(point({x: 0.0, y: 0.0}), point({x: 3.5, y: 3.5}))",
    )
    .await?;
    cases.extend([
        s("point.withinBBox(p.loc, $ll, $ur)")
            .param("ll", ll_v)
            .param("ur", ur_v),
        s("distance(p.loc, $c) < $r")
            .param("c", o_v.clone())
            .param("r", Value::Float(5.0)),
        s("distance(p.loc, $c) <= $r")
            .param("c", o_v.clone())
            .param("r", d.clone()),
        s("distance(p.loc, $c) < $r")
            .param("c", o_v.clone())
            .param("r", d),
        s("$r >= distance($c, p.loc)")
            .param("c", o_v)
            .param("r", Value::Int(5)),
    ]);
    // Not served.
    let u = |w: &str| Case::unserved(q("Spot", w));
    cases.extend([
        u(&format!("distance(p.loc, {o}) > 5.0")),
        u(&format!("distance(p.loc, {o}) = 5.0")),
        u(&format!("p.loc = {o}")),
        u("distance(p.loc, p.loc) < 1.0"),
        u(&format!("distance(p.loc, {o}) < 5.0 OR p.n = 1")),
        u(&format!("distance(p.loc, {o}) < null")),
        u(&format!("distance(p.loc, {o}) < '5'")),
    ]);
    Ok(cases)
}

#[tokio::test]
async fn point_differential_cartesian() -> Result<()> {
    let diff = Differential::open(XY_SCHEMA, XY_INDEX, "Spot", "loc").await?;
    let cases = xy_cases(&diff).await?;
    diff.stages(&xy_writes(), &cases).await
}

/// Declared over flushed rows: the key columns are backfilled from the
/// stored struct.
#[tokio::test]
async fn point_differential_index_after_data() -> Result<()> {
    let diff = Differential::open(XY_SCHEMA, "RETURN 1", "Spot", "loc").await?;
    let w = xy_writes();
    for stmt in &w.initial {
        diff.write(stmt).await?;
    }
    diff.flush().await?;
    diff.write("MATCH (p:Spot {id: 10}) DELETE p").await?;
    diff.flush().await?;
    super::harness::run(&diff.indexed, XY_INDEX).await?;
    let cases = xy_cases(&diff).await?;
    diff.check("declared after a flush", &cases, true).await?;
    for stmt in &w.changes {
        diff.write(stmt).await?;
    }
    diff.check("changed, unflushed", &cases, false).await?;
    diff.flush().await?;
    diff.check("changed, flushed", &cases, true).await
}

/// A 3-D column: its x and y are keyed, z is not.
#[tokio::test]
async fn point_differential_cartesian_3d() -> Result<()> {
    let diff = Differential::open(
        &["CREATE LABEL Pos (id INT, loc POINT3D)"],
        "CREATE INDEX idx_loc FOR (p:Pos) ON (p.loc)",
        "Pos",
        "loc",
    )
    .await?;
    let writes = Writes {
        initial: vec![
            "CREATE (:Pos {id: 1, loc: point({x: 1.0, y: 2.0, z: 2.0})}), \
                    (:Pos {id: 2, loc: point({x: 1.0, y: 2.0, z: 30.0})}), \
                    (:Pos {id: 3, loc: point({x: -0.0, y: 0.0, z: 0.0})}), \
                    (:Pos {id: 4, loc: point({x: 9.0, y: 9.0, z: 0.0})})"
                .to_string(),
        ],
        changes: vec![
            "MATCH (p:Pos {id: 4}) SET p.loc = point({x: 0.5, y: 0.5, z: 0.5})".to_string(),
        ],
    };
    let c = "point({x: 0.0, y: 0.0, z: 0.0})";
    let cases = vec![
        Case::served(q("Pos", &format!("distance(p.loc, {c}) <= 3.0"))),
        Case::served(q("Pos", &format!("distance(p.loc, {c}) < 2.0"))),
        Case::served(q(
            "Pos",
            "point.withinBBox(p.loc, point({x: 0.0, y: 0.0, z: 0.0}), point({x: 1.0, y: 2.0, z: 0.0}))",
        )),
        // A 2-D center is another CRS: `distance` raises (checked below).
    ];
    diff.stages(&writes, &cases).await?;
    let wrong = vec![Case::unserved(q(
        "Pos",
        "distance(p.loc, point({x: 0.0, y: 0.0})) < 3.0",
    ))];
    same_outcome(&diff, "3-D, 2-D center", &wrong).await
}

// ---------------------------------------------------------------------------
// WGS-84
// ---------------------------------------------------------------------------

const GEO_SCHEMA: &[&str] = &["CREATE LABEL Place (id INT, loc GEOPOINT, n INT)"];
const GEO_INDEX: &str = "CREATE INDEX idx_loc FOR (p:Place) ON (p.loc)";

/// Cities, points on both sides of the antimeridian and at ±180°, points
/// around the north pole on opposite meridians, the south pole region, the
/// poles themselves, and `-0.0` coordinates.
const GEO_ROWS: &[(i64, f64, f64)] = &[
    (1, 51.5074, -0.1278), // London
    (2, 48.8566, 2.3522),  // Paris
    (3, 40.7128, -74.006), // New York
    (4, 0.0, 179.9),
    (5, 0.0, -179.9),
    (6, 0.0, 180.0),
    (7, 0.0, -180.0),
    (8, 10.0, 170.0),
    (9, 10.0, -170.0),
    (10, 89.9, 0.0),
    (11, 89.9, 180.0),
    (12, 89.95, -90.0),
    (13, 90.0, 0.0),
    (14, -89.99, 45.0),
    (15, -90.0, -120.0),
    (16, 0.0, 0.0),
    (17, -0.0, -0.0),
    (18, -33.8688, 151.2093), // Sydney
    (19, 0.0, 0.5),
    (20, 60.0, 179.5),
    (21, 60.0, -179.5),
];

fn fmt(f: f64) -> String {
    if f == 0.0 && f.is_sign_negative() {
        "-0.0".to_string()
    } else {
        format!("{f:?}")
    }
}

fn geo_point(lat: f64, lon: f64) -> String {
    format!("point({{latitude: {}, longitude: {}}})", fmt(lat), fmt(lon))
}

fn geo_writes() -> Writes {
    let creates: Vec<String> = GEO_ROWS
        .iter()
        .map(|(id, lat, lon)| format!("(:Place {{id: {id}, loc: {}}})", geo_point(*lat, *lon)))
        .collect();
    Writes {
        initial: vec![format!("CREATE {}", creates.join(", "))],
        changes: vec![
            format!(
                "MATCH (p:Place {{id: 3}}) SET p.loc = {}",
                geo_point(50.0, 1.0)
            ),
            "MATCH (p:Place {id: 4}) DELETE p".to_string(),
            format!(
                "MATCH (p:Place {{id: 13}}) SET p.loc = {}",
                geo_point(-0.0, 179.99)
            ),
            "MATCH (p:Place {id: 1}) SET p.n = 1".to_string(),
            format!(
                "CREATE (:Place {{id: 30, loc: {}}}), (:Place {{id: 31, loc: {}}})",
                geo_point(89.99, -179.0),
                geo_point(-0.001, -179.999)
            ),
        ],
    }
}

async fn geo_cases(diff: &Differential) -> Result<Vec<Case>> {
    let s = |w: &str| Case::served(q("Place", w));
    let p = geo_point;
    let london = p(51.5074, -0.1278);
    let mut cases = vec![
        // Europe.
        s(&format!(
            "point.withinBBox(p.loc, {}, {})",
            p(40.0, -10.0),
            p(60.0, 10.0)
        )),
        // Across the antimeridian: lower-left longitude east of
        // upper-right.
        s(&format!(
            "point.withinBBox(p.loc, {}, {})",
            p(-20.0, 170.0),
            p(20.0, -170.0)
        )),
        s(&format!(
            "point.withinBBox(p.loc, {}, {})",
            p(0.0, 179.9),
            p(70.0, -179.9)
        )),
        // Edges at exactly 0, ±180 and the poles; `-0.0` corners.
        s(&format!(
            "point.withinBBox(p.loc, {}, {})",
            p(0.0, 0.0),
            p(90.0, 180.0)
        )),
        s(&format!(
            "point.withinBBox(p.loc, {}, {})",
            p(-0.0, -0.0),
            p(0.0, 0.5)
        )),
        s(&format!(
            "point.withinBBox(p.loc, {}, {})",
            p(-90.0, -180.0),
            p(-89.0, 180.0)
        )),
        // Distance from London: Paris is ~344 km away.
        s(&format!("distance(p.loc, {london}) < 400000.0")),
        s(&format!("distance({london}, p.loc) <= 400000")),
        s(&format!("400000.0 > distance(p.loc, {london})")),
        s(&format!("distance(p.loc, {london}) < 300000.0")),
        // Across the antimeridian: 0.1° of longitude at the equator is
        // ~11 km.
        s(&format!("distance(p.loc, {}) < 20000.0", p(0.0, 179.95))),
        s(&format!("distance(p.loc, {}) < 20000.0", p(0.0, -180.0))),
        s(&format!("distance(p.loc, {}) < 60000.0", p(60.0, 180.0))),
        // Near the north pole the disk spans every meridian: (89.9, 0) and
        // (89.9, 180) are ~22 km apart over the pole.
        s(&format!("distance(p.loc, {}) < 25000.0", p(89.9, 0.0))),
        s(&format!("distance(p.loc, {}) <= 20000.0", p(90.0, 0.0))),
        s(&format!("distance(p.loc, {}) < 5000.0", p(-90.0, 0.0))),
        // Half the globe; past the antipode the box bounds nothing, so the
        // index is not used.
        s(&format!("distance(p.loc, {}) < 10000000.0", p(0.0, 0.0))),
        Case::unserved(q(
            "Place",
            &format!("distance(p.loc, {}) <= 25000000.0", p(0.0, 0.0)),
        )),
        s(&format!("distance(p.loc, {}) <= 0.0", p(-0.0, 0.0))),
    ];
    // Parameters, and radii that are exactly the engine's distance to a row
    // (Paris from London; the pole-crossing pair).
    let london_v = value(diff, &london).await?;
    let to_paris = value(diff, &format!("distance({london}, {})", p(48.8566, 2.3522))).await?;
    let pole = value(diff, &p(89.9, 0.0)).await?;
    let over_pole = value(
        diff,
        &format!("distance({}, {})", p(89.9, 0.0), p(89.9, 180.0)),
    )
    .await?;
    let ll = value(diff, &p(-20.0, 170.0)).await?;
    let ur = value(diff, &p(20.0, -170.0)).await?;
    cases.extend([
        s("distance(p.loc, $c) <= $r")
            .param("c", london_v.clone())
            .param("r", to_paris.clone()),
        s("distance(p.loc, $c) < $r")
            .param("c", london_v.clone())
            .param("r", to_paris),
        s("distance($c, p.loc) <= $r")
            .param("c", pole)
            .param("r", over_pole),
        s("point.withinBBox(p.loc, $ll, $ur)")
            .param("ll", ll)
            .param("ur", ur),
        s("$r > distance(p.loc, $c)")
            .param("c", london_v)
            .param("r", Value::Int(400_000)),
    ]);
    let u = |w: &str| Case::unserved(q("Place", w));
    cases.extend([
        u(&format!("distance(p.loc, {london}) > 400000.0")),
        u(&format!("p.loc = {london}")),
        u(&format!("distance(p.loc, {london}) < 400000.0 OR p.n = 1")),
    ]);
    Ok(cases)
}

#[tokio::test]
async fn point_differential_geographic() -> Result<()> {
    let diff = Differential::open(GEO_SCHEMA, GEO_INDEX, "Place", "loc").await?;
    let cases = geo_cases(&diff).await?;
    diff.stages(&geo_writes(), &cases).await
}

// ---------------------------------------------------------------------------
// Other CRS
// ---------------------------------------------------------------------------

/// A constant of another CRS is not served: `distance` and `withinBBox`
/// raise, with the same error, with and without the index, at every stage.
#[tokio::test]
async fn point_differential_other_crs_constants_raise_alike() -> Result<()> {
    let geo = Differential::open(GEO_SCHEMA, GEO_INDEX, "Place", "loc").await?;
    let xy = Differential::open(XY_SCHEMA, XY_INDEX, "Spot", "loc").await?;
    let geo_cases = vec![
        Case::unserved(q("Place", "distance(p.loc, point({x: 1.0, y: 2.0})) < 5.0")),
        Case::unserved(q(
            "Place",
            "point.withinBBox(p.loc, point({x: 0.0, y: 0.0}), point({x: 10.0, y: 10.0}))",
        )),
        Case::unserved(q("Place", "distance(p.loc, $c) < 5.0"))
            .param("c", value(&xy, "point({x: 1.0, y: 2.0})").await?),
    ];
    let xy_cases = vec![
        Case::unserved(q(
            "Spot",
            "distance(p.loc, point({latitude: 1.0, longitude: 2.0})) < 5.0",
        )),
        Case::unserved(q(
            "Spot",
            "distance(p.loc, point({x: 1.0, y: 2.0, z: 0.0})) < 5.0",
        )),
        Case::unserved(q(
            "Spot",
            "point.withinBBox(p.loc, point({latitude: 0.0, longitude: 0.0}), point({latitude: 10.0, longitude: 10.0}))",
        )),
        // A map that is not a point: no CRS, so `distance` raises.
        Case::unserved(q("Spot", "distance(p.loc, $c) < 5.0")).param(
            "c",
            Value::Map(
                [
                    ("x".to_string(), Value::Float(1.0)),
                    ("y".to_string(), Value::Float(2.0)),
                ]
                .into(),
            ),
        ),
    ];
    for (diff, writes, cases) in [
        (&geo, geo_writes(), &geo_cases),
        (&xy, xy_writes(), &xy_cases),
    ] {
        for stmt in &writes.initial {
            diff.write(stmt).await?;
        }
        same_outcome(diff, "unflushed", cases).await?;
        diff.flush().await?;
        same_outcome(diff, "flushed", cases).await?;
    }
    Ok(())
}

/// A point of another CRS written into the column is stored NULL (a known
/// engine bug) or, a 3-D point in a 2-D column, with its own CRS name. Its
/// key is NULL, so it always reaches the predicate, which raises as it does
/// without the index.
#[tokio::test]
async fn point_differential_other_crs_rows_raise_alike() -> Result<()> {
    let xy = Differential::open(XY_SCHEMA, XY_INDEX, "Spot", "loc").await?;
    for stmt in &xy_writes().initial {
        xy.write(stmt).await?;
    }
    xy.flush().await?;
    xy.write("CREATE (:Spot {id: 40, loc: point({x: 900.0, y: 900.0, z: 1.0})})")
        .await?;
    let cases = vec![
        Case::unserved(q("Spot", "distance(p.loc, point({x: 0.0, y: 0.0})) < 5.0")),
        Case::unserved(q(
            "Spot",
            "point.withinBBox(p.loc, point({x: 0.0, y: 0.0}), point({x: 10.0, y: 10.0}))",
        )),
    ];
    // Served or not, the outcome must be the plain one; compare outcomes
    // only (an erroring query has no EXPLAIN obligation).
    let check = |stage: &'static str| {
        let cases = cases.clone();
        let xy = &xy;
        async move {
            for case in &cases {
                let plain = rows(&xy.plain, case).await.map_err(|e| format!("{e:#}"));
                let indexed = rows(&xy.indexed, case).await.map_err(|e| format!("{e:#}"));
                assert_eq!(indexed, plain, "{stage}: outcomes differ for {case:?}");
            }
            anyhow::Ok(())
        }
    };
    check("3-D row unflushed").await?;
    xy.flush().await?;
    check("3-D row flushed").await?;

    let geo = Differential::open(GEO_SCHEMA, GEO_INDEX, "Place", "loc").await?;
    for stmt in &geo_writes().initial {
        geo.write(stmt).await?;
    }
    geo.flush().await?;
    geo.write("CREATE (:Place {id: 40, loc: point({x: 1.0, y: 2.0})})")
        .await?;
    geo.flush().await?;
    let london = geo_point(51.5074, -0.1278);
    for case in [
        Case::unserved(q("Place", &format!("distance(p.loc, {london}) < 400000.0"))),
        Case::unserved(q(
            "Place",
            &format!(
                "point.withinBBox(p.loc, {}, {})",
                geo_point(40.0, -10.0),
                geo_point(60.0, 10.0)
            ),
        )),
    ] {
        let plain = rows(&geo.plain, &case).await.map_err(|e| format!("{e:#}"));
        let indexed = rows(&geo.indexed, &case)
            .await
            .map_err(|e| format!("{e:#}"));
        assert_eq!(
            indexed, plain,
            "a cartesian row in a geographic column: {case:?}"
        );
    }
    Ok(())
}
