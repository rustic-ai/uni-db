// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use uni_query::query::expr_eval::eval_scalar_function;

#[test]
fn test_point_geographic() {
    let arg = json!({"latitude": 12.9716, "longitude": 77.5946});
    let res = eval_scalar_function("POINT", &[arg]).unwrap();

    assert_eq!(res["crs"], "WGS84");
    assert_eq!(res["latitude"], 12.9716);
    assert_eq!(res["longitude"], 77.5946);
}

#[test]
fn test_point_cartesian() {
    let arg = json!({"x": 10.0, "y": 20.0});
    let res = eval_scalar_function("POINT", &[arg]).unwrap();

    assert_eq!(res["crs"], "Cartesian");
    assert_eq!(res["x"], 10.0);
    assert_eq!(res["y"], 20.0);
}

#[test]
fn test_distance_cartesian() {
    let p1 = json!({"crs": "Cartesian", "x": 0.0, "y": 0.0});
    let p2 = json!({"crs": "Cartesian", "x": 3.0, "y": 4.0});

    let res = eval_scalar_function("DISTANCE", &[p1, p2]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 5.0);
}

#[test]
fn test_distance_geographic() {
    // Bangalore to Mumbai (~840km)
    let p1 = json!({"crs": "WGS84", "latitude": 12.9716, "longitude": 77.5946});
    let p2 = json!({"crs": "WGS84", "latitude": 19.0760, "longitude": 72.8777});

    let res = eval_scalar_function("DISTANCE", &[p1, p2]).unwrap();
    let distance_km = res.as_f64().unwrap() / 1000.0;

    assert!(distance_km > 800.0 && distance_km < 900.0);
}

#[test]
fn test_within_bbox_cartesian() {
    let point = json!({"crs": "Cartesian", "x": 5.0, "y": 5.0});
    let lower_left = json!({"crs": "Cartesian", "x": 0.0, "y": 0.0});
    let upper_right = json!({"crs": "Cartesian", "x": 10.0, "y": 10.0});

    // Point inside bbox
    let res = eval_scalar_function(
        "POINT.WITHINBBOX",
        &[point.clone(), lower_left.clone(), upper_right.clone()],
    )
    .unwrap();
    assert!(res.as_bool().unwrap());

    // Point outside bbox
    let outside = json!({"crs": "Cartesian", "x": 15.0, "y": 5.0});
    let res =
        eval_scalar_function("POINT.WITHINBBOX", &[outside, lower_left, upper_right]).unwrap();
    assert!(!res.as_bool().unwrap());
}

#[test]
fn test_within_bbox_geographic() {
    // Point in San Francisco
    let sf = json!({"crs": "WGS84", "latitude": 37.7749, "longitude": -122.4194});
    // Bounding box covering California
    let ll = json!({"crs": "WGS84", "latitude": 32.0, "longitude": -125.0});
    let ur = json!({"crs": "WGS84", "latitude": 42.0, "longitude": -114.0});

    let res = eval_scalar_function("POINT.WITHINBBOX", &[sf, ll.clone(), ur.clone()]).unwrap();
    assert!(res.as_bool().unwrap());

    // Point in New York (outside California bbox)
    let ny = json!({"crs": "WGS84", "latitude": 40.7128, "longitude": -74.0060});
    let res = eval_scalar_function("POINT.WITHINBBOX", &[ny, ll, ur]).unwrap();
    assert!(!res.as_bool().unwrap());
}
