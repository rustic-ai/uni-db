// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use serde_json::{Value, json};

const EARTH_RADIUS_KM: f64 = 6371.0;

pub fn eval_spatial_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "POINT" => eval_point(args),
        "DISTANCE" => eval_distance(args),
        "POINT.WITHINBBOX" => eval_within_bbox(args),
        _ => Err(anyhow!("Unknown spatial function: {}", name)),
    }
}

fn eval_point(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("point() requires exactly 1 map argument"));
    }

    let map = args[0]
        .as_object()
        .ok_or_else(|| anyhow!("point() requires a map argument"))?;

    // Geographic point: {latitude, longitude}
    if map.contains_key("latitude") && map.contains_key("longitude") {
        let lat = map["latitude"]
            .as_f64()
            .ok_or_else(|| anyhow!("latitude must be a number"))?;
        let lon = map["longitude"]
            .as_f64()
            .ok_or_else(|| anyhow!("longitude must be a number"))?;

        if !(-90.0..=90.0).contains(&lat) {
            return Err(anyhow!("latitude must be between -90 and 90"));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(anyhow!("longitude must be between -180 and 180"));
        }

        return Ok(json!({
            "type": "Point",
            "crs": "WGS84",
            "latitude": lat,
            "longitude": lon
        }));
    }

    // Cartesian point: {x, y} or {x, y, z}
    if map.contains_key("x") && map.contains_key("y") {
        let x = map["x"]
            .as_f64()
            .ok_or_else(|| anyhow!("x must be a number"))?;
        let y = map["y"]
            .as_f64()
            .ok_or_else(|| anyhow!("y must be a number"))?;
        let z = map.get("z").and_then(|v| v.as_f64());

        return Ok(json!({
            "type": "Point",
            "crs": if z.is_some() { "Cartesian-3D" } else { "Cartesian" },
            "x": x,
            "y": y,
            "z": z
        }));
    }

    Err(anyhow!(
        "point() requires either {{latitude, longitude}} or {{x, y}}"
    ))
}

fn eval_distance(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("distance() requires exactly 2 point arguments"));
    }

    let p1 = args[0]
        .as_object()
        .ok_or_else(|| anyhow!("First argument must be a point"))?;
    let p2 = args[1]
        .as_object()
        .ok_or_else(|| anyhow!("Second argument must be a point"))?;

    let crs1 = p1.get("crs").and_then(|v| v.as_str()).unwrap_or("");
    let crs2 = p2.get("crs").and_then(|v| v.as_str()).unwrap_or("");

    if crs1 != crs2 {
        return Err(anyhow!(
            "Cannot compute distance between points with different CRS"
        ));
    }

    match crs1 {
        "WGS84" => {
            let (lat1, lon1) = get_geo_coords(p1, "First point")?;
            let (lat2, lon2) = get_geo_coords(p2, "Second point")?;
            let (lat1, lon1) = (lat1.to_radians(), lon1.to_radians());
            let (lat2, lon2) = (lat2.to_radians(), lon2.to_radians());

            let dlat = lat2 - lat1;
            let dlon = lon2 - lon1;

            let a =
                (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
            let c = 2.0 * a.sqrt().asin();

            Ok(json!(EARTH_RADIUS_KM * c * 1000.0)) // Return meters
        }
        "Cartesian" => {
            let (x1, y1) = get_cartesian_coords(p1, "First point")?;
            let (x2, y2) = get_cartesian_coords(p2, "Second point")?;

            Ok(json!(((x2 - x1).powi(2) + (y2 - y1).powi(2)).sqrt()))
        }
        "Cartesian-3D" => {
            let (x1, y1) = get_cartesian_coords(p1, "First point")?;
            let (x2, y2) = get_cartesian_coords(p2, "Second point")?;
            let z1 = p1["z"].as_f64().unwrap_or(0.0);
            let z2 = p2["z"].as_f64().unwrap_or(0.0);

            Ok(json!(
                ((x2 - x1).powi(2) + (y2 - y1).powi(2) + (z2 - z1).powi(2)).sqrt()
            ))
        }
        _ => Err(anyhow!("Unknown coordinate reference system: {}", crs1)),
    }
}

/// Extract geographic coordinates (latitude, longitude) from a point object.
fn get_geo_coords(point: &serde_json::Map<String, Value>, name: &str) -> Result<(f64, f64)> {
    let lat = point["latitude"]
        .as_f64()
        .ok_or_else(|| anyhow!("{} latitude must be a number", name))?;
    let lon = point["longitude"]
        .as_f64()
        .ok_or_else(|| anyhow!("{} longitude must be a number", name))?;
    Ok((lat, lon))
}

/// Extract Cartesian coordinates (x, y) from a point object.
fn get_cartesian_coords(point: &serde_json::Map<String, Value>, name: &str) -> Result<(f64, f64)> {
    let x = point["x"]
        .as_f64()
        .ok_or_else(|| anyhow!("{} x must be a number", name))?;
    let y = point["y"]
        .as_f64()
        .ok_or_else(|| anyhow!("{} y must be a number", name))?;
    Ok((x, y))
}

/// Check if a value is within a range (inclusive).
fn in_range(val: f64, min: f64, max: f64) -> bool {
    val >= min && val <= max
}

/// Check if a point is within a bounding box defined by lower-left and upper-right corners.
/// Usage: point.withinBBox(point, lowerLeft, upperRight)
fn eval_within_bbox(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(anyhow!(
            "point.withinBBox() requires 3 arguments: point, lowerLeft, upperRight"
        ));
    }

    let point = args[0]
        .as_object()
        .ok_or_else(|| anyhow!("First argument must be a point"))?;
    let lower_left = args[1]
        .as_object()
        .ok_or_else(|| anyhow!("Second argument must be a point (lower-left corner)"))?;
    let upper_right = args[2]
        .as_object()
        .ok_or_else(|| anyhow!("Third argument must be a point (upper-right corner)"))?;

    // For geographic points (WGS84)
    if point.contains_key("latitude") {
        let (lat, lon) = get_geo_coords(point, "Point")?;
        let (min_lat, min_lon) = get_geo_coords(lower_left, "Lower-left")?;
        let (max_lat, max_lon) = get_geo_coords(upper_right, "Upper-right")?;

        return Ok(json!(
            in_range(lat, min_lat, max_lat) && in_range(lon, min_lon, max_lon)
        ));
    }

    // For Cartesian points
    if point.contains_key("x") {
        let (x, y) = get_cartesian_coords(point, "Point")?;
        let (min_x, min_y) = get_cartesian_coords(lower_left, "Lower-left")?;
        let (max_x, max_y) = get_cartesian_coords(upper_right, "Upper-right")?;

        return Ok(json!(
            in_range(x, min_x, max_x) && in_range(y, min_y, max_y)
        ));
    }

    Err(anyhow!(
        "Point must have either latitude/longitude or x/y coordinates"
    ))
}
