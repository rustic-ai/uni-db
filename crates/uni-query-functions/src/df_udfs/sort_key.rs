// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Order-preserving binary sort-key encoding for Cypher values.
//!
//! [`encode_cypher_sort_key`] turns a [`uni_common::Value`] into a byte
//! string whose lexicographic (memcmp) order matches Cypher's ORDER BY
//! semantics — cross-type ordering by the leading type rank, then the
//! type's own payload encoding. The rest of the module is that payload
//! encoding, one helper per value shape.

use super::*;

/// Encode a Cypher value into an order-preserving binary sort key.
///
/// The resulting byte sequence has the property that lexicographic (memcmp)
/// comparison of two keys produces the same ordering as Cypher's ORDER BY
/// semantics, including cross-type ordering and within-type comparisons.
pub fn encode_cypher_sort_key(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    encode_sort_key_to_buf(value, &mut buf);
    buf
}

/// Recursive sort key encoder.
fn encode_sort_key_to_buf(value: &Value, buf: &mut Vec<u8>) {
    // Check for map-encoded temporals, nodes, edges, paths first
    if let Value::Map(map) = value {
        if let Some(tv) = sort_key_map_as_temporal(map) {
            buf.push(0x07); // Temporal rank
            encode_temporal_payload(&tv, buf);
            return;
        }
        let rank = sort_key_map_rank(map);
        if rank != 0 {
            // Node, Edge, or Path encoded as map
            buf.push(rank);
            match rank {
                0x01 => encode_map_as_node_payload(map, buf),
                0x02 => encode_map_as_edge_payload(map, buf),
                0x04 => encode_map_as_path_payload(map, buf),
                _ => {} // shouldn't happen
            }
            return;
        }
    }

    // Check for temporal strings
    if let Value::String(s) = value {
        if let Some(tv) = sort_key_string_as_temporal(s) {
            buf.push(0x07); // Temporal rank
            encode_temporal_payload(&tv, buf);
            return;
        }
        // Wide temporal: out-of-range dates that eval_datetime_function couldn't fit in i64 nanos.
        // Parse directly with chrono and encode with i128 nanos for correct ordering.
        if let Some(temporal_type) = crate::datetime::classify_temporal(s) {
            buf.push(0x07); // Temporal rank
            if encode_wide_temporal_sort_key(s, temporal_type, buf) {
                return;
            }
            // If wide parse failed, remove the temporal rank byte we just pushed
            buf.pop();
        }
    }

    let rank = sort_key_type_rank(value);
    buf.push(rank);

    match value {
        Value::Null => {}                   // rank byte 0x0A is sufficient
        Value::Float(f) if f.is_nan() => {} // rank byte 0x09 is sufficient
        Value::Bool(b) => buf.push(if *b { 0x01 } else { 0x00 }),
        Value::Int(i) => {
            // f64 bucket (coarse position, shared with Float so the two
            // interleave) plus the exact offset of the true integer from that
            // bucket, which distinguishes i64 values above 2^53 that a bare
            // `i as f64` cast would collapse to identical bytes.
            let primary = *i as f64;
            let int_delta = (*i as i128 - primary as i128) as i64;
            encode_numeric_payload(primary, int_delta, buf);
        }
        Value::Float(f) => {
            // Delta 0: an integer exactly representable as this float (incl. all
            // |i| < 2^53) also yields delta 0, so Int(n) and Float(n.0) produce
            // byte-identical keys (Cypher `n = n.0`, load-bearing for join-key
            // unification). The constant tie-break must be present so a Float key
            // is never a byte-prefix of the matching Int key.
            encode_numeric_payload(*f, 0, buf);
        }
        Value::String(s) => {
            byte_stuff_terminate(s.as_bytes(), buf);
        }
        Value::Temporal(tv) => {
            encode_temporal_payload(tv, buf);
        }
        Value::List(items) => {
            encode_list_payload(items, buf);
        }
        Value::Map(map) => {
            encode_map_payload(map, buf);
        }
        Value::Node(node) => {
            encode_node_payload(node, buf);
        }
        Value::Edge(edge) => {
            encode_edge_payload(edge, buf);
        }
        Value::Path(path) => {
            encode_path_payload(path, buf);
        }
        // Bytes and Vector get rank 0x0B - just encode raw bytes
        Value::Bytes(b) => {
            byte_stuff_terminate(b, buf);
        }
        Value::Vector(v) => {
            for f in v {
                buf.extend_from_slice(&encode_order_preserving_f64(*f as f64));
            }
        }
        _ => {} // Future variants: rank byte is sufficient
    }
}

/// Type rank for sort key encoding.
///
/// Matches the fallback executor's `order_by_type_rank` at core.rs:401.
fn sort_key_type_rank(v: &Value) -> u8 {
    match v {
        Value::Map(map) => sort_key_map_rank(map),
        Value::Node(_) => 0x01,
        Value::Edge(_) => 0x02,
        Value::List(_) => 0x03,
        Value::Path(_) => 0x04,
        Value::String(_) => 0x05,
        Value::Bool(_) => 0x06,
        Value::Temporal(_) => 0x07,
        Value::Int(_) => 0x08,
        Value::Float(f) if f.is_nan() => 0x09,
        Value::Float(_) => 0x08,
        Value::Null => 0x0A,
        Value::Bytes(_) | Value::Vector(_) => 0x0B,
        _ => 0x0B, // Future variants
    }
}

/// Rank maps that represent other types (mirrors `map_order_rank` from core.rs:420).
fn sort_key_map_rank(map: &std::collections::HashMap<String, Value>) -> u8 {
    if sort_key_map_as_temporal(map).is_some() {
        0x07
    } else if map.contains_key("nodes")
        && (map.contains_key("relationships") || map.contains_key("edges"))
    {
        0x04 // Path
    } else if map.contains_key("_eid")
        || map.contains_key("_src")
        || map.contains_key("_dst")
        || map.contains_key("_type")
        || map.contains_key("_type_name")
    {
        0x02 // Edge
    } else if map.contains_key("_vid") || map.contains_key("_labels") || map.contains_key("_label")
    {
        0x01 // Node
    } else {
        0x00 // Regular map
    }
}

/// Try to interpret a map as a temporal value.
///
/// Delegates to the shared implementation in `expr_eval`.
fn sort_key_map_as_temporal(
    map: &std::collections::HashMap<String, Value>,
) -> Option<uni_common::TemporalValue> {
    crate::expr_eval::temporal_from_map_wrapper(map)
}

/// Try to parse a string as a temporal value.
///
/// Delegates to the shared implementation in `expr_eval`.
pub(super) fn sort_key_string_as_temporal(s: &str) -> Option<uni_common::TemporalValue> {
    crate::expr_eval::temporal_from_value(&Value::String(s.to_string()))
}

/// Encode a wide (out-of-range) temporal sort key directly from a formatted string.
///
/// When `eval_datetime_function` returns `Value::String` because the nanos don't fit in i64,
/// we parse the formatted string directly with chrono and encode the sort key using i128 nanos.
/// This is called from `encode_sort_key_to_buf` as a fallback when `sort_key_string_as_temporal`
/// returns None but `classify_temporal` recognizes the string.
fn encode_wide_temporal_sort_key(
    s: &str,
    temporal_type: uni_common::TemporalType,
    buf: &mut Vec<u8>,
) -> bool {
    match temporal_type {
        uni_common::TemporalType::LocalDateTime => {
            if let Some(ndt) = parse_naive_datetime(s) {
                buf.push(0x03); // LocalDateTime variant
                let wide_nanos = naive_datetime_to_wide_nanos(&ndt);
                buf.extend_from_slice(&encode_order_preserving_i128(wide_nanos));
                return true;
            }
            false
        }
        uni_common::TemporalType::DateTime => {
            // Strip optional [timezone] suffix
            let base = if let Some(bracket_pos) = s.find('[') {
                &s[..bracket_pos]
            } else {
                s
            };
            if let Ok(dt) = chrono::DateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S%.f%:z") {
                buf.push(0x04); // DateTime variant
                let utc = dt.naive_utc();
                let wide_nanos = naive_datetime_to_wide_nanos(&utc);
                buf.extend_from_slice(&encode_order_preserving_i128(wide_nanos));
                return true;
            }
            if let Ok(dt) = chrono::DateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S%:z") {
                buf.push(0x04); // DateTime variant
                let utc = dt.naive_utc();
                let wide_nanos = naive_datetime_to_wide_nanos(&utc);
                buf.extend_from_slice(&encode_order_preserving_i128(wide_nanos));
                return true;
            }
            false
        }
        uni_common::TemporalType::Date => {
            if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                && let Some(epoch) = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            {
                buf.push(0x00); // Date variant
                let days = nd.signed_duration_since(epoch).num_days() as i32;
                buf.extend_from_slice(&encode_order_preserving_i32(days));
                return true;
            }
            false
        }
        _ => false,
    }
}

/// Parse a naive datetime string in ISO format.
fn parse_naive_datetime(s: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())
}

/// Compute nanoseconds since Unix epoch as i128 for a NaiveDateTime.
/// This handles dates outside the i64 nanos range (~1677-2262).
fn naive_datetime_to_wide_nanos(ndt: &chrono::NaiveDateTime) -> i128 {
    let secs = ndt.and_utc().timestamp() as i128;
    let subsec_nanos = ndt.and_utc().timestamp_subsec_nanos() as i128;
    secs * 1_000_000_000 + subsec_nanos
}

/// Encode a map that looks like a node into the node sort key payload.
fn encode_map_as_node_payload(map: &std::collections::HashMap<String, Value>, buf: &mut Vec<u8>) {
    // Extract labels
    let mut labels: Vec<String> = Vec::new();
    if let Some(Value::List(lbls)) = map.get("_labels") {
        for l in lbls {
            if let Value::String(s) = l {
                labels.push(s.clone());
            }
        }
    } else if let Some(Value::String(lbl)) = map.get("_label") {
        labels.push(lbl.clone());
    }
    labels.sort();

    // Extract vid. Reading `_vid` as an integer specifically meant every other
    // spelling of the same id — `_id`, or the serde form `"Vid(7)"` — collapsed
    // to 0, so every such node compared equal and `ORDER BY n` returned them in
    // an arbitrary order that looked deterministic.
    let vid = match uni_common::value::entity_ref_from_map(map) {
        Some(uni_common::value::EntityRef::Vertex(vid)) => vid.as_u64(),
        _ => 0,
    };

    // Labels
    let labels_joined = labels.join("\x01");
    byte_stuff_terminate(labels_joined.as_bytes(), buf);

    // VID
    buf.extend_from_slice(&vid.to_be_bytes());

    // Properties (all keys except internal ones)
    let mut props: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for (k, v) in map {
        if !k.starts_with('_') {
            props.insert(k.clone(), v.clone());
        }
    }
    encode_map_payload(&props, buf);
}

/// Encode a map that looks like an edge into the edge sort key payload.
fn encode_map_as_edge_payload(map: &std::collections::HashMap<String, Value>, buf: &mut Vec<u8>) {
    // Through the accessor: reading `_type`/`_type_name` by hand meant a
    // numeric `_type` (how CREATE spells it) silently sorted under the empty
    // string, so every such edge tied.
    let edge_type = Value::Map(map.clone())
        .edge_type_ref()
        .map(|t| match t {
            uni_common::value::EdgeTypeRef::Name(n) => n,
            // No schema here, so an id cannot become a name. A U+0001 prefix
            // keeps distinct ids distinct and sorts them ahead of every real
            // name, instead of collapsing them all onto "".
            uni_common::value::EdgeTypeRef::Id(id) => format!("\u{1}{id}"),
        })
        .unwrap_or_default();

    byte_stuff_terminate(edge_type.as_bytes(), buf);

    let src = map.get("_src").and_then(|v| v.as_i64()).unwrap_or(0) as u64;
    let dst = map.get("_dst").and_then(|v| v.as_i64()).unwrap_or(0) as u64;
    let eid = match uni_common::value::entity_ref_from_map(map) {
        Some(uni_common::value::EntityRef::Edge(eid)) => eid.as_u64(),
        _ => 0,
    };

    buf.extend_from_slice(&src.to_be_bytes());
    buf.extend_from_slice(&dst.to_be_bytes());
    buf.extend_from_slice(&eid.to_be_bytes());

    // Properties (all keys except internal ones)
    let mut props: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for (k, v) in map {
        if !k.starts_with('_') {
            props.insert(k.clone(), v.clone());
        }
    }
    encode_map_payload(&props, buf);
}

/// Encode a map that looks like a path into the path sort key payload.
fn encode_map_as_path_payload(map: &std::collections::HashMap<String, Value>, buf: &mut Vec<u8>) {
    // Nodes
    if let Some(Value::List(nodes)) = map.get("nodes") {
        encode_list_payload(nodes, buf);
    } else {
        buf.push(0x00); // empty list terminator
    }
    // Edges/relationships
    let edges = map.get("relationships").or_else(|| map.get("edges"));
    if let Some(Value::List(edges)) = edges {
        encode_list_payload(edges, buf);
    } else {
        buf.push(0x00); // empty list terminator
    }
}

// ─── Encoding helpers ───────────────────────────────────────────────────

/// Order-preserving encoding of f64.
///
/// Transforms IEEE 754 bit pattern so that memcmp gives the correct
/// numeric order: -inf < negatives < -0 = +0 < positives < +inf < NaN.
pub(super) fn encode_order_preserving_f64(f: f64) -> [u8; 8] {
    let bits = f.to_bits();
    let encoded = if bits >> 63 == 1 {
        // Negative: flip all bits
        !bits
    } else {
        // Non-negative: flip sign bit only
        bits ^ (1u64 << 63)
    };
    encoded.to_be_bytes()
}

/// Order-preserving encoding of i64.
fn encode_order_preserving_i64(i: i64) -> [u8; 8] {
    // XOR with sign bit to flip ordering
    ((i as u64) ^ (1u64 << 63)).to_be_bytes()
}

/// Appends the unified numeric sort-key payload for rank `0x08` (Int and Float).
///
/// Emits an 8-byte order-preserving `primary` f64 bucket followed by an 8-byte
/// order-preserving `int_delta`. Because round-to-nearest is monotonic, values
/// in different buckets order correctly by `primary`; within one shared bucket
/// both operands measure `int_delta` from the same reference, so the tie-break
/// orders by true value — making the full i64 range exact while Int and Float
/// still interleave. Both arms MUST emit this identical 16-byte layout so that
/// `Int(n)` and `Float(n.0)` stay byte-identical (join-key equality).
fn encode_numeric_payload(primary: f64, int_delta: i64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&encode_order_preserving_f64(primary));
    buf.extend_from_slice(&encode_order_preserving_i64(int_delta));
}

/// Order-preserving encoding of i32.
fn encode_order_preserving_i32(i: i32) -> [u8; 4] {
    ((i as u32) ^ (1u32 << 31)).to_be_bytes()
}

/// Order-preserving encoding of i128.
fn encode_order_preserving_i128(i: i128) -> [u8; 16] {
    ((i as u128) ^ (1u128 << 127)).to_be_bytes()
}

/// Byte-stuff and terminate: every 0x00 in data becomes 0x00 0xFF,
/// then append 0x00 0x00 as terminator.
///
/// This preserves lexicographic order because 0x00 0xFF > 0x00 0x00.
fn byte_stuff_terminate(data: &[u8], buf: &mut Vec<u8>) {
    byte_stuff(data, buf);
    buf.push(0x00);
    buf.push(0x00);
}

/// Byte-stuff without terminator.
fn byte_stuff(data: &[u8], buf: &mut Vec<u8>) {
    for &b in data {
        buf.push(b);
        if b == 0x00 {
            buf.push(0xFF);
        }
    }
}

/// Encode a list payload: each element wrapped, then end marker.
///
/// Format: `[0x01, stuffed(encode(elem)), 0x00, 0x00]...` then `0x00` end marker.
/// Shorter list < longer list because 0x00 (end) < 0x01 (more elements).
fn encode_list_payload(items: &[Value], buf: &mut Vec<u8>) {
    for item in items {
        buf.push(0x01); // element marker
        let elem_key = encode_cypher_sort_key(item);
        byte_stuff_terminate(&elem_key, buf);
    }
    buf.push(0x00); // end marker
}

/// Encode a map payload: entries sorted by key, then end marker.
fn encode_map_payload(map: &std::collections::HashMap<String, Value>, buf: &mut Vec<u8>) {
    let mut pairs: Vec<(&String, &Value)> = map.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);

    for (key, value) in pairs {
        buf.push(0x01); // entry marker
        byte_stuff_terminate(key.as_bytes(), buf);
        let val_key = encode_cypher_sort_key(value);
        byte_stuff_terminate(&val_key, buf);
    }
    buf.push(0x00); // end marker
}

/// Encode node sort key payload.
///
/// Format: `stuffed(sorted_labels_joined_by_\x01), 0x00 0x00, vid_be, map_payload`
fn encode_node_payload(node: &uni_common::Node, buf: &mut Vec<u8>) {
    let mut labels = node.labels.clone();
    labels.sort();
    let labels_joined = labels.join("\x01");
    byte_stuff_terminate(labels_joined.as_bytes(), buf);

    buf.extend_from_slice(&node.vid.as_u64().to_be_bytes());

    encode_map_payload(&node.properties, buf);
}

/// Encode edge sort key payload.
///
/// Format: `stuffed(edge_type), 0x00 0x00, src_be, dst_be, eid_be, map_payload`
fn encode_edge_payload(edge: &uni_common::Edge, buf: &mut Vec<u8>) {
    byte_stuff_terminate(edge.edge_type.as_bytes(), buf);

    buf.extend_from_slice(&edge.src.as_u64().to_be_bytes());
    buf.extend_from_slice(&edge.dst.as_u64().to_be_bytes());
    buf.extend_from_slice(&edge.eid.as_u64().to_be_bytes());

    encode_map_payload(&edge.properties, buf);
}

/// Encode path sort key payload.
///
/// Nodes encoded as list of node sort keys, edges encoded as list of edge sort keys.
fn encode_path_payload(path: &uni_common::Path, buf: &mut Vec<u8>) {
    // Nodes as list
    for node in &path.nodes {
        buf.push(0x01); // element marker
        let mut node_key = Vec::new();
        node_key.push(0x01); // Node rank
        encode_node_payload(node, &mut node_key);
        byte_stuff_terminate(&node_key, buf);
    }
    buf.push(0x00); // end nodes list

    // Edges as list
    for edge in &path.edges {
        buf.push(0x01); // element marker
        let mut edge_key = Vec::new();
        edge_key.push(0x02); // Edge rank
        encode_edge_payload(edge, &mut edge_key);
        byte_stuff_terminate(&edge_key, buf);
    }
    buf.push(0x00); // end edges list
}

/// Encode temporal value payload.
fn encode_temporal_payload(tv: &uni_common::TemporalValue, buf: &mut Vec<u8>) {
    match tv {
        uni_common::TemporalValue::Date { days_since_epoch } => {
            buf.push(0x00); // variant rank: Date
            buf.extend_from_slice(&encode_order_preserving_i32(*days_since_epoch));
        }
        uni_common::TemporalValue::LocalTime {
            nanos_since_midnight,
        } => {
            buf.push(0x01); // variant rank: LocalTime
            buf.extend_from_slice(&encode_order_preserving_i64(*nanos_since_midnight));
        }
        uni_common::TemporalValue::Time {
            nanos_since_midnight,
            offset_seconds,
        } => {
            buf.push(0x02); // variant rank: Time
            let utc_nanos =
                *nanos_since_midnight as i128 - (*offset_seconds as i128) * 1_000_000_000;
            buf.extend_from_slice(&encode_order_preserving_i128(utc_nanos));
        }
        uni_common::TemporalValue::LocalDateTime { nanos_since_epoch } => {
            buf.push(0x03); // variant rank: LocalDateTime
            // Use i128 for consistent width with wide (out-of-range) temporal sort keys
            buf.extend_from_slice(&encode_order_preserving_i128(*nanos_since_epoch as i128));
        }
        uni_common::TemporalValue::DateTime {
            nanos_since_epoch, ..
        } => {
            buf.push(0x04); // variant rank: DateTime
            // Use i128 for consistent width with wide (out-of-range) temporal sort keys
            buf.extend_from_slice(&encode_order_preserving_i128(*nanos_since_epoch as i128));
        }
        uni_common::TemporalValue::Duration {
            months,
            days,
            nanos,
        } => {
            buf.push(0x05); // variant rank: Duration
            buf.extend_from_slice(&encode_order_preserving_i64(*months));
            buf.extend_from_slice(&encode_order_preserving_i64(*days));
            buf.extend_from_slice(&encode_order_preserving_i64(*nanos));
        }
        uni_common::TemporalValue::Btic { lo, hi, meta } => {
            buf.push(0x06); // variant rank: Btic
            // BTIC has its own order-preserving encoding via sign-flip + big-endian
            if let Ok(btic) = uni_btic::Btic::new(*lo, *hi, *meta) {
                buf.extend_from_slice(&uni_btic::encode::encode(&btic));
            } else {
                buf.extend_from_slice(&encode_order_preserving_i64(*lo));
                buf.extend_from_slice(&encode_order_preserving_i64(*hi));
            }
        }
    }
}
