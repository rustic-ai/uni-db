// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! MessagePack-based binary encoding for CypherValue (uni_common::Value).
//!
//! # Design
//!
//! All property values are stored as self-describing binary blobs in Arrow
//! `LargeBinary` columns. Each blob has the format:
//!
//! ```text
//! [tag_byte: u8][msgpack_payload: bytes]
//! ```
//!
//! The tag byte provides O(1) type identification without deserialization.
//! MessagePack preserves int/float distinction natively (unlike JSON).
//!
//! # Tag Constants
//!
//! | Tag | Type | Payload |
//! |-----|------|---------|
//! | 0 | Null | empty |
//! | 1 | Bool | msgpack bool |
//! | 2 | Int | msgpack i64 |
//! | 3 | Float | msgpack f64 |
//! | 4 | String | msgpack string |
//! | 5 | List | msgpack array of recursively-encoded blobs |
//! | 6 | Map | msgpack map of string → recursively-encoded blobs |
//! | 7 | Bytes | msgpack binary |
//! | 8 | Node | msgpack {vid, label, props} |
//! | 9 | Edge | msgpack {eid, type, src, dst, props} |
//! | 10 | Path | msgpack {nodes, rels} |
//! | 11 | Date | msgpack i32 (days since epoch) |
//! | 12 | Time | msgpack i64 (nanoseconds since midnight) |
//! | 13 | DateTime | msgpack i64 (nanoseconds since epoch) |
//! | 14 | Duration | msgpack {months, days, nanos} |
//! | 15 | Handle | 8-byte id of an interned value (see [`handle`]) |
//! | 16 | Vector | msgpack array of f32 |
//! | 17 | LocalTime | msgpack i64 (nanoseconds since midnight) |
//! | 18 | LocalDateTime | msgpack i64 (nanoseconds since epoch) |
//! | 19 | Btic | 24-byte packed BTIC (lo, hi, meta) |
//! | 20 | SparseVector | packed sparse-vector encoding (indices + weights) |
//! | 21 | BinaryVector | msgpack binary (packed `u8` lanes) |
//!
//! Nested values (List elements, Map values, Node/Edge properties) are
//! recursively encoded as `[tag][payload]` blobs.

use crate::api::error::UniError;
use crate::core::id::{Eid, Vid};
use crate::value::{Edge, Node, Path, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

// Tag constants
pub const TAG_NULL: u8 = 0;
pub const TAG_BOOL: u8 = 1;
pub const TAG_INT: u8 = 2;
pub const TAG_FLOAT: u8 = 3;
pub const TAG_STRING: u8 = 4;
pub const TAG_LIST: u8 = 5;
pub const TAG_MAP: u8 = 6;
pub const TAG_BYTES: u8 = 7;
pub const TAG_NODE: u8 = 8;
pub const TAG_EDGE: u8 = 9;
pub const TAG_PATH: u8 = 10;
pub const TAG_DATE: u8 = 11;
pub const TAG_TIME: u8 = 12;
pub const TAG_DATETIME: u8 = 13;
pub const TAG_DURATION: u8 = 14;
/// An interned value: an 8-byte id naming an entry in [`handle`]'s registry.
///
/// A `collect()` of *n* entities is one opaque blob, and every operator above
/// copies its input columns forward once per output row — so a traversal
/// re-materialises the whole list per fan-out row, `rows × list_size` bytes,
/// which is what OOM-kills LDBC IC3 and IC12 at SF1. A handle makes the copy
/// 9 bytes regardless of the list's size. See the module docs on [`handle`].
pub const TAG_HANDLE: u8 = 15;
pub const TAG_VECTOR: u8 = 16;
pub const TAG_LOCALTIME: u8 = 17;
pub const TAG_LOCALDATETIME: u8 = 18;
pub const TAG_BTIC: u8 = 19;
pub const TAG_SPARSE_VECTOR: u8 = 20;
pub const TAG_BINARY_VECTOR: u8 = 21;

pub mod handle {
    //! Interning for values whose encoded form is large and copied per row.
    //!
    //! [`super::TAG_HANDLE`] blobs carry an id into the registry here instead
    //! of the value's own bytes, so `arrow::compute::take` and
    //! `ScalarValue::to_array_of_size` copy 9 bytes per row rather than the
    //! whole encoding. [`super::decode`] resolves a handle transparently, which
    //! is why none of its ~45 call sites had to change.
    //!
    //! # Why the registry is global
    //!
    //! Resolution has to be ambient: `decode(&[u8])` cannot grow a registry
    //! argument without touching every call site. A thread-local will not do
    //! either — DataFusion encodes on the worker thread that ran the aggregate
    //! and decodes on whichever thread later evaluates a predicate, so a
    //! thread-local would strand the value and fail an otherwise sound query.
    //!
    //! Lifetime is bounded by [`HandleScope`], not by the process: the scope
    //! owns every id registered through it and removes them on drop. One scope
    //! lives for one query execution.
    //!
    //! # A handle must never escape
    //!
    //! It is meaningless in another process and dangling after its scope ends,
    //! so it must not reach durable storage, a plugin, or a returned result.
    //! Use [`materialize`] at those boundaries. Resolution fails loudly rather
    //! than yielding `Null`, so an escape is a visible error and never a
    //! silently wrong answer.

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, OnceLock, RwLock};

    use crate::value::Value;

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    fn registry() -> &'static RwLock<HashMap<u64, Arc<Value>>> {
        static REGISTRY: OnceLock<RwLock<HashMap<u64, Arc<Value>>>> = OnceLock::new();
        REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
    }

    /// Owns the ids registered through it and releases them when dropped.
    ///
    /// Hold one for the duration of a query execution. Dropping it is what
    /// keeps the global registry from growing without bound.
    #[derive(Debug, Default)]
    pub struct HandleScope {
        ids: RwLock<Vec<u64>>,
    }

    impl HandleScope {
        /// A scope owning no ids yet.
        pub fn new() -> Self {
            Self::default()
        }

        /// Intern `value`, returning the id that names it.
        pub fn register(&self, value: Arc<Value>) -> u64 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut reg) = registry().write() {
                reg.insert(id, value);
            }
            if let Ok(mut ids) = self.ids.write() {
                ids.push(id);
            }
            id
        }

        /// Ids this scope currently owns — for tests and diagnostics.
        pub fn len(&self) -> usize {
            self.ids.read().map(|i| i.len()).unwrap_or(0)
        }

        /// Whether this scope owns no ids.
        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }
    }

    impl Drop for HandleScope {
        fn drop(&mut self) {
            let ids = match self.ids.get_mut() {
                Ok(ids) => std::mem::take(ids),
                Err(poisoned) => std::mem::take(poisoned.into_inner()),
            };
            if ids.is_empty() {
                return;
            }
            if let Ok(mut reg) = registry().write() {
                for id in ids {
                    reg.remove(&id);
                }
            }
        }
    }

    /// The interned value for `id`, or `None` if no live scope owns it.
    pub fn resolve(id: u64) -> Option<Arc<Value>> {
        registry().read().ok()?.get(&id).cloned()
    }
}

// ---------------------------------------------------------------------------
// rmp_serde + UniError::Storage wrappers
// ---------------------------------------------------------------------------

/// Deserialize a MessagePack payload, wrapping any error in
/// `UniError::Storage` with a uniform `"failed to decode <type>: <e>"`
/// message. Used by every decode arm in this module.
fn decode_msgpack<'de, T: Deserialize<'de>>(
    payload: &'de [u8],
    type_name: &'static str,
) -> Result<T, UniError> {
    rmp_serde::from_slice(payload).map_err(|e| UniError::Storage {
        message: format!("failed to decode {type_name}: {e}"),
        source: None,
    })
}

/// Push `tag` onto `buf`, then append the MessagePack encoding of `value`.
/// Encoding into a `Vec<u8>` is infallible in practice; we keep the panic
/// path to match the historical contract.
fn encode_msgpack<T: Serialize>(buf: &mut Vec<u8>, tag: u8, value: &T, type_name: &'static str) {
    buf.push(tag);
    rmp_serde::encode::write(buf, value).unwrap_or_else(|_| panic!("{type_name} encode failed"));
}

/// Canonicalize a `(indices, values)` pair into a valid [`uni_sparse_vector::SparseVector`].
///
/// Defensive, infallible counterpart to ingest validation: sorts term ids, sums the
/// weights of duplicates, and drops non-finite weights (mirroring the auto-embed
/// canonicalizer) so the durable [`encode`] path can never panic on a value that
/// bypassed the executor's `coerce_and_validate_property_value` (issue #95). Mismatched
/// array lengths collapse to the shorter side rather than aborting the write.
fn canonical_sparse_vector(indices: &[u32], values: &[f32]) -> uni_sparse_vector::SparseVector {
    let pairs: Vec<(u32, f32)> = indices
        .iter()
        .copied()
        .zip(values.iter().copied())
        .filter(|&(_, w)| w.is_finite())
        .collect();
    // `from_pairs` over finite weights only re-errors if a duplicate-term summation
    // overflows to ±inf; fall back to the empty vector so encoding never panics.
    uni_sparse_vector::SparseVector::from_pairs(pairs).unwrap_or_else(|_| {
        uni_sparse_vector::SparseVector::new(Vec::new(), Vec::new())
            .expect("empty sparse vector is always valid")
    })
}

// ---------------------------------------------------------------------------
// Public encode/decode API
// ---------------------------------------------------------------------------

/// Encode a Value to tagged MessagePack bytes.
pub fn encode(value: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_to_buf(value, &mut buf);
    buf
}

/// Encode a reference to an interned value: `[TAG_HANDLE][id as 8 LE bytes]`.
///
/// Nine bytes regardless of how large the interned value is — that is the
/// entire point. See [`handle`] for the lifetime rules, and [`materialize`]
/// for the boundaries at which this must be turned back into real bytes.
pub fn encode_handle(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(TAG_HANDLE);
    buf.extend_from_slice(&id.to_le_bytes());
    buf
}

/// Whether `bytes` is a handle blob rather than a self-contained encoding.
pub fn is_handle(bytes: &[u8]) -> bool {
    bytes.first() == Some(&TAG_HANDLE)
}

/// Resolve any handle in `bytes` into a self-contained encoding.
///
/// Call this at every boundary a handle must not cross — durable storage, the
/// plugin/IPC surface, and returned results. Bytes that contain no handle are
/// returned unchanged, so this is safe to apply broadly.
///
/// # Errors
///
/// If a handle's scope has already ended. That is a bug in scoping rather than
/// bad input, and failing here is deliberate: the alternative is persisting an
/// id that means nothing.
pub fn materialize(bytes: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>, UniError> {
    if !is_handle(bytes) {
        return Ok(std::borrow::Cow::Borrowed(bytes));
    }
    Ok(std::borrow::Cow::Owned(encode(&decode(bytes)?)))
}

/// Decode tagged MessagePack bytes to a Value.
pub fn decode(bytes: &[u8]) -> Result<Value, UniError> {
    if bytes.is_empty() {
        return Err(UniError::Storage {
            message: "empty CypherValue bytes".to_string(),
            source: None,
        });
    }
    let tag = bytes[0];
    let payload = &bytes[1..];

    match tag {
        TAG_NULL => Ok(Value::Null),
        // Resolved through the registry rather than parsed. Fails loudly on a
        // dangling id: a handle that outlived its scope is a scoping bug, and
        // returning Null here would turn it into a wrong answer.
        TAG_HANDLE => {
            let id_bytes: [u8; 8] = payload.try_into().map_err(|_| UniError::Storage {
                message: format!("handle payload must be 8 bytes, got {}", payload.len()),
                source: None,
            })?;
            let id = u64::from_le_bytes(id_bytes);
            handle::resolve(id)
                .map(|v| (*v).clone())
                .ok_or_else(|| UniError::Storage {
                    message: format!(
                        "interned value {id} is no longer live — a handle escaped the query \
                         scope that owned it"
                    ),
                    source: None,
                })
        }
        TAG_BOOL => Ok(Value::Bool(decode_msgpack(payload, "bool")?)),
        TAG_INT => Ok(Value::Int(decode_msgpack(payload, "int")?)),
        TAG_FLOAT => Ok(Value::Float(decode_msgpack(payload, "float")?)),
        TAG_STRING => Ok(Value::String(decode_msgpack(payload, "string")?)),
        TAG_BYTES => Ok(Value::Bytes(decode_msgpack(payload, "bytes")?)),
        TAG_LIST => {
            let blobs: Vec<Vec<u8>> = decode_msgpack(payload, "list")?;
            let items: Result<Vec<Value>, UniError> = blobs.iter().map(|b| decode(b)).collect();
            Ok(Value::List(items?))
        }
        TAG_MAP => {
            let blob_map: HashMap<String, Vec<u8>> = decode_msgpack(payload, "map")?;
            let mut map = HashMap::new();
            for (k, v_blob) in blob_map {
                map.insert(k, decode(&v_blob)?);
            }
            Ok(Value::Map(map))
        }
        TAG_NODE => {
            let np: NodePayload = decode_msgpack(payload, "node")?;
            let mut props = HashMap::new();
            for (k, v_blob) in np.properties {
                props.insert(k, decode(&v_blob)?);
            }
            Ok(Value::Node(Node {
                vid: np.vid,
                labels: np.labels,
                properties: props,
            }))
        }
        TAG_EDGE => {
            let ep: EdgePayload = decode_msgpack(payload, "edge")?;
            let mut props = HashMap::new();
            for (k, v_blob) in ep.properties {
                props.insert(k, decode(&v_blob)?);
            }
            Ok(Value::Edge(Edge {
                eid: ep.eid,
                edge_type: ep.edge_type,
                src: ep.src,
                dst: ep.dst,
                properties: props,
            }))
        }
        TAG_PATH => {
            let pp: PathPayload = decode_msgpack(payload, "path")?;
            let nodes: Result<Vec<Node>, UniError> = pp
                .nodes
                .iter()
                .map(|b| match decode(b)? {
                    Value::Node(n) => Ok(n),
                    _ => Err(UniError::Storage {
                        message: "path node blob is not a Node".to_string(),
                        source: None,
                    }),
                })
                .collect();
            let edges: Result<Vec<Edge>, UniError> = pp
                .edges
                .iter()
                .map(|b| match decode(b)? {
                    Value::Edge(e) => Ok(e),
                    _ => Err(UniError::Storage {
                        message: "path edge blob is not an Edge".to_string(),
                        source: None,
                    }),
                })
                .collect();
            Ok(Value::Path(Path {
                nodes: nodes?,
                edges: edges?,
            }))
        }
        TAG_VECTOR => Ok(Value::Vector(decode_msgpack(payload, "vector")?)),
        TAG_BINARY_VECTOR => Ok(Value::BinaryVector(decode_msgpack(
            payload,
            "binary vector",
        )?)),
        TAG_DATE => Ok(Value::Temporal(crate::value::TemporalValue::Date {
            days_since_epoch: decode_msgpack(payload, "date")?,
        })),
        TAG_LOCALTIME => Ok(Value::Temporal(crate::value::TemporalValue::LocalTime {
            nanos_since_midnight: decode_msgpack(payload, "localtime")?,
        })),
        TAG_TIME => {
            let tp: TimePayload = decode_msgpack(payload, "time")?;
            Ok(Value::Temporal(crate::value::TemporalValue::Time {
                nanos_since_midnight: tp.nanos,
                offset_seconds: tp.offset,
            }))
        }
        TAG_LOCALDATETIME => Ok(Value::Temporal(
            crate::value::TemporalValue::LocalDateTime {
                nanos_since_epoch: decode_msgpack(payload, "localdatetime")?,
            },
        )),
        TAG_DATETIME => {
            let dp: DateTimePayload = decode_msgpack(payload, "datetime")?;
            Ok(Value::Temporal(crate::value::TemporalValue::DateTime {
                nanos_since_epoch: dp.nanos,
                offset_seconds: dp.offset,
                timezone_name: dp.tz_name,
            }))
        }
        TAG_DURATION => {
            let dp: DurationPayload = decode_msgpack(payload, "duration")?;
            Ok(Value::Temporal(crate::value::TemporalValue::Duration {
                months: dp.months,
                days: dp.days,
                nanos: dp.nanos,
            }))
        }
        TAG_BTIC => {
            let btic = uni_btic::encode::decode_slice(payload).map_err(|e| UniError::Storage {
                message: format!("failed to decode BTIC: {e}"),
                source: None,
            })?;
            Ok(Value::Temporal(crate::value::TemporalValue::Btic {
                lo: btic.lo(),
                hi: btic.hi(),
                meta: btic.meta(),
            }))
        }
        TAG_SPARSE_VECTOR => {
            let sv = uni_sparse_vector::encode::decode_slice(payload).map_err(|e| {
                UniError::Storage {
                    message: format!("failed to decode SparseVector: {e}"),
                    source: None,
                }
            })?;
            let (indices, values) = sv.into_parts();
            Ok(Value::SparseVector { indices, values })
        }
        _ => Err(UniError::Storage {
            message: format!("unknown CypherValue tag: {tag}"),
            source: None,
        }),
    }
}

// ---------------------------------------------------------------------------
// O(1) introspection API (no deserialization)
// ---------------------------------------------------------------------------

/// Peek at the tag byte without deserializing.
pub fn peek_tag(bytes: &[u8]) -> Option<u8> {
    bytes.first().copied()
}

/// Fast null check.
pub fn is_null(bytes: &[u8]) -> bool {
    peek_tag(bytes) == Some(TAG_NULL)
}

// ---------------------------------------------------------------------------
// Fast typed decode (skip Value construction)
// ---------------------------------------------------------------------------

/// Decode an int directly without constructing a Value.
pub fn decode_int(bytes: &[u8]) -> Option<i64> {
    if bytes.first().copied() != Some(TAG_INT) {
        return None;
    }
    rmp_serde::from_slice(&bytes[1..]).ok()
}

/// Decode a float directly without constructing a Value.
pub fn decode_float(bytes: &[u8]) -> Option<f64> {
    if bytes.first().copied() != Some(TAG_FLOAT) {
        return None;
    }
    rmp_serde::from_slice(&bytes[1..]).ok()
}

/// Decode a bool directly without constructing a Value.
pub fn decode_bool(bytes: &[u8]) -> Option<bool> {
    if bytes.first().copied() != Some(TAG_BOOL) {
        return None;
    }
    rmp_serde::from_slice(&bytes[1..]).ok()
}

/// Decode a string directly without constructing a Value.
pub fn decode_string(bytes: &[u8]) -> Option<String> {
    if bytes.first().copied() != Some(TAG_STRING) {
        return None;
    }
    rmp_serde::from_slice(&bytes[1..]).ok()
}

// ---------------------------------------------------------------------------
// Fast typed encode (skip Value construction)
// ---------------------------------------------------------------------------

/// Encode an int directly without constructing a Value.
pub fn encode_int(value: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(TAG_INT);
    rmp_serde::encode::write(&mut buf, &value).expect("int encode failed");
    buf
}

/// Encode a float directly without constructing a Value.
pub fn encode_float(value: f64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(TAG_FLOAT);
    rmp_serde::encode::write(&mut buf, &value).expect("float encode failed");
    buf
}

/// Encode a bool directly without constructing a Value.
pub fn encode_bool(value: bool) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(TAG_BOOL);
    rmp_serde::encode::write(&mut buf, &value).expect("bool encode failed");
    buf
}

/// Encode a string directly without constructing a Value.
pub fn encode_string(value: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(TAG_STRING);
    rmp_serde::encode::write(&mut buf, value).expect("string encode failed");
    buf
}

/// Encode null directly.
pub fn encode_null() -> Vec<u8> {
    vec![TAG_NULL]
}

/// Extract a map entry as raw bytes without decoding the entire map.
///
/// This is useful for extracting a single property from overflow JSON
/// without paying the cost of decoding all other properties.
///
/// Returns `None` if:
/// - The blob is not a TAG_MAP
/// - The key doesn't exist in the map
/// - Deserialization fails
pub fn extract_map_entry_raw(blob: &[u8], key: &str) -> Option<Vec<u8>> {
    if blob.first().copied() != Some(TAG_MAP) {
        return None;
    }
    let payload = &blob[1..];
    let blob_map: HashMap<String, Vec<u8>> = rmp_serde::from_slice(payload).ok()?;
    blob_map.get(key).cloned()
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn encode_to_buf(value: &Value, buf: &mut Vec<u8>) {
    match value {
        Value::Null => buf.push(TAG_NULL),
        Value::Bool(b) => encode_msgpack(buf, TAG_BOOL, b, "bool"),
        Value::Int(i) => encode_msgpack(buf, TAG_INT, i, "int"),
        Value::Float(f) => encode_msgpack(buf, TAG_FLOAT, f, "float"),
        Value::String(s) => encode_msgpack(buf, TAG_STRING, s, "string"),
        Value::Bytes(b) => encode_msgpack(buf, TAG_BYTES, b, "bytes"),
        Value::List(items) => {
            let blobs: Vec<Vec<u8>> = items.iter().map(encode).collect();
            encode_msgpack(buf, TAG_LIST, &blobs, "list");
        }
        Value::Map(map) => {
            let blob_map: BTreeMap<String, Vec<u8>> =
                map.iter().map(|(k, v)| (k.clone(), encode(v))).collect();
            encode_msgpack(buf, TAG_MAP, &blob_map, "map");
        }
        Value::Node(node) => {
            let mut props_blobs: Vec<(String, Vec<u8>)> = node
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), encode(v)))
                .collect();
            props_blobs.sort_by(|a, b| a.0.cmp(&b.0));
            let payload = NodePayload {
                vid: node.vid,
                labels: node.labels.clone(),
                properties: props_blobs,
            };
            encode_msgpack(buf, TAG_NODE, &payload, "node");
        }
        Value::Edge(edge) => {
            let mut props_blobs: Vec<(String, Vec<u8>)> = edge
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), encode(v)))
                .collect();
            props_blobs.sort_by(|a, b| a.0.cmp(&b.0));
            let payload = EdgePayload {
                eid: edge.eid,
                edge_type: edge.edge_type.clone(),
                src: edge.src,
                dst: edge.dst,
                properties: props_blobs,
            };
            encode_msgpack(buf, TAG_EDGE, &payload, "edge");
        }
        Value::Path(path) => {
            let payload = PathPayload {
                nodes: path
                    .nodes
                    .iter()
                    .map(|n| encode(&Value::Node(n.clone())))
                    .collect(),
                edges: path
                    .edges
                    .iter()
                    .map(|e| encode(&Value::Edge(e.clone())))
                    .collect(),
            };
            encode_msgpack(buf, TAG_PATH, &payload, "path");
        }
        Value::Vector(v) => encode_msgpack(buf, TAG_VECTOR, v, "vector"),
        Value::BinaryVector(b) => encode_msgpack(buf, TAG_BINARY_VECTOR, b, "binary vector"),
        Value::SparseVector { indices, values } => {
            buf.push(TAG_SPARSE_VECTOR);
            // `encode` is infallible and runs on the durable WAL path, so it must never
            // panic (M-PANIC-IS-STOP). User writes are canonicalized + validated at ingest
            // (`coerce_and_validate_property_value`), so on every normal path this is a
            // no-op re-canonicalization. A value that somehow arrives non-canonical here
            // (e.g. a direct Rust-API construction bypassing the executor) is sorted, its
            // duplicate term ids summed, and any non-finite weight dropped — matching the
            // auto-embed canonicalizer — instead of aborting the write.
            let sv = canonical_sparse_vector(indices, values);
            buf.extend_from_slice(&uni_sparse_vector::encode::encode(&sv));
        }
        Value::Temporal(t) => match t {
            crate::value::TemporalValue::Date { days_since_epoch } => {
                encode_msgpack(buf, TAG_DATE, days_since_epoch, "date");
            }
            crate::value::TemporalValue::LocalTime {
                nanos_since_midnight,
            } => encode_msgpack(buf, TAG_LOCALTIME, nanos_since_midnight, "localtime"),
            crate::value::TemporalValue::Time {
                nanos_since_midnight,
                offset_seconds,
            } => {
                let payload = TimePayload {
                    nanos: *nanos_since_midnight,
                    offset: *offset_seconds,
                };
                encode_msgpack(buf, TAG_TIME, &payload, "time");
            }
            crate::value::TemporalValue::LocalDateTime { nanos_since_epoch } => {
                encode_msgpack(buf, TAG_LOCALDATETIME, nanos_since_epoch, "localdatetime");
            }
            crate::value::TemporalValue::DateTime {
                nanos_since_epoch,
                offset_seconds,
                timezone_name,
            } => {
                let payload = DateTimePayload {
                    nanos: *nanos_since_epoch,
                    offset: *offset_seconds,
                    tz_name: timezone_name.clone(),
                };
                encode_msgpack(buf, TAG_DATETIME, &payload, "datetime");
            }
            crate::value::TemporalValue::Duration {
                months,
                days,
                nanos,
            } => {
                let payload = DurationPayload {
                    months: *months,
                    days: *days,
                    nanos: *nanos,
                };
                encode_msgpack(buf, TAG_DURATION, &payload, "duration");
            }
            crate::value::TemporalValue::Btic { lo, hi, meta } => {
                buf.push(TAG_BTIC);
                let btic = uni_btic::Btic::new(*lo, *hi, *meta).expect("invalid BTIC value");
                buf.extend_from_slice(&uni_btic::encode::encode(&btic));
            }
        },
    }
}

// ---------------------------------------------------------------------------
// Serde-compatible payload structs for complex types
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct NodePayload {
    vid: Vid,
    labels: Vec<String>,
    properties: Vec<(String, Vec<u8>)>,
}

#[derive(Serialize, Deserialize)]
struct EdgePayload {
    eid: Eid,
    edge_type: String,
    src: Vid,
    dst: Vid,
    properties: Vec<(String, Vec<u8>)>,
}

#[derive(Serialize, Deserialize)]
struct PathPayload {
    nodes: Vec<Vec<u8>>,
    edges: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct TimePayload {
    nanos: i64,
    offset: i32,
}

#[derive(Serialize, Deserialize)]
struct DateTimePayload {
    nanos: i64,
    offset: i32,
    tz_name: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct DurationPayload {
    months: i64,
    days: i64,
    nanos: i64,
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_null() {
        let v = Value::Null;
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_NULL);
        assert_eq!(bytes.len(), 1);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_bool() {
        for b in [true, false] {
            let v = Value::Bool(b);
            let bytes = encode(&v);
            assert_eq!(bytes[0], TAG_BOOL);
            let decoded = decode(&bytes).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn test_round_trip_int() {
        for i in [-100, 0, 42, i64::MAX, i64::MIN] {
            let v = Value::Int(i);
            let bytes = encode(&v);
            assert_eq!(bytes[0], TAG_INT);
            let decoded = decode(&bytes).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn test_round_trip_float() {
        for f in [-3.15, 0.0, 42.5, f64::MAX, f64::MIN] {
            let v = Value::Float(f);
            let bytes = encode(&v);
            assert_eq!(bytes[0], TAG_FLOAT);
            let decoded = decode(&bytes).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn test_round_trip_string() {
        for s in ["", "hello", "unicode: 🦀"] {
            let v = Value::String(s.to_string());
            let bytes = encode(&v);
            assert_eq!(bytes[0], TAG_STRING);
            let decoded = decode(&bytes).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn test_round_trip_bytes() {
        let v = Value::Bytes(vec![1, 2, 3, 255]);
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_BYTES);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_list() {
        let v = Value::List(vec![
            Value::Int(1),
            Value::String("two".to_string()),
            Value::Float(3.0),
            Value::Null,
        ]);
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_LIST);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_nested_list() {
        let v = Value::List(vec![
            Value::Int(1),
            Value::List(vec![
                Value::String("nested".to_string()),
                Value::List(vec![Value::Bool(true)]),
            ]),
        ]);
        let bytes = encode(&v);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_map() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::Int(1));
        map.insert("b".to_string(), Value::String("two".to_string()));
        map.insert("c".to_string(), Value::Null);
        let v = Value::Map(map);
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_MAP);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_node() {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        let v = Value::Node(Node {
            vid: Vid::from(123),
            labels: vec!["Person".to_string()],
            properties: props,
        });
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_NODE);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_edge() {
        let mut props = HashMap::new();
        props.insert("since".to_string(), Value::Int(2020));
        let v = Value::Edge(Edge {
            eid: Eid::from(456),
            edge_type: "KNOWS".to_string(),
            src: Vid::from(1),
            dst: Vid::from(2),
            properties: props,
        });
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_EDGE);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_path() {
        let v = Value::Path(Path {
            nodes: vec![Node {
                vid: Vid::from(1),
                labels: vec!["A".to_string()],
                properties: HashMap::new(),
            }],
            edges: vec![Edge {
                eid: Eid::from(1),
                edge_type: "REL".to_string(),
                src: Vid::from(1),
                dst: Vid::from(2),
                properties: HashMap::new(),
            }],
        });
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_PATH);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_vector() {
        let v = Value::Vector(vec![0.1, 0.2, 0.3]);
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_VECTOR);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_binary_vector() {
        let v = Value::BinaryVector(vec![0x00, 0xFF, 0xA5, 0x3C]);
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_BINARY_VECTOR);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_sparse_vector() {
        let v = Value::SparseVector {
            indices: vec![1, 7, 42],
            values: vec![0.25, -1.5, 3.0],
        };
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_SPARSE_VECTOR);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn encode_canonicalizes_non_canonical_sparse_without_panicking() {
        // Regression for issue #95: a `Value::SparseVector` with unsorted/duplicate
        // term ids or a non-finite weight previously `.expect()`-panicked here on the
        // durable WAL path. Encoding must now canonicalize defensively and never panic.
        // Unsorted + duplicate term ids are sorted and summed.
        let v = Value::SparseVector {
            indices: vec![9, 1, 9],
            values: vec![1.0, 2.0, 0.5],
        };
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_SPARSE_VECTOR);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(
            decoded,
            Value::SparseVector {
                indices: vec![1, 9],
                values: vec![2.0, 1.5],
            }
        );

        // A NaN / ±inf weight is dropped rather than panicking.
        let v = Value::SparseVector {
            indices: vec![1, 5],
            values: vec![f32::NAN, 2.0],
        };
        let bytes = encode(&v);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(
            decoded,
            Value::SparseVector {
                indices: vec![5],
                values: vec![2.0],
            }
        );

        // A length mismatch collapses to the shorter side instead of aborting.
        let v = Value::SparseVector {
            indices: vec![1, 2, 3],
            values: vec![1.0],
        };
        let _ = encode(&v); // must not panic
    }

    #[test]
    fn test_round_trip_sparse_vector_empty() {
        let v = Value::SparseVector {
            indices: vec![],
            values: vec![],
        };
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_SPARSE_VECTOR);
        assert_eq!(decode(&bytes).unwrap(), v);
    }

    #[test]
    fn test_round_trip_sparse_vector_nested_in_map() {
        // Nested-in-Map exercises the CV path used for non-declared/nested
        // sparse values (the tag framing must survive map recursion).
        let mut m = std::collections::HashMap::new();
        m.insert(
            "emb".to_string(),
            Value::SparseVector {
                indices: vec![3, 9],
                values: vec![1.0, 2.0],
            },
        );
        let v = Value::Map(m);
        let bytes = encode(&v);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_peek_tag() {
        assert_eq!(peek_tag(&encode(&Value::Null)), Some(TAG_NULL));
        assert_eq!(peek_tag(&encode(&Value::Bool(true))), Some(TAG_BOOL));
        assert_eq!(peek_tag(&encode(&Value::Int(42))), Some(TAG_INT));
        assert_eq!(peek_tag(&encode(&Value::Float(3.15))), Some(TAG_FLOAT));
        assert_eq!(
            peek_tag(&encode(&Value::String("x".to_string()))),
            Some(TAG_STRING)
        );
        assert_eq!(peek_tag(&[]), None);
    }

    #[test]
    fn test_is_null() {
        assert!(is_null(&encode(&Value::Null)));
        assert!(!is_null(&encode(&Value::Int(0))));
        assert!(!is_null(&[]));
    }

    #[test]
    fn test_fast_decode_int() {
        let bytes = encode(&Value::Int(42));
        assert_eq!(decode_int(&bytes), Some(42));
        assert_eq!(decode_int(&encode(&Value::Float(42.0))), None);
        assert_eq!(decode_int(&encode(&Value::String("42".to_string()))), None);
    }

    #[test]
    fn test_fast_decode_float() {
        let bytes = encode(&Value::Float(3.15));
        assert_eq!(decode_float(&bytes), Some(3.15));
        assert_eq!(decode_float(&encode(&Value::Int(3))), None);
    }

    #[test]
    fn test_fast_decode_bool() {
        let bytes = encode(&Value::Bool(true));
        assert_eq!(decode_bool(&bytes), Some(true));
        assert_eq!(decode_bool(&encode(&Value::Int(1))), None);
    }

    #[test]
    fn test_fast_decode_string() {
        let bytes = encode(&Value::String("hello".to_string()));
        assert_eq!(decode_string(&bytes), Some("hello".to_string()));
        assert_eq!(decode_string(&encode(&Value::Int(42))), None);
    }

    #[test]
    fn test_int_float_distinction() {
        // This is the key win: JSON loses the int/float distinction
        let int_val = Value::Int(42);
        let float_val = Value::Float(42.0);

        let int_bytes = encode(&int_val);
        let float_bytes = encode(&float_val);

        // Different tags
        assert_eq!(int_bytes[0], TAG_INT);
        assert_eq!(float_bytes[0], TAG_FLOAT);

        // Different payloads
        assert_ne!(int_bytes, float_bytes);

        // Decode preserves distinction
        assert_eq!(decode(&int_bytes).unwrap(), Value::Int(42));
        assert_eq!(decode(&float_bytes).unwrap(), Value::Float(42.0));
    }

    #[test]
    fn test_round_trip_btic_epoch_instant() {
        let v = Value::Temporal(crate::value::TemporalValue::Btic {
            lo: 0,
            hi: 1,
            meta: 0x0000_0000_0000_0000,
        });
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_BTIC);
        assert_eq!(bytes.len(), 25); // 1 tag + 24 packed
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_btic_year_1985() {
        let meta = 0x7700_0000_0000_0000u64; // year/year, definite/definite
        let v = Value::Temporal(crate::value::TemporalValue::Btic {
            lo: 473_385_600_000,
            hi: 504_921_600_000,
            meta,
        });
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_BTIC);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_btic_unbounded() {
        let v = Value::Temporal(crate::value::TemporalValue::Btic {
            lo: i64::MIN,
            hi: i64::MAX,
            meta: 0,
        });
        let bytes = encode(&v);
        assert_eq!(bytes[0], TAG_BTIC);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn test_round_trip_btic_with_certainty() {
        // approximate certainty on both bounds
        let meta = 0x7750_0000_0000_0000u64; // year/year, approximate/approximate
        let v = Value::Temporal(crate::value::TemporalValue::Btic {
            lo: -77_914_137_600_000, // 500 BCE
            hi: -77_882_601_600_000,
            meta,
        });
        let bytes = encode(&v);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }
}

#[cfg(test)]
mod handle_tests {
    use super::handle::{HandleScope, resolve};
    use super::*;
    use std::sync::Arc;

    fn list_of(n: usize) -> Value {
        Value::List((0..n).map(|i| Value::Int(i as i64)).collect())
    }

    #[test]
    fn a_handle_round_trips_to_the_interned_value() {
        let scope = HandleScope::new();
        let v = list_of(64);
        let id = scope.register(Arc::new(v.clone()));
        assert_eq!(decode(&encode_handle(id)).unwrap(), v);
    }

    /// The reason for the whole design: the blob a row carries is 9 bytes
    /// however large the value is, so replicating it costs nothing.
    #[test]
    fn a_handle_blob_does_not_grow_with_the_value() {
        let scope = HandleScope::new();
        let small = scope.register(Arc::new(list_of(1)));
        let large = scope.register(Arc::new(list_of(10_000)));
        assert_eq!(encode_handle(small).len(), 9);
        assert_eq!(encode_handle(large).len(), 9);
        assert!(
            encode(&list_of(10_000)).len() > 10_000,
            "the inline encoding is what we are avoiding"
        );
    }

    #[test]
    fn a_handle_nested_in_a_list_or_map_resolves() {
        let scope = HandleScope::new();
        let inner = Value::String("interned".to_string());
        let id = scope.register(Arc::new(inner.clone()));

        // Hand-build the nesting the way encode_to_buf would, with the handle
        // blob in place of a self-contained element encoding.
        let nested_list: Vec<Vec<u8>> = vec![encode(&Value::Int(1)), encode_handle(id)];
        let mut buf = vec![TAG_LIST];
        buf.extend_from_slice(&rmp_serde::to_vec(&nested_list).unwrap());
        assert_eq!(
            decode(&buf).unwrap(),
            Value::List(vec![Value::Int(1), inner])
        );
    }

    /// A dangling handle must be a loud error. Returning Null would convert a
    /// scoping bug into a silently wrong answer.
    #[test]
    fn a_handle_whose_scope_ended_fails_loudly() {
        let id = {
            let scope = HandleScope::new();
            scope.register(Arc::new(list_of(8)))
        };
        assert!(resolve(id).is_none(), "drop must release the id");
        let err = decode(&encode_handle(id)).expect_err("must not resolve");
        assert!(
            err.to_string().contains("no longer live"),
            "unhelpful error: {err}"
        );
    }

    #[test]
    fn scopes_do_not_release_each_others_ids() {
        let outer = HandleScope::new();
        let kept = outer.register(Arc::new(Value::Int(7)));
        {
            let inner = HandleScope::new();
            inner.register(Arc::new(Value::Int(8)));
            assert_eq!(inner.len(), 1);
        }
        assert!(resolve(kept).is_some(), "the outer scope still owns its id");
    }

    /// DataFusion encodes on the worker thread that ran the aggregate and
    /// decodes on whichever thread evaluates the predicate later. A
    /// thread-local registry would strand the value; this test is what pins
    /// that choice.
    #[test]
    fn a_handle_registered_on_one_thread_resolves_on_another() {
        let scope = HandleScope::new();
        let v = list_of(32);
        let id = scope.register(Arc::new(v.clone()));
        let decoded = std::thread::spawn(move || decode(&encode_handle(id)).unwrap())
            .join()
            .expect("decoder thread panicked");
        assert_eq!(decoded, v);
    }

    #[test]
    fn materialize_replaces_a_handle_and_leaves_other_bytes_alone() {
        let scope = HandleScope::new();
        let v = list_of(16);
        let id = scope.register(Arc::new(v.clone()));

        let blob = encode_handle(id);
        let materialized = materialize(&blob).unwrap();
        assert!(!is_handle(&materialized));
        assert_eq!(decode(&materialized).unwrap(), v);

        // Self-contained bytes are returned untouched, so applying this
        // broadly at a boundary costs nothing.
        let plain = encode(&Value::Int(3));
        assert!(matches!(
            materialize(&plain).unwrap(),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    /// Materialized bytes must survive their scope — that is what makes them
    /// safe to persist.
    #[test]
    fn materialized_bytes_outlive_the_scope() {
        let v = list_of(24);
        let bytes = {
            let scope = HandleScope::new();
            let id = scope.register(Arc::new(v.clone()));
            materialize(&encode_handle(id)).unwrap().into_owned()
        };
        assert_eq!(decode(&bytes).unwrap(), v);
    }
}
