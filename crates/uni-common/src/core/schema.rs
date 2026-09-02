// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::core::edge_type::{
    MAX_SCHEMA_TYPE_ID, VIRTUAL_EDGE_TYPE_ID_SENTINEL, VIRTUAL_EDGE_TYPE_ID_START,
    is_schemaless_edge_type, make_schemaless_id,
};
use crate::sync::{acquire_read, acquire_write};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

mod index_types;
pub use index_types::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum SchemaElementState {
    Active,
    Hidden {
        since: DateTime<Utc>,
        last_active_snapshot: String, // SnapshotId
    },
    Tombstone {
        since: DateTime<Utc>,
    },
}

use arrow_schema::{DataType as ArrowDataType, Field, Fields, TimeUnit};

/// Returns the canonical struct field definitions for DateTime encoding in Arrow.
///
/// DateTime is encoded as a 3-field struct to preserve timezone information:
/// - `nanos_since_epoch`: i64 nanoseconds since Unix epoch (UTC)
/// - `offset_seconds`: i32 seconds offset from UTC (e.g., +3600 for +01:00)
/// - `timezone_name`: Optional IANA timezone name (e.g., "America/New_York")
pub fn datetime_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new(
            "nanos_since_epoch",
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("offset_seconds", ArrowDataType::Int32, true),
        Field::new("timezone_name", ArrowDataType::Utf8, true),
    ])
}

/// Returns the canonical struct field definitions for Time encoding in Arrow.
///
/// Time is encoded as a 2-field struct to preserve timezone offset:
/// - `nanos_since_midnight`: i64 nanoseconds since midnight (0-86,399,999,999,999)
/// - `offset_seconds`: i32 seconds offset from UTC (e.g., +3600 for +01:00)
pub fn time_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new(
            "nanos_since_midnight",
            ArrowDataType::Time64(TimeUnit::Nanosecond),
            true,
        ),
        Field::new("offset_seconds", ArrowDataType::Int32, true),
    ])
}

/// Detects if an Arrow DataType is the canonical DateTime struct.
pub fn is_datetime_struct(arrow_dt: &ArrowDataType) -> bool {
    matches!(arrow_dt, ArrowDataType::Struct(fields) if *fields == datetime_struct_fields())
}

/// Detects if an Arrow DataType is the canonical Time struct.
pub fn is_time_struct(arrow_dt: &ArrowDataType) -> bool {
    matches!(arrow_dt, ArrowDataType::Struct(fields) if *fields == time_struct_fields())
}

/// The canonical Arrow struct fields for a `DataType::SparseVector` column:
/// `Struct { indices: List<UInt32>, values: List<Float32> }`. Two parallel
/// variable-length lists in one struct. Both lists are non-null — an empty
/// sparse vector stores as two empty lists, never null. The write and read
/// sides both route through this one definition so they cannot drift (the same
/// lockstep discipline as the temporal structs above).
pub fn sparse_vector_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new(
            "indices",
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::UInt32, true))),
            false,
        ),
        Field::new(
            "values",
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float32, true))),
            false,
        ),
    ])
}

/// Detects if an Arrow DataType is the canonical SparseVector struct.
pub fn is_sparse_vector_struct(arrow_dt: &ArrowDataType) -> bool {
    matches!(arrow_dt, ArrowDataType::Struct(fields) if *fields == sparse_vector_struct_fields())
}

/// Field metadata marking an Arrow `LargeBinary` field as a raw `DataType::Bytes`
/// value rather than a tagged CypherValue/Duration blob.
///
/// Stamped on the child field of `List(Bytes)` / `Map(_, Bytes)` container types so
/// the read path decodes each element verbatim instead of through the tagged codec
/// (which would read `byte[0]` as a type tag). CV-encoded containers carry no such
/// marker and keep the codec path. See the read-side honoring in `arrow_convert`.
pub fn raw_bytes_field_metadata() -> HashMap<String, String> {
    HashMap::from([("uni_raw_bytes".to_string(), "true".to_string())])
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum CrdtType {
    GCounter,
    GSet,
    ORSet,
    LWWRegister,
    LWWMap,
    Rga,
    VectorClock,
    VCRegister,
}

impl CrdtType {
    /// Returns the canonical variant name for this CRDT type.
    ///
    /// The returned strings must stay in sync with `uni_crdt::Crdt::type_name`,
    /// so a written CRDT value can be validated against its schema-declared
    /// variant (see uni-store's write-time CRDT enforcement).
    ///
    /// # Examples
    /// ```
    /// use uni_common::core::schema::CrdtType;
    /// assert_eq!(CrdtType::GCounter.type_name(), "GCounter");
    /// ```
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            CrdtType::GCounter => "GCounter",
            CrdtType::GSet => "GSet",
            CrdtType::ORSet => "ORSet",
            CrdtType::LWWRegister => "LWWRegister",
            CrdtType::LWWMap => "LWWMap",
            CrdtType::Rga => "Rga",
            CrdtType::VectorClock => "VectorClock",
            CrdtType::VCRegister => "VCRegister",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum PointType {
    Geographic,  // WGS84
    Cartesian2D, // x, y
    Cartesian3D, // x, y, z
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum DataType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Timestamp,
    Date,
    Time,
    DateTime,
    Duration,
    CypherValue,
    Bytes,
    Point(PointType),
    Vector {
        dimensions: usize,
    },
    /// Learned-sparse vector (SPLADE / BGE-M3). `dimensions` is the term-space
    /// cardinality (max term id + 1) used for validation and index config.
    SparseVector {
        dimensions: usize,
    },
    /// Binary/bit vector for Hamming/Jaccard similarity. `dimensions` is the
    /// number of `u8` lanes (each lane holds 8 bits), stored as
    /// `FixedSizeList<UInt8>`. Exact/brute-force only — Lance ANN over binary
    /// metrics is not wired, so a `BinaryVector` column builds no ANN index.
    BinaryVector {
        dimensions: usize,
    },
    Btic,
    Crdt(CrdtType),
    List(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
}

impl DataType {
    // Alias for compatibility/convenience if needed, but preferable to use exact types.
    #[allow(non_upper_case_globals)]
    pub const Float: DataType = DataType::Float64;
    #[allow(non_upper_case_globals)]
    pub const Int: DataType = DataType::Int64;

    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            DataType::String => ArrowDataType::Utf8,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Timestamp => {
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            DataType::Date => ArrowDataType::Date32,
            DataType::Time => ArrowDataType::Struct(time_struct_fields()),
            DataType::DateTime => ArrowDataType::Struct(datetime_struct_fields()),
            DataType::Duration => ArrowDataType::LargeBinary, // Lance doesn't support Interval(MonthDayNano); use CypherValue codec
            DataType::CypherValue => ArrowDataType::LargeBinary, // MessagePack-tagged binary encoding
            DataType::Bytes => ArrowDataType::LargeBinary, // raw byte buffer (no codec wrapping)
            DataType::Point(pt) => match pt {
                PointType::Geographic => ArrowDataType::Struct(Fields::from(vec![
                    Field::new("latitude", ArrowDataType::Float64, false),
                    Field::new("longitude", ArrowDataType::Float64, false),
                    Field::new("crs", ArrowDataType::Utf8, false),
                ])),
                PointType::Cartesian2D => ArrowDataType::Struct(Fields::from(vec![
                    Field::new("x", ArrowDataType::Float64, false),
                    Field::new("y", ArrowDataType::Float64, false),
                    Field::new("crs", ArrowDataType::Utf8, false),
                ])),
                PointType::Cartesian3D => ArrowDataType::Struct(Fields::from(vec![
                    Field::new("x", ArrowDataType::Float64, false),
                    Field::new("y", ArrowDataType::Float64, false),
                    Field::new("z", ArrowDataType::Float64, false),
                    Field::new("crs", ArrowDataType::Utf8, false),
                ])),
            },
            DataType::Vector { dimensions } => ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, true)),
                *dimensions as i32,
            ),
            DataType::SparseVector { .. } => ArrowDataType::Struct(sparse_vector_struct_fields()),
            DataType::BinaryVector { dimensions } => ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::UInt8, true)),
                *dimensions as i32,
            ),
            DataType::Btic => ArrowDataType::FixedSizeBinary(24),
            DataType::Crdt(_) => ArrowDataType::Binary, // Store CRDT as binary MessagePack
            DataType::List(inner) => {
                // A raw `Bytes` element maps to Arrow `LargeBinary`, indistinguishable
                // from a CV-encoded element by type alone; mark the child field so the
                // read path decodes it verbatim rather than through the tagged codec.
                let item = Field::new("item", inner.to_arrow(), true);
                let item = if matches!(**inner, DataType::Bytes) {
                    item.with_metadata(raw_bytes_field_metadata())
                } else {
                    item
                };
                ArrowDataType::List(Arc::new(item))
            }
            DataType::Map(key, value) => {
                // The value child's Arrow storage MUST agree with `build_map_column` in
                // uni-store: typed scalars use their own Arrow type; `Bytes` is a
                // raw-bytes-marked `LargeBinary`; every other (nested/non-scalar) value type
                // is CypherValue-encoded into an UNMARKED `LargeBinary` (decoded back through
                // the tagged codec on read). Gated by `map_value_is_typed` so the two sites
                // can't drift.
                let value_field = if value.map_value_is_typed() {
                    let f = Field::new("value", value.to_arrow(), true);
                    if matches!(**value, DataType::Bytes) {
                        f.with_metadata(raw_bytes_field_metadata())
                    } else {
                        f
                    }
                } else {
                    Field::new("value", ArrowDataType::LargeBinary, true)
                };
                ArrowDataType::List(Arc::new(Field::new(
                    "item",
                    ArrowDataType::Struct(Fields::from(vec![
                        Field::new("key", key.to_arrow(), false),
                        value_field,
                    ])),
                    true,
                )))
            }
        }
    }

    /// Whether a `Map(_, self)` VALUE is stored as a typed Arrow child (this set) versus a
    /// CypherValue-encoded `LargeBinary` fallback child (everything else, e.g. `Vector`,
    /// `List`, `Map`, temporal). This MUST stay in lockstep with the explicit value-type
    /// arms of `build_map_column` in uni-store (the `_` arm there is the CV fallback).
    pub fn map_value_is_typed(&self) -> bool {
        matches!(
            self,
            DataType::String
                | DataType::Int64
                | DataType::Int32
                | DataType::Float64
                | DataType::Float32
                | DataType::Bool
                | DataType::Bytes
        )
    }

    /// Returns `true` if `value` is directly storable in this column type without loss.
    ///
    /// This is the schema-level type guard used by the write path. `Value::Null` is
    /// always accepted — column nullability is enforced separately by the `nullable`
    /// flag, not here. `CypherValue`, `Crdt`, and `Point` columns accept any value.
    /// For every other declared type, only the `Value` variants that the storage layer
    /// persists *without silently nulling* are accepted (see the per-type converters in
    /// `uni-store`'s `arrow_convert`), plus the intentional lossless widenings
    /// `Int`→`Float`, `Int`→`Int32`, and `Temporal`→`Timestamp`.
    ///
    /// A `Value::String` destined for a `Date`/`Time`/`DateTime`/`Duration` column is
    /// intentionally *not* accepted here: the write path first coerces such strings into
    /// the proper `Temporal` value (matching the Cypher temporal constructors), then the
    /// coerced value passes this check. This keeps `accepts` a pure, allocation-free
    /// predicate.
    ///
    /// # Examples
    /// ```
    /// use uni_common::core::schema::DataType;
    /// use uni_common::Value;
    ///
    /// assert!(DataType::Float64.accepts(&Value::Int(3))); // Int widens to Float
    /// assert!(DataType::Bool.accepts(&Value::Null)); // Null always accepted
    /// assert!(!DataType::DateTime.accepts(&Value::String("2026-01-01T00:00:00Z".into())));
    /// ```
    pub fn accepts(&self, value: &crate::value::Value) -> bool {
        use crate::value::{TemporalValue, Value};

        // Null is universally accepted; nullability is a separate concern.
        if matches!(value, Value::Null) {
            return true;
        }

        match self {
            // Opaque / dynamically-typed columns accept any value.
            DataType::CypherValue | DataType::Crdt(_) | DataType::Point(_) => true,

            DataType::String => matches!(value, Value::String(_)),
            DataType::Int32 | DataType::Int64 => matches!(value, Value::Int(_)),
            // Int widens to Float losslessly for the ranges we care about.
            DataType::Float32 | DataType::Float64 => {
                matches!(value, Value::Int(_) | Value::Float(_))
            }
            DataType::Bool => matches!(value, Value::Bool(_)),

            // Non-struct timestamp column: storage parses strings and accepts ints,
            // so both are lossless here (unlike the DateTime struct column below).
            DataType::Timestamp => matches!(
                value,
                Value::String(_)
                    | Value::Int(_)
                    | Value::Temporal(
                        TemporalValue::DateTime { .. } | TemporalValue::LocalDateTime { .. }
                    )
            ),
            DataType::DateTime => matches!(
                value,
                Value::Temporal(
                    TemporalValue::DateTime { .. } | TemporalValue::LocalDateTime { .. }
                )
            ),
            DataType::Date => {
                matches!(
                    value,
                    Value::Int(_) | Value::Temporal(TemporalValue::Date { .. })
                )
            }
            DataType::Time => matches!(
                value,
                Value::Int(_)
                    | Value::Temporal(TemporalValue::Time { .. } | TemporalValue::LocalTime { .. })
            ),
            DataType::Duration => {
                matches!(value, Value::Temporal(TemporalValue::Duration { .. }))
            }
            DataType::Bytes => matches!(value, Value::Bytes(_)),
            // FixedSizeBinary(24) converter accepts the Btic temporal, raw strings, and lists.
            DataType::Btic => matches!(
                value,
                Value::String(_) | Value::List(_) | Value::Temporal(TemporalValue::Btic { .. })
            ),
            // Shape-only: declared dimensions are enforced by `check_vector_dims`,
            // which the write paths call alongside this predicate (issue #137).
            DataType::Vector { .. } => matches!(value, Value::Vector(_) | Value::List(_)),
            // `Value::Map` is the degraded form a `SparseVector` collapses into
            // when round-tripped through `#[serde(untagged)]` persistence (e.g.
            // the WAL's serde_json mutation log); accept it like `Vector` accepts
            // `List`. The column builder re-extracts `{indices, values}`.
            DataType::SparseVector { .. } => {
                matches!(value, Value::SparseVector { .. } | Value::Map(_))
            }
            // Shape-only: the declared lane count is enforced by
            // `check_vector_dims`. A `Value::List` of byte-valued integers is the
            // literal input form, coerced to `BinaryVector` by the write path.
            DataType::BinaryVector { .. } => {
                matches!(value, Value::BinaryVector(_) | Value::List(_))
            }
            // Shape-only: for `List(Vector)` multi-vector columns, per-token
            // dimensions are enforced by `check_vector_dims` (issue #137).
            DataType::List(_) => matches!(value, Value::List(_)),
            DataType::Map(_, _) => matches!(value, Value::Map(_)),
        }
    }

    /// Checks a value's dimensions against a declared vector column type.
    ///
    /// The dimension-aware companion to [`DataType::accepts`] (which is shape-only):
    /// for `Vector { dimensions }` columns the value must be a `Value::Vector` or an
    /// all-numeric `Value::List` of exactly `dimensions` elements; for
    /// `List(Vector { dimensions })` multi-vector columns every token must satisfy the
    /// same rule (an empty token list is a legal empty multi-vector). `Value::Null` is
    /// always accepted — nullability is enforced separately — and every non-vector
    /// `DataType` returns `Ok(())`, so callers may invoke this unconditionally.
    ///
    /// Guards against the silent data loss of issue #137, where wrong-length vectors
    /// were accepted at write time and nulled at flush by the Arrow converters.
    ///
    /// # Errors
    /// Returns a [`VectorDimError`] describing the first offending value: a length
    /// mismatch, a non-numeric list element, a non-vector value in a vector column,
    /// or their per-token counterparts for multi-vector columns.
    pub fn check_vector_dims(&self, value: &crate::value::Value) -> Result<(), VectorDimError> {
        use crate::value::Value;

        if matches!(value, Value::Null) {
            return Ok(());
        }

        match self {
            DataType::Vector { dimensions } => check_dense_vector_value(value, *dimensions),
            DataType::BinaryVector { dimensions } => check_binary_vector_value(value, *dimensions),
            DataType::List(inner) => {
                let DataType::Vector { dimensions } = inner.as_ref() else {
                    return Ok(());
                };
                let Value::List(tokens) = value else {
                    return Err(VectorDimError::NotATokenList {
                        actual: value_variant_name(value),
                    });
                };
                for (token, token_value) in tokens.iter().enumerate() {
                    check_dense_vector_value(token_value, *dimensions)
                        .map_err(|e| e.for_token(token))?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Why a value cannot be stored in a declared `VECTOR(dim)` or multi-vector column.
///
/// Produced by [`DataType::check_vector_dims`] and [`check_dense_vector_value`].
/// Messages carry the declared and actual lengths so write-path errors are
/// actionable; callers prefix the property name and declared type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorDimError {
    /// The vector has the wrong number of elements.
    WrongLength {
        /// Declared column dimensions.
        expected: usize,
        /// Actual element count of the offending value.
        actual: usize,
    },
    /// A list element is not `Int` or `Float`.
    NonNumericElement {
        /// Zero-based index of the offending element.
        index: usize,
    },
    /// The value is not a vector or list at all.
    NotAVector {
        /// Variant name of the offending value.
        actual: &'static str,
    },
    /// A multi-vector token has the wrong number of elements.
    TokenWrongLength {
        /// Zero-based token index within the multi-vector.
        token: usize,
        /// Declared per-token dimensions.
        expected: usize,
        /// Actual element count of the offending token.
        actual: usize,
    },
    /// A multi-vector token contains a non-numeric element.
    TokenNonNumericElement {
        /// Zero-based token index within the multi-vector.
        token: usize,
        /// Zero-based index of the offending element within the token.
        index: usize,
    },
    /// A multi-vector token is not a vector or list.
    TokenNotAVector {
        /// Zero-based token index within the multi-vector.
        token: usize,
        /// Variant name of the offending token.
        actual: &'static str,
    },
    /// The value for a multi-vector column is not a list of tokens.
    NotATokenList {
        /// Variant name of the offending value.
        actual: &'static str,
    },
}

impl VectorDimError {
    /// Maps a dense-kernel error to its per-token counterpart for multi-vector columns.
    fn for_token(self, token: usize) -> Self {
        match self {
            Self::WrongLength { expected, actual } => Self::TokenWrongLength {
                token,
                expected,
                actual,
            },
            Self::NonNumericElement { index } => Self::TokenNonNumericElement { token, index },
            Self::NotAVector { actual } => Self::TokenNotAVector { token, actual },
            other => other,
        }
    }
}

impl std::fmt::Display for VectorDimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongLength { expected, actual } => write!(
                f,
                "got a vector of length {actual}, expected {expected} dimensions"
            ),
            Self::NonNumericElement { index } => {
                write!(f, "element {index} is not numeric")
            }
            Self::NotAVector { actual } => {
                write!(f, "got a non-vector value of type {actual}")
            }
            Self::TokenWrongLength {
                token,
                expected,
                actual,
            } => write!(
                f,
                "token {token} has {actual} dimensions, expected {expected}"
            ),
            Self::TokenNonNumericElement { token, index } => {
                write!(f, "token {token} element {index} is not numeric")
            }
            Self::TokenNotAVector { token, actual } => {
                write!(f, "token {token} is not a vector (got {actual})")
            }
            Self::NotATokenList { actual } => write!(
                f,
                "got a non-list value of type {actual} for a multi-vector column"
            ),
        }
    }
}

impl std::error::Error for VectorDimError {}

/// Checks one dense vector value against a declared dimension count.
///
/// The per-token kernel behind [`DataType::check_vector_dims`], exposed so the
/// storage-layer converters can validate individual multi-vector tokens with the
/// same semantics: `Value::Null` is accepted (a legal null row), a `Value::Vector`
/// must have exactly `dimensions` elements, and a `Value::List` must additionally
/// be all-numeric (`Int` or `Float`).
///
/// # Errors
/// Returns [`VectorDimError::WrongLength`] on a length mismatch (an empty list
/// reports `actual: 0`), [`VectorDimError::NonNumericElement`] for a `String`,
/// `Null`, or other non-numeric list element, and [`VectorDimError::NotAVector`]
/// for any other value variant.
pub fn check_dense_vector_value(
    value: &crate::value::Value,
    dimensions: usize,
) -> Result<(), VectorDimError> {
    use crate::value::Value;

    match value {
        Value::Null => Ok(()),
        Value::Vector(v) => {
            if v.len() == dimensions {
                Ok(())
            } else {
                Err(VectorDimError::WrongLength {
                    expected: dimensions,
                    actual: v.len(),
                })
            }
        }
        Value::List(items) => {
            if items.len() != dimensions {
                return Err(VectorDimError::WrongLength {
                    expected: dimensions,
                    actual: items.len(),
                });
            }
            if let Some(index) = items.iter().position(|e| !e.is_number()) {
                return Err(VectorDimError::NonNumericElement { index });
            }
            Ok(())
        }
        other => Err(VectorDimError::NotAVector {
            actual: value_variant_name(other),
        }),
    }
}

/// Validates a value against a declared `BinaryVector(dimensions)` column.
///
/// The binary-vector companion to [`check_dense_vector_value`]: `dimensions` is
/// the number of `u8` lanes. Accepts a [`Value::BinaryVector`](crate::value::Value::BinaryVector)
/// of exactly that many bytes, or the literal input form — a
/// [`Value::List`](crate::value::Value::List) of exactly that many integers each
/// in `[0, 255]` (coerced to bytes by the write path).
/// [`Value::Null`](crate::value::Value::Null) passes.
///
/// # Errors
/// Returns [`VectorDimError::WrongLength`] on a lane-count mismatch,
/// [`VectorDimError::NonNumericElement`] if a list element is not an integer in
/// `[0, 255]`, or [`VectorDimError::NotAVector`] for any other value.
pub fn check_binary_vector_value(
    value: &crate::value::Value,
    dimensions: usize,
) -> Result<(), VectorDimError> {
    use crate::value::Value;

    match value {
        Value::Null => Ok(()),
        Value::BinaryVector(bytes) => {
            if bytes.len() == dimensions {
                Ok(())
            } else {
                Err(VectorDimError::WrongLength {
                    expected: dimensions,
                    actual: bytes.len(),
                })
            }
        }
        Value::List(items) => {
            if items.len() != dimensions {
                return Err(VectorDimError::WrongLength {
                    expected: dimensions,
                    actual: items.len(),
                });
            }
            if let Some(index) = items
                .iter()
                .position(|e| !matches!(e.as_i64(), Some(0..=255)))
            {
                return Err(VectorDimError::NonNumericElement { index });
            }
            Ok(())
        }
        other => Err(VectorDimError::NotAVector {
            actual: value_variant_name(other),
        }),
    }
}

/// Returns a short variant name for a `Value`, used in dimension-mismatch messages.
fn value_variant_name(value: &crate::value::Value) -> &'static str {
    use crate::value::Value;

    match value {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int(_) => "Int",
        Value::Float(_) => "Float",
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Node(_) => "Node",
        Value::Edge(_) => "Edge",
        Value::Path(_) => "Path",
        Value::Vector(_) => "Vector",
        Value::SparseVector { .. } => "SparseVector",
        Value::BinaryVector(_) => "BinaryVector",
        Value::Temporal(_) => "Temporal",
    }
}

fn default_created_at() -> DateTime<Utc> {
    Utc::now()
}

fn default_state() -> SchemaElementState {
    SchemaElementState::Active
}

fn default_version_1() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropertyMeta {
    pub r#type: DataType,
    pub nullable: bool,
    #[serde(default = "default_version_1")]
    pub added_in: u32, // SchemaVersion
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
    #[serde(default)]
    pub generation_expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelMeta {
    pub id: u16, // LabelId
    #[serde(default = "default_created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeTypeMeta {
    /// See [`crate::core::edge_type::EdgeTypeId`] for bit-layout details.
    pub id: u32,
    pub src_labels: Vec<String>,
    pub dst_labels: Vec<String>,
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ConstraintType {
    Unique {
        properties: Vec<String>,
    },
    Exists {
        property: String,
    },
    Check {
        expression: String,
    },
    /// Composite node key: the property tuple must be unique AND every listed
    /// property must be present (non-null). Equivalent to `Unique` over the tuple
    /// plus `Exists` on each member, enforced together at write time.
    NodeKey {
        properties: Vec<String>,
    },
}

impl ConstraintType {
    /// The property tuple whose combination must be unique.
    ///
    /// Returns `Some` for the uniqueness-enforcing kinds — `Unique` and `NodeKey`
    /// (a node key is a unique tuple plus NOT-NULL) — and `None` otherwise. Lets
    /// the write path share one key-collection/probe path across both kinds.
    #[must_use]
    pub fn unique_properties(&self) -> Option<&[String]> {
        match self {
            ConstraintType::Unique { properties } | ConstraintType::NodeKey { properties } => {
                Some(properties)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ConstraintTarget {
    Label(String),
    EdgeType(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Constraint {
    pub name: String,
    pub constraint_type: ConstraintType,
    pub target: ConstraintTarget,
    pub enabled: bool,
}

/// Bidirectional registry for dynamically-assigned schemaless edge type IDs.
///
/// Edge types not defined in the schema are assigned IDs at runtime with
/// bit 31 set (see [`crate::core::edge_type`]). This registry maintains
/// the name-to-ID and ID-to-name mappings for those types.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemalessEdgeTypeRegistry {
    name_to_id: HashMap<String, u32>,
    id_to_name: HashMap<u32, String>,
    /// Next local ID to assign (0 is reserved for invalid).
    next_local_id: u32,
}

impl SchemalessEdgeTypeRegistry {
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            id_to_name: HashMap::new(),
            next_local_id: 1,
        }
    }

    /// Returns the schemaless ID for `type_name`, assigning a new one if needed.
    pub fn get_or_assign_id(&mut self, type_name: &str) -> u32 {
        if let Some(&id) = self.name_to_id.get(type_name) {
            return id;
        }

        let id = make_schemaless_id(self.next_local_id);
        self.next_local_id += 1;

        self.name_to_id.insert(type_name.to_string(), id);
        self.id_to_name.insert(id, type_name.to_string());

        id
    }

    /// Looks up the edge type name for a schemaless ID.
    pub fn type_name_by_id(&self, type_id: u32) -> Option<&str> {
        self.id_to_name.get(&type_id).map(String::as_str)
    }

    /// Returns `true` if `type_name` has already been assigned a schemaless ID.
    pub fn contains(&self, type_name: &str) -> bool {
        self.name_to_id.contains_key(type_name)
    }

    /// Looks up the schemaless ID for `type_name` (exact match, read-only).
    pub fn id_by_name(&self, type_name: &str) -> Option<u32> {
        self.name_to_id.get(type_name).copied()
    }

    /// Looks up the edge type ID for `type_name` with case-insensitive matching.
    pub fn id_by_name_case_insensitive(&self, type_name: &str) -> Option<u32> {
        self.name_to_id
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(type_name))
            .map(|(_, &id)| id)
    }

    /// Returns all registered schemaless type IDs.
    pub fn all_type_ids(&self) -> Vec<u32> {
        self.id_to_name.keys().copied().collect()
    }

    /// Returns true if the registry has any schemaless types.
    pub fn is_empty(&self) -> bool {
        self.name_to_id.is_empty()
    }
}

impl Default for SchemalessEdgeTypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// First virtual (catalog-resolved) label ID. Label IDs in
/// `VIRTUAL_LABEL_ID_START..VIRTUAL_LABEL_ID_SENTINEL` are owned by
/// plugin-registered `CatalogProvider`s and allocated lazily by the
/// planner via `PluginRegistry::register_virtual_label`. Native label
/// allocation (`SchemaManager::add_label`) refuses IDs in this range.
pub const VIRTUAL_LABEL_ID_START: u16 = 0xFF00;
/// Sentinel "no label" marker, kept distinct from any allocatable ID.
pub const VIRTUAL_LABEL_ID_SENTINEL: u16 = 0xFFFF;

/// Maximum byte length of a label or edge-type name. (L6)
///
/// Generous; the cap is hygiene — the name lands in on-disk dataset paths
/// and Lance branch names — not a storage limit.
const MAX_SCHEMA_NAME_LEN: usize = 255;

/// Returns `true` if `id` is in the virtual (catalog-resolved) range.
#[inline]
pub fn is_virtual_label_id(id: u16) -> bool {
    (VIRTUAL_LABEL_ID_START..VIRTUAL_LABEL_ID_SENTINEL).contains(&id)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub schema_version: u32,
    pub labels: HashMap<String, LabelMeta>,
    pub edge_types: HashMap<String, EdgeTypeMeta>,
    pub properties: HashMap<String, HashMap<String, PropertyMeta>>,
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
    #[serde(default)]
    pub constraints: Vec<Constraint>,
    /// Registry for schemaless edge types (dynamically assigned IDs)
    #[serde(default)]
    pub schemaless_registry: SchemalessEdgeTypeRegistry,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            schema_version: 1,
            labels: HashMap::new(),
            edge_types: HashMap::new(),
            properties: HashMap::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
            schemaless_registry: SchemalessEdgeTypeRegistry::new(),
        }
    }
}

impl Schema {
    /// Bumps `schema_version` to invalidate cached query plans.
    ///
    /// Called at the end of every DDL mutation that changes the schema's
    /// shape (labels, edge types, properties, indexes, constraints). The
    /// plan-cache eviction guard keys on `schema_version`, so a stale plan
    /// built against an older shape is discarded once this advances. Uses
    /// wrapping arithmetic: the value is a coarse change token, not a count,
    /// so wraparound only risks a missed eviction after 2^32 DDL operations.
    fn bump_version(&mut self) {
        self.schema_version = self.schema_version.wrapping_add(1);
    }

    /// Returns the label name for a given label ID.
    ///
    /// Performs a linear scan over all labels. This is efficient because
    /// the number of labels in a schema is typically small.
    pub fn label_name_by_id(&self, label_id: u16) -> Option<&str> {
        self.labels
            .iter()
            .find(|(_, meta)| meta.id == label_id)
            .map(|(name, _)| name.as_str())
    }

    /// Returns the label ID for a given label name.
    pub fn label_id_by_name(&self, label_name: &str) -> Option<u16> {
        self.labels.get(label_name).map(|meta| meta.id)
    }

    /// Returns the edge type name for a given type ID.
    ///
    /// Performs a linear scan over all edge types. This is efficient because
    /// the number of edge types in a schema is typically small.
    pub fn edge_type_name_by_id(&self, type_id: u32) -> Option<&str> {
        self.edge_types
            .iter()
            .find(|(_, meta)| meta.id == type_id)
            .map(|(name, _)| name.as_str())
    }

    /// Returns the edge type ID for a given type name.
    pub fn edge_type_id_by_name(&self, type_name: &str) -> Option<u32> {
        self.edge_types.get(type_name).map(|meta| meta.id)
    }

    /// Returns the vector index configuration for a given label and property.
    ///
    /// Performs a linear scan over all indexes. This is efficient because
    /// the number of indexes in a schema is typically small.
    pub fn vector_index_for_property(
        &self,
        label: &str,
        property: &str,
    ) -> Option<&VectorIndexConfig> {
        self.indexes.iter().find_map(|idx| {
            if let IndexDefinition::Vector(config) = idx
                && config.label == label
                && config.property == property
                && config.metadata.status == IndexStatus::Online
            {
                return Some(config);
            }
            None
        })
    }

    /// Returns the scored sparse-vector index configuration for a label/property.
    pub fn sparse_index_for_property(
        &self,
        label: &str,
        property: &str,
    ) -> Option<&SparseVectorIndexConfig> {
        self.indexes.iter().find_map(|idx| {
            if let IndexDefinition::Sparse(config) = idx
                && config.label == label
                && config.property == property
                && config.metadata.status == IndexStatus::Online
            {
                return Some(config);
            }
            None
        })
    }

    /// Returns the full-text index configuration for a given label and property.
    ///
    /// A full-text index covers one or more properties. This returns the config
    /// if the specified property is among the indexed properties.
    pub fn fulltext_index_for_property(
        &self,
        label: &str,
        property: &str,
    ) -> Option<&FullTextIndexConfig> {
        self.indexes.iter().find_map(|idx| {
            if let IndexDefinition::FullText(config) = idx
                && config.label == label
                && config.properties.iter().any(|p| p == property)
                && config.metadata.status == IndexStatus::Online
            {
                return Some(config);
            }
            None
        })
    }

    /// Get label metadata with case-insensitive lookup.
    ///
    /// This allows queries to match labels regardless of case, providing
    /// better user experience when label names vary in casing.
    pub fn get_label_case_insensitive(&self, name: &str) -> Option<&LabelMeta> {
        self.labels
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    }

    /// Get the schema-canonical spelling of a label, matched case-insensitively.
    ///
    /// Returns the stored label name whose spelling differs only in case from
    /// `name`, or `None` if no such label is registered. Callers use this to
    /// normalize a user-supplied label to the canonical form the storage tier
    /// keys on, so case variants resolve to the same vertex table.
    pub fn canonical_label_name(&self, name: &str) -> Option<String> {
        self.labels
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(k, _)| k.clone())
    }

    /// Get label ID with case-insensitive lookup.
    pub fn label_id_by_name_case_insensitive(&self, label_name: &str) -> Option<u16> {
        self.get_label_case_insensitive(label_name)
            .map(|meta| meta.id)
    }

    /// Get edge type metadata with case-insensitive lookup.
    ///
    /// This allows queries to match edge types regardless of case, providing
    /// better user experience when type names vary in casing.
    pub fn get_edge_type_case_insensitive(&self, name: &str) -> Option<&EdgeTypeMeta> {
        self.edge_types
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    }

    /// Get edge type ID with case-insensitive lookup (schema-defined types only).
    pub fn edge_type_id_by_name_case_insensitive(&self, type_name: &str) -> Option<u32> {
        self.get_edge_type_case_insensitive(type_name)
            .map(|meta| meta.id)
    }

    /// Get edge type ID with case-insensitive lookup, checking both
    /// schema-defined and schemaless registries.
    pub fn edge_type_id_unified_case_insensitive(&self, type_name: &str) -> Option<u32> {
        self.edge_type_id_by_name_case_insensitive(type_name)
            .or_else(|| {
                self.schemaless_registry
                    .id_by_name_case_insensitive(type_name)
            })
    }

    /// Returns the edge type ID for `type_name`, checking the schema first
    /// and falling back to the schemaless registry (assigning a new ID if needed).
    ///
    /// Requires `&mut self` because it may assign a new schemaless ID.
    /// Use [`edge_type_id_by_name`](Self::edge_type_id_by_name) for read-only schema lookups.
    pub fn get_or_assign_edge_type_id(&mut self, type_name: &str) -> u32 {
        if let Some(id) = self.edge_type_id_unified(type_name) {
            return id;
        }
        // Reaching here means the type is brand-new to *both* the schema map and
        // the schemaless registry (the early return above mirrors exactly what
        // `edge_type_id_unified` checks). Minting a new schemaless edge type
        // changes the result of `all_edge_type_ids()`, which untyped traversals
        // bake into cached plans keyed on `schema_version`. Bump the version so
        // those stale plans are evicted — otherwise a `MATCH ()-[r]->()` plan
        // built before this type existed silently drops edges of the new type.
        let id = self.schemaless_registry.get_or_assign_id(type_name);
        self.bump_version();
        id
    }

    /// Read-only unified exact lookup: schema-defined edge type id, falling
    /// back to an already-assigned schemaless id.
    ///
    /// Mirrors exactly the checks [`Self::get_or_assign_edge_type_id`]
    /// performs before assigning, so a `Some` here means the assigning path
    /// would be a no-op — the basis for `SchemaManager`'s read-lock fast path.
    pub fn edge_type_id_unified(&self, type_name: &str) -> Option<u32> {
        self.edge_type_id_by_name(type_name)
            .or_else(|| self.schemaless_registry.id_by_name(type_name))
    }

    /// Returns the edge type name for `type_id`, checking both the schema
    /// and schemaless registries. Returns an owned `String` because the
    /// name may come from either registry.
    pub fn edge_type_name_by_id_unified(&self, type_id: u32) -> Option<String> {
        if is_schemaless_edge_type(type_id) {
            self.schemaless_registry
                .type_name_by_id(type_id)
                .map(str::to_owned)
        } else {
            self.edge_type_name_by_id(type_id).map(str::to_owned)
        }
    }

    /// Returns all edge type IDs, including both schema-defined and schemaless types.
    /// Used when MATCH queries don't specify an edge type and need to scan all edges.
    pub fn all_edge_type_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.edge_types.values().map(|m| m.id).collect();
        ids.extend(self.schemaless_registry.all_type_ids());
        ids.sort_unstable();
        ids
    }
}

pub struct SchemaManager {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
    schema: RwLock<Arc<Schema>>,
}

impl SchemaManager {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("Invalid schema path"))?;
        let filename = path
            .file_name()
            .ok_or_else(|| anyhow!("Invalid schema filename"))?
            .to_str()
            .ok_or_else(|| anyhow!("Invalid utf8 filename"))?;

        let store = Arc::new(LocalFileSystem::new_with_prefix(parent)?);
        let obj_path = ObjectStorePath::from(filename);

        Self::load_from_store(store, &obj_path).await
    }

    pub async fn load_from_store(
        store: Arc<dyn ObjectStore>,
        path: &ObjectStorePath,
    ) -> Result<Self> {
        match store.get(path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8(bytes.to_vec())?;
                let mut schema: Schema = serde_json::from_str(&content)?;
                // Self-heal catalogs that grew super-linearly under the
                // pre-fix `add_index` (issue rustic-ai/uni-db#63). Collapse
                // duplicate index entries by name, keeping the *last*
                // occurrence — matches the upsert semantics in `add_index`
                // and preserves whatever metadata the most recent rebuild
                // wrote. The dedup persists on the next mutation that
                // calls `save()`.
                let original_len = schema.indexes.len();
                if original_len > 0 {
                    let mut seen: std::collections::HashSet<String> =
                        std::collections::HashSet::with_capacity(original_len);
                    let mut dedup: Vec<IndexDefinition> = schema
                        .indexes
                        .iter()
                        .rev()
                        .filter(|idx| seen.insert(idx.name().to_string()))
                        .cloned()
                        .collect();
                    dedup.reverse();
                    if dedup.len() != original_len {
                        tracing::warn!(
                            collapsed = original_len - dedup.len(),
                            kept = dedup.len(),
                            "schema.indexes: collapsed duplicate entries on load (issue #63)"
                        );
                        schema.indexes = dedup;
                    }
                }
                Ok(Self {
                    store,
                    path: path.clone(),
                    schema: RwLock::new(Arc::new(schema)),
                })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self {
                store,
                path: path.clone(),
                schema: RwLock::new(Arc::new(Schema::default())),
            }),
            Err(e) => Err(anyhow::Error::from(e)),
        }
    }

    pub async fn save(&self) -> Result<()> {
        let content = {
            let schema_guard = acquire_read(&self.schema, "schema")?;
            serde_json::to_string_pretty(&**schema_guard)?
        };
        self.store
            .put(&self.path, content.into())
            .await
            .map_err(anyhow::Error::from)?;
        Ok(())
    }

    pub fn path(&self) -> &ObjectStorePath {
        &self.path
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema
            .read()
            .expect("Schema lock poisoned - a thread panicked while holding it")
            .clone()
    }

    /// Normalize function names in an expression to uppercase for case-insensitive matching.
    /// Examples: "lower(email)" -> "LOWER(email)", "trim(name)" -> "TRIM(name)"
    fn normalize_function_names(expr: &str) -> String {
        let mut result = String::with_capacity(expr.len());
        let mut chars = expr.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch.is_alphabetic() {
                // Collect identifier
                let mut ident = String::new();
                ident.push(ch);

                while let Some(&next) = chars.peek() {
                    if next.is_alphanumeric() || next == '_' {
                        ident.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                // If followed by '(', it's a function call - uppercase it
                if chars.peek() == Some(&'(') {
                    result.push_str(&ident.to_uppercase());
                } else {
                    result.push_str(&ident); // Keep property names as-is
                }
            } else {
                result.push(ch);
            }
        }

        result
    }

    /// Generate a consistent internal column name for an expression index.
    /// Uses a hash suffix to ensure uniqueness for different expressions that
    /// might sanitize to the same string (e.g., "a+b" and "a-b" both become "a_b").
    ///
    /// IMPORTANT: Uses FNV-1a hash which is stable across Rust versions and platforms.
    /// DefaultHasher is not guaranteed to be stable and could break persistent data
    /// if the hash changes after a compiler upgrade.
    pub fn generated_column_name(expr: &str) -> String {
        // Normalize function names to uppercase for case-insensitive matching
        let normalized = Self::normalize_function_names(expr);

        let sanitized = normalized
            .replace(|c: char| !c.is_alphanumeric(), "_")
            .trim_matches('_')
            .to_string();

        // FNV-1a 64-bit hash - stable across Rust versions and platforms
        const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;

        let mut hash = FNV_OFFSET_BASIS;
        for byte in normalized.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }

        format!("_gen_{}_{:x}", sanitized, hash)
    }

    pub fn replace_schema(&self, new_schema: Schema) {
        let mut schema = self
            .schema
            .write()
            .expect("Schema lock poisoned - a thread panicked while holding it");
        *schema = Arc::new(new_schema);
    }

    /// Build a fork-scoped manager whose schema is `primary ⊕ overlay`.
    ///
    /// Used by `UniInner::at_fork` to give a forked session a schema view
    /// that includes any labels/edge-types/properties the fork has
    /// introduced on top of primary. The returned manager owns its own
    /// in-memory `Arc<Schema>` — mutations to it never reach primary's
    /// schema file. The returned manager is *not* intended for `.save()`;
    /// fork-overlay persistence is owned by the registry layer
    /// (`catalog/fork_schemas/{fork_id}.json`).
    ///
    /// In Phase 1 the delta is always empty, so the merge is a clone.
    /// Phase 2 starts populating it when on-the-fly label creation lands.
    #[must_use]
    pub fn with_overlay(&self, overlay: &crate::core::fork::SchemaDelta) -> Arc<Self> {
        let primary = self.schema();
        let merged = if overlay.is_empty() {
            (*primary).clone()
        } else {
            let mut merged = (*primary).clone();
            for (name, label) in &overlay.added_labels {
                merged.labels.insert(name.clone(), label.clone());
            }
            for (name, edge_type) in &overlay.added_edge_types {
                merged.edge_types.insert(name.clone(), edge_type.clone());
            }
            for addition in &overlay.added_properties {
                let props = merged.properties.entry(addition.owner.clone()).or_default();
                props.insert(
                    addition.property.clone(),
                    PropertyMeta {
                        r#type: addition.data_type.clone(),
                        nullable: addition.nullable,
                        added_in: merged.schema_version,
                        state: SchemaElementState::Active,
                        generation_expression: None,
                        description: None,
                    },
                );
            }
            merged
        };

        Arc::new(Self {
            store: self.store.clone(),
            path: self.path.clone(),
            schema: RwLock::new(Arc::new(merged)),
        })
    }

    pub fn next_label_id(&self) -> u16 {
        self.schema()
            .labels
            .values()
            .map(|l| l.id)
            .max()
            .unwrap_or(0)
            + 1
    }

    pub fn next_type_id(&self) -> u32 {
        let max_schema_id = self
            .schema()
            .edge_types
            .values()
            .map(|t| t.id)
            .max()
            .unwrap_or(0);

        // Ensure we stay in schema'd ID space (bit 31 = 0)
        if max_schema_id >= MAX_SCHEMA_TYPE_ID {
            panic!("Schema edge type ID exhaustion");
        }

        max_schema_id + 1
    }

    /// Validate a label or edge-type name at definition time. (L6)
    ///
    /// Names flow into on-disk dataset paths (`vertices_{name}.lance`) and
    /// Lance branch names (`fork_{id}_{…}`); a name with a path separator,
    /// whitespace, or a control character corrupts those paths and breaks
    /// fork creation. Such names were never actually usable, so they are
    /// rejected up front rather than failing later. `.` is allowed
    /// (path-safe and common in qualified names).
    ///
    /// Public so the fork-create path can apply the same rule as a backstop
    /// over names that entered the schema through an infallible interning
    /// path (e.g. schemaless `get_or_assign_edge_type_id`).
    ///
    /// # Errors
    /// Returns an error if `name` is empty/all-whitespace, exceeds
    /// `MAX_SCHEMA_NAME_LEN` bytes, or contains a control, whitespace,
    /// `/`, or `\` character.
    pub fn validate_schema_element_name(kind: &str, name: &str) -> Result<()> {
        if name.is_empty() || name.chars().all(char::is_whitespace) {
            return Err(anyhow!(
                "{kind} name must be non-empty and not all whitespace"
            ));
        }
        if name.len() > MAX_SCHEMA_NAME_LEN {
            return Err(anyhow!("{kind} name exceeds {MAX_SCHEMA_NAME_LEN} bytes"));
        }
        if let Some(c) = name
            .chars()
            .find(|c| c.is_control() || c.is_whitespace() || matches!(c, '/' | '\\'))
        {
            return Err(anyhow!(
                "{kind} name '{name}' contains an unsafe character ({c:?})"
            ));
        }
        Ok(())
    }

    pub fn add_label(&self, name: &str) -> Result<u16> {
        self.add_label_with_desc(name, None)
    }

    pub fn add_label_with_desc(&self, name: &str, description: Option<String>) -> Result<u16> {
        Self::validate_schema_element_name("Label", name)?;
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        if schema.labels.contains_key(name) {
            return Err(anyhow!("Label '{}' already exists", name));
        }

        let id = schema.labels.values().map(|l| l.id).max().unwrap_or(0) + 1;
        if id >= VIRTUAL_LABEL_ID_START {
            return Err(anyhow!(
                "Native label space exhausted (next id {id:#x} would enter the \
                 virtual range {VIRTUAL_LABEL_ID_START:#x}..{VIRTUAL_LABEL_ID_SENTINEL:#x} \
                 reserved for catalog-resolved labels)"
            ));
        }
        schema.labels.insert(
            name.to_string(),
            LabelMeta {
                id,
                created_at: Utc::now(),
                state: SchemaElementState::Active,
                description,
            },
        );
        schema.bump_version();
        Ok(id)
    }

    pub fn add_edge_type(
        &self,
        name: &str,
        src_labels: Vec<String>,
        dst_labels: Vec<String>,
    ) -> Result<u32> {
        self.add_edge_type_with_desc(name, src_labels, dst_labels, None)
    }

    pub fn add_edge_type_with_desc(
        &self,
        name: &str,
        src_labels: Vec<String>,
        dst_labels: Vec<String>,
        description: Option<String>,
    ) -> Result<u32> {
        Self::validate_schema_element_name("Edge type", name)?;
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        if schema.edge_types.contains_key(name) {
            return Err(anyhow!("Edge type '{}' already exists", name));
        }

        let id = schema.edge_types.values().map(|t| t.id).max().unwrap_or(0) + 1;

        // Stay in the schema-defined sub-range (bit 31 = 0, and below the
        // virtual reservation `VIRTUAL_EDGE_TYPE_ID_START`) — same bound as
        // `add_edge_type`, so the two entry points cannot disagree on the
        // legal ceiling.
        if id >= VIRTUAL_EDGE_TYPE_ID_START {
            return Err(anyhow!(
                "Native edge type space exhausted (next id {id:#x} would enter the \
                 virtual range {VIRTUAL_EDGE_TYPE_ID_START:#x}..{VIRTUAL_EDGE_TYPE_ID_SENTINEL:#x} \
                 reserved for catalog-resolved edge types)"
            ));
        }

        schema.edge_types.insert(
            name.to_string(),
            EdgeTypeMeta {
                id,
                src_labels,
                dst_labels,
                state: SchemaElementState::Active,
                description,
            },
        );
        schema.bump_version();
        Ok(id)
    }

    /// Delegates to [`Schema::get_or_assign_edge_type_id`].
    ///
    /// Read-lock fast path: the type name is almost always already known
    /// (it is constant per statement but resolved per row by the CREATE
    /// executor), and the slow path's write lock + `Arc::make_mut` deep-clones
    /// the whole `Schema` whenever the Arc is shared — which under SSI it
    /// always is. Double-checked: on a miss, `Schema::get_or_assign_edge_type_id`
    /// re-checks under the write lock, so two racing assigners converge on one id.
    pub fn get_or_assign_edge_type_id(&self, type_name: &str) -> u32 {
        {
            let guard = acquire_read(&self.schema, "schema")
                .expect("Schema lock poisoned - a thread panicked while holding it");
            if let Some(id) = guard.edge_type_id_unified(type_name) {
                return id;
            }
        }
        let mut guard = acquire_write(&self.schema, "schema")
            .expect("Schema lock poisoned - a thread panicked while holding it");
        let schema = Arc::make_mut(&mut *guard);
        schema.get_or_assign_edge_type_id(type_name)
    }

    /// Delegates to [`Schema::edge_type_name_by_id_unified`].
    pub fn edge_type_name_by_id_unified(&self, type_id: u32) -> Option<String> {
        let schema = acquire_read(&self.schema, "schema")
            .expect("Schema lock poisoned - a thread panicked while holding it");
        schema.edge_type_name_by_id_unified(type_id)
    }

    pub fn add_property(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Result<()> {
        self.add_property_with_desc(label_or_type, prop_name, data_type, nullable, None)
    }

    pub fn add_property_with_desc(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        nullable: bool,
        description: Option<String>,
    ) -> Result<()> {
        validate_property_name(prop_name)?;
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let version = schema.schema_version;
        let props = schema
            .properties
            .entry(label_or_type.to_string())
            .or_default();

        if props.contains_key(prop_name) {
            return Err(anyhow!(
                "Property '{}' already exists for '{}'",
                prop_name,
                label_or_type
            ));
        }

        props.insert(
            prop_name.to_string(),
            PropertyMeta {
                r#type: data_type,
                nullable,
                added_in: version,
                state: SchemaElementState::Active,
                generation_expression: None,
                description,
            },
        );
        // Bump after stamping `added_in` with the pre-bump `version`.
        schema.bump_version();
        Ok(())
    }

    /// Declares a property, idempotent when an identical declaration already exists.
    ///
    /// The schema-builder counterpart to [`Self::add_property_with_desc`] (which
    /// hard-errors on *any* re-add, as DDL `ALTER` semantics require). Re-applying a
    /// schema is common — every `apply()` re-declares — so an existing property with
    /// the same `data_type` and `nullable` is a no-op (`Ok(false)`; a differing
    /// `description` is ignored as docs-only). A differing type or nullability is a
    /// hard conflict: silently swallowing it let `VECTOR(4)` be "re-declared" as
    /// `VECTOR(8)` while the column stayed 4-dimensional (issue #137).
    ///
    /// Returns `true` if this call newly inserted the property.
    ///
    /// # Errors
    /// Returns an error when the property exists with a different type or nullability
    /// (the message deliberately does not contain "already exists", which callers
    /// historically string-matched to ignore benign re-adds), or when the property
    /// name is invalid.
    pub fn declare_property(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        nullable: bool,
        description: Option<String>,
    ) -> Result<bool> {
        validate_property_name(prop_name)?;
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let version = schema.schema_version;
        let props = schema
            .properties
            .entry(label_or_type.to_string())
            .or_default();

        if let Some(existing) = props.get(prop_name) {
            if existing.r#type == data_type && existing.nullable == nullable {
                return Ok(false); // identical re-declaration (idempotent)
            }
            return Err(anyhow!(
                "Property '{}' on '{}' is declared as {:?} (nullable: {}); cannot re-declare \
                 as {:?} (nullable: {}). Property types are immutable — use a new property \
                 name or migrate the data",
                prop_name,
                label_or_type,
                existing.r#type,
                existing.nullable,
                data_type,
                nullable
            ));
        }

        props.insert(
            prop_name.to_string(),
            PropertyMeta {
                r#type: data_type,
                nullable,
                added_in: version,
                state: SchemaElementState::Active,
                generation_expression: None,
                description,
            },
        );
        // Bump after stamping `added_in` with the pre-bump `version`.
        schema.bump_version();
        Ok(true)
    }

    /// Register an INTERNAL property (underscore-prefixed name allowed) that is
    /// materialised by the storage layer, not written by the user — e.g. the MUVERA
    /// `__fde_*` derived column. Bypasses the user-facing underscore-prefix rule but
    /// still rejects storage-layer name collisions. Idempotent: a no-op if the property
    /// already exists with the same type (so re-creating an index is safe).
    ///
    /// Returns `true` if this call newly inserted the property, `false` if it already
    /// existed (idempotent). The check-and-insert is atomic under the schema write lock,
    /// so for concurrent callers exactly one observes `true` — letting callers gate
    /// expensive one-time work (e.g. the MUVERA backfill) on the winner.
    pub fn add_internal_property(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Result<bool> {
        validate_reserved_property_name(prop_name)?;
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let version = schema.schema_version;
        let props = schema
            .properties
            .entry(label_or_type.to_string())
            .or_default();

        if let Some(existing) = props.get(prop_name) {
            if existing.r#type == data_type {
                return Ok(false); // already present (idempotent re-registration)
            }
            return Err(anyhow!(
                "Internal property '{}' already exists for '{}' with a different type",
                prop_name,
                label_or_type
            ));
        }

        props.insert(
            prop_name.to_string(),
            PropertyMeta {
                r#type: data_type,
                nullable,
                added_in: version,
                state: SchemaElementState::Active,
                generation_expression: None,
                description: None,
            },
        );
        schema.bump_version();
        Ok(true)
    }

    pub fn add_generated_property(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        expr: String,
    ) -> Result<()> {
        // System-generated `_gen_*` columns bypass the underscore-prefix rule
        // but must still avoid storage-layer column-name collisions.
        validate_reserved_property_name(prop_name)?;
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let version = schema.schema_version;
        let props = schema
            .properties
            .entry(label_or_type.to_string())
            .or_default();

        if props.contains_key(prop_name) {
            return Err(anyhow!("Property '{}' already exists", prop_name));
        }

        props.insert(
            prop_name.to_string(),
            PropertyMeta {
                r#type: data_type,
                nullable: true,
                added_in: version,
                state: SchemaElementState::Active,
                generation_expression: Some(expr),
                description: None,
            },
        );
        // Bump after stamping `added_in` with the pre-bump `version`.
        schema.bump_version();
        Ok(())
    }

    pub fn set_label_description(&self, name: &str, description: Option<String>) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let meta = schema
            .labels
            .get_mut(name)
            .ok_or_else(|| anyhow!("Label '{}' does not exist", name))?;
        meta.description = description;
        Ok(())
    }

    pub fn set_edge_type_description(&self, name: &str, description: Option<String>) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let meta = schema
            .edge_types
            .get_mut(name)
            .ok_or_else(|| anyhow!("Edge type '{}' does not exist", name))?;
        meta.description = description;
        Ok(())
    }

    pub fn set_property_description(
        &self,
        entity: &str,
        prop_name: &str,
        description: Option<String>,
    ) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let props = schema
            .properties
            .get_mut(entity)
            .ok_or_else(|| anyhow!("Entity '{}' does not exist", entity))?;
        let meta = props
            .get_mut(prop_name)
            .ok_or_else(|| anyhow!("Property '{}' does not exist on '{}'", prop_name, entity))?;
        meta.description = description;
        Ok(())
    }

    /// Register an index definition on the schema, **upsert by name**.
    ///
    /// If an index with the same `IndexDefinition::name()` already exists, it
    /// is replaced in place; otherwise the def is appended. Idempotent under
    /// repeat invocation, which makes `SchemaBuilder::apply()` re-applicable
    /// without bloating `schema.indexes` and lets the rebuild epilogue inside
    /// every `IndexManager::create_*_index` re-record metadata updates without
    /// duplicating entries (issue rustic-ai/uni-db#63).
    pub fn add_index(&self, index_def: IndexDefinition) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        if let Some(existing) = schema
            .indexes
            .iter_mut()
            .find(|i| i.name() == index_def.name())
        {
            *existing = index_def;
        } else {
            schema.indexes.push(index_def);
        }
        schema.bump_version();
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<IndexDefinition> {
        let schema = self.schema.read().expect("Schema lock poisoned");
        schema.indexes.iter().find(|i| i.name() == name).cloned()
    }

    /// Updates the lifecycle metadata for an index by name.
    ///
    /// The closure receives a mutable reference to the index's `IndexMetadata`,
    /// allowing callers to update status, timestamps, etc.
    pub fn update_index_metadata(
        &self,
        index_name: &str,
        f: impl FnOnce(&mut IndexMetadata),
    ) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let idx = schema
            .indexes
            .iter_mut()
            .find(|i| i.name() == index_name)
            .ok_or_else(|| anyhow!("Index '{}' not found", index_name))?;
        f(idx.metadata_mut());
        Ok(())
    }

    pub fn remove_index(&self, name: &str) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        if let Some(pos) = schema.indexes.iter().position(|i| i.name() == name) {
            schema.indexes.remove(pos);
            schema.bump_version();
            Ok(())
        } else {
            Err(anyhow!("Index '{}' not found", name))
        }
    }

    pub fn add_constraint(&self, constraint: Constraint) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        if schema.constraints.iter().any(|c| c.name == constraint.name) {
            return Err(anyhow!("Constraint '{}' already exists", constraint.name));
        }
        schema.constraints.push(constraint);
        schema.bump_version();
        Ok(())
    }

    pub fn drop_constraint(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        if let Some(pos) = schema.constraints.iter().position(|c| c.name == name) {
            schema.constraints.remove(pos);
            schema.bump_version();
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Constraint '{}' not found", name))
        }
    }

    pub fn drop_property(&self, label_or_type: &str, prop_name: &str) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let Some(props) = schema.properties.get_mut(label_or_type) else {
            return Err(anyhow!("Label or Edge Type '{}' not found", label_or_type));
        };
        if props.remove(prop_name).is_none() {
            return Err(anyhow!(
                "Property '{}' not found for '{}'",
                prop_name,
                label_or_type
            ));
        }
        schema.bump_version();
        Ok(())
    }

    pub fn rename_property(
        &self,
        label_or_type: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        // Validate the new name like declare_property/add_property do — otherwise
        // a rename bypasses the reserved-storage-column guard and the leading-
        // underscore rule, letting a user property collide with an internal Arrow
        // column (e.g. `_vid`, `src_vid`, `overflow_json`).
        validate_property_name(new_name)?;
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        let Some(props) = schema.properties.get_mut(label_or_type) else {
            return Err(anyhow!("Label or Edge Type '{}' not found", label_or_type));
        };
        let Some(meta) = props.remove(old_name) else {
            return Err(anyhow!(
                "Property '{}' not found for '{}'",
                old_name,
                label_or_type
            ));
        };
        if props.contains_key(new_name) {
            // Rollback removal? Or just error.
            props.insert(old_name.to_string(), meta); // Restore
            return Err(anyhow!("Property '{}' already exists", new_name));
        }
        props.insert(new_name.to_string(), meta);
        schema.bump_version();
        Ok(())
    }

    pub fn drop_label(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        if let Some(label_meta) = schema.labels.get_mut(name) {
            label_meta.state = SchemaElementState::Tombstone { since: Utc::now() };
            // Do not remove properties; they are implicitly tombstoned by the label
            schema.bump_version();
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Label '{}' not found", name))
        }
    }

    pub fn drop_edge_type(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut guard = acquire_write(&self.schema, "schema")?;
        let schema = Arc::make_mut(&mut *guard);
        if let Some(edge_meta) = schema.edge_types.get_mut(name) {
            edge_meta.state = SchemaElementState::Tombstone { since: Utc::now() };
            // Do not remove properties; they are implicitly tombstoned by the edge type
            schema.bump_version();
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Edge Type '{}' not found", name))
        }
    }
}

/// Validate identifier names to prevent injection and ensure compatibility.
pub fn validate_identifier(name: &str) -> Result<()> {
    // Length check
    if name.is_empty() || name.len() > 64 {
        return Err(anyhow!("Identifier '{}' must be 1-64 characters", name));
    }

    // First character must be letter or underscore
    let first = name.chars().next().unwrap();
    if !first.is_alphabetic() && first != '_' {
        return Err(anyhow!(
            "Identifier '{}' must start with letter or underscore",
            name
        ));
    }

    // Remaining characters: alphanumeric or underscore
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(anyhow!(
            "Identifier '{}' must contain only alphanumeric and underscore",
            name
        ));
    }

    // Reserved words
    const RESERVED: &[&str] = &[
        "MATCH", "CREATE", "DELETE", "SET", "RETURN", "WHERE", "MERGE", "CALL", "YIELD", "WITH",
        "UNION", "ORDER", "LIMIT",
    ];
    if RESERVED.contains(&name.to_uppercase().as_str()) {
        return Err(anyhow!("Identifier '{}' cannot be a reserved word", name));
    }

    Ok(())
}

/// Reject user-declared property names that collide with internal Arrow column
/// names used by the storage layer.
///
/// Without this, declaring a property named e.g. `ext_id` produces an Arrow
/// schema with two `ext_id` fields at flush time, which Lance rejects with
/// "Duplicate field name" — silently losing all in-session writes on shutdown.
pub fn validate_property_name(name: &str) -> Result<()> {
    if name.starts_with('_') {
        return Err(anyhow!(
            "Property name '{}' is reserved: names starting with '_' are reserved by the storage layer",
            name
        ));
    }
    validate_reserved_property_name(name)
}

/// Reject names that collide with storage-layer Arrow column names.
///
/// Used both by `validate_property_name` (user-facing path) and directly by
/// `add_generated_property` (system-generated `_gen_*` path) — the latter
/// needs to bypass the underscore-prefix rule but must still reject the
/// fixed-name collisions below.
fn validate_reserved_property_name(name: &str) -> Result<()> {
    // Unprefixed names that get appended alongside user properties in the
    // per-label vertex (`storage/vertex.rs`), per-edge-type edge
    // (`storage/edge.rs`), or per-edge-type delta (`storage/delta.rs`)
    // Arrow schemas — declaring one of these as a user property produces a
    // duplicate Arrow field and a Lance "Duplicate field name" error at
    // flush time. Fixed-schema-only columns (`type`, `props_json`,
    // `labels` in the main tables) are NOT listed: those tables don't
    // append user properties, so no collision can occur.
    const RESERVED_PROPS: &[&str] = &[
        "ext_id",
        "overflow_json",
        "eid",
        "src_vid",
        "dst_vid",
        "op",
        // Internal planner sentinel: a column-name marker used by
        // `mark_set_item_variables` (uni-query::query::planner) to request
        // narrow structural projection without full-schema expansion.
        // Reserved here defensively so an internal `add_generated_property`
        // path can't accidentally create a colliding user-facing column.
        // The user-facing `validate_property_name` already rejects this
        // via the underscore-prefix rule, so this is belt-and-suspenders.
        "__set_struct__",
    ];
    if RESERVED_PROPS.contains(&name) {
        return Err(anyhow!(
            "Property name '{}' is reserved by the storage layer; please choose a different name",
            name
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{TemporalValue, Value};
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    #[test]
    fn binary_vector_metrics_exact() {
        // Hamming = number of differing bits. 0x00 vs 0xFF = 8 bits; 0xA5 vs 0xA5
        // = 0; 0x0F vs 0x00 = 4 bits.
        assert_eq!(
            DistanceMetric::Hamming.compute_distance_binary(&[0x00], &[0xFF]),
            8.0
        );
        assert_eq!(
            DistanceMetric::Hamming.compute_distance_binary(&[0xA5, 0x0F], &[0xA5, 0x00]),
            4.0
        );
        assert_eq!(
            DistanceMetric::Hamming.compute_distance_binary(&[0xA5], &[0xA5]),
            0.0
        );

        // Jaccard = 1 − |A∩B|/|A∪B|. 0b1100 & 0b1010 = 0b1000 (1 bit);
        // 0b1100 | 0b1010 = 0b1110 (3 bits) → 1 − 1/3 = 2/3.
        let j = DistanceMetric::Jaccard.compute_distance_binary(&[0b1100], &[0b1010]);
        assert!((j - (2.0 / 3.0)).abs() < 1e-6, "got {j}");
        // Identical vectors → distance 0.
        assert_eq!(
            DistanceMetric::Jaccard.compute_distance_binary(&[0xFF], &[0xFF]),
            0.0
        );
        // Two all-zero vectors are defined as distance 0 (empty union).
        assert_eq!(
            DistanceMetric::Jaccard.compute_distance_binary(&[0x00, 0x00], &[0x00, 0x00]),
            0.0
        );
    }

    #[test]
    fn binary_metrics_are_binary_and_route_correctly() {
        assert!(DistanceMetric::Hamming.is_binary());
        assert!(DistanceMetric::Jaccard.is_binary());
        assert!(!DistanceMetric::L2.is_binary());
        assert!(!DistanceMetric::L1.is_binary());
    }

    #[test]
    #[should_panic(expected = "binary-vector metric")]
    fn float_compute_distance_rejects_binary_metric() {
        DistanceMetric::Hamming.compute_distance(&[1.0], &[0.0]);
    }

    #[test]
    fn check_binary_vector_value_guards() {
        let ty = DataType::BinaryVector { dimensions: 3 };
        assert!(
            ty.check_vector_dims(&Value::BinaryVector(vec![1, 2, 3]))
                .is_ok()
        );
        assert!(ty.check_vector_dims(&Value::Null).is_ok());
        // Wrong lane count.
        assert!(
            ty.check_vector_dims(&Value::BinaryVector(vec![1, 2]))
                .is_err()
        );
        // List of byte-ints is the literal form.
        assert!(
            ty.check_vector_dims(&Value::List(vec![
                Value::Int(0),
                Value::Int(255),
                Value::Int(128)
            ]))
            .is_ok()
        );
        // Out-of-byte-range element.
        assert!(
            ty.check_vector_dims(&Value::List(vec![
                Value::Int(0),
                Value::Int(256),
                Value::Int(1)
            ]))
            .is_err()
        );
    }

    #[test]
    fn test_datatype_accepts_matrix() {
        let dt = || TemporalValue::DateTime {
            nanos_since_epoch: 0,
            offset_seconds: 0,
            timezone_name: None,
        };

        // Null is accepted by every type (nullability checked separately).
        for ty in [
            DataType::String,
            DataType::Int64,
            DataType::Bool,
            DataType::DateTime,
            DataType::Float64,
        ] {
            assert!(ty.accepts(&Value::Null), "{ty:?} must accept Null");
        }

        // Exact-type matches.
        assert!(DataType::String.accepts(&Value::String("x".into())));
        assert!(DataType::Int64.accepts(&Value::Int(1)));
        assert!(DataType::Bool.accepts(&Value::Bool(true)));
        assert!(DataType::DateTime.accepts(&Value::Temporal(dt())));

        // Intentional lossless widenings remain allowed.
        assert!(
            DataType::Float64.accepts(&Value::Int(3)),
            "Int widens to Float"
        );
        assert!(DataType::Int32.accepts(&Value::Int(3)), "Int fits Int32");
        assert!(DataType::Timestamp.accepts(&Value::Temporal(dt())));
        assert!(
            DataType::Timestamp.accepts(&Value::String("2026-01-01T00:00:00Z".into())),
            "storage parses strings for non-struct Timestamp columns"
        );

        // The #68 data-loss cases must be rejected (coercion handles strings separately).
        assert!(
            !DataType::DateTime.accepts(&Value::String("2026-01-01T00:00:00Z".into())),
            "String into a DateTime struct column nulls silently — reject here"
        );
        assert!(!DataType::Bool.accepts(&Value::Int(1)));
        assert!(!DataType::Int64.accepts(&Value::Bool(true)));
        assert!(!DataType::Int64.accepts(&Value::Float(1.5)));
        assert!(
            !DataType::String.accepts(&Value::Int(10)),
            "no implicit stringification"
        );
        assert!(!DataType::Duration.accepts(&Value::String("P1D".into())));

        // Opaque columns accept anything.
        assert!(DataType::CypherValue.accepts(&Value::Map(Default::default())));
    }

    #[test]
    fn test_check_vector_dims_matrix() {
        let vec3 = DataType::Vector { dimensions: 3 };
        let multi2 = DataType::List(Box::new(DataType::Vector { dimensions: 2 }));
        let flist = |vals: &[f64]| Value::List(vals.iter().map(|f| Value::Float(*f)).collect());

        // Null is accepted everywhere (nullability enforced separately).
        assert!(vec3.check_vector_dims(&Value::Null).is_ok());
        assert!(multi2.check_vector_dims(&Value::Null).is_ok());

        // Correct-dimension values pass; Int elements are numeric.
        assert!(
            vec3.check_vector_dims(&Value::Vector(vec![1.0, 2.0, 3.0]))
                .is_ok()
        );
        assert!(vec3.check_vector_dims(&flist(&[1.0, 2.0, 3.0])).is_ok());
        assert!(
            vec3.check_vector_dims(&Value::List(vec![
                Value::Int(1),
                Value::Float(2.0),
                Value::Int(3)
            ]))
            .is_ok()
        );

        // The #137 cases: wrong length, empty list, non-numeric element, wrong shape.
        assert_eq!(
            vec3.check_vector_dims(&Value::Vector(vec![1.0, 2.0])),
            Err(VectorDimError::WrongLength {
                expected: 3,
                actual: 2
            })
        );
        assert_eq!(
            vec3.check_vector_dims(&flist(&[1.0, 2.0, 3.0, 4.0, 5.0])),
            Err(VectorDimError::WrongLength {
                expected: 3,
                actual: 5
            })
        );
        assert_eq!(
            vec3.check_vector_dims(&Value::List(vec![])),
            Err(VectorDimError::WrongLength {
                expected: 3,
                actual: 0
            })
        );
        assert_eq!(
            vec3.check_vector_dims(&Value::List(vec![
                Value::Float(1.0),
                Value::String("x".into()),
                Value::Float(3.0),
            ])),
            Err(VectorDimError::NonNumericElement { index: 1 })
        );
        assert_eq!(
            vec3.check_vector_dims(&Value::List(vec![
                Value::Float(1.0),
                Value::Null,
                Value::Float(3.0)
            ])),
            Err(VectorDimError::NonNumericElement { index: 1 })
        );
        assert_eq!(
            vec3.check_vector_dims(&Value::String("not a vector".into())),
            Err(VectorDimError::NotAVector { actual: "String" })
        );

        // Multi-vector: empty token list is a legal empty multi-vector; each
        // token must match the declared per-token dimensions.
        assert!(multi2.check_vector_dims(&Value::List(vec![])).is_ok());
        assert!(
            multi2
                .check_vector_dims(&Value::List(vec![flist(&[1.0, 2.0]), flist(&[3.0, 4.0])]))
                .is_ok()
        );
        assert_eq!(
            multi2.check_vector_dims(&Value::List(vec![
                flist(&[1.0, 2.0]),
                flist(&[9.0, 9.0, 9.0])
            ])),
            Err(VectorDimError::TokenWrongLength {
                token: 1,
                expected: 2,
                actual: 3
            })
        );
        assert_eq!(
            multi2.check_vector_dims(&Value::List(vec![Value::String("tok".into())])),
            Err(VectorDimError::TokenNotAVector {
                token: 0,
                actual: "String"
            })
        );
        assert_eq!(
            multi2.check_vector_dims(&Value::Vector(vec![1.0, 2.0])),
            Err(VectorDimError::NotATokenList { actual: "Vector" })
        );

        // Non-vector declared types never object, so callers may check unconditionally.
        assert!(
            DataType::Int64
                .check_vector_dims(&Value::String("x".into()))
                .is_ok()
        );
        assert!(
            DataType::List(Box::new(DataType::Float64))
                .check_vector_dims(&Value::List(vec![Value::String("x".into())]))
                .is_ok()
        );
        assert!(
            DataType::SparseVector { dimensions: 8 }
                .check_vector_dims(&Value::Map(Default::default()))
                .is_ok()
        );

        // Error rendering carries both lengths so write errors are actionable.
        let msg = VectorDimError::WrongLength {
            expected: 4,
            actual: 5,
        }
        .to_string();
        assert!(msg.contains('4') && msg.contains('5'), "message: {msg}");
    }

    #[tokio::test]
    async fn test_declare_property_idempotent_and_conflicting() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = SchemaManager::load_from_store(store.clone(), &path).await?;

        manager.add_label("Doc")?;
        let vec4 = DataType::Vector { dimensions: 4 };

        // First declaration inserts.
        assert!(manager.declare_property("Doc", "embedding", vec4.clone(), true, None)?);

        // Identical re-declaration is an idempotent no-op — the register-on-every-open
        // pattern; a differing description is docs-only and also ignored.
        assert!(!manager.declare_property("Doc", "embedding", vec4.clone(), true, None)?);
        assert!(!manager.declare_property(
            "Doc",
            "embedding",
            vec4.clone(),
            true,
            Some("new docs".into())
        )?);

        // A dimension change is a conflict (#137 case c), and the message must not
        // contain "already exists" (historically string-matched and swallowed).
        let err = manager
            .declare_property(
                "Doc",
                "embedding",
                DataType::Vector { dimensions: 8 },
                true,
                None,
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains('4') && err.contains('8'), "message: {err}");
        assert!(!err.contains("already exists"), "message: {err}");

        // Nullability flips are conflicts too — they change NOT NULL enforcement.
        assert!(
            manager
                .declare_property("Doc", "embedding", vec4.clone(), false, None)
                .is_err()
        );

        // The schema still holds the original declaration.
        let schema = manager.schema();
        let meta = &schema.properties["Doc"]["embedding"];
        assert_eq!(meta.r#type, vec4);
        assert!(meta.nullable);
        Ok(())
    }

    #[tokio::test]
    async fn test_schema_management() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = SchemaManager::load_from_store(store.clone(), &path).await?;

        // Labels
        let lid = manager.add_label("Person")?;
        assert_eq!(lid, 1);
        assert!(manager.add_label("Person").is_err());

        // Properties
        manager.add_property("Person", "name", DataType::String, false)?;
        assert!(
            manager
                .add_property("Person", "name", DataType::String, false)
                .is_err()
        );

        // Edge types
        let tid = manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
        assert_eq!(tid, 1);

        manager.save().await?;
        // Check file exists
        assert!(store.get(&path).await.is_ok());

        let manager2 = SchemaManager::load_from_store(store, &path).await?;
        assert!(manager2.schema().labels.contains_key("Person"));
        assert!(
            manager2
                .schema()
                .properties
                .get("Person")
                .unwrap()
                .contains_key("name")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reserved_property_names_rejected() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = SchemaManager::load_from_store(store, &path).await?;

        manager.add_label("Tiny")?;

        // Unprefixed reserved names — these collide with internal Arrow
        // columns in storage tables and previously caused Lance
        // "Duplicate field name" errors at flush time.
        for reserved in &["ext_id", "overflow_json", "eid", "src_vid", "dst_vid", "op"] {
            let err = manager
                .add_property("Tiny", reserved, DataType::String, true)
                .expect_err(&format!("expected '{reserved}' to be rejected"));
            assert!(
                err.to_string().contains("reserved"),
                "error for '{reserved}' should mention 'reserved', got: {err}"
            );
        }

        // Planner sentinel — reserved in RESERVED_PROPS (belt-and-suspenders
        // alongside the underscore-prefix rule). Confirms an internal
        // `add_generated_property` path cannot accidentally create a column
        // that collides with the SET-target structural-projection marker.
        let err = manager
            .add_property("Tiny", "__set_struct__", DataType::String, true)
            .expect_err("expected '__set_struct__' to be rejected");
        assert!(
            err.to_string().contains("reserved"),
            "__set_struct__ rejection should mention 'reserved', got: {err}"
        );

        // Leading-underscore pattern rule.
        for reserved in &["_vid", "_uid", "_eid", "_version", "_created_at"] {
            assert!(
                manager
                    .add_property("Tiny", reserved, DataType::String, true)
                    .is_err(),
                "expected '{reserved}' to be rejected"
            );
        }

        // Names that merely contain a reserved substring should still be
        // accepted.
        manager.add_property("Tiny", "ext_id_foo", DataType::String, true)?;
        manager.add_property("Tiny", "user_op", DataType::String, true)?;
        manager.add_property("Tiny", "type_name", DataType::String, true)?;

        // Same check applies to edge-type properties (single dispatch).
        manager.add_edge_type("knows", vec!["Tiny".into()], vec!["Tiny".into()])?;
        assert!(
            manager
                .add_property("knows", "src_vid", DataType::Int64, true)
                .is_err()
        );

        // And to generated properties.
        assert!(
            manager
                .add_generated_property(
                    "Tiny",
                    "ext_id",
                    DataType::String,
                    "concat('x', name)".into()
                )
                .is_err()
        );

        Ok(())
    }

    #[test]
    fn test_normalize_function_names() {
        assert_eq!(
            SchemaManager::normalize_function_names("lower(email)"),
            "LOWER(email)"
        );
        assert_eq!(
            SchemaManager::normalize_function_names("LOWER(email)"),
            "LOWER(email)"
        );
        assert_eq!(
            SchemaManager::normalize_function_names("Lower(email)"),
            "LOWER(email)"
        );
        assert_eq!(
            SchemaManager::normalize_function_names("trim(lower(email))"),
            "TRIM(LOWER(email))"
        );
    }

    #[test]
    fn test_generated_column_name_case_insensitive() {
        let col1 = SchemaManager::generated_column_name("lower(email)");
        let col2 = SchemaManager::generated_column_name("LOWER(email)");
        let col3 = SchemaManager::generated_column_name("Lower(email)");
        assert_eq!(col1, col2);
        assert_eq!(col2, col3);
        assert!(col1.starts_with("_gen_LOWER_email_"));
    }

    #[test]
    fn test_index_metadata_serde_backward_compat() {
        // Simulate old JSON without metadata field
        let json = r#"{
            "type": "Scalar",
            "name": "idx_person_name",
            "label": "Person",
            "properties": ["name"],
            "index_type": "BTree",
            "where_clause": null
        }"#;
        let def: IndexDefinition = serde_json::from_str(json).unwrap();
        let meta = def.metadata();
        assert_eq!(meta.status, IndexStatus::Online);
        assert!(meta.last_built_at.is_none());
        assert!(meta.row_count_at_build.is_none());
    }

    #[test]
    fn test_index_metadata_serde_roundtrip() {
        let now = Utc::now();
        let def = IndexDefinition::Scalar(ScalarIndexConfig {
            name: "idx_test".to_string(),
            label: "Test".to_string(),
            properties: vec!["prop".to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: IndexMetadata {
                status: IndexStatus::Building,
                last_built_at: Some(now),
                row_count_at_build: Some(42),
            },
        });

        let json = serde_json::to_string(&def).unwrap();
        let parsed: IndexDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.metadata().status, IndexStatus::Building);
        assert_eq!(parsed.metadata().row_count_at_build, Some(42));
        assert!(parsed.metadata().last_built_at.is_some());
    }

    #[tokio::test]
    async fn test_update_index_metadata() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = SchemaManager::load_from_store(store, &path).await?;

        manager.add_label("Person")?;
        let idx = IndexDefinition::Scalar(ScalarIndexConfig {
            name: "idx_test".to_string(),
            label: "Person".to_string(),
            properties: vec!["name".to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: Default::default(),
        });
        manager.add_index(idx)?;

        // Verify initial status is Online
        let initial = manager.get_index("idx_test").unwrap();
        assert_eq!(initial.metadata().status, IndexStatus::Online);

        // Update to Building
        manager.update_index_metadata("idx_test", |m| {
            m.status = IndexStatus::Building;
            m.row_count_at_build = Some(100);
        })?;

        let updated = manager.get_index("idx_test").unwrap();
        assert_eq!(updated.metadata().status, IndexStatus::Building);
        assert_eq!(updated.metadata().row_count_at_build, Some(100));

        // Non-existent index should error
        assert!(manager.update_index_metadata("nope", |_| {}).is_err());

        Ok(())
    }

    /// `add_internal_property` reports whether THIS call inserted the property: `true` on
    /// first insert, `false` on idempotent re-registration, `Err` on a type conflict. The
    /// MUVERA backfill gates on this (only the inserter backfills), so two concurrent
    /// creates of the same index can't both run the full-table rewrite (issue #107).
    #[tokio::test]
    async fn add_internal_property_reports_newly_added() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = SchemaManager::load_from_store(store, &path).await?;
        manager.add_label("Doc")?;

        let dt = DataType::Vector { dimensions: 16 };
        // First registration: newly added.
        assert!(manager.add_internal_property("Doc", "__fde_x", dt.clone(), true)?);
        // Idempotent re-registration with the same type: NOT newly added.
        assert!(!manager.add_internal_property("Doc", "__fde_x", dt.clone(), true)?);
        // Same name, conflicting type: hard error (no silent divergence).
        assert!(
            manager
                .add_internal_property("Doc", "__fde_x", DataType::Vector { dimensions: 8 }, true)
                .is_err()
        );
        Ok(())
    }

    /// `add_index` is upsert-by-name (issue rustic-ai/uni-db#63). Repeat
    /// invocations with the same `IndexDefinition::name()` must replace
    /// the entry in place rather than appending. Subsequent `add_index`
    /// calls also reflect metadata updates from the new definition.
    #[tokio::test]
    async fn test_add_index_is_upsert_by_name() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = SchemaManager::load_from_store(store, &path).await?;
        manager.add_label("Person")?;

        let initial = IndexDefinition::Scalar(ScalarIndexConfig {
            name: "idx_test".to_string(),
            label: "Person".to_string(),
            properties: vec!["name".to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: IndexMetadata {
                status: IndexStatus::Building,
                ..Default::default()
            },
        });
        manager.add_index(initial.clone())?;
        assert_eq!(manager.schema().indexes.len(), 1);

        // Re-add the identical def — must remain a single entry.
        manager.add_index(initial.clone())?;
        assert_eq!(
            manager.schema().indexes.len(),
            1,
            "duplicate add_index by name must not append"
        );

        // Re-add with updated metadata — must replace in place, len unchanged.
        let mut updated_cfg = match initial {
            IndexDefinition::Scalar(c) => c,
            _ => unreachable!(),
        };
        updated_cfg.metadata.status = IndexStatus::Online;
        updated_cfg.metadata.row_count_at_build = Some(42);
        manager.add_index(IndexDefinition::Scalar(updated_cfg))?;
        assert_eq!(manager.schema().indexes.len(), 1);
        let stored = manager.get_index("idx_test").unwrap();
        assert_eq!(stored.metadata().status, IndexStatus::Online);
        assert_eq!(stored.metadata().row_count_at_build, Some(42));

        // A *different* name appends as a new entry.
        let other = IndexDefinition::Scalar(ScalarIndexConfig {
            name: "idx_other".to_string(),
            label: "Person".to_string(),
            properties: vec!["age".to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: IndexMetadata::default(),
        });
        manager.add_index(other)?;
        assert_eq!(manager.schema().indexes.len(), 2);

        Ok(())
    }

    /// `load_from_store` self-heals catalogs that were bloated by the
    /// pre-fix `add_index` (kept the *last* def per name).
    #[tokio::test]
    async fn test_load_dedups_bloated_indexes() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");

        // Seed disk with a hand-crafted bloated schema: 50 entries, all
        // sharing the same name. The last entry has distinct metadata so
        // we can assert "last writer wins" semantics.
        let mut schema = Schema::default();
        schema.labels.insert(
            "Person".to_string(),
            LabelMeta {
                id: 1,
                created_at: chrono::Utc::now(),
                state: SchemaElementState::Active,
                description: None,
            },
        );
        let make = |status: IndexStatus, count: Option<u64>| {
            IndexDefinition::Scalar(ScalarIndexConfig {
                name: "idx_dup".to_string(),
                label: "Person".to_string(),
                properties: vec!["name".to_string()],
                index_type: ScalarIndexType::BTree,
                where_clause: None,
                metadata: IndexMetadata {
                    status,
                    row_count_at_build: count,
                    ..Default::default()
                },
            })
        };
        for _ in 0..49 {
            schema.indexes.push(make(IndexStatus::Building, None));
        }
        schema.indexes.push(make(IndexStatus::Online, Some(123)));
        let json = serde_json::to_string_pretty(&schema)?;
        store.put(&path, json.into()).await?;

        let manager = SchemaManager::load_from_store(store, &path).await?;
        let schema = manager.schema();
        assert_eq!(
            schema.indexes.len(),
            1,
            "load() must collapse 50 duplicates by name to 1"
        );
        // Last-writer-wins: the kept entry is the final push (Online, 123).
        assert_eq!(schema.indexes[0].metadata().status, IndexStatus::Online);
        assert_eq!(schema.indexes[0].metadata().row_count_at_build, Some(123));

        Ok(())
    }

    #[test]
    fn test_vector_index_for_property_skips_non_online() {
        let mut schema = Schema::default();
        schema.labels.insert(
            "Document".to_string(),
            LabelMeta {
                id: 1,
                created_at: chrono::Utc::now(),
                state: SchemaElementState::Active,
                description: None,
            },
        );

        // Add a vector index with Stale status
        schema
            .indexes
            .push(IndexDefinition::Vector(VectorIndexConfig {
                name: "vec_doc_embedding".to_string(),
                label: "Document".to_string(),
                property: "embedding".to_string(),
                index_type: VectorIndexType::Flat,
                metric: DistanceMetric::Cosine,
                embedding_config: None,
                metadata: IndexMetadata {
                    status: IndexStatus::Stale,
                    ..Default::default()
                },
                default_refine_factor: None,
            }));

        // Stale index should NOT be returned
        assert!(
            schema
                .vector_index_for_property("Document", "embedding")
                .is_none()
        );

        // Set to Online — should now be returned
        if let IndexDefinition::Vector(cfg) = &mut schema.indexes[0] {
            cfg.metadata.status = IndexStatus::Online;
        }
        let result = schema.vector_index_for_property("Document", "embedding");
        assert!(result.is_some());
        assert_eq!(result.unwrap().metric, DistanceMetric::Cosine);
    }

    #[tokio::test]
    async fn with_overlay_empty_clones_primary_in_isolation() -> Result<()> {
        use crate::core::fork::SchemaDelta;

        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let primary = SchemaManager::load_from_store(store, &path).await?;
        primary.add_label("Person")?;

        let overlay = primary.with_overlay(&SchemaDelta::empty());
        assert_eq!(overlay.schema().labels.len(), 1);

        // Phase 1 invariant: mutating the overlay manager must not bleed
        // into primary's schema.
        overlay.add_label("Forked")?;
        assert!(overlay.schema().labels.contains_key("Forked"));
        assert!(!primary.schema().labels.contains_key("Forked"));

        Ok(())
    }

    #[tokio::test]
    async fn with_overlay_merges_added_labels_and_edge_types() -> Result<()> {
        use crate::core::fork::SchemaDelta;
        use chrono::Utc;

        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let primary = SchemaManager::load_from_store(store, &path).await?;
        primary.add_label("Existing")?;

        let label_meta = LabelMeta {
            id: 99,
            created_at: Utc::now(),
            state: SchemaElementState::Active,
            description: None,
        };
        let edge_meta = EdgeTypeMeta {
            id: 99,
            src_labels: vec!["NewLabel".into()],
            dst_labels: vec!["NewLabel".into()],
            state: SchemaElementState::Active,
            description: None,
        };
        let delta = SchemaDelta {
            added_labels: vec![("NewLabel".to_string(), label_meta)],
            added_edge_types: vec![("NewEdge".to_string(), edge_meta)],
            added_properties: vec![],
        };

        let overlay = primary.with_overlay(&delta);
        let merged = overlay.schema();
        assert!(merged.labels.contains_key("Existing"));
        assert!(merged.labels.contains_key("NewLabel"));
        assert!(merged.edge_types.contains_key("NewEdge"));

        // Primary unchanged.
        assert!(!primary.schema().labels.contains_key("NewLabel"));
        Ok(())
    }

    /// N threads racing `get_or_assign_edge_type_id` for the same new name
    /// must converge on a single id (the read-lock fast path double-checks
    /// under the write lock); a schema-defined type must win over the
    /// schemaless registry.
    #[tokio::test]
    async fn test_get_or_assign_edge_type_id_concurrent() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = Arc::new(SchemaManager::load_from_store(store, &path).await?);

        let mut handles = Vec::new();
        for _ in 0..16 {
            let m = manager.clone();
            handles.push(std::thread::spawn(move || {
                m.get_or_assign_edge_type_id("RACED")
            }));
        }
        let ids: Vec<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(
            ids.iter().all(|&id| id == ids[0]),
            "all racers must observe one id, got {ids:?}"
        );
        // Fast path returns the same id afterwards.
        assert_eq!(manager.get_or_assign_edge_type_id("RACED"), ids[0]);

        // Schema-defined type wins over the schemaless registry.
        manager.add_label("A")?;
        let declared = manager.add_edge_type("DECLARED", vec!["A".into()], vec!["A".into()])?;
        assert_eq!(manager.get_or_assign_edge_type_id("DECLARED"), declared);
        Ok(())
    }

    /// Minting a brand-new schemaless edge type must bump `schema_version`
    /// (the plan cache keys on it; untyped traversals bake `all_edge_type_ids()`
    /// into the plan, so a stale plan would silently drop edges of the new
    /// type). Re-resolving an existing type must NOT bump. (review C5)
    #[test]
    fn test_new_schemaless_edge_type_bumps_schema_version() {
        let mut schema = Schema::default();
        let v0 = schema.schema_version;

        let id1 = schema.get_or_assign_edge_type_id("FRESH");
        assert_eq!(
            schema.schema_version,
            v0.wrapping_add(1),
            "minting a new edge type must bump schema_version"
        );

        // Re-resolving the same type is a no-op — no further bump.
        let id1_again = schema.get_or_assign_edge_type_id("FRESH");
        assert_eq!(id1, id1_again);
        assert_eq!(
            schema.schema_version,
            v0.wrapping_add(1),
            "resolving an existing edge type must not bump schema_version"
        );

        // A second distinct new type bumps again.
        let _id2 = schema.get_or_assign_edge_type_id("OTHER");
        assert_eq!(
            schema.schema_version,
            v0.wrapping_add(2),
            "a second new edge type must bump schema_version again"
        );
    }

    /// L6: label/edge-type names with path separators, whitespace, or
    /// control chars are rejected at definition; benign names (incl. `.`)
    /// are accepted.
    #[test]
    fn validate_schema_element_name_rejects_unsafe() {
        for bad in ["", "   ", "a/b", "a b", "a\nb", "a\\b", "x\0y"] {
            assert!(
                SchemaManager::validate_schema_element_name("Label", bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
        for good in ["Person", "My.Label", "edge_2", "KNOWS"] {
            assert!(
                SchemaManager::validate_schema_element_name("Label", good).is_ok(),
                "expected {good:?} to be accepted"
            );
        }
        // Over-length is rejected.
        let long = "x".repeat(MAX_SCHEMA_NAME_LEN + 1);
        assert!(SchemaManager::validate_schema_element_name("Label", &long).is_err());
    }
}
