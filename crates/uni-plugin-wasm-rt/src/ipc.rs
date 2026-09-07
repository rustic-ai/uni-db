//! Arrow IPC bridge — `RecordBatch` ↔ wire-stream bytes.
//!
//! Both the Extism loader (bytes-in/bytes-out via `Plugin::call`) and
//! the Component Model loader (alloc/copy/free through linear memory)
//! cross the host↔plugin boundary by shipping Arrow IPC stream bytes.
//! Standardizing on the wire format means the executor's columnar
//! contract is identical regardless of which ABI delivered a batch.
//!
//! Host call pattern:
//!
//! 1. Serialize arguments / state via [`encode_batch`].
//! 2. Pass the byte slice through the loader-specific call boundary.
//! 3. Read the returned bytes; deserialize via [`decode_batch`] (or
//!    [`decode_batches`] for procedure `YIELD` streaming).

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;

use crate::error::IpcError;

/// FU-2: Arrow extension name tagging a `secret-handle` column.
///
/// Columns whose `Field::metadata` contains
/// `"ARROW:extension:name" = SECRET_HANDLE_EXTENSION` are blocked at
/// the IPC boundary — see `reject_secret_handles`. The host's
/// `SecretStore` returns `secret-handle` resources via the
/// `host.secrets.acquire` WIT import; the IPC membrane ensures those
/// opaque handles cannot be exfiltrated as raw bytes inside a plugin's
/// output `RecordBatch`.
pub const SECRET_HANDLE_EXTENSION: &str = "uni-db.secret-handle";

/// Arrow metadata key for extension-type names.
const ARROW_EXTENSION_KEY: &str = "ARROW:extension:name";

/// Walk every field of `batch.schema()` and return
/// [`IpcError::SecretLeakAttempt`] if any field carries the
/// `uni-db.secret-handle` extension marker.
///
/// Called on every encode and decode path ([`encode_batch`],
/// [`encode_batches`], [`decode_batch`], [`decode_batches`]) via
/// [`reject_all`] so neither direction can carry a secret-handle column across
/// the wasm boundary. Every nested `DataType` is walked recursively — struct
/// fields, the five list flavours, map entries, union variants, run-end-encoded
/// children and dictionary value types — because `decode_batches` takes its
/// schema from the guest-supplied IPC stream, so the guest picks the nesting.
fn reject_secret_handles(batch: &RecordBatch) -> Result<(), IpcError> {
    reject_secret_handles_in_schema(batch.schema().as_ref())
}

/// The schema half of [`reject_secret_handles`]. Split out because the membrane
/// is a property of the schema alone, which lets the exotic nested types be
/// covered without hand-building a `UnionArray` or `RunArray` per test.
fn reject_secret_handles_in_schema(schema: &arrow_schema::Schema) -> Result<(), IpcError> {
    fn walk(field: &arrow_schema::Field) -> Result<(), IpcError> {
        if field
            .metadata()
            .get(ARROW_EXTENSION_KEY)
            .is_some_and(|name| name == SECRET_HANDLE_EXTENSION)
        {
            return Err(IpcError::SecretLeakAttempt {
                column: field.name().clone(),
            });
        }
        walk_type(field.data_type())
    }

    // Deny-by-default: every nested variant recurses and every scalar variant
    // is named. There is deliberately no `_` arm — an allow-list over an open
    // enum reopens the membrane every time Arrow adds a nested type, which is
    // how `Union`, `RunEndEncoded`, the two `ListView`s and `Dictionary` came
    // to be walked past. A future variant must now fail the build here rather
    // than let a tagged child through.
    fn walk_type(dt: &arrow_schema::DataType) -> Result<(), IpcError> {
        use arrow_schema::DataType;
        match dt {
            DataType::Struct(fields) => fields.iter().try_for_each(|f| walk(f.as_ref())),
            DataType::List(item)
            | DataType::ListView(item)
            | DataType::LargeList(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => walk(item.as_ref()),
            DataType::Union(fields, _) => fields.iter().try_for_each(|(_, f)| walk(f.as_ref())),
            DataType::RunEndEncoded(run_ends, values) => {
                walk(run_ends.as_ref())?;
                walk(values.as_ref())
            }
            // A dictionary's value type is a bare `DataType` with no `Field`
            // wrapper, so there is no metadata to check — but it can still be a
            // struct or list holding a tagged child.
            DataType::Dictionary(key, value) => {
                walk_type(key.as_ref())?;
                walk_type(value.as_ref())
            }
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => Ok(()),
        }
    }

    schema.fields().iter().try_for_each(|f| walk(f.as_ref()))
}

/// Run [`reject_secret_handles`] over every batch — the FU-2 membrane shared by
/// all encode/decode paths so a secret-handle column is rejected regardless of
/// single- vs multi-batch shape.
fn reject_all(batches: &[RecordBatch]) -> Result<(), IpcError> {
    batches.iter().try_for_each(reject_secret_handles)
}

/// Encode a `RecordBatch` as Arrow IPC stream bytes.
///
/// Output: schema header + one record batch + end-of-stream marker —
/// suitable for one-shot transmission across a wasm boundary.
///
/// # Errors
///
/// Returns [`IpcError::Arrow`] if the writer cannot serialize the
/// batch (e.g., schema-incompatible types).
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, IpcError> {
    reject_secret_handles(batch)?;
    let mut buf: Vec<u8> = Vec::with_capacity(estimate_size(batch));
    write_stream(&mut buf, batch.schema(), std::slice::from_ref(batch))?;
    Ok(buf)
}

/// Encode multiple `RecordBatch`es sharing a schema as one IPC stream.
///
/// Useful for procedure plugins that ship a series of yielded rows in
/// one call. All batches must use the same schema (Arrow IPC stream
/// invariant).
///
/// # Errors
///
/// - [`IpcError::EmptyBatchInput`] if `batches` is empty.
/// - [`IpcError::Arrow`] if the writer rejects the batches.
pub fn encode_batches(batches: &[RecordBatch]) -> Result<Vec<u8>, IpcError> {
    let first = batches.first().ok_or(IpcError::EmptyBatchInput)?;
    reject_all(batches)?;
    let mut buf: Vec<u8> = Vec::with_capacity(estimate_size(first).saturating_mul(batches.len()));
    write_stream(&mut buf, first.schema(), batches)?;
    Ok(buf)
}

/// Write `batches` (assumed to share `schema`) to `buf` as one IPC stream.
fn write_stream(
    buf: &mut Vec<u8>,
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<(), IpcError> {
    let mut w = StreamWriter::try_new(buf, schema.as_ref())
        .map_err(|e| IpcError::Arrow(format!("writer setup: {e}")))?;
    for b in batches {
        w.write(b)
            .map_err(|e| IpcError::Arrow(format!("write batch: {e}")))?;
    }
    w.finish()
        .map_err(|e| IpcError::Arrow(format!("finish: {e}")))?;
    Ok(())
}

/// Decode the single `RecordBatch` from Arrow IPC stream bytes.
///
/// `encode_batch` writes exactly one batch, so any well-formed stream
/// from this codec carries one batch (or zero, when the plugin produced
/// no rows). Multiple batches indicate a malformed or malicious sender
/// and are rejected.
///
/// Returns `None` if the stream contained only an end-of-stream marker.
///
/// # Errors
///
/// Returns [`IpcError::Arrow`] if the bytes are malformed or if the
/// stream contains more than one batch. The previous form used
/// `Vec::pop()` and silently returned the *last* batch when more than
/// one was present, contradicting the "first batch" contract its
/// documentation promised.
pub fn decode_batch(bytes: &[u8]) -> Result<Option<RecordBatch>, IpcError> {
    let batches = read_stream(bytes, "read batch")?;
    // FU-2: a single-batch stream is still an inbound boundary — reject any
    // secret-handle column, symmetric with `decode_batches` / `encode_batch`.
    // (decode_batch is the hot path used by every scalar/aggregate adapter.)
    reject_all(&batches)?;
    match batches.len() {
        0 => Ok(None),
        1 => Ok(batches.into_iter().next()),
        n => Err(IpcError::Arrow(format!(
            "decode_batch expects a single-batch stream, got {n} batches"
        ))),
    }
}

/// Decode every `RecordBatch` from Arrow IPC stream bytes.
///
/// # Errors
///
/// Returns [`IpcError::Arrow`] if the bytes are malformed.
pub fn decode_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, IpcError> {
    let batches = read_stream(bytes, "read batches")?;
    // FU-2: reject any incoming batch that carries a secret-handle column.
    // Symmetric with the encode path so a malicious plugin can't smuggle a
    // handle back across the boundary either.
    reject_all(&batches)?;
    Ok(batches)
}

/// Build a `StreamReader` over `bytes` and collect all batches.
/// `read_label` is used only for error messages so each caller's
/// failure context (`"read batch"` vs `"read batches"`) is preserved.
fn read_stream(bytes: &[u8], read_label: &str) -> Result<Vec<RecordBatch>, IpcError> {
    let reader = StreamReader::try_new(bytes, None)
        .map_err(|e| IpcError::Arrow(format!("reader setup: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| IpcError::Arrow(format!("{read_label}: {e}")))
}

fn estimate_size(batch: &RecordBatch) -> usize {
    // ~16 bytes/cell + 4 KiB schema overhead. Writer grows on demand.
    let rows = batch.num_rows();
    let cols = batch.num_columns();
    rows.saturating_mul(cols).saturating_mul(16) + 4096
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
        ListArray, StringArray, StructArray, TimestampMillisecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

    fn schema_for(name: &str, dt: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, dt, true)]))
    }

    fn one_col_batch(name: &str, col: Arc<dyn arrow::array::Array>) -> RecordBatch {
        let dt = col.data_type().clone();
        let schema = schema_for(name, dt);
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn round_trip_int64() {
        let arr: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = one_col_batch("x", arr);
        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap().unwrap();
        assert_eq!(decoded.num_rows(), 3);
    }

    #[test]
    fn round_trip_int32_float32_float64() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i32", DataType::Int32, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
        ]));
        let i: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![1, 2]));
        let f32a: Arc<dyn arrow::array::Array> = Arc::new(Float32Array::from(vec![1.5_f32, 2.5]));
        let f64a: Arc<dyn arrow::array::Array> = Arc::new(Float64Array::from(vec![10.5_f64, 20.5]));
        let batch = RecordBatch::try_new(schema, vec![i, f32a, f64a]).unwrap();
        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap().unwrap();
        assert_eq!(decoded.num_rows(), 2);
        let f64_out = decoded
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((f64_out.value(1) - 20.5).abs() < f64::EPSILON);
    }

    #[test]
    fn round_trip_utf8_strings_including_unicode() {
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(StringArray::from(vec!["hello", "naïve", "🌳", ""]));
        let batch = one_col_batch("s", arr);
        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap().unwrap();
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(2), "🌳");
        assert_eq!(col.value(3), "");
    }

    #[test]
    fn round_trip_booleans_with_nulls() {
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        let batch = one_col_batch("b", arr);
        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap().unwrap();
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.is_null(1));
        assert!(col.value(0));
        assert!(!col.value(2));
    }

    #[test]
    fn round_trip_timestamp_ms() {
        let arr: Arc<dyn arrow::array::Array> = Arc::new(
            TimestampMillisecondArray::from(vec![1_700_000_000_000_i64, 1_800_000_000_000])
                .with_timezone_opt::<&str>(None),
        );
        let batch = one_col_batch("ts", arr);
        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap().unwrap();
        assert!(matches!(
            decoded.schema().field(0).data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, _)
        ));
    }

    #[test]
    fn round_trip_large_binary_for_cypher_values() {
        let arr: Arc<dyn arrow::array::Array> = Arc::new(LargeBinaryArray::from(vec![
            &[1_u8, 2, 3][..],
            &[4, 5, 6, 7],
        ]));
        let batch = one_col_batch("v", arr);
        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap().unwrap();
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(col.value(0), &[1, 2, 3]);
        assert_eq!(col.value(1), &[4, 5, 6, 7]);
    }

    #[test]
    fn round_trip_list_of_int64() {
        let values: Arc<dyn arrow::array::Array> =
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5, 6]));
        let offsets = OffsetBuffer::new(vec![0_i32, 2, 5, 6].into());
        let field = Arc::new(Field::new("item", DataType::Int64, true));
        let list = ListArray::new(field, offsets, values, None);
        let arr: Arc<dyn arrow::array::Array> = Arc::new(list);
        let batch = one_col_batch("xs", arr);
        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap().unwrap();
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(col.len(), 3);
        assert_eq!(col.value_length(1), 3);
    }

    #[test]
    fn round_trip_struct_array() {
        let id: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![10, 20]));
        let label: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(vec!["a", "b"]));
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        let s = StructArray::new(fields, vec![id, label], None);
        let arr: Arc<dyn arrow::array::Array> = Arc::new(s);
        let batch = one_col_batch("rec", arr);
        let encoded = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&encoded).unwrap().unwrap();
        assert_eq!(decoded.num_rows(), 2);
        assert!(matches!(
            decoded.schema().field(0).data_type(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn decode_empty_stream_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, schema.as_ref()).unwrap();
            w.finish().unwrap();
        }
        assert!(decode_batch(&buf).unwrap().is_none());
    }

    #[test]
    fn decode_garbage_bytes_is_arrow_ipc_error() {
        let err = decode_batch(b"not arrow ipc").unwrap_err();
        assert!(matches!(err, IpcError::Arrow(_)));
    }

    #[test]
    fn encode_batches_emits_multiple_in_one_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let a: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let b: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![3_i64, 4, 5]));
        let ba = RecordBatch::try_new(schema.clone(), vec![a]).unwrap();
        let bb = RecordBatch::try_new(schema, vec![b]).unwrap();
        let encoded = encode_batches(&[ba, bb]).unwrap();
        let all = decode_batches(&encoded).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].num_rows(), 2);
        assert_eq!(all[1].num_rows(), 3);
    }

    #[test]
    fn encode_batches_rejects_empty_input() {
        let err = encode_batches(&[]).unwrap_err();
        assert!(matches!(err, IpcError::EmptyBatchInput));
    }

    // ── FU-2: secret-handle leak rejection ─────────────────────────

    fn secret_tagged_field(name: &str) -> Field {
        Field::new(name, DataType::FixedSizeBinary(8), false).with_metadata(
            std::collections::HashMap::from([(
                "ARROW:extension:name".to_owned(),
                SECRET_HANDLE_EXTENSION.to_owned(),
            )]),
        )
    }

    /// The membrane walked Struct / List / LargeList / FixedSizeList / Map and
    /// returned `Ok(())` for every other `DataType`, so a tagged field buried in
    /// a union, a run-end-encoded child, either list-view flavour or a
    /// dictionary value crossed the boundary unrejected. `decode_batches` takes
    /// its schema straight from the guest-supplied IPC stream, so the guest
    /// chooses the nesting — each of these was reachable.
    #[test]
    fn membrane_rejects_secret_handle_inside_every_nested_type() {
        use arrow_schema::{UnionFields, UnionMode};
        let secret = || Arc::new(secret_tagged_field("handle"));
        let plain = || Arc::new(Field::new("id", DataType::Int64, false));

        let nested: Vec<(&str, DataType)> = vec![
            (
                "union_sparse",
                DataType::Union(
                    UnionFields::try_new(vec![0, 1], vec![plain(), secret()]).unwrap(),
                    UnionMode::Sparse,
                ),
            ),
            (
                "union_dense",
                DataType::Union(
                    UnionFields::try_new(vec![0, 1], vec![plain(), secret()]).unwrap(),
                    UnionMode::Dense,
                ),
            ),
            (
                "run_end_values",
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    secret(),
                ),
            ),
            ("list_view", DataType::ListView(secret())),
            ("large_list_view", DataType::LargeListView(secret())),
            (
                "dictionary_value",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Struct(Fields::from(vec![
                        Field::new("id", DataType::Int64, false),
                        secret_tagged_field("handle"),
                    ]))),
                ),
            ),
            // The already-covered cases, kept here so the whole nesting family
            // is asserted in one place.
            (
                "struct",
                DataType::Struct(Fields::from(vec![secret_tagged_field("handle")])),
            ),
            ("list", DataType::List(secret())),
            ("map", DataType::Map(secret(), false)),
        ];

        for (name, dt) in nested {
            let schema = Schema::new(vec![Field::new(name, dt, false)]);
            match reject_secret_handles_in_schema(&schema) {
                Err(IpcError::SecretLeakAttempt { column }) => {
                    assert_eq!(column, "handle", "wrong column named for {name}");
                }
                Ok(()) => panic!("{name} let a secret-handle field through the membrane"),
                Err(other) => panic!("{name}: expected SecretLeakAttempt, got {other:?}"),
            }
        }
    }

    /// The schema-level cases above share their walk with the batch path; this
    /// proves a real `RecordBatch` carrying a union still reaches it, so the
    /// seam added for testability did not bypass the membrane.
    #[test]
    fn encode_batch_rejects_secret_handle_inside_union() {
        use arrow::array::{Int64Array, UnionArray};
        use arrow::buffer::ScalarBuffer;
        use arrow_schema::{UnionFields, UnionMode};

        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Arc::new(Field::new("id", DataType::Int64, false)),
                Arc::new(secret_tagged_field("handle")),
            ],
        )
        .unwrap();
        let ids: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1, 2]));
        let handles: Arc<dyn arrow::array::Array> = Arc::new(
            arrow::array::FixedSizeBinaryArray::try_from_iter(
                [[0u8; 8], [1; 8]].iter().map(|b| b.as_slice()),
            )
            .unwrap(),
        );
        let union = UnionArray::try_new(
            fields.clone(),
            ScalarBuffer::from(vec![0i8, 1]),
            None,
            vec![ids, handles],
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(fields, UnionMode::Sparse),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(union)]).unwrap();
        match encode_batch(&batch) {
            Err(IpcError::SecretLeakAttempt { column }) => assert_eq!(column, "handle"),
            Ok(_) => panic!("encode_batch must reject a secret handle inside a union"),
            Err(other) => panic!("expected SecretLeakAttempt, got {other:?}"),
        }
    }

    /// FU-2 acceptance: `encode_batch` refuses any column tagged with
    /// the `uni-db.secret-handle` Arrow extension and returns
    /// `IpcError::SecretLeakAttempt` naming the offending column.
    #[test]
    fn encode_batch_rejects_secret_handle_column() {
        use arrow::array::FixedSizeBinaryArray;
        let schema = Arc::new(Schema::new(vec![secret_tagged_field("api_key_handle")]));
        let arr =
            FixedSizeBinaryArray::try_from_iter([[0u8; 8], [1; 8]].iter().map(|b| b.as_slice()))
                .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        match encode_batch(&batch) {
            Ok(_) => panic!("encode_batch must reject secret-handle columns"),
            Err(IpcError::SecretLeakAttempt { column }) => {
                assert_eq!(column, "api_key_handle");
            }
            Err(other) => panic!("expected SecretLeakAttempt, got {other:?}"),
        }
    }

    /// FU-2 acceptance: `decode_batches` symmetrically rejects an
    /// incoming stream that smuggles a secret-handle column back
    /// across the boundary.
    #[test]
    fn decode_batches_rejects_secret_handle_column() {
        use arrow::array::FixedSizeBinaryArray;
        let plain_field = Field::new("api_key_handle", DataType::FixedSizeBinary(8), false);
        let schema = Arc::new(Schema::new(vec![plain_field]));
        let arr =
            FixedSizeBinaryArray::try_from_iter([[0u8; 8]].iter().map(|b| b.as_slice())).unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let encoded = encode_batch(&batch).unwrap();
        // Now corrupt the encoded bytes by re-encoding with the
        // extension marker present. This simulates a hostile plugin
        // tagging its output column to try to exfiltrate a handle.
        let tagged_schema = Arc::new(Schema::new(vec![secret_tagged_field("api_key_handle")]));
        let arr2 =
            FixedSizeBinaryArray::try_from_iter([[0u8; 8]].iter().map(|b| b.as_slice())).unwrap();
        let tagged = RecordBatch::try_new(tagged_schema, vec![Arc::new(arr2)]).unwrap();
        // Build the tagged stream directly (bypassing `encode_batch`
        // which would have rejected it).
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, tagged.schema().as_ref()).unwrap();
            w.write(&tagged).unwrap();
            w.finish().unwrap();
        }
        // The decode side must reject it.
        match decode_batches(&buf) {
            Ok(_) => panic!("decode_batches must reject secret-handle columns"),
            Err(IpcError::SecretLeakAttempt { column }) => {
                assert_eq!(column, "api_key_handle");
            }
            Err(other) => panic!("expected SecretLeakAttempt, got {other:?}"),
        }
        // Sanity-check: encoding the *un-tagged* version works.
        assert!(!encoded.is_empty());
    }

    /// FU-2 regression: the single-batch `decode_batch` path (the hot path for
    /// every scalar/aggregate adapter) must reject a smuggled secret-handle
    /// column too — not just the multi-batch `decode_batches`.
    #[test]
    fn decode_batch_rejects_secret_handle_column() {
        use arrow::array::FixedSizeBinaryArray;
        let tagged_schema = Arc::new(Schema::new(vec![secret_tagged_field("api_key_handle")]));
        let arr =
            FixedSizeBinaryArray::try_from_iter([[0u8; 8]].iter().map(|b| b.as_slice())).unwrap();
        let tagged = RecordBatch::try_new(tagged_schema, vec![Arc::new(arr)]).unwrap();
        // Build a single-batch tagged stream directly (bypassing `encode_batch`,
        // which would have rejected it on the way out).
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, tagged.schema().as_ref()).unwrap();
            w.write(&tagged).unwrap();
            w.finish().unwrap();
        }
        match decode_batch(&buf) {
            Ok(_) => panic!("decode_batch must reject secret-handle columns"),
            Err(IpcError::SecretLeakAttempt { column }) => {
                assert_eq!(column, "api_key_handle");
            }
            Err(other) => panic!("expected SecretLeakAttempt, got {other:?}"),
        }
    }

    /// FU-2 acceptance: nested struct/list fields are walked, so a
    /// plugin can't bury a secret-handle inside a struct column.
    #[test]
    fn encode_batch_rejects_secret_handle_inside_struct() {
        use arrow::array::Int64Array;
        let plain = Field::new("id", DataType::Int64, false);
        let secret = secret_tagged_field("handle");
        let struct_field = Field::new(
            "rec",
            DataType::Struct(Fields::from(vec![plain, secret])),
            false,
        );
        let schema = Arc::new(Schema::new(vec![struct_field]));
        let id_arr: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![1, 2]));
        let secret_arr: Arc<dyn arrow::array::Array> = Arc::new(
            arrow::array::FixedSizeBinaryArray::try_from_iter(
                [[0u8; 8], [1; 8]].iter().map(|b| b.as_slice()),
            )
            .unwrap(),
        );
        let s = StructArray::new(
            Fields::from(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("handle", DataType::FixedSizeBinary(8), false).with_metadata(
                    std::collections::HashMap::from([(
                        "ARROW:extension:name".to_owned(),
                        SECRET_HANDLE_EXTENSION.to_owned(),
                    )]),
                ),
            ]),
            vec![id_arr, secret_arr],
            None,
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(s)]).unwrap();
        match encode_batch(&batch) {
            Ok(_) => panic!("encode_batch must reject nested secret-handle"),
            Err(IpcError::SecretLeakAttempt { column }) => {
                assert_eq!(column, "handle");
            }
            Err(other) => panic!("expected SecretLeakAttempt, got {other:?}"),
        }
    }
}
