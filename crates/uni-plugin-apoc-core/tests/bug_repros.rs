//! Runnable repros for the verified correctness findings in
//! `uni-plugin-apoc-core`. Each test drives the REAL public surface: the
//! procedure is installed into a `PluginRegistry` exactly as the host does,
//! looked up by qname, and invoked with the same `ColumnarValue` scalars the
//! executor's `value_to_columnar` would hand it.
//!
//! These are additive test-only files; no production source is modified.

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use uni_common::Value;
use uni_plugin::traits::procedure::ProcedureContext;
use uni_plugin::{Plugin, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_apoc_core::ApocCorePlugin;

/// Install `ApocCorePlugin` into a fresh registry (mirrors the host loader).
fn install() -> PluginRegistry {
    let registry = PluginRegistry::new();
    let plugin = ApocCorePlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut r).expect("register");
    r.commit_to_registry().expect("commit");
    registry
}

/// Invoke `apoc-core::<local>` with `args`, returning the first result batch's
/// single column downcast to `StringArray` value, as a String.
async fn invoke_string(local: &str, args: Vec<ColumnarValue>) -> String {
    let registry = install();
    let q = QName::new("apoc-core", local);
    let entry = registry.procedure(&q).expect("procedure registered");
    let mut stream = entry
        .procedure
        .invoke(ProcedureContext::default(), &args)
        .expect("invoke");
    let batch = stream.next().await.expect("row").expect("ok");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    col.value(0).to_owned()
}

/// Invoke `apoc-core::<local>` returning the first `Float64Array` value.
async fn invoke_f64(local: &str, args: Vec<ColumnarValue>) -> f64 {
    let registry = install();
    let q = QName::new("apoc-core", local);
    let entry = registry.procedure(&q).expect("procedure registered");
    let mut stream = entry
        .procedure
        .invoke(ProcedureContext::default(), &args)
        .expect("invoke");
    let batch = stream.next().await.expect("row").expect("ok");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    col.value(0)
}

/// [1] number.rs:153 — number.toString widens an Int64 scalar through
/// `*v as f64`, silently corrupting integers with magnitude > 2^53 before
/// formatting.
///
/// `9007199254740993` = 2^53 + 1 is an exact i64 not representable in f64;
/// widening rounds it to 9007199254740992.0.
#[tokio::test]
async fn repro_number_tostring_int64_precision_loss() {
    let n: i64 = 9_007_199_254_740_993; // 2^53 + 1
    let out = invoke_string(
        "number.toString",
        vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(n)))],
    )
    .await;

    // BUG: expected "9007199254740993", got "9007199254740992"
    // (repro for src/procedures/number.rs:153 via support.rs:231 `*v as f64`).
    // FIXED (number.rs): Int64 is formatted exactly, no f64 widening.
    assert_eq!(
        out,
        n.to_string(),
        "integer must format exactly (got {out})"
    );
}

/// [2] text.rs — text.repeat's `MAX_SYNTHESIZED_LEN` bound.
///
/// Originally the cap was applied to the repeat COUNT rather than the total
/// synthesized length, so a 100-byte base repeated 1_000_000 times produced
/// 100_000_000 bytes — 100x the intended bound. That was fixed by capping the
/// total length instead.
///
/// **Updated for P3 (breaking).** Capping by clamping is itself the defect:
/// an over-cap call returned a *truncated* string the caller could not tell
/// apart from a complete one. The bound is now enforced by erroring, so this
/// test asserts the error rather than a clipped length. The at-the-cap happy
/// path below proves the bound itself is still exactly 1_000_000 bytes.
#[tokio::test]
async fn repro_text_repeat_cap_ignores_total_length() {
    let base: String = "x".repeat(100); // 100-byte base string
    let count: i64 = 1_000_000; // 100 x 1_000_000 = 100 MB, way over the cap
    let err = invoke_err(
        "text.repeat",
        vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(base))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(count))),
        ],
    )
    .await;
    assert!(
        err.contains("text.repeat") && err.contains("1000000"),
        "over-cap repeat must error and name the cap, got: {err}"
    );

    // Happy path, at exactly the bound: 100 bytes x 10_000 = 1_000_000 bytes.
    let base: String = "x".repeat(100);
    let out = invoke_string(
        "text.repeat",
        vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(base))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(10_000))),
        ],
    )
    .await;
    assert_eq!(
        out.len(),
        1_000_000,
        "at-the-cap repeat must still succeed at exactly MAX_SYNTHESIZED_LEN bytes"
    );
}

/// [3] math.rs:164 — math.round computes `scale = 10f64.powi(precision as i32)`.
/// For precision >= 309 the scale overflows to +inf, making the result NaN
/// (inf/inf) instead of the correctly-rounded value.
#[tokio::test]
async fn repro_math_round_large_precision_yields_nan() {
    let out = invoke_f64(
        "math.round",
        vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(3.5))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(400))),
        ],
    )
    .await;

    // FIXED (math.rs): rounding 3.5 to 400 decimals is a no-op past f64
    // resolution -> 3.5, not NaN.
    assert_eq!(out, 3.5, "round(3.5, 400) must be a no-op, got {out}");
}

// ---------------------------------------------------------------------------
// APOC-compatibility fixes P1-P5
// ---------------------------------------------------------------------------

/// Invoke and return the first `Int64Array` value, or `None` when NULL.
async fn invoke_opt_i64(local: &str, args: Vec<ColumnarValue>) -> Option<i64> {
    let registry = install();
    let q = QName::new("apoc-core", local);
    let entry = registry.procedure(&q).expect("procedure registered");
    let mut stream = entry
        .procedure
        .invoke(ProcedureContext::default(), &args)
        .expect("invoke");
    let batch = stream.next().await.expect("row").expect("ok");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array");
    (!col.is_null(0)).then(|| col.value(0))
}

/// Invoke and return the first `Float64Array` value, or `None` when NULL.
async fn invoke_opt_f64(local: &str, args: Vec<ColumnarValue>) -> Option<f64> {
    let registry = install();
    let q = QName::new("apoc-core", local);
    let entry = registry.procedure(&q).expect("procedure registered");
    let mut stream = entry
        .procedure
        .invoke(ProcedureContext::default(), &args)
        .expect("invoke");
    let batch = stream.next().await.expect("row").expect("ok");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    (!col.is_null(0)).then(|| col.value(0))
}

/// Invoke and return the first `StringArray` value, or `None` when NULL.
async fn invoke_opt_string(local: &str, args: Vec<ColumnarValue>) -> Option<String> {
    let registry = install();
    let q = QName::new("apoc-core", local);
    let entry = registry.procedure(&q).expect("procedure registered");
    let mut stream = entry
        .procedure
        .invoke(ProcedureContext::default(), &args)
        .expect("invoke");
    let batch = stream.next().await.expect("row").expect("ok");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    (!col.is_null(0)).then(|| col.value(0).to_owned())
}

/// Invoke expecting a rejection; returns the error's `Display` string.
async fn invoke_err(local: &str, args: Vec<ColumnarValue>) -> String {
    let registry = install();
    let q = QName::new("apoc-core", local);
    let entry = registry.procedure(&q).expect("procedure registered");
    match entry.procedure.invoke(ProcedureContext::default(), &args) {
        Ok(_) => panic!("{local}: expected an error, got a result stream"),
        Err(e) => e.to_string(),
    }
}

/// P1 — `apoc.convert.toString` on a list must render `[1, 2, 3]`, not NULL.
///
/// The argument is declared `ArgType::CypherValue`, so a list reaches the
/// procedure as an opaque `LargeBinary` envelope. Both live encoders are
/// exercised: the tagged `cypher_value_codec` (scalar-function adapter) and
/// `serde_json` (the procedure CALL dispatcher's `value_to_columnar`).
#[tokio::test]
async fn p1_convert_tostring_renders_list_and_map() {
    let list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);

    let codec_bytes = uni_common::cypher_value_codec::encode(&list);
    let got = invoke_opt_string(
        "convert.toString",
        vec![ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
            codec_bytes,
        )))],
    )
    .await;
    assert_eq!(
        got.as_deref(),
        Some("[1, 2, 3]"),
        "codec-encoded list must render like Neo4j, not NULL"
    );

    let json_bytes = serde_json::to_vec(&list).expect("json encode");
    let got = invoke_opt_string(
        "convert.toString",
        vec![ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
            json_bytes,
        )))],
    )
    .await;
    assert_eq!(
        got.as_deref(),
        Some("[1, 2, 3]"),
        "json-encoded list must render like Neo4j, not NULL"
    );

    // A map renders `{k: v}`.
    let mut m = std::collections::HashMap::new();
    m.insert("a".to_owned(), Value::Int(1));
    let map_bytes = uni_common::cypher_value_codec::encode(&Value::Map(m));
    let got = invoke_opt_string(
        "convert.toString",
        vec![ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
            map_bytes,
        )))],
    )
    .await;
    assert_eq!(got.as_deref(), Some("{a: 1}"), "map must render `{{k: v}}`");

    // NULL stays NULL, and the primitive happy path is untouched.
    let null_bytes = uni_common::cypher_value_codec::encode(&Value::Null);
    assert_eq!(
        invoke_opt_string(
            "convert.toString",
            vec![ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
                null_bytes
            )))],
        )
        .await,
        None,
        "null must stay NULL"
    );
    assert_eq!(
        invoke_opt_string(
            "convert.toString",
            vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(42)))],
        )
        .await
        .as_deref(),
        Some("42"),
        "the primitive path must still work"
    );
}

/// P2 — `apoc.create.uuids(n)` over the cap must error, not silently truncate.
#[tokio::test]
async fn p2_create_uuids_over_cap_errors() {
    let err = invoke_err(
        "create.uuids",
        vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(2_000_000)))],
    )
    .await;
    assert!(
        err.contains("create.uuids") && err.contains("1000000"),
        "over-cap uuids must error and name the cap, got: {err}"
    );

    // Happy path: an in-budget request still returns the requested rows.
    let registry = install();
    let entry = registry
        .procedure(&QName::new("apoc-core", "create.uuids"))
        .expect("registered");
    let mut stream = entry
        .procedure
        .invoke(
            ProcedureContext::default(),
            &[ColumnarValue::Scalar(ScalarValue::Int64(Some(3)))],
        )
        .expect("in-budget request must succeed");
    let batch = stream.next().await.expect("row").expect("ok");
    assert_eq!(batch.num_rows(), 3, "in-budget request must yield 3 rows");
}

/// P4 — `apoc.text.indexOf` must return a CHARACTER index, matching Neo4j
/// and this module's own `text.length`.
#[tokio::test]
async fn p4_text_indexof_is_a_character_index() {
    let got = invoke_opt_i64(
        "text.indexOf",
        vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("cafés".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("s".to_owned()))),
        ],
    )
    .await;
    assert_eq!(
        got,
        Some(4),
        "indexOf('cafés','s') is 4 in Neo4j (chars), not 5 (bytes)"
    );

    // The unit now agrees with `text.length`, which counts chars.
    let len = invoke_opt_i64(
        "text.length",
        vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "cafés".to_owned(),
        )))],
    )
    .await;
    assert_eq!(len, Some(5), "text.length counts chars");

    // ASCII happy path and the not-found sentinel are unchanged.
    assert_eq!(
        invoke_opt_i64(
            "text.indexOf",
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_owned()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("ll".to_owned()))),
            ],
        )
        .await,
        Some(2)
    );
    assert_eq!(
        invoke_opt_i64(
            "text.indexOf",
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_owned()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("zz".to_owned()))),
            ],
        )
        .await,
        Some(-1)
    );
}

/// P5 — the four-function `DecimalFormat` family: `number.parseInt`,
/// `number.parseFloat`, `convert.toInteger`, `convert.toFloat`.
#[tokio::test]
async fn p5_decimal_format_parsing_family() {
    let utf8 = |s: &str| vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.to_owned())))];

    // Integer variants: truncate toward zero, accept grouping separators.
    for proc in ["number.parseInt", "convert.toInteger"] {
        assert_eq!(
            invoke_opt_i64(proc, utf8("3.7")).await,
            Some(3),
            "{proc}: \"3.7\" must be 3"
        );
        assert_eq!(
            invoke_opt_i64(proc, utf8("-3.7")).await,
            Some(-3),
            "{proc}: truncation is toward zero"
        );
        assert_eq!(
            invoke_opt_i64(proc, utf8("1,234")).await,
            Some(1234),
            "{proc}: \"1,234\" must be 1234"
        );
        // Happy path preserved.
        assert_eq!(invoke_opt_i64(proc, utf8("42")).await, Some(42));
        // Genuine garbage stays NULL.
        assert_eq!(
            invoke_opt_i64(proc, utf8("not a number")).await,
            None,
            "{proc}: garbage must stay NULL"
        );
    }

    // Float variants.
    for proc in ["number.parseFloat", "convert.toFloat"] {
        assert_eq!(
            invoke_opt_f64(proc, utf8("1,234.5")).await,
            Some(1234.5),
            "{proc}: grouping separators must parse"
        );
        assert_eq!(invoke_opt_f64(proc, utf8("2.5")).await, Some(2.5));
        assert_eq!(
            invoke_opt_f64(proc, utf8("nope")).await,
            None,
            "{proc}: garbage must stay NULL"
        );
    }
}
