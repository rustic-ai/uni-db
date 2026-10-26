#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5g — ephemeral (transient, in-query) graph entities.
//!
//! Covers:
//! - `Vid::ephemeral` / `Eid::ephemeral` bit-tagging round-trips.
//! - `QueryProcedureHost::allocate_transient_id()` monotonicity + range.
//! - `uni.create.vNode` / `uni.create.vEdge` procedure invocations.
//! - `UniError::EphemeralWriteAttempt` reachability.
//!
//! Storage write-path rejection (the planner-level `execute_set_*` /
//! `execute_delete_*` gate added in
//! `crates/uni-query/src/query/executor/write.rs`) is exercised
//! indirectly via the integration suites under `crates/uni/tests/`
//! because the executor needs a full Writer / L0Manager stack to drive
//! end-to-end.

// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Int64Array, LargeBinaryArray, StringArray, StructArray, UInt64Array};
use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use tempfile::tempdir;
use uni_algo::algo::AlgorithmRegistry;
use uni_common::UniError;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::SchemaManager;
use uni_plugin::traits::procedure::{ProcedureContext, ProcedurePlugin};
use uni_query::procedures_plugin::create::{VEdgeProcedure, VNodeProcedure};
use uni_query::query::executor::procedure_host::QueryProcedureHost;
use uni_store::storage::manager::StorageManager;

// ─────────────────────────── id bit-tagging ──────────────────────────────

#[test]
fn vid_ephemeral_round_trip() {
    let v = Vid::ephemeral(42);
    assert!(v.is_ephemeral());
    assert_eq!(v.transient_id(), Some(42));
    assert!(!v.is_invalid());
    assert!(v.as_u64() & (1u64 << 63) != 0);
    assert_eq!(v.as_u64() & !(1u64 << 63), 42);
}

#[test]
fn eid_ephemeral_round_trip() {
    let e = Eid::ephemeral(7);
    assert!(e.is_ephemeral());
    assert_eq!(e.transient_id(), Some(7));
    assert!(!e.is_invalid());
}

#[test]
fn stored_vid_is_not_ephemeral() {
    let v = Vid::new(123);
    assert!(!v.is_ephemeral());
    assert_eq!(v.transient_id(), None);
}

#[test]
fn invalid_vid_is_not_classified_as_ephemeral() {
    assert!(!Vid::INVALID.is_ephemeral());
    assert_eq!(Vid::INVALID.transient_id(), None);
    assert!(!Eid::INVALID.is_ephemeral());
}

#[test]
fn ephemeral_overflow_returns_invalid() {
    let bad = Vid::ephemeral(1u64 << 63);
    assert!(bad.is_invalid());
    assert!(!bad.is_ephemeral());
}

#[test]
fn ephemeral_write_attempt_error_format() {
    let e = UniError::EphemeralWriteAttempt {
        kind: "node",
        id: 17,
    };
    let msg = format!("{e}");
    assert!(msg.contains("ephemeral"), "msg: {msg}");
    assert!(msg.contains("node"), "msg: {msg}");
    assert!(msg.contains("17"), "msg: {msg}");
}

// ────────────────────── procedure invocations ────────────────────────

async fn fresh_host() -> QueryProcedureHost {
    let tmp = tempdir().expect("tempdir");
    let schema_manager = SchemaManager::load(&tmp.path().join("schema.json"))
        .await
        .expect("schema");
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(tmp.path().join("storage").to_str().unwrap(), schema_manager)
            .await
            .expect("storage"),
    );
    let algo_registry: Arc<AlgorithmRegistry> = Arc::new(AlgorithmRegistry::new());
    // Leak tempdir so the storage handle stays alive for the test.
    Box::leak(Box::new(tmp));
    QueryProcedureHost::from_components(storage, Some(algo_registry), None)
}

async fn collect_one(
    stream: datafusion::execution::SendableRecordBatchStream,
) -> arrow_array::RecordBatch {
    let mut s = stream;
    let first = s.next().await.expect("stream empty").expect("stream error");
    assert!(s.next().await.is_none(), "expected single batch");
    first
}

#[tokio::test]
async fn host_allocate_transient_id_monotonic_within_63_bits() {
    let host = fresh_host().await;
    let a = host.allocate_transient_id();
    let b = host.allocate_transient_id();
    let c = host.allocate_transient_id();
    assert!(a < b && b < c);
    let mask = !(1u64 << 63);
    assert_eq!(a & mask, a);
    assert_eq!(b & mask, b);
    assert_eq!(c & mask, c);
}

#[tokio::test]
async fn vnode_emits_single_typed_node_column() {
    // M5g — vNode now yields a single canonical Node column (`vid`,
    // Int64, tagged `_yield_kind = node_vid_source`) rather than the
    // legacy 4-primitive `(vid, labels, properties, ephemeral)` quad.
    // The planner expands this single yield into the full node-shape
    // column tuple when the caller writes `YIELD node` against a
    // surrounding query; standalone unit invocations see the bare
    // Int64 column declared by the signature.
    let host = fresh_host().await;
    let ctx = ProcedureContext::new().with_host(&host);

    let labels_arg = ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
        br#"["Tag","Inner"]"#.to_vec(),
    )));
    let props_arg = ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
        br#"{"x":1,"y":"hi"}"#.to_vec(),
    )));

    let proc = VNodeProcedure;
    let stream = proc.invoke(ctx, &[labels_arg, props_arg]).expect("invoke");
    let batch = collect_one(stream).await;

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1, "expected single typed Node column");

    let field = batch.schema().field(0).clone();
    assert_eq!(field.name(), "vid");
    assert_eq!(field.data_type(), &DataType::Int64);
    assert_eq!(
        field.metadata().get("_yield_kind").map(String::as_str),
        Some("node_vid_source"),
        "signature must opt into planner node-shape expansion via metadata tag"
    );

    let vid_raw = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("vid Int64")
        .value(0);
    let v = Vid::new(vid_raw as u64);
    assert!(
        v.is_ephemeral(),
        "vid must have EPHEMERAL_BIT set: {vid_raw:#x}"
    );
}

#[tokio::test]
async fn vnode_each_invocation_mints_distinct_id() {
    let host = fresh_host().await;
    let proc = VNodeProcedure;
    let mut seen = std::collections::HashSet::new();
    for _ in 0..8 {
        let ctx = ProcedureContext::new().with_host(&host);
        let stream = proc.invoke(ctx, &[]).expect("invoke");
        let batch = collect_one(stream).await;
        let vid = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert!(seen.insert(vid), "duplicate transient vid: {vid}");
        assert!(Vid::new(vid as u64).is_ephemeral());
    }
}

#[tokio::test]
async fn vedge_emits_single_typed_edge_struct() {
    // M5g — vEdge yields a single canonical edge `Struct(_eid,
    // _type_name, _src, _dst, properties)` column rather than the
    // legacy 6-primitive quintuple.
    let host = fresh_host().await;
    let ctx = ProcedureContext::new().with_host(&host);

    let src_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(11)));
    let type_arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("KNOWS".to_owned())));
    let props_arg = ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
        br#"{"weight":0.75}"#.to_vec(),
    )));
    let dst_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(22)));

    let proc = VEdgeProcedure;
    let stream = proc
        .invoke(ctx, &[src_arg, type_arg, props_arg, dst_arg])
        .expect("invoke");
    let batch = collect_one(stream).await;

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1, "expected single typed Edge column");

    let field = batch.schema().field(0).clone();
    assert_eq!(field.name(), "edge");
    let DataType::Struct(struct_fields) = field.data_type() else {
        panic!("expected Struct edge column, got {:?}", field.data_type());
    };
    let names: Vec<&str> = struct_fields.iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["_eid", "_type_name", "_src", "_dst", "properties"]
    );

    let struct_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("edge column is StructArray");
    let eid = struct_arr
        .column_by_name("_eid")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert!(Eid::new(eid).is_ephemeral());
    assert_eq!(
        struct_arr
            .column_by_name("_type_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "KNOWS"
    );
    assert_eq!(
        struct_arr
            .column_by_name("_src")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        11
    );
    assert_eq!(
        struct_arr
            .column_by_name("_dst")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        22
    );
    // Properties decode back to a Map carrying the original entries.
    let props_bytes = struct_arr
        .column_by_name("properties")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .value(0);
    let decoded = uni_common::cypher_value_codec::decode(props_bytes).expect("decode props");
    let uni_common::Value::Map(map) = decoded else {
        panic!("expected Map for edge properties");
    };
    assert_eq!(map.get("weight"), Some(&uni_common::Value::Float(0.75)));
}

#[tokio::test]
async fn vedge_requires_host() {
    let proc = VEdgeProcedure;
    let ctx = ProcedureContext::new();
    let src_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
    let type_arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("R".to_owned())));
    let props_arg = ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(b"{}".to_vec())));
    let dst_arg = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));
    let res = proc.invoke(ctx, &[src_arg, type_arg, props_arg, dst_arg]);
    let err = match res {
        Ok(_) => panic!("vedge invocation without host must fail"),
        Err(e) => e,
    };
    let msg = format!("{err:?}");
    assert!(
        msg.contains("QueryProcedureHost"),
        "expected host-required error, got: {msg}"
    );
}

// ─────────────────── round-trip via Cypher planner (M5g) ────────────────
//
// The single-column Node yield must round-trip through the planner:
// `CALL uni.create.vNode(...) YIELD node` followed by downstream
// expression evaluation reading `node.foo` exercises the
// `node_vid_source` expansion path end-to-end.

#[tokio::test]
async fn vnode_typed_yield_round_trips_through_cypher_expression() {
    use uni_query::query::executor::Executor;
    use uni_query::query::planner::QueryPlanner;

    let tmp = tempdir().expect("tempdir");
    let schema_manager = SchemaManager::load(&tmp.path().join("schema.json"))
        .await
        .expect("schema");
    let schema_manager_arc = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            tmp.path().join("storage").to_str().unwrap(),
            Arc::clone(&schema_manager_arc),
        )
        .await
        .expect("storage"),
    );

    // `Executor::new` auto-wires the default host plugin registry,
    // which already includes `uni.create.vNode` via
    // `procedures_plugin::default_host_plugin_registry`.
    let executor = Executor::new(Arc::clone(&storage));

    let planner = QueryPlanner::new(storage.schema_manager().schema());
    let ast = uni_cypher::parse(
        r#"CALL uni.create.vNode(['Ghost'], {answer: 42}) YIELD node RETURN node.answer AS got"#,
    )
    .expect("parse");
    let plan = planner.plan(ast).expect("plan");

    let prop_manager = Arc::new(uni_store::runtime::property_manager::PropertyManager::new(
        Arc::clone(&storage),
        storage.schema_manager_arc(),
        100,
    ));

    let rows = executor
        .execute(plan, &prop_manager, &std::collections::HashMap::new())
        .await
        .expect("execute");

    assert_eq!(rows.len(), 1, "expected one row from CALL: {rows:?}");
    let got = rows[0]
        .get("got")
        .or_else(|| rows[0].get("node.answer"))
        .unwrap_or_else(|| panic!("missing `got` column; full row: {:?}", rows[0]));
    // The property was JSON-decoded as a number; downstream conversion
    // may surface it as either Int or Float depending on codec path —
    // accept either provided the numeric value matches. If it's a
    // Bytes (CypherValue codec) decode it.
    let n = got
        .as_i64()
        .map(|i| i as f64)
        .or_else(|| got.as_f64())
        .or_else(|| match got {
            uni_common::Value::Bytes(b) => {
                let v = uni_common::cypher_value_codec::decode(b).ok()?;
                v.as_i64().map(|i| i as f64).or_else(|| v.as_f64())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("got column is non-numeric: {got:?}"));
    assert!((n - 42.0).abs() < f64::EPSILON, "expected 42, got {got:?}");
}

// ───────────────────── ephemeral entities as values ─────────────────────
//
// A virtual node is a node (APOC: `apoc.create.vNode` returns a Node with a
// negative id). Its id has the ephemeral bit — the sign bit — set, so it
// travels as a negative `Int64`, and the entity recognisers that required a
// non-negative id read the yielded `{_vid, _labels, ...}` struct as a plain
// map: `YIELD node RETURN node` returned a Map.

/// Plan and run `cypher` against a fresh, empty store.
async fn run_cypher(cypher: &str) -> Vec<std::collections::HashMap<String, uni_common::Value>> {
    use uni_query::query::executor::Executor;
    use uni_query::query::planner::QueryPlanner;

    let tmp = tempdir().expect("tempdir");
    let schema_manager = Arc::new(
        SchemaManager::load(&tmp.path().join("schema.json"))
            .await
            .expect("schema"),
    );
    let storage = Arc::new(
        StorageManager::new(
            tmp.path().join("storage").to_str().unwrap(),
            Arc::clone(&schema_manager),
        )
        .await
        .expect("storage"),
    );
    let executor = Executor::new(Arc::clone(&storage));
    let planner = QueryPlanner::new(storage.schema_manager().schema());
    let plan = planner
        .plan(uni_cypher::parse(cypher).expect("parse"))
        .expect("plan");
    let prop_manager = Arc::new(uni_store::runtime::property_manager::PropertyManager::new(
        Arc::clone(&storage),
        storage.schema_manager_arc(),
        100,
    ));
    executor
        .execute(plan, &prop_manager, &std::collections::HashMap::new())
        .await
        .expect("execute")
}

#[tokio::test]
async fn vnode_yields_a_node_with_a_negative_id() {
    let rows = run_cypher(
        "CALL uni.create.vNode(['Ghost'], {answer: 42}) YIELD node \
         RETURN node, labels(node) AS l, id(node) AS i",
    )
    .await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    let node = match row.get("node") {
        Some(uni_common::Value::Node(n)) => n,
        other => panic!("`node` should be a Node, got {other:?}"),
    };
    assert!(node.vid.is_ephemeral(), "{node:?}");
    assert_eq!(node.labels, vec!["Ghost".to_string()]);
    assert_eq!(
        row.get("l"),
        Some(&uni_common::Value::List(vec![uni_common::Value::String(
            "Ghost".into()
        )]))
    );
    match row.get("i") {
        Some(uni_common::Value::Int(i)) => {
            assert!(*i < 0, "an ephemeral id reads as negative: {i}");
            #[allow(clippy::cast_sign_loss)]
            let bits = *i as u64;
            assert_eq!(bits, node.vid.as_u64(), "id(node) is the node's vid bits");
        }
        other => panic!("id(node) should be an integer, got {other:?}"),
    }
}

/// A virtual relationship between two virtual nodes.
///
/// The second `CALL` runs after another clause, so it is planned as a
/// per-row `Apply` sub-plan. Its output was typed by a logical walk that saw a
/// node yield as one `Utf8` column, so `id(b)` failed to plan with
/// `No field named "b._vid"`.
#[tokio::test]
async fn vedge_between_vnodes_connects_them() {
    let rows = run_cypher(
        "CALL uni.create.vNode(['A'], {}) YIELD node AS a \
         CALL uni.create.vNode(['B'], {k: 2}) YIELD node AS b \
         CALL uni.create.vEdge(id(a), 'T', {}, id(b)) YIELD edge \
         RETURN a, b, edge, labels(b) AS lb, b.k AS bk, id(a) <> id(b) AS distinct_ids",
    )
    .await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    let (a, b) = match (row.get("a"), row.get("b")) {
        (Some(uni_common::Value::Node(a)), Some(uni_common::Value::Node(b))) => (a, b),
        other => panic!("`a` and `b` should be Nodes, got {other:?}"),
    };
    assert!(a.vid.is_ephemeral() && b.vid.is_ephemeral(), "{a:?} {b:?}");
    assert_eq!(b.labels, vec!["B".to_string()]);
    assert_eq!(
        row.get("lb"),
        Some(&uni_common::Value::List(vec![uni_common::Value::String(
            "B".into()
        )]))
    );
    assert_eq!(row.get("bk"), Some(&uni_common::Value::Int(2)));
    assert_eq!(
        row.get("distinct_ids"),
        Some(&uni_common::Value::Bool(true))
    );
    match row.get("edge") {
        Some(uni_common::Value::Edge(e)) => {
            assert!(e.eid.is_ephemeral(), "{e:?}");
            assert_eq!((e.src, e.dst), (a.vid, b.vid), "{e:?}");
            assert_eq!(e.edge_type, "T");
        }
        other => panic!("`edge` should be an Edge, got {other:?}"),
    }
}

/// A node-yielding `CALL` per input row: each row gets its own node, with the
/// property and labels it was minted with.
#[tokio::test]
async fn vnode_called_per_unwound_row() {
    let rows = run_cypher(
        "UNWIND [1, 2, 3] AS i \
         CALL uni.create.vNode(['V'], {i: i}) YIELD node \
         RETURN i, node.i AS ni, labels(node) AS l, id(node) AS id, node \
         ORDER BY i",
    )
    .await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    let mut ids = std::collections::HashSet::new();
    for (k, row) in rows.iter().enumerate() {
        let i = i64::try_from(k).unwrap() + 1;
        assert_eq!(row.get("i"), Some(&uni_common::Value::Int(i)), "{row:?}");
        assert_eq!(row.get("ni"), Some(&uni_common::Value::Int(i)), "{row:?}");
        assert_eq!(
            row.get("l"),
            Some(&uni_common::Value::List(vec![uni_common::Value::String(
                "V".into()
            )]))
        );
        match (row.get("id"), row.get("node")) {
            (Some(uni_common::Value::Int(id)), Some(uni_common::Value::Node(n))) => {
                assert!(*id < 0, "{row:?}");
                assert!(n.vid.is_ephemeral(), "{row:?}");
                assert_eq!(n.properties.get("i"), Some(&uni_common::Value::Int(i)));
                ids.insert(*id);
            }
            other => panic!("expected an id and a Node, got {other:?}"),
        }
    }
    assert_eq!(ids.len(), 3, "each row should mint its own node");
}
