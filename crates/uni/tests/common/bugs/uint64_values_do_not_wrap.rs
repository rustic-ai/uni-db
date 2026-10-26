// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An unsigned 64-bit value above `i64::MAX` is not read as a negative integer.
//!
//! Cypher has one integer type, a signed 64-bit one. A `UInt64` column (a
//! plugin procedure's declared yield, for one) was read into it with `as i64`,
//! which wraps: `u64::MAX` came back as `-1` and `2^63` as `i64::MIN`, a
//! confident wrong answer that also compared, ordered and summed as negative.
//! Such a value now continues as the float it is, as an integer sum does past
//! the 64-bit range.
//!
//! Identities are not values: a node or relationship id with the ephemeral
//! bit set (`uni.create.vNode`) is carried as the `Int64` with the same bits,
//! which `uni.create.vEdge` reads back. That encoding is a bijection and stays.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(uint64_values_do_not_wrap)'

// Rust guideline compliant

use std::sync::Arc;

use anyhow::{Result, bail};
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use uni_db::{Uni, Value};
use uni_plugin::adapter_common::batch_builder::batch_into_stream;
use uni_plugin::traits::procedure::{
    ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, QName};

/// `2^63`, the first unsigned value a signed 64-bit integer cannot hold.
const TWO_POW_63: u64 = 1 << 63;

/// `u64probe.big()` — yields `u64::MAX`, `2^63` and `5` as a `UInt64` column.
struct Big {
    signature: ProcedureSignature,
}

fn signature() -> ProcedureSignature {
    ProcedureSignature {
        args: vec![],
        yields: vec![Field::new("big", DataType::UInt64, true)],
        mode: ProcedureMode::Read,
        side_effects: uni_plugin::SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "Yields unsigned 64-bit values.".to_owned(),
    }
}

impl ProcedurePlugin for Big {
    fn signature(&self) -> &ProcedureSignature {
        &self.signature
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        _args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("big", DataType::UInt64, true)]));
        let arr = Arc::new(UInt64Array::from(vec![u64::MAX, TWO_POW_63, 5])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| FnError::new(0x600, format!("u64probe.big: {e}")))?;
        Ok(batch_into_stream(batch))
    }
}

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let caps = CapabilitySet::from_iter_of([Capability::Procedure]);
    let mut r = PluginRegistrar::new(PluginId::new("u64probe"), &caps, db.plugin_registry());
    r.procedure(
        QName::new("u64probe", "big"),
        signature(),
        Arc::new(Big {
            signature: signature(),
        }),
    )?;
    r.commit_to_registry()?;
    Ok(db)
}

/// The first column of every row of `query`.
async fn column(db: &Uni, query: &str) -> Result<Vec<Value>> {
    let result = db.session().query(query).await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| r.values()[0].clone())
        .collect())
}

#[tokio::test]
async fn uint64_values_do_not_wrap() -> Result<()> {
    let db = open().await?;
    let max = Value::Float(u64::MAX as f64);
    let half = Value::Float(TWO_POW_63 as f64);
    let call = "CALL u64probe.big() YIELD big";
    let cases: Vec<(String, Vec<Value>)> = vec![
        (
            format!("{call} RETURN big"),
            vec![max.clone(), half.clone(), Value::Int(5)],
        ),
        (
            format!("{call} WITH big RETURN big"),
            vec![max.clone(), half.clone(), Value::Int(5)],
        ),
        (
            format!("{call} RETURN big ORDER BY big"),
            vec![Value::Int(5), half.clone(), max.clone()],
        ),
        (
            format!("{call} WITH big WHERE big > 0 RETURN big ORDER BY big"),
            vec![Value::Int(5), half.clone(), max.clone()],
        ),
        (
            format!("{call} RETURN collect(big)"),
            vec![Value::List(vec![max.clone(), half.clone(), Value::Int(5)])],
        ),
        (format!("{call} RETURN max(big)"), vec![max.clone()]),
        (
            format!("{call} RETURN sum(big)"),
            vec![Value::Float(u64::MAX as f64 + TWO_POW_63 as f64 + 5.0)],
        ),
        (
            format!("{call} RETURN toString(big) ORDER BY big"),
            vec![
                Value::String("5".to_owned()),
                Value::String("9223372036854775808.0".to_owned()),
                Value::String("18446744073709551616.0".to_owned()),
            ],
        ),
    ];
    let mut failures = Vec::new();
    for (query, want) in cases {
        match column(&db, &query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}: expected {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}

/// An ephemeral node's id is an identity, not a value: it reads back as the
/// `Int64` with the same bits, and `uni.create.vEdge` accepts it.
#[tokio::test]
async fn uint64_values_do_not_wrap_an_ephemeral_id() -> Result<()> {
    let db = open().await?;
    let ids = column(
        &db,
        "CALL uni.create.vNode(['L'], '{}') YIELD node RETURN id(node)",
    )
    .await?;
    let [Value::Int(id)] = ids.as_slice() else {
        bail!("expected one integer id, got {ids:?}");
    };
    assert!(*id < 0, "the ephemeral bit is the sign bit: {id}");

    // The id reads back as the same vertex.
    let result = db
        .session()
        .query(
            "CALL uni.create.vNode(['L'], '{}') YIELD node \
             WITH node, id(node) AS v \
             CALL uni.create.vEdge(v, 'T', '{}', v) YIELD edge \
             RETURN v, edge",
        )
        .await?;
    let [row] = result.rows() else {
        bail!("expected one row, got {:?}", result.rows());
    };
    let Value::Int(v) = row.values()[0] else {
        bail!("expected an integer id, got {:?}", row.values());
    };
    // Its endpoints are that vertex, whichever encoding the edge comes back in.
    let (src, dst) = match &row.values()[1] {
        Value::Edge(e) => (
            Value::from_id_bits(e.src.as_u64()),
            Value::from_id_bits(e.dst.as_u64()),
        ),
        Value::Map(m) => (
            m.get("_src").cloned().unwrap_or(Value::Null),
            m.get("_dst").cloned().unwrap_or(Value::Null),
        ),
        other => bail!("expected an edge, got {other:?}"),
    };
    assert_eq!((src, dst), (Value::Int(v), Value::Int(v)), "{row:?}");
    Ok(())
}

/// Returning an id and evaluating it must use the same signed identity bits.
#[tokio::test]
async fn ephemeral_id_expressions_are_signed() -> Result<()> {
    let db = open().await?;
    let call = "CALL uni.create.vNode(['L'], '{}') YIELD node";
    for query in [
        format!("{call} RETURN id(node) < 0"),
        format!("{call} WITH id(node) AS v RETURN v < 0"),
        format!("{call} WITH node WHERE id(node) < 0 RETURN true"),
        format!("{call} RETURN elementId(node) = toString(id(node))"),
        format!("{call} WITH collect(node) AS ns UNWIND ns AS n RETURN id(n) < 0"),
        format!(
            "{call} WITH collect(node) AS ns UNWIND ns AS n RETURN elementId(n) = toString(id(n))"
        ),
        format!("{call} RETURN id(node) IN [id(node)]"),
        format!("{call} RETURN id(node) + 1 > id(node)"),
        format!(
            "{call} WITH id(node) AS v CALL uni.create.vEdge(v, 'T', '{{}}', v) \
                 YIELD edge RETURN id(edge) < 0"
        ),
        format!(
            "{call} WITH id(node) AS v CALL uni.create.vEdge(v, 'T', '{{}}', v) \
                 YIELD edge RETURN elementId(edge) = toString(id(edge))"
        ),
    ] {
        assert_eq!(
            column(&db, &query).await?,
            vec![Value::Bool(true)],
            "{query}"
        );
    }
    assert_eq!(
        column(
            &db,
            &format!("{call} RETURN id(node) = -9223372036854775808")
        )
        .await?,
        vec![Value::Bool(true)]
    );
    let query = format!("{call} WITH [id(node), 0] AS ids UNWIND ids AS v RETURN v ORDER BY v");
    assert_eq!(
        column(&db, &query).await?,
        vec![Value::Int(i64::MIN), Value::Int(0)]
    );
    let result = db
        .session()
        .query_with(&format!(
            "{call} WITH node WHERE id(node) = $id RETURN id(node) AS id"
        ))
        .param("id", i64::MIN)
        .fetch_all()
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int(i64::MIN)]);
    assert_eq!(column(&db, "RETURN id(null)").await?, vec![Value::Null]);
    Ok(())
}
