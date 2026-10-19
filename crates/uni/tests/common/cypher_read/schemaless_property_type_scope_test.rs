// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A property declared on one label does not type the same-named property
//! of another label.
//!
//! The schemaless scan (a label with no schema, a multi-label pattern, or
//! `MATCH (n)`) typed each projected property from the declarations of
//! every label in the schema, merged by name. With `F.v` declared
//! `Float64`, `s.v` on the schemaless `S` was read as a `Float64` column:
//! the stored integer `9007199254740993` came back `9007199254740992.0`,
//! and a string stored there was lost. The write path coerces only under a
//! vertex's own labels, so the stored values were right and only the read
//! was wrong.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(schemaless_property_type_scope)'

// Rust guideline compliant

use uni_db::{DataType, Uni, UniConfig, Value};

const BIG: i64 = (1 << 53) + 1;

fn cfg() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

async fn column(db: &Uni, q: &str) -> Vec<Value> {
    let rows = db
        .session()
        .query(q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    rows.rows().iter().map(|r| r.values()[0].clone()).collect()
}

fn identical(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| match (x, y) {
            (Value::Int(x), Value::Int(y)) => x == y,
            (Value::Float(x), Value::Float(y)) => x.to_bits() == y.to_bits(),
            (x, y) => x == y,
        })
}

#[tokio::test]
async fn schemaless_property_type_scope() {
    let mut failures = Vec::new();
    for flush in [false, true] {
        let db = Uni::in_memory().config(cfg()).build().await.unwrap();
        db.schema()
            .label("F")
            .property("v", DataType::Float64)
            .property("k", DataType::Int64)
            .done()
            .apply()
            .await
            .unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute(&format!(
            "CREATE (:S {{k: 1, v: {BIG}}}), (:S {{k: 2, v: 'x'}}), (:S:T {{k: 3, v: {BIG}}}), \
             (:F {{k: 4, v: 2.5}})"
        ))
        .await
        .unwrap();
        tx.commit().await.unwrap();
        if flush {
            db.flush().await.unwrap();
        }
        for (q, want) in [
            (
                "MATCH (s:S) RETURN s.v ORDER BY s.k",
                vec![Value::Int(BIG), Value::String("x".into()), Value::Int(BIG)],
            ),
            (
                "MATCH (s:S:T) RETURN s.v ORDER BY s.k",
                vec![Value::Int(BIG)],
            ),
            (
                "MATCH (n) RETURN n.v ORDER BY n.k",
                vec![
                    Value::Int(BIG),
                    Value::String("x".into()),
                    Value::Int(BIG),
                    Value::Float(2.5),
                ],
            ),
            ("MATCH (f:F) RETURN f.v", vec![Value::Float(2.5)]),
            (
                &format!("MATCH (s:S) WHERE s.v = {BIG} RETURN s.k ORDER BY s.k"),
                vec![Value::Int(1), Value::Int(3)],
            ),
        ] {
            let got = column(&db, q).await;
            if !identical(&got, &want) {
                failures.push(format!("flush={flush} {q}: want {want:?}, got {got:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
