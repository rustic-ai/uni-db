//! `CREATE LABEL … (v INT)` must hold a Cypher integer.
//!
//! Cypher integers are 64-bit. Two mappings from the DDL keyword to a storage
//! type existed and disagreed: the procedure path read `INT` as `Int64`, while
//! the `CREATE LABEL` path read it as `Int32`. Values beyond 32 bits were then
//! silently wrapped on the way in — no error, no warning:
//!
//! | written | read back |
//! |---|---|
//! | `1234567890123` | `1912276171` |
//! | `2147483648` | `-2147483648` |
//! | `4294967296` | `0` |
//!
//! The same values declared through the builder API (`DataType::Int64`) or
//! stored schemalessly round-tripped correctly, which is what made this
//! survivable for so long — the DDL path is the one LDBC and most examples use.
//!
//! Found while writing `datetime({epochMillis: …})` tests: a stored birthday of
//! `1234567890123` came back as `1912276171`, which is exactly that value modulo
//! 2^32.

use uni_db::{Uni, Value};

/// Values that a 32-bit column cannot hold, including the boundaries where
/// wrapping is most obviously wrong.
const WIDE: &[i64] = &[
    2_147_483_647,      // i32::MAX — the last value that fits
    2_147_483_648,      // wrapped to i32::MIN
    4_294_967_296,      // wrapped to 0
    1_234_567_890_123,  // an epoch-millis timestamp
    -1_234_567_890_123, // and its negation
    i64::MAX,
    i64::MIN,
];

#[tokio::test]
async fn ddl_int_round_trips_64_bit_values() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL D (n INT, v INT)").await.unwrap();
    for (i, v) in WIDE.iter().enumerate() {
        tx.execute(&format!("CREATE (:D {{n: {i}, v: {v}}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let r = db
        .session()
        .query("MATCH (d:D) RETURN d.n AS n, d.v AS v")
        .await
        .unwrap();
    let mut got: Vec<(i64, i64)> = r
        .rows()
        .iter()
        .map(|row| match (&row.values()[0], &row.values()[1]) {
            (Value::Int(n), Value::Int(v)) => (*n, *v),
            other => panic!("expected two integers, got {other:?}"),
        })
        .collect();
    got.sort_by_key(|(n, _)| *n);
    let values: Vec<i64> = got.into_iter().map(|(_, v)| v).collect();
    assert_eq!(values, WIDE);
}

/// `INTEGER` is the spelling the openCypher type system uses; it must agree.
#[tokio::test]
async fn ddl_integer_spelling_agrees_with_int() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL D (v INTEGER)").await.unwrap();
    tx.execute("CREATE (:D {v: 1234567890123})").await.unwrap();
    tx.commit().await.unwrap();

    let r = db.session().query("MATCH (d:D) RETURN d.v").await.unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(1_234_567_890_123));
}

/// The builder API was already correct; this pins the two surfaces together so
/// they cannot drift apart again.
#[tokio::test]
async fn the_builder_api_and_the_ddl_agree() {
    let via_ddl = Uni::in_memory().build().await.unwrap();
    let tx = via_ddl.session().tx().await.unwrap();
    tx.execute("CREATE LABEL D (v INT)").await.unwrap();
    tx.execute("CREATE (:D {v: 1234567890123})").await.unwrap();
    tx.commit().await.unwrap();

    let via_builder = Uni::in_memory().build().await.unwrap();
    via_builder
        .schema()
        .label("D")
        .property("v", uni_db::DataType::Int64)
        .done()
        .apply()
        .await
        .unwrap();
    let tx2 = via_builder.session().tx().await.unwrap();
    tx2.execute("CREATE (:D {v: 1234567890123})").await.unwrap();
    tx2.commit().await.unwrap();

    let a = via_ddl
        .session()
        .query("MATCH (d:D) RETURN d.v")
        .await
        .unwrap();
    let b = via_builder
        .session()
        .query("MATCH (d:D) RETURN d.v")
        .await
        .unwrap();
    assert_eq!(a.rows()[0].values()[0], b.rows()[0].values()[0]);
}

/// `INT32` stays a narrow column — the fix widens the ambiguous spellings, not
/// the explicit one.
#[tokio::test]
async fn explicit_int32_is_still_narrow() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL N (v INT32)").await.unwrap();
    tx.execute("CREATE (:N {v: 7})").await.unwrap();
    tx.commit().await.unwrap();
    let r = db.session().query("MATCH (n:N) RETURN n.v").await.unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(7));
}

/// `FLOAT` had the same divergence as `INT`: the procedure path read it as
/// `Float64` while `CREATE LABEL` read it as `Float32`. The loss is quieter than
/// integer wrapping — trailing precision rather than a sign flip — but it is the
/// same defect, and a Cypher float is 64-bit.
#[tokio::test]
async fn ddl_float_keeps_64_bit_precision() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL F (v FLOAT)").await.unwrap();
    tx.execute("CREATE (:F {v: 0.1234567890123456})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let r = db.session().query("MATCH (f:F) RETURN f.v").await.unwrap();
    match r.rows()[0].values()[0] {
        Value::Float(v) => assert_eq!(v, 0.1234567890123456_f64),
        ref other => panic!("expected a float, got {other:?}"),
    }
}

/// `FLOAT32` stays narrow, as `INT32` does.
#[tokio::test]
async fn explicit_float32_is_still_narrow() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL G (v FLOAT32)").await.unwrap();
    tx.execute("CREATE (:G {v: 0.5})").await.unwrap();
    tx.commit().await.unwrap();
    let r = db.session().query("MATCH (g:G) RETURN g.v").await.unwrap();
    match r.rows()[0].values()[0] {
        Value::Float(v) => assert_eq!(v, 0.5),
        ref other => panic!("expected a float, got {other:?}"),
    }
}
