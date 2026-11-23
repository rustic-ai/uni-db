#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Proposal §19 acceptance criterion #24 — cross-ABI parity.
//!
//! Both example plugins (`example-extism-geo` and `example-wasm-geo`)
//! implement `ai.example.geo.haversine` over the same Arrow-IPC wire
//! format. The host's `ColumnarValue → IPC → wasm → IPC →
//! ColumnarValue` boundaries are identical regardless of which ABI
//! delivered the batch. The compute math is identical. Therefore the
//! returned f64 bytes from both ABIs must match for the same input.
//!
//! Hard-fails if either artifact is missing; run
//! `./scripts/build-wasm-fixtures.sh` first.

// Rust guideline compliant

#![cfg(all(feature = "extism-plugins", feature = "wasm-plugins"))]

use std::sync::Arc;

use arrow_array::Float64Array;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_db::Uni;
use uni_plugin::{Capability, CapabilitySet, QName};
use uni_plugin_extism::ExtismLoader;
use uni_plugin_wasm::WasmLoader;

const EXTISM_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-extism-geo/target/wasm32-unknown-unknown/release/example_extism_geo.wasm",
);
const COMPONENT_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm",
);

fn read_artifact(path: &str) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| {
        panic!(
            "wasm artifact missing at {path}: {e}\n\
             Run `./scripts/build-wasm-fixtures.sh` first."
        );
    })
}

#[tokio::test]
async fn cross_abi_haversine_results_match() -> anyhow::Result<()> {
    // Both plugins register the same qname (ai.example.geo.haversine)
    // and would collide on a single registry. Use two `Uni` instances.
    let uni_extism = Uni::in_memory().build().await?;
    let uni_component = Uni::in_memory().build().await?;

    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    let ext_bytes = read_artifact(EXTISM_WASM);
    let ext_outcome = uni_extism.load_wasm_extism(
        &ExtismLoader::new(),
        &ext_bytes,
        &CapabilitySet::new(),
        &caps,
    )?;
    assert_eq!(ext_outcome.plugin_id, "ai.example.geo");

    let comp_bytes = read_artifact(COMPONENT_WASM);
    let comp_outcome = uni_component.load_wasm_component(
        &WasmLoader::new(),
        &comp_bytes,
        &CapabilitySet::new(),
        &caps,
    )?;
    assert_eq!(comp_outcome.plugin_id, "ai.example.geo");

    // Look up adapters from each registry.
    let qname = QName::parse("ai.example.geo.haversine").unwrap();
    let ext_entry = uni_extism
        .plugin_registry()
        .scalar_fn(&qname)
        .expect("extism scalar registered");
    let comp_entry = uni_component
        .plugin_registry()
        .scalar_fn(&qname)
        .expect("component scalar registered");

    // 5 test rows spanning trivial + non-trivial cases.
    let lat1: Float64Array = vec![48.8566, 0.0, 0.0, 90.0, 35.6762].into();
    let lon1: Float64Array = vec![2.3522, 0.0, 0.0, 0.0, 139.6503].into();
    let lat2: Float64Array = vec![51.5074, 0.0, 0.0, -90.0, 40.7128].into();
    let lon2: Float64Array = vec![-0.1278, 0.0, 180.0, 0.0, -74.0060].into();
    let n = lat1.len();

    let args_ext = vec![
        ColumnarValue::Array(Arc::new(lat1.clone())),
        ColumnarValue::Array(Arc::new(lon1.clone())),
        ColumnarValue::Array(Arc::new(lat2.clone())),
        ColumnarValue::Array(Arc::new(lon2.clone())),
    ];
    let args_comp = vec![
        ColumnarValue::Array(Arc::new(lat1)),
        ColumnarValue::Array(Arc::new(lon1)),
        ColumnarValue::Array(Arc::new(lat2)),
        ColumnarValue::Array(Arc::new(lon2)),
    ];

    let out_ext = ext_entry.function.invoke(&args_ext, n)?;
    let out_comp = comp_entry.function.invoke(&args_comp, n)?;

    let arr_ext = match out_ext {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("extism returned scalar"),
    };
    let arr_comp = match out_comp {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("component returned scalar"),
    };
    let f_ext = arr_ext
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("extism Float64Array");
    let f_comp = arr_comp
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("component Float64Array");

    assert_eq!(f_ext.len(), f_comp.len(), "row counts differ");
    assert_eq!(f_ext.len(), n);

    // Byte-equal claim — the strict §19 #24 acceptance.
    for i in 0..n {
        let e = f_ext.value(i);
        let c = f_comp.value(i);
        assert_eq!(
            e.to_bits(),
            c.to_bits(),
            "row {i}: extism={e} ({:#018x}) vs component={c} ({:#018x}) — \
             cross-ABI byte-parity violation",
            e.to_bits(),
            c.to_bits()
        );
    }
    Ok(())
}

/// A load whose registrar grants do not cover a surface the plugin registers
/// fails the same way on both ABIs, and as a native load does: the
/// registrar's `CapabilityRequired` reaches the caller as a typed
/// `InvalidArgument` naming the capability, not as an internal error.
#[tokio::test]
async fn missing_registration_capability_is_a_typed_error_on_both_abis() -> anyhow::Result<()> {
    use uni_db::UniError;

    let uni_extism = Uni::in_memory().build().await?;
    let uni_component = Uni::in_memory().build().await?;
    let no_caps = CapabilitySet::new();

    let ext_err = uni_extism
        .load_wasm_extism(
            &ExtismLoader::new(),
            &read_artifact(EXTISM_WASM),
            &no_caps,
            &no_caps,
        )
        .expect_err("extism load without ScalarFn must fail");
    let comp_err = uni_component
        .load_wasm_component(
            &WasmLoader::new(),
            &read_artifact(COMPONENT_WASM),
            &no_caps,
            &no_caps,
        )
        .expect_err("component load without ScalarFn must fail");
    for err in [&ext_err, &comp_err] {
        match err {
            UniError::InvalidArgument { message, .. } => assert!(
                message.contains("ScalarFn"),
                "the error names the missing capability: {message}"
            ),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }
    Ok(())
}
