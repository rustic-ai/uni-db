// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

// Links clang's runtime library into the extension on macOS.
//
// ONNX Runtime's CoreML objects (`host_utils.mm.o`, `model.mm.o` in the
// static `libonnxruntime.a`) call `__isPlatformVersionAtLeast`, the helper
// clang emits for `@available`. It lives in `libclang_rt.osx.a`, which the
// clang driver adds only to a default link, and rustc links with
// `-nodefaultlibs`. Whether the symbol was resolved then depended on the
// rest of the link: a local `maturin build` happened to pull it in, but a
// `maturin build --target aarch64-apple-darwin`, the way the release
// workflow builds wheels, did not, in the `release` and `dist` profiles
// alike. A Python extension is linked with `-undefined dynamic_lookup`, so
// the link succeeded and `import uni_db` failed with "symbol not found in
// flat namespace '___isPlatformVersionAtLeast'". Linking the archive
// explicitly resolves it in every build. Elsewhere this script does nothing.
//
// The variant packages (`uni-db-onnx`, `-metal`, `-cuda`, …) build the same
// sources; each has a one-line `build.rs` that `include!`s this file, since
// maturin refuses a `build` path outside the package. Hence plain comments
// rather than `//!`: inner doc comments are not allowed in included items.

use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Relative to the package being built: the shared script itself, from
    // `bindings/uni-db` and from each variant that includes it.
    println!("cargo:rerun-if-changed=../uni-db/build.rs");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=DEVELOPER_DIR");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }
    match clang_runtime_archive() {
        Some(archive) => println!("cargo:rustc-link-arg={}", archive.display()),
        None => println!(
            "cargo:warning=libclang_rt.osx.a not found; the extension may fail to import \
             with a missing ___isPlatformVersionAtLeast"
        ),
    }
}

/// `libclang_rt.osx.a` in the runtime directory of the clang that links.
fn clang_runtime_archive() -> Option<PathBuf> {
    let candidates: [&[&str]; 2] = [&["xcrun", "clang"], &["cc"]];
    candidates.iter().find_map(|cmd| {
        let output = Command::new(cmd[0])
            .args(&cmd[1..])
            .arg("--print-runtime-dir")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let dir = String::from_utf8(output.stdout).ok()?;
        let archive = PathBuf::from(dir.trim()).join("libclang_rt.osx.a");
        archive.is_file().then_some(archive)
    })
}
