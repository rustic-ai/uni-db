//! Rhai-side manifest reader.
//!
//! Rhai plugins declare their identity, version, capabilities, and
//! provided functions by exporting a `uni_manifest()` function that
//! returns a Rhai map. This module compiles the script, calls the
//! function, and walks the returned `rhai::Map` into a structured
//! manifest the loader can use.
//!
//! Expected shape (Rhai source):
//!
//! ```rhai
//! fn uni_manifest() {
//!   #{
//!     id:          "ai.example.score",
//!     version:     "0.1.0",
//!     determinism: "pure",
//!     scalar_fns: [
//!       #{ name: "score", args: ["float","float"], returns: "float" },
//!     ],
//!     aggregate_fns: [
//!       #{ name: "stats", args: ["float"], returns: "map", state: "map" },
//!     ],
//!     procedures: [
//!       #{ name: "rows", args: [], yields: ["int","string"] },
//!     ],
//!   }
//! }
//! ```

#![cfg(feature = "rhai-runtime")]

use rhai::{AST, Dynamic, Engine, Map, Scope};

use crate::error::RhaiError;

/// Result of parsing a `uni_manifest()` return value.
#[derive(Debug, Clone, Default)]
pub struct RhaiManifest {
    /// Plugin id (`"ai.example.score"`).
    pub id: String,
    /// Plugin version (semver string).
    pub version: String,
    /// Determinism: `"pure"`, `"session"`, or `"nondeterministic"`.
    pub determinism: String,
    /// Declared scalar functions.
    pub scalar_fns: Vec<ScalarEntry>,
    /// Declared aggregate functions.
    pub aggregate_fns: Vec<AggregateEntry>,
    /// Declared procedures.
    pub procedures: Vec<ProcedureEntry>,
    /// Declared GraphCompute algorithms.
    pub algorithms: Vec<AlgorithmEntry>,
}

/// One GraphCompute algorithm entry.
///
/// Mirrors [`ProcedureEntry`] but its guest function receives a `GcSession`
/// handle as its first argument and drives the coarse kernels, emitting its
/// result via `session.emit(...)` rather than returning row maps (proposal
/// §4.6). The declared `yields` become the `AlgorithmSignature::output_fields`.
#[derive(Debug, Clone)]
pub struct AlgorithmEntry {
    /// Algorithm name (also the Rhai callable driven per invocation).
    pub name: String,
    /// Argument type names, excluding the leading injected `GcSession`.
    pub args: Vec<String>,
    /// Yielded columns (in declaration order).
    pub yields: Vec<YieldField>,
}

/// One scalar fn entry from the Rhai manifest.
#[derive(Debug, Clone)]
pub struct ScalarEntry {
    /// Function name as declared in the script (also the Rhai callable).
    pub name: String,
    /// Argument type names (`"float"`, `"int"`, …).
    pub args: Vec<String>,
    /// Return type name.
    pub returns: String,
    /// Opt-in vectorised mode — the function takes column userdata.
    /// Defaults to `false` (row mode).
    pub vectorized: bool,
}

/// One aggregate fn entry.
#[derive(Debug, Clone)]
pub struct AggregateEntry {
    /// Aggregate name; must also be the name of a `const` map in the
    /// script carrying `init` / `accumulate` / `merge` / `finalize`
    /// closures.
    pub name: String,
    /// Input type names.
    pub args: Vec<String>,
    /// Final return type name.
    pub returns: String,
    /// State type — informational; v1 always wraps as JSON-blob
    /// `LargeBinary` regardless.
    pub state: String,
}

/// One declared procedure yield column.
///
/// A yield is declared as a string that is either a bare type (`"int"`) —
/// carrying no caller-facing name, so the loader falls back to a positional
/// `col{i}` name — or a `"name:type"` pair (`"id:int"`) that names the column.
/// The name must match the key the procedure uses in its returned row maps;
/// otherwise the column reads back as all-NULL.
#[derive(Debug, Clone)]
pub struct YieldField {
    /// Column name as it appears in the returned row maps, when declared.
    pub name: Option<String>,
    /// Yielded column type name (`"int"`, `"string"`, …).
    pub type_name: String,
}

/// One procedure entry.
#[derive(Debug, Clone)]
pub struct ProcedureEntry {
    /// Procedure name.
    pub name: String,
    /// Argument type names.
    pub args: Vec<String>,
    /// Yielded columns (in declaration order).
    pub yields: Vec<YieldField>,
    /// Mode: `"read"`, `"write"`, `"schema"`, or `"dbms"`. Default
    /// `"read"`.
    pub mode: String,
}

/// Compile a Rhai script into an AST.
///
/// Returns [`RhaiError::ParseFailed`] on syntax errors, with the Rhai
/// position information preserved in the error message.
pub fn compile(engine: &Engine, script: &str) -> Result<AST, RhaiError> {
    engine
        .compile(script)
        .map_err(|e| RhaiError::ParseFailed(format!("{e}")))
}

/// Call the script's `uni_manifest()` function and parse the returned
/// map into a [`RhaiManifest`].
pub fn parse_manifest(engine: &Engine, ast: &AST) -> Result<RhaiManifest, RhaiError> {
    let mut scope = Scope::new();
    let dynamic: Dynamic = engine
        .call_fn(&mut scope, ast, "uni_manifest", ())
        .map_err(|e| RhaiError::ManifestInvalid(format!("calling uni_manifest: {e}")))?;

    let map: Map = dynamic
        .try_cast::<Map>()
        .ok_or_else(|| RhaiError::ManifestInvalid("uni_manifest() must return a map".into()))?;

    let id = required_string(&map, "id")?;
    let version = required_string(&map, "version")?;
    // An ABSENT `determinism` keeps the documented default of `"pure"`, which
    // matches the Python binding's `determinism: str = 'pure'`. Changing that
    // default is a product decision, not a bug fix — see
    // `docs/proposals/issue_233_remaining_work_2026-09-06.md`. What changed is
    // that a *declared* value can no longer be silently discarded.
    let determinism = optional_string(&map, "determinism")?.unwrap_or_else(|| "pure".into());

    let scalar_fns = parse_scalar_entries(&map)?;
    let aggregate_fns = parse_aggregate_entries(&map)?;
    let procedures = parse_procedure_entries(&map)?;
    let algorithms = parse_algorithm_entries(&map)?;

    Ok(RhaiManifest {
        id,
        version,
        determinism,
        scalar_fns,
        aggregate_fns,
        procedures,
        algorithms,
    })
}

/// Parse the array under `key` into a `Vec<T>`, building each entry from
/// its map via `build`.
///
/// A missing `key` yields an empty vec (the field is optional). A present
/// but non-array value, or a non-map element, is a `ManifestInvalid` error
/// labelled with `key`.
fn parse_entry_array<T>(
    map: &Map,
    key: &str,
    build: impl Fn(&Map) -> Result<T, RhaiError>,
) -> Result<Vec<T>, RhaiError> {
    let Some(arr) = map.get(key) else {
        return Ok(vec![]);
    };
    let arr = arr
        .clone()
        .try_cast::<rhai::Array>()
        .ok_or_else(|| RhaiError::ManifestInvalid(format!("{key} must be an array of maps")))?;
    let mut entries = Vec::with_capacity(arr.len());
    for d in arr {
        let m = d
            .try_cast::<Map>()
            .ok_or_else(|| RhaiError::ManifestInvalid(format!("{key} entry must be a map")))?;
        entries.push(build(&m)?);
    }
    Ok(entries)
}

fn parse_scalar_entries(map: &Map) -> Result<Vec<ScalarEntry>, RhaiError> {
    parse_entry_array(map, "scalar_fns", |m| {
        Ok(ScalarEntry {
            name: required_string(m, "name")?,
            args: required_string_array(m, "args")?,
            returns: required_string(m, "returns")?,
            vectorized: optional_bool(m, "vectorized")?.unwrap_or(false),
        })
    })
}

fn parse_aggregate_entries(map: &Map) -> Result<Vec<AggregateEntry>, RhaiError> {
    parse_entry_array(map, "aggregate_fns", |m| {
        Ok(AggregateEntry {
            name: required_string(m, "name")?,
            args: required_string_array(m, "args")?,
            returns: required_string(m, "returns")?,
            state: optional_string(m, "state")?.unwrap_or_else(|| "map".into()),
        })
    })
}

fn parse_procedure_entries(map: &Map) -> Result<Vec<ProcedureEntry>, RhaiError> {
    parse_entry_array(map, "procedures", |m| {
        Ok(ProcedureEntry {
            name: required_string(m, "name")?,
            args: required_string_array(m, "args")?,
            yields: parse_yield_fields(m)?,
            mode: optional_string(m, "mode")?.unwrap_or_else(|| "read".into()),
        })
    })
}

fn parse_algorithm_entries(map: &Map) -> Result<Vec<AlgorithmEntry>, RhaiError> {
    parse_entry_array(map, "algorithms", |m| {
        Ok(AlgorithmEntry {
            name: required_string(m, "name")?,
            args: required_string_array(m, "args")?,
            yields: parse_yield_fields(m)?,
        })
    })
}

/// Parse a procedure's `yields` string array into named [`YieldField`]s.
///
/// Each element is a string: a bare type (`"int"`) or a `"name:type"` pair
/// (`"id:int"`). The named form is required whenever the procedure returns row
/// maps keyed by natural names — the column name must equal the row-map key,
/// or the column reads all-NULL. A flat string array (rather than nested maps)
/// keeps the manifest expression within the sandbox complexity limit.
///
/// # Errors
/// Returns [`RhaiError::ManifestInvalid`] if `yields` is missing, is not an
/// array, or contains a non-string element.
fn parse_yield_fields(map: &Map) -> Result<Vec<YieldField>, RhaiError> {
    let specs = required_string_array(map, "yields")?;
    Ok(specs.iter().map(|s| parse_yield_spec(s)).collect())
}

/// Split a single yield spec string into an optional name and a type name.
///
/// `"id:int"` → name `Some("id")`, type `"int"`; a bare `"int"` → name `None`.
fn parse_yield_spec(spec: &str) -> YieldField {
    match spec.split_once(':') {
        Some((name, ty)) => YieldField {
            name: Some(name.trim().to_string()),
            type_name: ty.trim().to_string(),
        },
        None => YieldField {
            name: None,
            type_name: spec.trim().to_string(),
        },
    }
}

fn required_string(map: &Map, key: &str) -> Result<String, RhaiError> {
    let dyn_val = map
        .get(key)
        .ok_or_else(|| RhaiError::ManifestInvalid(format!("missing required field `{key}`")))?;
    dyn_val
        .clone()
        .into_string()
        .map_err(|t| RhaiError::ManifestInvalid(format!("`{key}` must be a string (got {t})")))
}

/// Read an optional string field, rejecting a present-but-wrong-typed value.
///
/// # Errors
///
/// Returns [`RhaiError::ManifestInvalid`] if `key` is present but is not a
/// string.
///
/// #233: this returned a bare `Option`, so "the author did not declare this"
/// and "the author declared it and got the type wrong" both became `None` and
/// took the caller's default. For `determinism` that default is `"pure"`,
/// which maps to `Volatility::Immutable` — so a declared-but-mistyped value
/// silently told DataFusion it could constant-fold a nondeterministic
/// function. An absent key still takes the documented default; a declared one
/// is no longer discarded.
fn optional_string(map: &Map, key: &str) -> Result<Option<String>, RhaiError> {
    let Some(d) = map.get(key) else {
        return Ok(None);
    };
    d.clone().into_string().map(Some).map_err(|actual| {
        RhaiError::ManifestInvalid(format!(
            "field `{key}` must be a string, got {actual}; it was declared, so it is not \
             silently defaulted"
        ))
    })
}

/// Read an optional boolean field, rejecting a present-but-wrong-typed value.
///
/// # Errors
///
/// Returns [`RhaiError::ManifestInvalid`] if `key` is present but is not a
/// boolean. See [`optional_string`] for why (#233).
fn optional_bool(map: &Map, key: &str) -> Result<Option<bool>, RhaiError> {
    let Some(d) = map.get(key) else {
        return Ok(None);
    };
    d.as_bool().map(Some).map_err(|actual| {
        RhaiError::ManifestInvalid(format!(
            "field `{key}` must be a boolean, got {actual}; it was declared, so it is not \
             silently defaulted"
        ))
    })
}

fn required_string_array(map: &Map, key: &str) -> Result<Vec<String>, RhaiError> {
    let dyn_val = map
        .get(key)
        .ok_or_else(|| RhaiError::ManifestInvalid(format!("missing required field `{key}`")))?;
    let arr = dyn_val
        .clone()
        .try_cast::<rhai::Array>()
        .ok_or_else(|| RhaiError::ManifestInvalid(format!("`{key}` must be an array")))?;
    let mut out = Vec::with_capacity(arr.len());
    for (i, d) in arr.into_iter().enumerate() {
        let s = d.into_string().map_err(|t| {
            RhaiError::ManifestInvalid(format!("`{key}`[{i}] must be a string (got {t})"))
        })?;
        out.push(s);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::build_engine;
    use crate::host_fns::RhaiHostFnRegistry;
    use uni_plugin::CapabilitySet;

    fn engine() -> Engine {
        build_engine(&CapabilitySet::new(), &RhaiHostFnRegistry::new())
    }

    #[test]
    fn parses_minimal_manifest() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.min",
                    version: "0.1.0",
                    scalar_fns: [
                        #{ name: "score", args: ["float","float"], returns: "float" },
                    ],
                }
            }
            fn score(x, y) { x + y }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).expect("compiles");
        let m = parse_manifest(&eng, &ast).expect("parses");
        assert_eq!(m.id, "ai.test.min");
        assert_eq!(m.version, "0.1.0");
        assert_eq!(m.determinism, "pure");
        assert_eq!(m.scalar_fns.len(), 1);
        assert_eq!(m.scalar_fns[0].name, "score");
        assert_eq!(m.scalar_fns[0].args, vec!["float", "float"]);
        assert_eq!(m.scalar_fns[0].returns, "float");
        assert!(!m.scalar_fns[0].vectorized);
    }

    /// A declared-but-mistyped `determinism` must not become `"pure"`.
    ///
    /// #233. `optional_string` returned `None` for both "absent" and "present
    /// but not a string", and the caller defaulted to `"pure"` →
    /// `Volatility::Immutable`. So `determinism: 1` told DataFusion it could
    /// constant-fold or CSE a function the author never claimed was pure —
    /// a silent wrong answer for any nondeterministic Rhai scalar.
    ///
    /// An ABSENT key still defaults to `"pure"` (see the neighbouring
    /// `minimal_manifest_parses`): that default is documented and shared with
    /// the Python binding, and changing it is a product decision.
    #[test]
    fn a_declared_but_mistyped_determinism_is_rejected_not_defaulted() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.mistyped",
                    version: "0.1.0",
                    determinism: 1,
                    scalar_fns: [
                        #{ name: "score", args: ["float"], returns: "float" },
                    ],
                }
            }
            fn score(x) { x }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).expect("compiles");
        let err = parse_manifest(&eng, &ast)
            .expect_err("a mistyped determinism must not silently become `pure`");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("determinism"),
            "the error should name the offending field, got: {msg}"
        );
    }

    /// The same guard on the other optional fields.
    #[test]
    fn a_declared_but_mistyped_vectorized_is_rejected() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.mistyped2",
                    version: "0.1.0",
                    scalar_fns: [
                        #{ name: "score", args: ["float"], returns: "float", vectorized: "yes" },
                    ],
                }
            }
            fn score(x) { x }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).expect("compiles");
        assert!(
            parse_manifest(&eng, &ast).is_err(),
            "a mistyped `vectorized` must not silently select the scalar path"
        );
    }

    #[test]
    fn missing_id_rejected() {
        let script = r#"
            fn uni_manifest() { #{ version: "0.1.0" } }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).unwrap();
        let err = parse_manifest(&eng, &ast).unwrap_err();
        assert!(matches!(err, RhaiError::ManifestInvalid(_)));
    }

    #[test]
    fn parses_aggregate_and_procedure_entries() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.agg",
                    version: "0.1.0",
                    aggregate_fns: [
                        #{ name: "stats", args: ["float"], returns: "map", state: "map" },
                    ],
                    procedures: [
                        #{ name: "rows", args: [], yields: ["int","string"], mode: "read" },
                    ],
                }
            }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).unwrap();
        let m = parse_manifest(&eng, &ast).unwrap();
        assert_eq!(m.aggregate_fns.len(), 1);
        assert_eq!(m.aggregate_fns[0].name, "stats");
        assert_eq!(m.procedures.len(), 1);
        let ytypes: Vec<&str> = m.procedures[0]
            .yields
            .iter()
            .map(|y| y.type_name.as_str())
            .collect();
        assert_eq!(ytypes, vec!["int", "string"]);
        // Bare type strings carry no caller-facing name.
        assert!(m.procedures[0].yields.iter().all(|y| y.name.is_none()));
    }

    #[test]
    fn parses_named_procedure_yields() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.named",
                    version: "0.1.0",
                    procedures: [
                        #{ name: "rows", args: [],
                           yields: ["id:int", "label:string"],
                           mode: "read" },
                    ],
                }
            }
        "#;
        let eng = engine();
        let ast = compile(&eng, script).unwrap();
        let m = parse_manifest(&eng, &ast).unwrap();
        let ys = &m.procedures[0].yields;
        assert_eq!(ys[0].name.as_deref(), Some("id"));
        assert_eq!(ys[0].type_name, "int");
        assert_eq!(ys[1].name.as_deref(), Some("label"));
        assert_eq!(ys[1].type_name, "string");
    }
}
