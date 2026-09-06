//! M9-shipped procedures fronting the declared-plugin store.

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, StringBuilder};
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use semver::Version;
use uni_cypher::parse_expression;
use uni_plugin::adapter_common::batch_builder::batch_into_stream;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::{ArgType, ScalarPluginFn};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId,
    PluginManifest, PluginRegistrar, PluginRegistry, ProvidedSurfaces, QName, Scope, SideEffects,
};

use super::{CustomError, DeclaredPlugin, DeclaredPluginStore, DeclaredScalarFn, Persistence};
use crate::decode::{declared_plugin_id, local_part, map_plugin_error, type_str_to_arrow};

// -------------------------------------------------------------
// Signatures
// -------------------------------------------------------------

/// Signature for `uni.plugin.listDeclared`.
#[must_use]
pub fn list_declared_signature() -> ProcedureSignature {
    ProcedureSignature {
        args: vec![],
        yields: vec![
            Field::new("qname", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("declared_by", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "List every declared plugin (apoc.custom analogue) with its kind, declarer, and active state.".to_owned(),
    }
}

/// Signature for `uni.plugin.dropDeclared`.
#[must_use]
pub fn drop_declared_signature() -> ProcedureSignature {
    write_signature(
        vec![named_arg(
            "qname",
            DataType::Utf8,
            "Qualified name of the declared plugin to drop.",
        )],
        "removed",
        "Drop a previously declared plugin. Errors if other declared plugins depend on it.",
    )
}

fn named_arg(name: &str, ty: DataType, doc: &str) -> NamedArgType {
    NamedArgType {
        name: smol_str::SmolStr::new(name),
        ty: ArgType::Primitive(ty),
        default: None,
        doc: doc.to_owned(),
    }
}

/// Variant of [`named_arg`] that records a default value for the
/// arg.
///
/// Note: today's procedure dispatch in
/// `crates/uni-query/src/query/df_graph/procedure_call.rs` does not
/// auto-fill defaults from the signature; the declare* procedures
/// instead read the default through [`extract_string_or`]. The
/// `default` field stays informative for tooling and the eventual
/// dispatch-side default expansion.
fn named_arg_default(name: &str, ty: DataType, doc: &str, default: &str) -> NamedArgType {
    NamedArgType {
        name: smol_str::SmolStr::new(name),
        ty: ArgType::Primitive(ty),
        default: Some(ScalarValue::Utf8(Some(default.to_owned()))),
        doc: doc.to_owned(),
    }
}

/// Doc string for the trailing `deps_json` arg shared by every
/// declare* signature.
const DEPS_JSON_DOC: &str =
    "JSON array of qualified names this declaration depends on (empty by default).";

fn deps_arg() -> NamedArgType {
    named_arg_default("deps_json", DataType::Utf8, DEPS_JSON_DOC, "[]")
}

/// Build a `Write`-mode [`ProcedureSignature`] that yields a single
/// boolean column.
///
/// Shared by `dropDeclared` and the four `declare*` signatures,
/// which differ only in their args, the yielded column name, and
/// the docstring.
fn write_signature(args: Vec<NamedArgType>, yield_col: &str, docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args,
        yields: vec![Field::new(yield_col, DataType::Boolean, false)],
        mode: ProcedureMode::Write,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

/// Signature for `uni.plugin.declareFunction`.
#[must_use]
pub fn declare_function_signature() -> ProcedureSignature {
    write_signature(
        vec![
            named_arg("qname", DataType::Utf8, "Qualified name to register."),
            named_arg("body", DataType::Utf8, "Cypher expression body."),
            named_arg(
                "return_type",
                DataType::Utf8,
                "Return type ('string', 'int', 'float', 'bool').",
            ),
            named_arg(
                "arg_names_json",
                DataType::Utf8,
                "JSON array of argument names, in positional order.",
            ),
            deps_arg(),
        ],
        "registered",
        "Declare a new scalar function. Body is a Cypher expression; arguments are bound by name (positional).",
    )
}

/// Signature for `uni.plugin.declareProcedure`.
#[must_use]
pub fn declare_procedure_signature() -> ProcedureSignature {
    write_signature(
        vec![
            named_arg("qname", DataType::Utf8, "Qualified name to register."),
            named_arg("body", DataType::Utf8, "Cypher query body."),
            named_arg("mode", DataType::Utf8, "'READ' or 'WRITE'."),
            named_arg(
                "yield_json",
                DataType::Utf8,
                "JSON array describing yielded columns.",
            ),
            deps_arg(),
        ],
        "registered",
        "Declare a new procedure. The body is a full Cypher query; arguments are bound by name.",
    )
}

/// Signature for `uni.plugin.declareAggregate`.
#[must_use]
pub fn declare_aggregate_signature() -> ProcedureSignature {
    write_signature(
        vec![
            named_arg("qname", DataType::Utf8, "Qualified name to register."),
            named_arg(
                "init_expr",
                DataType::Utf8,
                "Init state expression (no parameters).",
            ),
            named_arg(
                "update_expr",
                DataType::Utf8,
                "Update step expression; binds `$state` plus per-row args.",
            ),
            named_arg(
                "finalize_expr",
                DataType::Utf8,
                "Finalize expression; binds `$state`.",
            ),
            named_arg_default(
                "return_type",
                DataType::Utf8,
                "Return type ('string', 'int', 'float', 'bool').",
                "float",
            ),
            named_arg_default(
                "arg_names_json",
                DataType::Utf8,
                "JSON array of update-arg names, in positional order.",
                "[]",
            ),
            deps_arg(),
        ],
        "registered",
        "Declare a new aggregate function from Cypher init / update / finalize expressions.",
    )
}

/// Signature for `uni.plugin.declareTrigger`.
#[must_use]
pub fn declare_trigger_signature() -> ProcedureSignature {
    write_signature(
        vec![
            named_arg("qname", DataType::Utf8, "Qualified name to register."),
            named_arg(
                "event_filter",
                DataType::Utf8,
                "Event filter (label or relationship pattern).",
            ),
            named_arg(
                "body",
                DataType::Utf8,
                "Cypher body to execute when the trigger fires.",
            ),
            deps_arg(),
        ],
        "registered",
        "Declare a new trigger that fires the given Cypher body on matched mutation events.",
    )
}

// -------------------------------------------------------------
// listDeclared / dropDeclared
// -------------------------------------------------------------

/// Implementation of `uni.plugin.listDeclared`.
#[derive(Debug)]
pub struct ListDeclaredProcedure {
    store: Arc<DeclaredPluginStore>,
}

impl ListDeclaredProcedure {
    /// Construct.
    #[must_use]
    pub fn new(store: Arc<DeclaredPluginStore>) -> Self {
        Self { store }
    }
}

impl ProcedurePlugin for ListDeclaredProcedure {
    fn signature(&self) -> &ProcedureSignature {
        static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
        SIG.get_or_init(list_declared_signature)
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        _args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let rows = self.store.list();
        let mut qname = StringBuilder::new();
        let mut kind = StringBuilder::new();
        let mut declared_by = StringBuilder::new();
        let mut active = BooleanBuilder::new();
        for r in rows {
            qname.append_value(&r.qname);
            kind.append_value(&r.kind);
            declared_by.append_value(&r.declared_by);
            active.append_value(r.active);
        }
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("qname", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("declared_by", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]));
        let cols: Vec<Arc<dyn Array>> = vec![
            Arc::new(qname.finish()),
            Arc::new(kind.finish()),
            Arc::new(declared_by.finish()),
            Arc::new(active.finish()),
        ];
        let batch = RecordBatch::try_new(schema, cols)
            .map_err(|e| FnError::new(0xB00, format!("listDeclared: {e}")))?;
        Ok(batch_into_stream(batch))
    }
}

/// Implementation of `uni.plugin.dropDeclared`.
#[derive(Debug)]
pub struct DropDeclaredProcedure {
    store: Arc<DeclaredPluginStore>,
    persistence: Arc<dyn Persistence>,
    registry: Arc<PluginRegistry>,
}

impl DropDeclaredProcedure {
    /// Construct.
    #[must_use]
    pub fn new(
        store: Arc<DeclaredPluginStore>,
        persistence: Arc<dyn Persistence>,
        registry: Arc<PluginRegistry>,
    ) -> Self {
        Self {
            store,
            persistence,
            registry,
        }
    }
}

impl ProcedurePlugin for DropDeclaredProcedure {
    fn signature(&self) -> &ProcedureSignature {
        static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
        SIG.get_or_init(drop_declared_signature)
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let qname = extract_string(args, 0, "qname")?;
        let existed = self
            .store
            .drop_declared(&qname)
            .map_err(|e| FnError::new(0xB01, format!("dropDeclared: {e}")))?;
        if existed {
            // Remove ONLY this declared surface from the registry, not the
            // whole namespace plugin id — sibling declarations (e.g.
            // `mycorp.f2` when dropping `mycorp.f1`) share that id and must
            // survive. `remove_named_unique` is idempotent and scoped to the
            // qname. Also clear uni-cypher's plugin-aggregate hint (idempotent
            // for functions) so a dropped aggregate stops routing through
            // aggregate translation.
            let pid = PluginId::new(declared_plugin_id(&qname));
            if let Ok(qn) = QName::parse(&qname) {
                self.registry.remove_named_unique(&pid, &qn);
            }
            uni_cypher::unregister_plugin_aggregate(&qname);
            self.persistence
                .delete(&qname)
                .map_err(|e| FnError::new(0xB01, format!("dropDeclared persist: {e}")))?;
        }
        single_bool("removed", existed)
    }
}

// -------------------------------------------------------------
// declareFunction
// -------------------------------------------------------------

/// Implementation of `uni.plugin.declareFunction`.
#[derive(Debug)]
pub struct DeclareFunctionProcedure {
    store: Arc<DeclaredPluginStore>,
    persistence: Arc<dyn Persistence>,
    registry: Arc<PluginRegistry>,
}

impl DeclareFunctionProcedure {
    /// Construct.
    #[must_use]
    pub fn new(
        store: Arc<DeclaredPluginStore>,
        persistence: Arc<dyn Persistence>,
        registry: Arc<PluginRegistry>,
    ) -> Self {
        Self {
            store,
            persistence,
            registry,
        }
    }
}

impl ProcedurePlugin for DeclareFunctionProcedure {
    fn signature(&self) -> &ProcedureSignature {
        static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
        SIG.get_or_init(declare_function_signature)
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let qname = extract_string(args, 0, "qname")?;
        let body = extract_string(args, 1, "body")?;
        let return_type = extract_string(args, 2, "return_type")?;
        let arg_names_json = extract_string(args, 3, "arg_names_json")?;
        let arg_names: Vec<String> = serde_json::from_str(&arg_names_json).map_err(|e| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("declareFunction: arg_names_json parse: {e}"),
            )
        })?;
        let dependencies = parse_deps(args, 4)?;
        let declared_by = principal_id(&ctx);

        let record = DeclaredPlugin {
            qname: qname.clone(),
            kind: "function".to_owned(),
            body,
            signature_json: serde_json::to_string(&serde_json::json!({
                "return_type": return_type,
                "arg_names": arg_names,
            }))
            .unwrap_or_else(|_| "{}".to_owned()),
            dependencies,
            declared_by,
            active: true,
        };

        // A genuine re-declaration is one where THIS store already owned the
        // qname (checked before we overwrite the store entry below). Only then
        // do we drop the prior registry entry before re-installing, so
        // re-registration is not misread as shadowing a native fn. A FIRST
        // declaration that collides with a native fn sharing the namespace must
        // NOT remove it — it correctly surfaces as NativeShadow below.
        let is_redeclare = self.store.get(&qname).is_some();

        self.store
            .declare(record.clone())
            .map_err(custom_to_fn_err)?;

        if is_redeclare {
            let pid = uni_plugin::PluginId::new(declared_plugin_id(&qname));
            if let Ok(qn) = QName::parse(&qname) {
                self.registry.remove_named_unique(&pid, &qn);
            }
        }

        match install_function_into_registry(&self.registry, &record) {
            Ok(()) => {}
            Err(CustomError::NativeShadow(_)) => {
                let mut record = record.clone();
                record.active = false;
                self.store.replace(record.clone());
                self.persistence
                    .save(&record)
                    .map_err(|e| FnError::new(0xB20, format!("declareFunction persist: {e}")))?;
                return single_bool("registered", false);
            }
            Err(e) => {
                // Roll back the store entry on registration failure.
                let _ = self.store.drop_declared(&qname);
                return Err(custom_to_fn_err(e));
            }
        }

        self.persistence
            .save(&record)
            .map_err(|e| FnError::new(0xB20, format!("declareFunction persist: {e}")))?;

        single_bool("registered", true)
    }
}

/// Compile a declared-function record into a [`DeclaredScalarFn`]
/// and register it into `registry` under a synthetic plugin id
/// derived from the qname's namespace.
///
/// # Errors
///
/// Returns [`CustomError::BodyParse`] if the body fails Cypher
/// expression parsing, [`CustomError::NativeShadow`] if the qname
/// is already taken in `registry`, or [`CustomError::Registration`]
/// on other registrar errors.
pub fn install_function_into_registry(
    registry: &Arc<PluginRegistry>,
    record: &DeclaredPlugin,
) -> Result<(), CustomError> {
    let parsed_body =
        parse_expression(&record.body).map_err(|e| CustomError::BodyParse(format!("{e:?}")))?;
    let sig_meta: serde_json::Value = serde_json::from_str(&record.signature_json)
        .map_err(|e| CustomError::BodyParse(format!("signature_json: {e}")))?;
    // #233 Tier 1: these defaulted to `"string"` and an empty argument list,
    // so a signature that failed to decode came back after a restart as
    // `RETURNS string` taking no arguments — a different procedure with the
    // same name, reported as if it were the declared one.
    let return_type_str = sig_meta
        .get("return_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            CustomError::BodyParse("signature_json: missing or non-string `return_type`".to_owned())
        })?;
    let arg_names: Vec<String> = sig_meta
        .get("arg_names")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .ok_or_else(|| {
            CustomError::BodyParse("signature_json: missing or non-array `arg_names`".to_owned())
        })?;

    let return_dt = type_str_to_arrow(return_type_str).ok_or_else(|| {
        CustomError::BodyParse(format!("unknown return type `{return_type_str}`"))
    })?;
    let arg_pairs: Vec<(String, DataType)> = arg_names
        .iter()
        .map(|n| (n.clone(), DataType::Utf8))
        .collect();
    let signature = DeclaredScalarFn::build_signature(return_dt, &arg_pairs);
    let scalar_fn = DeclaredScalarFn::new(parsed_body, arg_names, signature.clone());

    // Cypher canonicalizes function names to lowercase at
    // lookup time; mirror that here so user-declared camelCase
    // qnames are still resolvable.
    let qname = QName::new(
        declared_plugin_id(&record.qname),
        local_part(&record.qname).to_ascii_lowercase(),
    );
    let plugin = SyntheticScalarPlugin {
        plugin_id: PluginId::new(declared_plugin_id(&record.qname)),
        qname,
        signature,
        function: Arc::new(scalar_fn) as Arc<dyn ScalarPluginFn>,
        manifest: std::sync::OnceLock::new(),
    };
    let manifest = plugin.manifest().clone();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id, &caps, registry);
    plugin
        .register(&mut r)
        .map_err(|e| map_plugin_error(e, &record.qname))?;
    r.commit_to_registry()
        .map_err(|e| map_plugin_error(e, &record.qname))?;
    Ok(())
}

/// Install a synthesized procedure (M9 cutover, M11 A.3).
///
/// The synthesizer builds a host-side `ProcedurePlugin` whose
/// `invoke()` runs the declared body via the write-enabled
/// `QueryProcedureHost::execute_inner_query`. We pull its
/// `signature()` and register it under the declared qname.
pub(super) fn install_synthesized_procedure(
    registry: &Arc<PluginRegistry>,
    record: &DeclaredPlugin,
    synthesizer: &dyn crate::ProcedureBodySynthesizer,
) -> Result<(), CustomError> {
    let plugin = synthesizer
        .synthesize(record)
        .map_err(CustomError::Registration)?;
    let qname = QName::new(
        declared_plugin_id(&record.qname),
        local_part(&record.qname).to_ascii_lowercase(),
    );
    let signature = plugin.signature().clone();
    let caps = {
        let mut s = uni_plugin::CapabilitySet::new();
        s.insert(uni_plugin::Capability::Procedure);
        // Inherit declared write/schema/dbms variant from the
        // signature so the registrar's capability gate accepts
        // the registration.
        match signature.mode {
            uni_plugin::traits::procedure::ProcedureMode::Write => {
                s.insert(uni_plugin::Capability::ProcedureWrites);
            }
            uni_plugin::traits::procedure::ProcedureMode::Schema => {
                s.insert(uni_plugin::Capability::ProcedureSchema);
            }
            uni_plugin::traits::procedure::ProcedureMode::Dbms => {
                s.insert(uni_plugin::Capability::ProcedureDbms);
            }
            // `Read` and any future modes require no extra
            // capability beyond the base `Procedure`.
            _ => {}
        }
        s
    };
    let plugin_id = uni_plugin::PluginId::new(declared_plugin_id(&record.qname));
    let mut r = PluginRegistrar::new(plugin_id, &caps, registry);
    r.procedure(qname, signature, plugin)
        .map_err(|e| map_plugin_error(e, &record.qname))?;
    r.commit_to_registry()
        .map_err(|e| map_plugin_error(e, &record.qname))?;
    Ok(())
}

/// Install a synthesized trigger (WS-A).
///
/// The synthesizer builds a host-side `TriggerPlugin` whose `fire()`
/// runs the declared action body via the write-enabled
/// `QueryProcedureHost::execute_inner_query`. Unlike
/// [`install_synthesized_procedure`], the plugin is registered into
/// the registry's **trigger** surface (`PluginRegistrar::trigger`),
/// so the transaction commit-path router fires it — it is NOT a
/// callable procedure. Requires the plugin id to carry
/// [`uni_plugin::Capability::Trigger`] (the registrar's gate).
pub(super) fn install_synthesized_trigger(
    registry: &Arc<PluginRegistry>,
    record: &DeclaredPlugin,
    synthesizer: &dyn crate::TriggerBodySynthesizer,
) -> Result<(), CustomError> {
    let plugin = synthesizer
        .synthesize(record)
        .map_err(CustomError::Registration)?;
    let mut caps = uni_plugin::CapabilitySet::new();
    caps.insert(uni_plugin::Capability::Trigger);
    let plugin_id = uni_plugin::PluginId::new(declared_plugin_id(&record.qname));
    let mut r = PluginRegistrar::new(plugin_id, &caps, registry);
    r.trigger(plugin)
        .map_err(|e| map_plugin_error(e, &record.qname))?;
    r.commit_to_registry()
        .map_err(|e| map_plugin_error(e, &record.qname))?;
    Ok(())
}

/// Synthetic [`Plugin`] wrapping a single declared scalar function.
struct SyntheticScalarPlugin {
    plugin_id: PluginId,
    qname: QName,
    signature: uni_plugin::traits::scalar::FnSignature,
    function: Arc<dyn ScalarPluginFn>,
    /// Lazily-built, then cached, manifest. Each synthetic plugin
    /// has a distinct manifest, so it cannot be a shared static;
    /// the `OnceLock` gives `manifest()` a stable `&` reference
    /// without leaking a fresh `Box` on every call.
    manifest: std::sync::OnceLock<PluginManifest>,
}

impl std::fmt::Debug for SyntheticScalarPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyntheticScalarPlugin")
            .field("plugin_id", &self.plugin_id)
            .field("qname", &self.qname)
            .finish_non_exhaustive()
    }
}

impl SyntheticScalarPlugin {
    fn build_manifest(&self) -> PluginManifest {
        PluginManifest {
            id: self.plugin_id.clone(),
            version: Version::new(0, 0, 1),
            abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::ScalarFn]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "Declared scalar function (apoc.custom analogue).".to_owned(),
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

impl Plugin for SyntheticScalarPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| self.build_manifest())
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.scalar_fn(
            self.qname.clone(),
            self.signature.clone(),
            Arc::clone(&self.function),
        )?;
        Ok(())
    }
}

// -------------------------------------------------------------
// declareAggregate
// -------------------------------------------------------------

/// Implementation of `uni.plugin.declareAggregate`.
///
/// Parses three Cypher expression bodies (`init` / `update` /
/// `finalize`) at declare time, persists a
/// [`DeclaredPlugin`] record with kind `"aggregate"`, and registers
/// a synthetic [`uni_plugin::traits::aggregate::AggregatePluginFn`]
/// (`DeclaredAggregateFn`) into the shared registry. The new
/// aggregate becomes invokable from Cypher (`RETURN myAgg(x)`) via
/// the planner fall-through to
/// `crate::query::df_udaf_plugin::PluginAggregateUdaf` in
/// `uni-query`.
#[derive(Debug)]
pub struct DeclareAggregateProcedure {
    store: Arc<DeclaredPluginStore>,
    persistence: Arc<dyn Persistence>,
    registry: Arc<PluginRegistry>,
}

impl DeclareAggregateProcedure {
    /// Construct.
    #[must_use]
    pub fn new(
        store: Arc<DeclaredPluginStore>,
        persistence: Arc<dyn Persistence>,
        registry: Arc<PluginRegistry>,
    ) -> Self {
        Self {
            store,
            persistence,
            registry,
        }
    }
}

impl ProcedurePlugin for DeclareAggregateProcedure {
    fn signature(&self) -> &ProcedureSignature {
        static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
        SIG.get_or_init(declare_aggregate_signature)
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let qname = extract_string(args, 0, "qname")?;
        let init_src = extract_string(args, 1, "init_expr")?;
        let update_src = extract_string(args, 2, "update_expr")?;
        let finalize_src = extract_string(args, 3, "finalize_expr")?;
        let return_type = extract_string_or(args, 4, "float");
        let arg_names_json = extract_string_or(args, 5, "[]");
        let arg_names: Vec<String> = serde_json::from_str(&arg_names_json).map_err(|e| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("declareAggregate: arg_names_json parse: {e}"),
            )
        })?;
        let dependencies = parse_deps(args, 6)?;
        let declared_by = principal_id(&ctx);

        let record = DeclaredPlugin {
            qname: qname.clone(),
            kind: "aggregate".to_owned(),
            // `body` is informational — the three Cypher source
            // strings travel through `signature_json` (single JSON
            // blob) so persistence round-trips them together.
            body: update_src.clone(),
            signature_json: serde_json::to_string(&serde_json::json!({
                "init": init_src,
                "update": update_src,
                "finalize": finalize_src,
                "return_type": return_type,
                "arg_names": arg_names,
            }))
            .unwrap_or_else(|_| "{}".to_owned()),
            dependencies,
            declared_by,
            active: true,
        };

        self.store
            .declare(record.clone())
            .map_err(custom_to_fn_err)?;

        match crate::aggregate::install_aggregate_into_registry(&self.registry, &record) {
            Ok(()) => {}
            Err(CustomError::NativeShadow(_)) => {
                let mut record = record.clone();
                record.active = false;
                self.store.replace(record.clone());
                self.persistence
                    .save(&record)
                    .map_err(|e| FnError::new(0xB21, format!("declareAggregate persist: {e}")))?;
                return single_bool("registered", false);
            }
            Err(e) => {
                let _ = self.store.drop_declared(&qname);
                return Err(custom_to_fn_err(e));
            }
        }

        self.persistence
            .save(&record)
            .map_err(|e| FnError::new(0xB21, format!("declareAggregate persist: {e}")))?;

        single_bool("registered", true)
    }
}

// -------------------------------------------------------------
// declareProcedure / declareTrigger
// (record-and-persist; full body execution rides on M11's
// write-enabled `ProcedureHost::execute_inner_query`)
// -------------------------------------------------------------

macro_rules! declare_kind_procedure {
    ($name:ident, $sig_fn:ident, $kind:literal, $field_count:literal) => {
        /// Record-and-persist implementation for a declare* kind.
        ///
        /// Stores the declaration through [`Persistence`]. When a
        /// host-supplied procedure-body synthesizer is attached,
        /// the declaration also installs an executable plugin via
        /// `crate::procedures::install_synthesized_procedure`
        /// (M11 A.3).
        #[derive(Debug)]
        pub struct $name {
            store: Arc<DeclaredPluginStore>,
            persistence: Arc<dyn Persistence>,
            registry: Arc<uni_plugin::PluginRegistry>,
            synthesizer: Option<Arc<dyn crate::ProcedureBodySynthesizer>>,
            /// WS-A trigger synthesizer, used only by the
            /// `declareTrigger` variant of this macro.
            trigger_synthesizer: Option<Arc<dyn crate::TriggerBodySynthesizer>>,
        }

        impl $name {
            /// Construct without a synthesizer (record-only).
            #[must_use]
            pub fn new(store: Arc<DeclaredPluginStore>, persistence: Arc<dyn Persistence>) -> Self {
                Self {
                    store,
                    persistence,
                    registry: Arc::new(uni_plugin::PluginRegistry::new()),
                    synthesizer: None,
                    trigger_synthesizer: None,
                }
            }

            /// Construct with a host-supplied procedure-body
            /// synthesizer so declarations install executable
            /// plugins at declare time (M11 A.3).
            #[must_use]
            pub fn new_with_synthesis(
                store: Arc<DeclaredPluginStore>,
                persistence: Arc<dyn Persistence>,
                registry: Arc<uni_plugin::PluginRegistry>,
                synthesizer: Arc<dyn crate::ProcedureBodySynthesizer>,
            ) -> Self {
                Self {
                    store,
                    persistence,
                    registry,
                    synthesizer: Some(synthesizer),
                    trigger_synthesizer: None,
                }
            }

            /// Construct with a host-supplied trigger-body
            /// synthesizer so declared triggers install executable
            /// `TriggerPlugin`s at declare time (WS-A).
            #[must_use]
            pub fn new_with_trigger_synthesis(
                store: Arc<DeclaredPluginStore>,
                persistence: Arc<dyn Persistence>,
                registry: Arc<uni_plugin::PluginRegistry>,
                trigger_synthesizer: Arc<dyn crate::TriggerBodySynthesizer>,
            ) -> Self {
                Self {
                    store,
                    persistence,
                    registry,
                    synthesizer: None,
                    trigger_synthesizer: Some(trigger_synthesizer),
                }
            }
        }

        impl ProcedurePlugin for $name {
            fn signature(&self) -> &ProcedureSignature {
                static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
                SIG.get_or_init($sig_fn)
            }

            fn invoke(
                &self,
                ctx: ProcedureContext<'_>,
                args: &[ColumnarValue],
            ) -> Result<SendableRecordBatchStream, FnError> {
                let qname = extract_string(args, 0, "qname")?;
                // Name the persisted keys after the declared
                // signature's positional args (e.g. `body`,
                // `event_filter`, `yield_json`) instead of opaque
                // `arg1`/`arg2` placeholders.
                let sig_args = $sig_fn().args;
                let mut sig = serde_json::Map::new();
                // `$field_count - 1` skips the trailing `deps_json`
                // arg, which is parsed separately via `parse_deps`.
                for i in 1..($field_count - 1) {
                    let key = sig_args
                        .get(i)
                        .map(|a| a.name.to_string())
                        .unwrap_or_else(|| format!("arg{i}"));
                    let v = extract_string(args, i, &key)?;
                    sig.insert(key, serde_json::Value::String(v));
                }
                // M11 A.1: for procedure-kind declarations, extract
                // the `mode` arg (position 2 — qname=0, body=1,
                // mode=2) and (a) gate WRITE-mode declarations on
                // the principal's `ProcedureWrites` capability,
                // (b) stash `mode` under a named key so the host's
                // `SyntheticProcedurePlugin` can read it back
                // without relying on positional `arg2`.
                if $kind == "procedure" {
                    if let Ok(mode_str) = extract_string(args, 2, "mode") {
                        let mode_uc = mode_str.to_ascii_uppercase();
                        if mode_uc == "WRITE" {
                            let has_writes = ctx
                                .principal
                                .map(|p| {
                                    p.capabilities
                                        .contains_variant(&uni_plugin::Capability::ProcedureWrites)
                                })
                                .unwrap_or(false);
                            if !has_writes {
                                return Err(FnError::new(
                                    0xB09,
                                    format!(
                                        "declareProcedure WRITE for `{qname}` denied: \
                                         principal lacks `Capability::ProcedureWrites`"
                                    ),
                                ));
                            }
                        }
                        sig.insert("mode".to_owned(), serde_json::Value::String(mode_uc));
                    }
                }
                let dependencies = parse_deps(args, $field_count - 1)?;
                let declared_by = principal_id(&ctx);
                // `body` mirrors the declared signature's `body`
                // argument by NAME, not by a hardcoded position.
                // The Cypher body sits at position 1 for
                // `declareProcedure` but at position 2 for
                // `declareTrigger` (whose position 1 is
                // `event_filter`); keying off the name stores the
                // real Cypher body in every case.
                let body = sig
                    .get("body")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_owned();
                let record = DeclaredPlugin {
                    qname: qname.clone(),
                    kind: $kind.to_owned(),
                    body,
                    signature_json: serde_json::to_string(&sig).unwrap_or_default(),
                    dependencies,
                    declared_by,
                    active: true,
                };
                self.store
                    .declare(record.clone())
                    .map_err(custom_to_fn_err)?;
                if $kind == "trigger" {
                    // WS-A: route trigger kinds through the trigger
                    // synthesizer so they land in `reg.triggers()`
                    // (fired by the commit-path router) rather than
                    // as a callable procedure. The synthesizer is
                    // run BEFORE persisting so an unsupported filter
                    // (notably `[SYNC]`) never persists a record that
                    // would fail on restart replay.
                    match self.trigger_synthesizer.as_ref() {
                        Some(synth) => {
                            match crate::procedures::install_synthesized_trigger(
                                &self.registry,
                                &record,
                                synth.as_ref(),
                            ) {
                                Ok(()) => {
                                    self.persistence.save(&record).map_err(|e| {
                                        FnError::new(0xB30, format!("declare persist: {e}"))
                                    })?;
                                }
                                // Qname already taken by a native
                                // trigger: downgrade to inactive.
                                Err(CustomError::NativeShadow(_)) => {
                                    let mut shadowed = record.clone();
                                    shadowed.active = false;
                                    self.store.replace(shadowed.clone());
                                    let _ = self.persistence.save(&shadowed);
                                }
                                // Unsupported filter (e.g. `[SYNC]`)
                                // or bad body: roll back the
                                // in-memory declaration and refuse —
                                // nothing is persisted.
                                Err(other) => {
                                    let _ = self.store.drop_declared(&qname);
                                    return Err(FnError::new(
                                        0xB31,
                                        format!("declare trigger synthesize: {other}"),
                                    ));
                                }
                            }
                        }
                        None => {
                            // Record-only (no trigger synthesizer).
                            self.persistence.save(&record).map_err(|e| {
                                FnError::new(0xB30, format!("declare persist: {e}"))
                            })?;
                        }
                    }
                } else {
                    self.persistence
                        .save(&record)
                        .map_err(|e| FnError::new(0xB30, format!("declare persist: {e}")))?;
                    // M11 A.3: if the host attached a synthesizer,
                    // install the executable plugin at declare time
                    // so subsequent `CALL <qname>(...)` invocations
                    // dispatch through it.
                    if let Some(synth) = self.synthesizer.as_ref() {
                        if let Err(e) = crate::procedures::install_synthesized_procedure(
                            &self.registry,
                            &record,
                            synth.as_ref(),
                        ) {
                            // NativeShadow is expected when the qname
                            // is already taken; downgrade the record
                            // to inactive but do not fail the
                            // declaration.
                            match e {
                                CustomError::NativeShadow(_) => {
                                    let mut shadowed = record.clone();
                                    shadowed.active = false;
                                    self.store.replace(shadowed.clone());
                                    let _ = self.persistence.save(&shadowed);
                                }
                                other => {
                                    return Err(FnError::new(
                                        0xB31,
                                        format!("declare synthesize: {other}"),
                                    ));
                                }
                            }
                        }
                    }
                }
                single_bool("registered", true)
            }
        }
    };
}

declare_kind_procedure!(
    DeclareProcedureProcedure,
    declare_procedure_signature,
    "procedure",
    5
);
declare_kind_procedure!(
    DeclareTriggerProcedure,
    declare_trigger_signature,
    "trigger",
    4
);

// -------------------------------------------------------------
// helpers
// -------------------------------------------------------------

/// Decode a present [`ColumnarValue`] into a Utf8 [`String`].
///
/// Returns `Some` only for a non-null Utf8 scalar or the first
/// non-null element of a `StringArray`; every other shape (null,
/// empty array, non-Utf8) yields `None`. Shared by [`extract_string`]
/// and [`extract_string_or`].
fn columnar_utf8(cv: &ColumnarValue) -> Option<String> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Some(s.clone()),
        ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .and_then(|a| a.iter().next().flatten().map(|s| s.to_owned())),
        _ => None,
    }
}

/// Like [`extract_string`] but returns `default` when the argument
/// is missing, null, or not a Utf8 string. Used for trailing
/// optional args (`deps_json`, defaulted-on-declare* signatures)
/// since the current procedure dispatch path does not auto-fill
/// defaults from the [`ProcedureSignature`].
fn extract_string_or(args: &[ColumnarValue], i: usize, default: &str) -> String {
    args.get(i)
        .and_then(columnar_utf8)
        .unwrap_or_else(|| default.to_owned())
}

/// Parse the `deps_json` arg at position `i` into a `Vec<String>`,
/// defaulting to an empty vec when absent or null.
fn parse_deps(args: &[ColumnarValue], i: usize) -> Result<Vec<String>, FnError> {
    let raw = extract_string_or(args, i, "[]");
    serde_json::from_str::<Vec<String>>(&raw).map_err(|e| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("declare: deps_json parse: {e}"),
        )
    })
}

fn extract_string(args: &[ColumnarValue], i: usize, name: &str) -> Result<String, FnError> {
    let cv = args.get(i).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("declare procedure missing arg `{name}` at position {i}"),
        )
    })?;
    if let Some(s) = columnar_utf8(cv) {
        return Ok(s);
    }
    // Present but unusable: keep the original diagnostics —
    // explicit null vs anything else (non-Utf8 scalar, non-string
    // array, empty / null-first array).
    let msg = match cv {
        ColumnarValue::Scalar(ScalarValue::Utf8(None) | ScalarValue::Null) => {
            format!("declare procedure arg `{name}` was null")
        }
        _ => format!("declare procedure arg `{name}` not Utf8"),
    };
    Err(FnError::new(FnError::CODE_TYPE_COERCION, msg))
}

/// Principal id of the declaring caller, or `"anonymous"` when the
/// invocation carries no principal. Shared by every `declare*`
/// procedure to stamp [`DeclaredPlugin::declared_by`].
fn principal_id(ctx: &ProcedureContext<'_>) -> String {
    ctx.principal
        .map(|p| p.id.clone())
        .unwrap_or_else(|| "anonymous".to_owned())
}

fn single_bool(col: &str, v: bool) -> Result<SendableRecordBatchStream, FnError> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(col, DataType::Boolean, false)]));
    let arr: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![v]));
    let batch = RecordBatch::try_new(schema, vec![arr])
        .map_err(|e| FnError::new(0xB02, format!("single bool: {e}")))?;
    Ok(batch_into_stream(batch))
}

fn custom_to_fn_err(e: CustomError) -> FnError {
    let code = match &e {
        CustomError::DependencyCycle(_) => 0xB03,
        CustomError::DependencyMissing { .. } => 0xB04,
        CustomError::NativeShadow(_) => 0xB05,
        CustomError::BodyParse(_) => 0xB06,
        CustomError::Persistence(_) => 0xB07,
        CustomError::Registration(_) => 0xB08,
        CustomError::CapabilityDenied(_) => 0xB09,
    };
    FnError::new(code, e.to_string())
}
