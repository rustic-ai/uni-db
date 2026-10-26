#!/usr/bin/env python3
"""Assert the Rust surface and the Python bindings have not drifted apart.

Four guards already police the Python bindings, and every one of them compares
Python to Python: `test_sync_async_parity.py` (sync class vs async twin),
`test_stub_drift.py` (runtime vs `__init__.pyi`, both directions), `stubtest`,
and `check_doc_symbols.py`. Nothing compares Rust to Python, so a field or an
error variant added on the Rust side and never bound is not a drift failure --
it is an absence, and absence is exactly what none of those detect.

That is how two defects reached a release. `UniConfig` grew to 40 public fields
while `config({...})` understood 12, and the extractor accepted an unrecognised
key in silence, so `config({"ssi_enabled": False})` asked for a setting, raised
nothing, and did nothing.

This does not demand that every Rust item be bound -- most of the unbound ones
are deliberate. It demands the gap be *written down*. A new field or variant
fails this check until somebody either binds it or adds it to the allowlist
below with a reason, which turns an invisible omission into a reviewed decision.

Exit code 0 = no undeclared drift, 1 = an item is neither bound nor allowlisted.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG_RS = ROOT / "crates/uni-common/src/config.rs"
CONVERT_RS = ROOT / "bindings/uni-db/src/convert.rs"
ERROR_RS = ROOT / "crates/uni-common/src/api/error.rs"
EXCEPTIONS_RS = ROOT / "bindings/uni-db/src/exceptions.rs"

# Builder / result parity (third axis).
RUST_BUILDER_SRC = [
    ROOT / "crates/uni/src/api/session.rs",
    ROOT / "crates/uni/src/api/transaction.rs",
    ROOT / "crates/uni/src/api/locy_builder.rs",
    # `tx.bulk_writer()` / `tx.appender()` hand out the `uni_bulk` builders
    # directly (re-exported through `uni::api::{bulk, appender}`).
    ROOT / "crates/uni-bulk/src/bulk.rs",
    ROOT / "crates/uni-bulk/src/appender.rs",
]
# `QueryResult` and `LocyResult` live outside the api/ builder files. Both
# `LocyResult` impls are included deliberately: `uni::api::locy_result` is the
# wrapper Python is handed and `uni_locy::result` is what it delegates to, so
# the surface a caller sees is the union.
RUST_RESULT_SRC = [
    ROOT / "crates/uni-query/src/types.rs",
    ROOT / "crates/uni/src/api/locy_result.rs",
    ROOT / "crates/uni-locy/src/result.rs",
]
PY_BINDING_SRC = [
    ROOT / "bindings/uni-db/src/builders.rs",
    ROOT / "bindings/uni-db/src/sync_api.rs",
    ROOT / "bindings/uni-db/src/types.rs",
]

# Rust type -> Python (sync) pyclass. The async twin is covered by
# `test_sync_async_parity.py`, which asserts exact method-set equality against
# the sync class, so checking both here would only double the allowlist.
BUILDER_PARITY = {
    "QueryBuilder": "SessionQueryBuilder",
    "TxQueryBuilder": "TxQueryBuilder",
    "LocyBuilder": "SessionLocyBuilder",
    "TxLocyBuilder": "TxLocyBuilder",
    "BulkWriterBuilder": "TxBulkWriterBuilder",
    "AppenderBuilder": "TxAppenderBuilder",
}
RESULT_PARITY = {
    "QueryResult": "QueryResult",
    "LocyResult": "LocyResult",
}

# Fourth axis: public *fields* of data structs that Python receives as a
# pyclass. The method axis above cannot see these -- `QueryMetrics` has no
# methods, so five counters added on the Rust side (`physical_plans`,
# `physical_plan_time`, `shortest_path_searches`, `subquery_executions`,
# `lance_iops`) were simply absent from Python and nothing noticed.
#
# Rust type -> (source file, Python pyclass name).
FIELD_PARITY = {
    "QueryMetrics": (ROOT / "crates/uni-query/src/types.rs", "QueryMetrics"),
    "ProfileOutput": (
        ROOT / "crates/uni-query/src/query/executor/core.rs",
        "ProfileOutput",
    ),
    "LocyStats": (ROOT / "crates/uni-locy/src/result.rs", "LocyStats"),
    "DatabaseMetrics": (ROOT / "crates/uni/src/api/mod.rs", "DatabaseMetrics"),
}
# A `Duration` field is bound as a float with its unit in the name; either
# suffix counts as the field being bound.
DURATION_SUFFIXES = ("_ms", "_secs")

# Fifth axis: `LocyConfig` fields. Python has two independent ways to build
# one, each with its own converter, so each is checked on its own: the dict
# form (`with_config({...})`, read key by key in `extract_locy_config`) and the
# `LocyConfig(...)` pyclass (its `#[new]` keyword arguments). A field bound in
# one and forgotten in the other is exactly the drift this exists to catch.
LOCY_CONFIG_RS = ROOT / "crates/uni-locy/src/config.rs"
TYPES_RS = ROOT / "bindings/uni-db/src/types.rs"

# ---------------------------------------------------------------------------
# Allowlists: a Rust item deliberately not reachable from Python.
#
# Each entry needs a reason. "Not bound yet" is a reason; the point is that
# somebody looked. Deleting an entry whose item got bound is the other half --
# a stale allowlist entry is reported too, so this cannot rot into a rubber
# stamp.
# ---------------------------------------------------------------------------
CONFIG_NOT_BOUND = {
    "max_frontier_size": "traversal safety cap; no user has asked to tune it",
    "auto_flush_threshold": "flush tuning, Rust-embedder concern",
    "auto_flush_interval": "flush tuning, Rust-embedder concern",
    "auto_flush_min_mutations": "flush tuning, Rust-embedder concern",
    "commit_channel_capacity": "internal channel sizing",
    "commit_timeout": "not bound yet; reachable need, no binding written",
    "file_sandbox": "nested struct; security policy set by the embedder",
    "object_store": "nested struct; Python uses cloud_config() instead",
    "index_rebuild": "nested struct; needs a Python dataclass to bind well",
    "max_compaction_rows": "compaction OOM guard, Rust-embedder concern",
    "max_recursive_cte_iterations": "not bound yet",
    "partial_lance_writes": "deprecated no-op; never bound, so Python rejects the key",
    "defer_embeddings": "not bound yet",
    "fork_fragment_warn_threshold": "operational warning threshold",
    "tx_id_reservoir_batch": "internal id allocation",
    "async_flush_enabled": "flush tuning, Rust-embedder concern",
    "max_pending_flushes": "flush tuning, Rust-embedder concern",
    "flush_stream_timeout": "flush tuning, Rust-embedder concern",
    "drop_fork_drain_timeout": "fork shutdown tuning",
    "flush_drain_timeout": "flush tuning, Rust-embedder concern",
    "fork_index_build_threshold": "fork index auto-builder tuning",
    "fork_index_builder_interval": "fork index auto-builder tuning",
    "disable_fork_index_builder": "test-only knob",
    "ssi_enabled": "not bound yet; defaults on, and turning it off is unsafe",
    "ec_flush_interval": "eventually-consistent flush tuning",
    "ec_flush_threshold": "eventually-consistent flush tuning",
}

ERROR_NO_DEDICATED_EXCEPTION = {
    "TransactionRollbackOnly": "collapses to UniError; no payload to lose",
    "GraphComputeIncomplete": "not bound yet; LocyIncomplete has one, this does not",
    "TriggerRejected": "triggers are a Rust-side surface",
    "AuthenticationFailed": "auth is not reachable through the Python entry points",
    "AuthorizationDenied": "auth is not reachable through the Python entry points",
    "EphemeralWriteAttempt": "scratch sessions are not bound to Python",
    "ForkNameInvalid": "collapses to UniError; message carries the reason",
    "PendingFlushTimeout": "not bound yet; the only fork-lifecycle variant without one",
    "ForkWritesNotYetSupported": "explicitly routed to base UniError",
    "BulkLoadUnsupported": "only a backend that cannot stage raises it; none is reachable from Python",
    "BulkLoadBlockedByTrigger": "synchronous before-triggers are a Rust-side surface",
}


# Keyed "RustType.method". Same contract as the two above: a gap has to be
# written down with a reason, and a stale or phantom entry is reported.
BUILDER_NOT_BOUND = {
    # --- Locy builders -------------------------------------------------------
    # Reachable from Python, just not as a chainable method.
    "LocyBuilder.allow_partial": "reachable via with_config(LocyConfig(allow_partial=...))",
    "TxLocyBuilder.allow_partial": "reachable via with_config(LocyConfig(allow_partial=...))",
    "LocyBuilder.params_map": "Rust convenience over `params`; Python `params` already takes a dict",
    "TxLocyBuilder.params_map": "Rust convenience over `params`; Python `params` already takes a dict",
    "TxLocyBuilder.params": "bulk params is session-only on the Locy arm; not bound yet",
    # --- Bulk builders -------------------------------------------------------
    "BulkWriterBuilder.new_unguarded": "constructor; Python gets the builder from tx.bulk_writer()",
    "AppenderBuilder.new_from_tx": "constructor; Python gets the builder from tx.appender_builder()",
    # --- QueryResult ---------------------------------------------------------
    # Python's QueryResult is a data class: Rust's constructors, internal
    # mutators and iterator adaptors have no counterpart by design.
    "QueryResult.new": "constructor",
    "QueryResult.set_counters": "internal; populated inside the executor",
    "QueryResult.set_frontend_timing": "internal; populated inside the executor",
    "QueryResult.update_parse_timing": "internal; populated inside the executor",
    "QueryResult.into_rows": "by-value Rust accessor; Python `rows` already owns its list",
    "QueryResult.iter": "Rust iterator; Python iterates the `rows` list",
    "QueryResult.len": "Python uses __len__",
    "QueryResult.is_empty": "Python uses __bool__",
    "QueryResult.has_warnings": "Python uses `bool(result.warnings)`",
    # --- LocyResult ----------------------------------------------------------
    "LocyResult.into_inner": "Rust unwrap; Python receives the converted fields",
    "LocyResult.into_parts": "Rust unwrap; Python receives the converted fields",
    "LocyResult.with_derived": "Rust constructor used by the engine",
    "LocyResult.with_lazy_derived": "Rust constructor used by the engine",
    "LocyResult.derived_rules": "bound as the `derived` property and `derived_async()`",
    "LocyResult.derived_rule": "bound as `derived_facts(rule)`",
    "LocyResult.evaluated_rules": "not bound; Python exposes only the complete `derived` view",
    "LocyResult.into_evaluated_rules": "by-value Rust accessor; not bound, as `evaluated_rules`",
    "LocyResult.lazily_derived_rules": "not bound yet; Python derives them on first `derived` access",
    "LocyResult.lazy_derived": "Rust handle the binding itself holds to derive on first access",
}


# `LocyConfig` fields absent from the `with_config({...})` dict form.
LOCY_DICT_NOT_BOUND = {
    "semiring": "not bound yet; needs a string mapping for SemiringKind",
    "neural_predicates_preview": "opt-out kill switch for CREATE MODEL; Rust-embedder concern",
    "classifier_cache": "shared Rust-side cache handle; no Python representation",
    "classifier_cache_max": "not bound yet",
    "classifier_provenance_store": "always set by the binding itself, not by the caller",
}

# `LocyConfig` fields absent from the `LocyConfig(...)` constructor.
LOCY_OBJECT_NOT_BOUND = {
    **LOCY_DICT_NOT_BOUND,
    "params": "bound on the builder (`.params(...)`) and in the dict form",
    "classifier_registry": "bound as the `register_classifier` method",
}

# Keyed "RustType.field".
FIELD_NOT_BOUND = {
    "ProfileOutput.explain": "bound as `plan_text`; the cost/index detail is session.explain()",
    "ProfileOutput.runtime_stats": "bound as `operators`, a list of dicts",
    "DatabaseMetrics.compaction_status": "bound as `compaction_in_progress`, its one flag",
}


def fail(lines: list[str]) -> None:
    print("\n".join(lines), file=sys.stderr)
    sys.exit(1)


def uni_config_fields() -> list[str]:
    """Public field names of `struct UniConfig`, in declaration order."""
    src = CONFIG_RS.read_text()
    m = re.search(r"pub struct UniConfig\s*\{(.*?)\n\}", src, re.S)
    if not m:
        fail(["could not locate `pub struct UniConfig` in %s" % CONFIG_RS])
    return re.findall(r"^\s+pub ([a-z_0-9]+):", m.group(1), re.M)


def python_config_keys() -> list[str]:
    """Keys listed in `UNI_CONFIG_KEYS`, the extractor's source of truth."""
    src = CONVERT_RS.read_text()
    m = re.search(r"pub const UNI_CONFIG_KEYS: &\[&str\] = &\[(.*?)\];", src, re.S)
    if not m:
        fail(["could not locate `UNI_CONFIG_KEYS` in %s" % CONVERT_RS])
    return re.findall(r'"([a-z_0-9]+)"', m.group(1))


# Nested config structs bound as dicts under one `config({...})` key, and the
# const in convert.rs listing the keys that dict accepts. Every field must be
# bound: these are small, user-facing, and have no allowlist.
NESTED_CONFIG = {
    "CompactionConfig": "COMPACTION_CONFIG_KEYS",
    "WriteThrottleConfig": "THROTTLE_CONFIG_KEYS",
}


def const_str_list(name: str) -> list[str]:
    """String entries of `pub const <name>: &[&str] = &[...]` in convert.rs."""
    src = CONVERT_RS.read_text()
    m = re.search(rf"pub const {name}: &\[&str\] = &\[(.*?)\];", src, re.S)
    if not m:
        fail([f"could not locate `{name}` in {CONVERT_RS}"])
    return re.findall(r'"([a-z_0-9]+)"', m.group(1))


def error_variants() -> list[str]:
    """Variant names of the public `UniError` enum.

    Non-greedy to the first closing brace at column zero: `error.rs` declares
    two more enums after this one (`LocyIncompleteReason`,
    `GraphComputeIncompleteReason`), and a greedy match swallows their variants
    too -- which showed up as `Timeout` and `IterationLimit` appearing twice.
    """
    src = ERROR_RS.read_text()
    m = re.search(r"pub enum UniError\s*\{(.*?)\n\}", src, re.S)
    if not m:
        fail(["could not locate `pub enum UniError` in %s" % ERROR_RS])
    # A variant is a CamelCase identifier at one indent level, not inside a
    # `#[...]` attribute or a doc comment.
    return re.findall(r"^    ([A-Z][A-Za-z0-9]*)\s*[({,]", m.group(1), re.M)


def mapped_error_variants() -> set[str]:
    """Variants mapped to a *dedicated* Python exception in `uni_error_to_pyerr`.

    The arms are bare identifiers, not `UniError::X` -- the function opens with
    `use uni_common::UniError::*`. Matching the qualified form finds nothing and
    reports every variant as unmapped, which is how this was first written.
    """
    src = EXCEPTIONS_RS.read_text()
    m = re.search(r"fn uni_error_to_pyerr\b.*?\n    match e \{(.*?)\n    \}", src, re.S)
    if not m:
        fail(["could not locate the `match e` block of `uni_error_to_pyerr` in %s"
              % EXCEPTIONS_RS])
    arms = re.findall(
        # `=>` is followed by a block for every payload-carrying arm, so the
        # optional `{` is load-bearing: without it those arms match nothing and
        # are silently dropped, which reported the nine richest exceptions
        # (`ForkInUse`, `LocyIncomplete`, ...) as unbound.
        r"^        ([A-Z][A-Za-z0-9]*)[^=]*=>\s*\{?\s*([A-Za-z_][A-Za-z0-9_]*)",
        m.group(1),
        re.M,
    )
    # An arm that routes to the base `UniError` is not a dedicated exception --
    # it lands the caller in exactly the same place as the `_ =>` wildcard, so
    # counting it as bound would let a variant look handled while its payload
    # and its type are both gone. `ForkWritesNotYetSupported` is written as an
    # explicit arm and is one of these.
    return {variant for variant, target in arms if target != "UniError"}


def _read(paths: list[Path]) -> str:
    return "\n".join(f.read_text() for f in paths)


def rust_type_methods(name: str) -> list[str]:
    """Public method names of the inherent `impl` block for `name`.

    Three shapes have to be tolerated, all present in these files: the impl
    carries lifetimes on both sides (`impl<'a> TxQueryBuilder<'a> {`), trait
    impls exist for the same types (`impl Drop for Transaction`), and a
    `#[cfg(test)] mod tests` sits at the end of the file. Requiring the opening
    brace on the impl line rejects the trait impls, and bounding the body at the
    first column-zero `}` keeps the test module out.

    `pub ` with the trailing space excludes `pub(crate)`, which is deliberate:
    a crate-internal method is not part of the surface Python should mirror.
    """
    src = _read(RUST_BUILDER_SRC + RUST_RESULT_SRC)
    pattern = (
        r"^impl(?:<[^>]*>)?\s+" + re.escape(name) + r"(?:<[^>]*>)?\s*\{(.*?)\n\}"
    )
    out: list[str] = []
    for body in re.findall(pattern, src, re.S | re.M):
        out += re.findall(r"^    pub (?:async )?fn ([a-z_0-9]+)", body, re.M)
    return out


def _pyclass_names() -> dict[str, str]:
    """Map a binding struct ident to the name Python sees.

    `#[pyclass(name = "X")]` sits on the *struct*, not on the `#[pymethods]`
    impl, and the renames are not cosmetic: `Database` is `Uni`,
    `PyTxQueryBuilder` is `TxQueryBuilder`. Without this map every rename reads
    as a missing class.
    """
    src = _read(PY_BINDING_SRC)
    mapping: dict[str, str] = {}
    for attrs, ident in re.findall(
        r"#\[pyclass([^\]]*)\][^\n]*\n(?:[^\n]*\n)??pub struct (\w+)", src
    ):
        rename = re.search(r'name\s*=\s*"([^"]+)"', attrs)
        mapping[ident] = rename.group(1) if rename else ident
    return mapping


def python_type_members(py_name: str) -> set[str]:
    """Methods and public fields Python sees on `py_name`.

    Result types are `#[pyclass(get_all)]` structs, so a Rust accessor may be
    mirrored by a *field* rather than a method; both count. Methods here are
    bare `fn`, not `pub fn` -- pymethods do not use `pub`. Anchoring on
    `#[pymethods]` is required, not tidiness: `sync_api.rs` has a plain
    `impl Transaction` of Rust-only helpers directly above the pymethods one.
    """
    src = _read(PY_BINDING_SRC)
    idents = [i for i, n in _pyclass_names().items() if n == py_name]
    members: set[str] = set()
    for ident in idents:
        for body in re.findall(
            r"^#\[pymethods\]\nimpl " + re.escape(ident) + r"\s*\{(.*?)\n\}",
            src,
            re.S | re.M,
        ):
            for attrs, fn in re.findall(
                r"((?:^    #\[[^\n]*\]\n)*)^    (?:pub )?(?:async )?fn ([a-z_0-9]+)",
                body,
                re.M,
            ):
                rename = re.search(r'#\[pyo3\(name\s*=\s*"([^"]+)"\)\]', attrs)
                members.add(rename.group(1) if rename else fn)
        for body in re.findall(
            r"^pub struct " + re.escape(ident) + r"\s*\{(.*?)\n\}", src, re.S | re.M
        ):
            members |= set(re.findall(r"^    pub ([a-z_0-9]+):", body, re.M))
    return {m for m in members if not m.startswith("__")}


def rust_struct_fields(path: Path, name: str) -> list[tuple[str, str]]:
    """`(field, type)` pairs of `pub struct name`, in declaration order.

    Bounded at the first column-zero `}`, which is the struct's own closing
    brace: these structs hold no nested braces at column zero.
    """
    src = path.read_text()
    m = re.search(r"^pub struct " + re.escape(name) + r"\s*\{(.*?)\n\}", src,
                  re.S | re.M)
    if not m:
        fail([f"could not locate `pub struct {name}` in {path}"])
    return re.findall(r"^    pub ([a-z_0-9]+):\s*([^,\n]+)", m.group(1), re.M)


def field_is_bound(field: str, ty: str, members: set[str]) -> bool:
    if field in members:
        return True
    # `Option<Duration>` too: `LocyConfig.timeout` is one, bound as `timeout_secs`.
    if re.search(r"\bDuration\b", ty):
        return any(field + suffix in members for suffix in DURATION_SUFFIXES)
    return False


def dict_converter_keys(fn_name: str) -> list[str]:
    """Keys a `convert.rs` function writes into its result dict."""
    src = CONVERT_RS.read_text()
    m = re.search(r"^pub fn " + re.escape(fn_name) + r"\b(.*?)\n\}", src,
                  re.S | re.M)
    if not m:
        fail([f"could not locate `fn {fn_name}` in {CONVERT_RS}"])
    return re.findall(r'set_item\(\s*"([a-z_0-9]+)"', m.group(1))


def python_locy_dict_keys() -> list[str]:
    """Keys `extract_locy_config` reads from the `with_config({...})` dict."""
    src = CONVERT_RS.read_text()
    m = re.search(r"^pub fn extract_locy_config\b(.*?)\n\}", src, re.S | re.M)
    if not m:
        fail([f"could not locate `fn extract_locy_config` in {CONVERT_RS}"])
    return re.findall(r'config\.get\(\s*"([a-z_0-9]+)"', m.group(1))


def python_locy_object_kwargs() -> list[str]:
    """Keyword arguments of the `LocyConfig(...)` pyclass constructor.

    Read from the `#[pyo3(signature = (...))]` on the `#[new]` of the
    `PyLocyConfig` pymethods, which is what Python actually accepts -- a Rust
    parameter missing from the signature is not reachable by keyword.
    """
    src = TYPES_RS.read_text()
    m = re.search(
        r"^impl PyLocyConfig\s*\{.*?#\[new\]\s*#\[pyo3\(signature\s*=\s*\((.*?)\)\)\]",
        src,
        re.S | re.M,
    )
    if not m:
        fail([f"could not locate the `PyLocyConfig` #[new] signature in {TYPES_RS}"])
    return re.findall(r"([a-z_0-9]+)\s*=", m.group(1))


def check(kind: str, rust_items: list[str], bound: set[str],
          allowlist: dict[str, str]) -> list[str]:
    problems: list[str] = []

    undeclared = [i for i in rust_items if i not in bound and i not in allowlist]
    if undeclared:
        problems.append(
            f"{kind}: {len(undeclared)} item(s) exist in Rust, are not reachable from\n"
            f"  Python, and are not allowlisted. Bind them, or add them to\n"
            f"  {Path(__file__).name} with a reason:\n"
            + "".join(f"    - {i}\n" for i in undeclared)
        )

    # A stale entry means the item got bound and nobody cleaned up; left alone,
    # the allowlist slowly becomes a place where anything can hide.
    stale = [i for i in allowlist if i in bound]
    if stale:
        problems.append(
            f"{kind}: {len(stale)} allowlist entr(ies) are now reachable from Python.\n"
            f"  Remove them from {Path(__file__).name}:\n"
            + "".join(f"    - {i}\n" for i in stale)
        )

    phantom = [i for i in allowlist if i not in rust_items]
    if phantom:
        problems.append(
            f"{kind}: {len(phantom)} allowlist entr(ies) name something that no longer\n"
            f"  exists in Rust. Remove them from {Path(__file__).name}:\n"
            + "".join(f"    - {i}\n" for i in phantom)
        )
    return problems


def main() -> int:
    fields = uni_config_fields()
    keys = python_config_keys()
    variants = error_variants()
    mapped = mapped_error_variants()

    # Guard against the checker itself going quiet: a regex that stops matching
    # would otherwise report a clean run over an empty set.
    if len(fields) < 20 or len(variants) < 20 or len(mapped) < 20:
        fail([
            "parity check parsed implausibly little and would pass vacuously:",
            f"  UniConfig fields parsed: {len(fields)}",
            f"  UniError variants parsed: {len(variants)}",
            f"  mapped exception arms parsed: {len(mapped)}",
            "Fix the parser rather than the threshold.",
        ])

    # Over-capture is the other parser failure, and it does not look like one:
    # a greedy enum match pulled in two neighbouring enums and duplicated
    # `Timeout` and `IterationLimit`. A Rust enum cannot repeat a variant name,
    # so a duplicate means the regex left its enum.
    dupes = sorted({v for v in variants if variants.count(v) > 1})
    if dupes:
        fail([
            "parity check over-captured: repeated UniError variant(s) "
            + ", ".join(dupes),
            "A Rust enum cannot repeat a variant, so the body regex ran past "
            "the enum. Fix the parser.",
        ])

    # A key accepted by Python that is not a real field is the mirror failure.
    unknown_keys = [k for k in keys if k not in fields]
    problems: list[str] = []
    if unknown_keys:
        problems.append(
            "config: UNI_CONFIG_KEYS names key(s) that are not UniConfig fields:\n"
            + "".join(f"    - {k}\n" for k in unknown_keys)
        )

    problems += check("config", fields, set(keys), CONFIG_NOT_BOUND)
    problems += check("error", variants, mapped, ERROR_NO_DEDICATED_EXCEPTION)

    # Third axis: named builders and result types, qualified "Type.method".
    # A curated set rather than every builder in the crate -- the allowlist is
    # the cost of coverage, and a list nobody maintains is a rubber stamp.
    rust_members: list[str] = []
    bound_members: set[str] = set()
    for rust_ty, py_ty in {**BUILDER_PARITY, **RESULT_PARITY}.items():
        methods = rust_type_methods(rust_ty)
        if not methods:
            fail([
                f"parity check found no public methods on Rust `{rust_ty}` and "
                "would pass vacuously for it.",
                "Its impl block moved, was renamed, or the regex stopped "
                "matching. Fix the parser rather than dropping the type.",
            ])
        members = python_type_members(py_ty)
        if not members:
            fail([
                f"parity check found no Python members on `{py_ty}` and would "
                "pass vacuously for it.",
                "Check the #[pyclass(name = ...)] mapping before anything else "
                "-- a rename is the usual cause.",
            ])
        rust_members += [f"{rust_ty}.{m}" for m in methods]
        bound_members |= {f"{rust_ty}.{m}" for m in methods if m in members}
    problems += check("builder", rust_members, bound_members, BUILDER_NOT_BOUND)

    # Fourth axis: data-struct fields, qualified "Type.field".
    rust_fields: list[str] = []
    bound_fields: set[str] = set()
    for rust_ty, (path, py_ty) in FIELD_PARITY.items():
        pairs = rust_struct_fields(path, rust_ty)
        members = python_type_members(py_ty)
        if len(pairs) < 3 or not members:
            fail([
                f"parity check parsed {len(pairs)} field(s) on Rust `{rust_ty}` "
                f"and {len(members)} member(s) on Python `{py_ty}`; it would "
                "pass vacuously. Fix the parser.",
            ])
        rust_fields += [f"{rust_ty}.{f}" for f, _ in pairs]
        bound_fields |= {
            f"{rust_ty}.{f}" for f, ty in pairs if field_is_bound(f, ty, members)
        }
    problems += check("field", rust_fields, bound_fields, FIELD_NOT_BOUND)

    # `ExecuteResult.metrics` is a dict built by a second converter, not the
    # `QueryMetrics` pyclass, so it can drift from the class on its own.
    dict_keys = dict_converter_keys("query_metrics_to_py")
    class_fields = python_type_members("QueryMetrics")
    if len(dict_keys) < 3:
        fail(["parity check parsed implausibly few keys from "
              "`query_metrics_to_py`; fix the parser."])
    if set(dict_keys) != class_fields or len(dict_keys) != len(set(dict_keys)):
        problems.append(
            "metrics dict: `query_metrics_to_py` (ExecuteResult.metrics) and the\n"
            "  `QueryMetrics` pyclass disagree:\n"
            + "".join(f"    - only in the dict: {k}\n"
                      for k in sorted(set(dict_keys) - class_fields))
            + "".join(f"    - only in the class: {k}\n"
                      for k in sorted(class_fields - set(dict_keys)))
            + "".join(f"    - repeated in the dict: {k}\n"
                      for k in sorted({k for k in dict_keys
                                       if dict_keys.count(k) > 1}))
        )

    # Fifth axis: `LocyConfig` fields, per Python construction form.
    locy_pairs = rust_struct_fields(LOCY_CONFIG_RS, "LocyConfig")
    locy_fields = [f for f, _ in locy_pairs]
    locy_forms = {
        "locy config dict": (python_locy_dict_keys(), LOCY_DICT_NOT_BOUND),
        "locy config object": (python_locy_object_kwargs(), LOCY_OBJECT_NOT_BOUND),
    }
    locy_bound_counts: dict[str, int] = {}
    for kind, (accepted, allowlist) in locy_forms.items():
        if len(locy_fields) < 15 or len(accepted) < 10:
            fail([
                f"parity check parsed {len(locy_fields)} LocyConfig field(s) and "
                f"{len(accepted)} {kind} key(s); it would pass vacuously. "
                "Fix the parser.",
            ])
        accepted_set = set(accepted)
        bound = {
            f for f, ty in locy_pairs if field_is_bound(f, ty, accepted_set)
        }
        locy_bound_counts[kind] = len(bound)
        unknown = [
            k for k in accepted
            if k not in locy_fields
            and not any(k == f + sfx for f in locy_fields for sfx in DURATION_SUFFIXES)
        ]
        if unknown:
            problems.append(
                f"{kind}: accepts key(s) that are not LocyConfig fields:\n"
                + "".join(f"    - {k}\n" for k in unknown)
            )
        problems += check(kind, locy_fields, bound, allowlist)

    # Sixth axis: nested config structs bound as dicts.
    for struct, const in NESTED_CONFIG.items():
        struct_fields = [f for f, _ in rust_struct_fields(CONFIG_RS, struct)]
        accepted_keys = const_str_list(const)
        if len(struct_fields) < 3:
            fail([f"parity check parsed {len(struct_fields)} {struct} field(s); "
                  "it would pass vacuously. Fix the parser."])
        if set(struct_fields) != set(accepted_keys):
            problems.append(
                f"nested config: `{const}` and `{struct}` disagree:\n"
                + "".join(f"    - field not accepted: {f}\n"
                          for f in sorted(set(struct_fields) - set(accepted_keys)))
                + "".join(f"    - key with no field: {k}\n"
                          for k in sorted(set(accepted_keys) - set(struct_fields)))
            )

    if problems:
        fail(["Rust <-> Python parity drift:", ""] + problems)

    print(
        f"OK: {len(keys)}/{len(fields)} UniConfig fields reachable from Python "
        f"({len(CONFIG_NOT_BOUND)} allowlisted); "
        f"{len(mapped)}/{len(variants)} UniError variants mapped to a dedicated "
        f"exception ({len(ERROR_NO_DEDICATED_EXCEPTION)} allowlisted); "
        f"{len(bound_members)}/{len(rust_members)} methods on "
        f"{len(BUILDER_PARITY) + len(RESULT_PARITY)} builder/result types bound "
        f"({len(BUILDER_NOT_BOUND)} allowlisted); "
        f"{len(bound_fields)}/{len(rust_fields)} fields on "
        f"{len(FIELD_PARITY)} data structs bound "
        f"({len(FIELD_NOT_BOUND)} allowlisted); "
        f"LocyConfig fields reachable: "
        f"{locy_bound_counts['locy config dict']}/{len(locy_fields)} via dict "
        f"({len(LOCY_DICT_NOT_BOUND)} allowlisted), "
        f"{locy_bound_counts['locy config object']}/{len(locy_fields)} via "
        f"LocyConfig(...) ({len(LOCY_OBJECT_NOT_BOUND)} allowlisted)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
