# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Guard against drift between the compiled pyo3 surface and the type stub.

``uni_db/__init__.pyi`` is hand-maintained, but the real API is whatever the
Rust ``#[pymethods]`` register at runtime. Nothing type-checks the stub against
the module, so the two silently diverge -- e.g. ``SessionLocyBuilder.profile()``
existed at runtime for a while but was missing from the stub, so IDEs and
type-checkers reported it as nonexistent.

This test reflects on the classes exported by the compiled ``_uni_db``
extension and compares their public surface against the ``.pyi`` stub in both
directions:

* runtime member missing from the stub -> a Rust method was added without
  updating the stub (the drift that bit us);
* stub member missing at runtime -> a Rust method was renamed/removed, or the
  stub has a typo, leaving a phantom entry.

Deliberate exceptions go in ``IGNORED_CLASSES`` / ``IGNORED_MEMBERS`` with a
reason, so the allowlist is the only place drift can hide -- and it is
reviewable.

Four directions, and all four are checked:

===========================  ============================================
runtime class  -> stub       ``test_stub_documents_every_runtime_class``
runtime member -> stub       ``test_stub_documents_every_runtime_member``
stub member    -> runtime    ``test_stub_has_no_phantom_members``
stub class     -> runtime    ``test_stub_has_no_phantom_classes``
===========================  ============================================

The last was missing until 2026-08-22 and five phantom classes had accumulated
behind it.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest
import uni_db._uni_db as _ext

import uni_db

# The stub lives next to the runtime ``__init__.py`` inside the package.
STUB_PATH = Path(uni_db.__file__).with_name("__init__.pyi")

# ---------------------------------------------------------------------------
# Intentional, documented exceptions. Keep this list short and justified; every
# entry is drift we are knowingly choosing not to document.
# ---------------------------------------------------------------------------

# Classes exported by the extension but deliberately absent from the stub.
IGNORED_CLASSES: set[str] = set()

# Per-class members allowed to differ between runtime and stub.
# Map: class name -> set of member names. Document the reason inline.
#
# The fork/locy exception payloads below are NOT phantom: they are real
# attributes attached per-instance via `val.setattr(...)` at raise time in
# `src/exceptions.rs`, so they exist on every raised exception but never appear
# in the class `__dict__` that runtime introspection can see. The stub documents
# them (they're useful to catchers), so we allowlist them on the reverse check.
_EXC_INSTANCE_ATTRS: dict[str, set[str]] = {
    "UniForkNotFoundError": {"name"},
    "UniForkAlreadyExistsError": {"name"},
    "UniForkInUseError": {"name", "holder_count"},
    "UniForkInflightTxError": {"name"},
    "UniForkHasChildrenError": {"name", "children"},
    "UniForkSubtreeInUseError": {"blockers"},
    "UniForkBudgetExceededError": {"current", "max"},
    "UniForkLifecycleError": {"name", "stage"},
    "UniLocyIncompleteError": {
        "reason",
        "elapsed_ms",
        "limit_ms",
        "max_iterations",
        "completed_strata",
        "total_strata",
        "incomplete_rules",
        "skipped_rules",
        "complement_rules_affected",
    },
}

# The WASM loaders are `#[cfg]`-gated on the `wasm-plugins` / `extism-plugins`
# features. The published wheel drops both for size (they remain available for
# source builds), so on a default build these stub entries have no runtime
# counterpart. They are documented in the stub deliberately — a source build
# does have them — so allowlist them on the reverse check rather than delete
# them, which would leave feature-enabled builds undocumented.
_FEATURE_GATED_MEMBERS: dict[str, set[str]] = {
    "Uni": {"load_wasm_component", "load_wasm_extism"},
    "AsyncUni": {"load_wasm_component", "load_wasm_extism"},
}

IGNORED_MEMBERS: dict[str, set[str]] = dict(_EXC_INSTANCE_ATTRS)
for _cls, _members in _FEATURE_GATED_MEMBERS.items():
    IGNORED_MEMBERS.setdefault(_cls, set()).update(_members)


def _is_dunder(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


def _public(name: str) -> bool:
    """Public API name: not private (`_x`) and not a dunder (`__x__`)."""
    return not name.startswith("_") and not _is_dunder(name)


def _runtime_classes() -> dict[str, type]:
    """Public classes registered by the compiled ``_uni_db`` extension.

    We enumerate the extension's own namespace rather than ``uni_db`` so that
    pure-Python helpers re-exported into the package (``probe``, retry helpers,
    ``VARIANT``, ...) are excluded -- the stub documents the native surface.
    """
    out: dict[str, type] = {}
    for name in dir(_ext):
        if not _public(name):
            continue
        obj = getattr(_ext, name)
        if inspect.isclass(obj):
            out[name] = obj
    return out


def _runtime_members(cls: type) -> set[str]:
    """Public members declared directly on a pyo3 class.

    Covers methods (``method_descriptor``), ``#[getter]`` properties
    (``getset_descriptor``) and ``#[classattr]`` constants -- i.e. every public,
    non-dunder name in the class's own ``__dict__``. Inherited members (from
    ``object`` / ``BaseException``) are intentionally excluded.
    """
    return {name for name in vars(cls) if _public(name)}


def _stub_surface() -> dict[str, set[str]]:
    """Map class name -> declared public member names parsed from the stub."""
    tree = ast.parse(STUB_PATH.read_text(), filename=str(STUB_PATH))
    surface: dict[str, set[str]] = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        members: set[str] = set()
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # Methods and @property/@overload getters; name is what matters.
                if _public(item.name):
                    members.add(item.name)
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                # Annotated attributes, e.g. ``total_time_ms: float``.
                if _public(item.target.id):
                    members.add(item.target.id)
            elif isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name) and _public(target.id):
                        members.add(target.id)
        # A class may be declared once; merge defensively just in case.
        surface.setdefault(node.name, set()).update(members)
    return surface


def _format(drift: dict[str, list[str]]) -> str:
    return "\n".join(
        f"  {cls}: {sorted(names)}" for cls, names in sorted(drift.items())
    )


def test_stub_documents_every_runtime_class() -> None:
    """Every native class must have a stub entry (or be explicitly ignored)."""
    runtime = _runtime_classes()
    stub = _stub_surface()
    missing = sorted(
        name for name in runtime if name not in stub and name not in IGNORED_CLASSES
    )
    assert not missing, (
        "Classes exported by the compiled module are missing from "
        f"uni_db/__init__.pyi: {missing}.\n"
        "Add a stub class for each, or list it in IGNORED_CLASSES with a reason."
    )


def test_stub_documents_every_runtime_member() -> None:
    """Every public runtime method/getter must appear in the stub."""
    runtime = _runtime_classes()
    stub = _stub_surface()
    drift: dict[str, list[str]] = {}
    for name, cls in runtime.items():
        if name in IGNORED_CLASSES or name not in stub:
            continue
        documented = stub[name] | IGNORED_MEMBERS.get(name, set())
        missing = sorted(_runtime_members(cls) - documented)
        if missing:
            drift[name] = missing
    assert not drift, (
        "Public methods/getters exist at runtime but are missing from the "
        f".pyi stub:\n{_format(drift)}\n"
        "Add the missing signatures, or extend IGNORED_MEMBERS with a reason."
    )


def _package_names() -> set[str]:
    """Public names reachable as ``uni_db.<name>``.

    Broader than :func:`_runtime_classes`, which enumerates the extension only.
    A stub class is legitimate if it is reachable from the package, whether it
    came from the extension or from a pure-Python helper module.
    """
    return {name for name in dir(uni_db) if _public(name)}


def test_stub_has_no_phantom_classes() -> None:
    """Every stub class must exist at runtime (catches whole-class rot).

    This is the fourth direction of the drift matrix, and it was missing. The
    other three are covered above: runtime class -> stub, runtime member ->
    stub, and stub member -> runtime. But that last one skips any class the
    runtime does not have, so a class that exists *only* in the stub sailed
    through every check.

    It was not hypothetical. When this test was written it failed immediately on
    five classes: ``BulkWriterBuilder``, ``AppenderBuilder`` and
    ``AsyncBulkWriterBuilder`` (byte-identical leftovers from the rename to the
    ``Tx*`` names) and ``ProfileBuilder`` / ``AsyncSessionProfileBuilder`` (a
    superseded ``param``/``run`` profiling API). Each was reachable only from
    its own fluent methods, so nothing referenced them and nothing complained.

    A phantom class is worse than a phantom method: annotating against one type
    checks clean and then fails at import, because the name has never existed at
    runtime at all.

    Compared against the package rather than the extension, so a genuine
    pure-Python re-export would still pass -- see :func:`_package_names`.
    """
    stub = _stub_surface()
    available = _package_names()
    phantom = sorted(
        name for name in stub if name not in available and name not in IGNORED_CLASSES
    )
    assert not phantom, (
        "Classes declared in uni_db/__init__.pyi do not exist at runtime: "
        f"{phantom}.\n"
        "They are unimportable, so any annotation against them fails at import "
        "while type-checking clean. Delete the stale entries, or list them in "
        "IGNORED_CLASSES with a reason."
    )


def test_stub_has_no_phantom_members() -> None:
    """Every stub member must exist at runtime (catches removals/typos)."""
    runtime = _runtime_classes()
    stub = _stub_surface()
    phantom: dict[str, list[str]] = {}
    for name, members in stub.items():
        if name not in runtime or name in IGNORED_CLASSES:
            # A stub class the extension does not export is out of scope *here*
            # -- there are no members to compare against. It is not unchecked:
            # `test_stub_has_no_phantom_classes` asserts it exists somewhere in
            # the package. That test exists because this skip once read as
            # "pure-Python helpers are out of scope" and was hiding five classes
            # that existed nowhere at all.
            continue
        allowed = _runtime_members(runtime[name]) | IGNORED_MEMBERS.get(name, set())
        extra = sorted(members - allowed)
        if extra:
            phantom[name] = extra
    assert not phantom, (
        "Members declared in the .pyi stub do not exist on the runtime class "
        f"(renamed/removed in Rust, or a stub typo):\n{_format(phantom)}\n"
        "Remove the stale entries, or extend IGNORED_MEMBERS with a reason."
    )


if __name__ == "__main__":
    # Allow `python tests/test_stub_drift.py` as a standalone drift check.
    raise SystemExit(pytest.main([__file__, "-v"]))
