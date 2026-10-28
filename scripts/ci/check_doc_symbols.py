#!/usr/bin/env python3
"""Check live documentation contracts against source.

Scan website pages/notebooks, the Black Book, and the maintained agent skill.
Check Python method names, explicit import/factory receiver types and arity,
curated class excerpts, complete UniConfig field/type inventories, and removed
Rust Locy result accesses or malformed raw programs. Partial snippets and
unknown receiver types are left alone; this is not a full language compiler.
Dated proposals/release notes and historical skill evaluations are excluded.

Exit code 0 = checked contracts match, 1 = drift.
"""

from __future__ import annotations

import ast
import json
import re
import sys
from functools import cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PYI = ROOT / "bindings/uni-db/uni_db/__init__.pyi"
# The Pydantic OGM is a *separate* package with its own session type, so its
# methods (`add`, `add_all`, `sync_schema`, ...) are legitimate on a `session`
# receiver in OGM documentation. Both surfaces count as known.
OGM_SRC = ROOT / "bindings/uni-pydantic/src/uni_pydantic"

DOC_ROOTS = ("website/docs", "skills/uni-db", "docs/UNI_BLACK_BOOK.md")
# `plugins/loaders` snippets run *inside* a guest plugin, where `db` is the
# plugin registrar rather than `uni_db.Uni`, so its receivers are unrelated.
EXCLUDED_PARTS = ("release_notes", "proposals", "plans", "archive", "loaders")

# Receivers whose type we are confident about.
RECEIVERS = {
    "db",
    "session",
    "tx",
    "txn",
    "xervo",
    "fork",
    "primary",
    "async_db",
    "async_session",
}

FENCE = re.compile(r"^\s*```(\w+)")
CALL = re.compile(r"\b(" + "|".join(sorted(RECEIVERS)) + r")\.([a-z_][a-z0-9_]*)\s*\(")

# Attribute-style facades that return another object; the *next* call is on that
# object, so `db.schema().label(...)` should check `schema` not `label`.
# Handled by only checking the first attribute after a known receiver.


def known_methods() -> set[str]:
    """Every public method/function name across both documented packages."""
    names: set[str] = set()
    sources = [PYI, *sorted(OGM_SRC.glob("*.py"))]
    for src in sources:
        try:
            tree = ast.parse(src.read_text())
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                names.add(node.name)
    return names


def live_docs() -> list[Path]:
    out: list[Path] = []
    for root in DOC_ROOTS:
        for p in [ROOT / root] if (ROOT / root).is_file() else (ROOT / root).rglob("*"):
            if p.suffix not in (".md", ".ipynb"):
                continue
            rel = p.relative_to(ROOT)
            if any(part in EXCLUDED_PARTS for part in rel.parts):
                continue
            out.append(p)
    return sorted(out)


def code_blocks(text: str, languages=("python", "py")):
    """Yield source lines and text for fenced blocks in selected languages."""
    lang = None
    buf: list[str] = []
    start = 0
    for i, line in enumerate(text.splitlines(), 1):
        if line.lstrip().startswith("```"):
            if lang in languages:
                yield start, "\n".join(buf)
            m = FENCE.match(line)
            lang = m.group(1) if m else None
            buf, start = [], i
        else:
            buf.append(line)
    if lang in languages:
        yield start, "\n".join(buf)


def python_blocks(text: str):
    return code_blocks(text)


def rust_locy_errors(block: str) -> list[str]:
    """Detect removed result fields and accidentally escaped Locy raw strings."""
    errors = []
    if re.search(r"\bresult\.derived\s*(?:\.|\{)", block):
        errors.append("Locy result.derived is private; use derived_rules().await?")
    if "result.derived().get(" in block:
        errors.append("derived() returns an optional mutation set, not rule facts")
    for program in re.findall(r'let program = r#"(.*?)"#;', block, re.DOTALL):
        if re.search(
            r"\\n(?:MATCH|CREATE|YIELD|WHERE|QUERY|DERIVE|FOLD|ALONG)", program
        ):
            errors.append(
                "Locy raw string contains escaped line separators instead of newlines"
            )
    return errors


@cache
def stub_classes():
    return {
        node.name: {
            method.name: method
            for method in node.body
            if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for node in ast.parse(PYI.read_text()).body
        if isinstance(node, ast.ClassDef)
    }


def declared_method_errors(block: str) -> list[str]:
    """Check curated Python class excerpts against the same named stub class.

    A global method-name union cannot detect Session.set (Params.set exists).
    Only inspect class declarations; ordinary `session` variables can also be
    OGM sessions, whose methods legitimately differ.
    """
    try:
        documented = ast.parse(block)
    except SyntaxError:
        return []  # Partial snippets are checked by the receiver scan.
    canonical = stub_classes()
    return [
        f"{node.name}.{method.name} is absent from that stub class"
        for node in documented.body
        if isinstance(node, ast.ClassDef)
        if node.name in canonical
        for method in node.body
        if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
        if method.name not in canonical[node.name]
    ]


@cache
def locy_config_keys() -> set[str]:
    source = (ROOT / "bindings/uni-db/src/convert.rs").read_text()
    declaration = re.search(
        r"pub const LOCY_CONFIG_KEYS: &\[&str\] = &\[(.*?)\];", source, re.DOTALL
    )
    if declaration is None:
        raise ValueError("cannot locate Python Locy config key inventory")
    return set(re.findall(r'"(\w+)"', declaration.group(1)))


def typed_call_errors(
    block: str,
    env: dict[str, str] | None = None,
    dicts: dict[str, ast.Dict] | None = None,
) -> list[str]:
    """Follow explicit uni_db imports and factory returns through call chains."""
    try:
        tree = ast.parse(block)
    except SyntaxError:
        return []
    classes = stub_classes()
    if env is None:
        env = {}
    if dicts is None:
        dicts = {}
    errors: set[str] = set()

    def infer(node):
        if isinstance(node, ast.Await):
            return infer(node.value)
        if isinstance(node, ast.Name):
            return env.get(node.id)
        if isinstance(node, ast.Attribute) and infer(node.value) == "module:uni_db":
            return node.attr if node.attr in classes else None
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                return env.get(node.func.id)  # class construction
            if not isinstance(node.func, ast.Attribute):
                return None
            owner = infer(node.func.value)
            if owner not in classes:
                return None
            method = classes[owner].get(node.func.attr)
            if method is None:
                errors.add(
                    f"line {node.lineno}: {owner}.{node.func.attr} is absent from that stub class"
                )
                return None
            if (
                owner
                in {
                    "SessionLocyBuilder",
                    "TxLocyBuilder",
                    "AsyncSessionLocyBuilder",
                    "AsyncTxLocyBuilder",
                }
                and node.func.attr == "with_config"
            ):
                config = (
                    node.args[0]
                    if node.args
                    else next(
                        (k.value for k in node.keywords if k.arg == "config"), None
                    )
                )
                if isinstance(config, ast.Name):
                    config = dicts.get(config.id)
                if isinstance(config, ast.Dict):
                    keys = {
                        key.value
                        for key in config.keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    }
                    unknown = keys - locy_config_keys()
                    if unknown:
                        errors.add(
                            f"line {node.lineno}: unknown Locy config dictionary keys: {sorted(unknown)}"
                        )
            positional = [*method.args.posonlyargs, *method.args.args]
            if positional and positional[0].arg in ("self", "cls"):
                positional = positional[1:]
            if not any(isinstance(arg, ast.Starred) for arg in node.args) and not any(
                k.arg is None for k in node.keywords
            ):
                names = {arg.arg for arg in positional + method.args.kwonlyargs}
                supplied = {arg.arg for arg in positional[: len(node.args)]} | {
                    k.arg for k in node.keywords
                }
                required = {
                    arg.arg
                    for arg in positional[: len(positional) - len(method.args.defaults)]
                }
                required |= {
                    arg.arg
                    for arg, default in zip(
                        method.args.kwonlyargs, method.args.kw_defaults
                    )
                    if default is None
                }
                if (
                    required - supplied
                    or (len(node.args) > len(positional) and method.args.vararg is None)
                    or (supplied - names and method.args.kwarg is None)
                ):
                    errors.add(
                        f"line {node.lineno}: arguments do not match {owner}.{node.func.attr} in the stub"
                    )
            if isinstance(method.returns, ast.Name) and method.returns.id in classes:
                return method.returns.id
        return None

    # Walk statements in source order. Imports from other packages never acquire
    # a Uni type just because their variable happens to be named `session`.
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for item in node.names:
                if item.name == "uni_db":
                    env[item.asname or item.name] = "module:uni_db"
        elif isinstance(node, ast.ImportFrom) and node.module == "uni_db":
            for item in node.names:
                if item.name in classes:
                    env[item.asname or item.name] = item.name
        elif isinstance(node, ast.Assign):
            kind = infer(node.value)
            for target in node.targets:
                if isinstance(target, ast.Name):
                    env.pop(target.id, None)
                    dicts.pop(target.id, None)
                    if isinstance(node.value, ast.Dict):
                        dicts[target.id] = node.value
                    if kind:
                        env[target.id] = kind
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                kind = infer(item.context_expr)
                if kind and isinstance(item.optional_vars, ast.Name):
                    env[item.optional_vars.id] = kind
        elif isinstance(node, ast.Call):
            infer(node)
    return sorted(errors)


@cache
def config_declaration_errors(text: str) -> list[str]:
    """A published UniConfig declaration must have the current field types."""
    source = (ROOT / "crates/uni-common/src/config.rs").read_text()
    errors = []
    for name in ("UniConfig",):
        pattern = rf"pub struct {name} \{{(.*?)\n\}}"
        actual = re.search(pattern, source, re.DOTALL)
        if actual is None:
            raise ValueError(f"cannot find {name} in source")
        fields = lambda body: dict(re.findall(r"pub (\w+): ([^,\n]+),", body))
        expected = fields(actual.group(1))
        for documented in re.finditer(pattern, text, re.DOTALL):
            found = fields(documented.group(1))
            if found != expected:
                errors.append(
                    f"{name} declaration differs: missing={sorted(expected.keys() - found.keys())}, "
                    f"extra={sorted(found.keys() - expected.keys())}, "
                    f"wrong types={sorted(k for k in found.keys() & expected.keys() if found[k] != expected[k])}"
                )
    return errors


def main() -> int:
    known = known_methods()
    failures: list[str] = []
    paths = live_docs()
    python_count = rust_count = 0

    for path in paths:
        if path.suffix == ".ipynb":
            # Notebook sources, not generated Markdown or captured output.
            notebook = json.loads(path.read_text())
            blocks = [
                (i, "".join(cell["source"]))
                for i, cell in enumerate(notebook["cells"], 1)
                if cell["cell_type"] == "code"
            ]
        else:
            blocks = python_blocks(path.read_text())
        for error in config_declaration_errors(path.read_text()):
            failures.append(f"{path.relative_to(ROOT)}: {error}")
        if path.suffix == ".ipynb" and path.parent.name == "rust":
            rust_blocks = blocks
            blocks = []
        else:
            rust_blocks = code_blocks(path.read_text(), ("rust",))
        for start, block in rust_blocks:
            rust_count += 1
            for error in rust_locy_errors(block):
                failures.append(f"{path.relative_to(ROOT)}:{start}: {error}")
        env: dict[str, str] = {}
        dicts: dict[str, ast.Dict] = {}
        for start, block in blocks:
            python_count += 1
            for error in declared_method_errors(block) + typed_call_errors(
                block, env, dicts
            ):
                failures.append(f"{path.relative_to(ROOT)}:{start}: {error}")
            for m in CALL.finditer(block):
                receiver, method = m.group(1), m.group(2)
                if method in known:
                    continue
                offset = block[: m.start()].count("\n")
                failures.append(
                    f"{path.relative_to(ROOT)}:{start + offset + 1} "
                    f"`{receiver}.{method}(` — no `{method}` in __init__.pyi"
                )

    if failures:
        print("Documentation contract check FAILED:\n")
        for f in dict.fromkeys(failures):
            print(f"  - {f}")
        print(
            f"\nGround truth: {PYI.relative_to(ROOT)}, UniConfig, and the Rust Locy API. "
            "Update documentation against the implementation."
        )
        return 1

    print(
        f"OK: checked contracts across {len(paths)} documents, {python_count} Python blocks and {rust_count} Rust blocks"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
