#!/usr/bin/env python3
"""
Generate companion schema JSON files for each TCK .feature file.

For each:
  crates/uni-tck/tck/features/**/X.feature
this script writes:
  crates/uni-tck/tck/features/**/X.schema.json

Schema extraction is intentionally conservative:
- parse only Gherkin docstrings (triple-double-quoted), where Cypher queries live
- collect tokens after ':' outside and inside relationship brackets
- tokens discovered inside [...] are treated as edge types
- tokens discovered outside [...] are treated as labels
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
FEATURES_DIR = ROOT / "tck" / "features"
GRAPHS_DIR = ROOT / "tck" / "graphs"


IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
BACKTICK_RE = re.compile(r"`([^`]+)`")


def extract_docstrings(text: str) -> Iterable[str]:
    # Gherkin docstrings are triple double-quotes.
    for m in re.finditer(r'"""(.*?)"""', text, flags=re.DOTALL):
        yield m.group(1)


def extract_named_graph_queries(feature_text: str) -> Iterable[str]:
    graph_names = {
        m.group(1).strip()
        for m in re.finditer(r"^\s*Given the ([^\n]+?) graph\s*$", feature_text, flags=re.MULTILINE)
    }
    for graph_name in sorted(graph_names):
        graph_dir = GRAPHS_DIR / graph_name
        meta_path = graph_dir / f"{graph_name}.json"
        if not meta_path.exists():
            continue
        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        scripts = metadata.get("scripts", [])
        if not isinstance(scripts, list):
            continue
        for script_name in scripts:
            script_path = graph_dir / f"{script_name}.cypher"
            if not script_path.exists():
                continue
            try:
                content = script_path.read_text(encoding="utf-8")
            except Exception:
                continue
            for stmt in content.split(";"):
                query = stmt.strip()
                if query:
                    yield query


def parse_type_expr(segment: str) -> list[str]:
    # Handle backtick-quoted and plain identifiers from type/label expressions.
    tokens: list[str] = []
    for m in BACKTICK_RE.finditer(segment):
        name = m.group(1).strip()
        if name:
            tokens.append(name)
    # Remove backtick chunks before plain identifier extraction
    no_backticks = BACKTICK_RE.sub(" ", segment)
    tokens.extend(IDENT_RE.findall(no_backticks))
    return tokens


def iter_groups(text: str, open_ch: str, close_ch: str) -> Iterable[str]:
    in_single = False
    in_double = False
    escaped = False
    depth = 0
    start = -1

    for i, ch in enumerate(text):
        if escaped:
            escaped = False
            continue
        if ch == "\\" and (in_single or in_double):
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if in_single or in_double:
            continue

        if ch == open_ch:
            if depth == 0:
                start = i + 1
            depth += 1
            continue
        if ch == close_ch and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                yield text[start:i]
                start = -1


def split_top_level(text: str, sep: str = ",") -> list[str]:
    parts: list[str] = []
    cur: list[str] = []
    in_single = False
    in_double = False
    escaped = False
    paren = 0
    square = 0
    curly = 0

    for ch in text:
        if escaped:
            cur.append(ch)
            escaped = False
            continue
        if ch == "\\" and (in_single or in_double):
            cur.append(ch)
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            cur.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            cur.append(ch)
            continue
        if in_single or in_double:
            cur.append(ch)
            continue

        if ch == "(":
            paren += 1
        elif ch == ")":
            paren = max(0, paren - 1)
        elif ch == "[":
            square += 1
        elif ch == "]":
            square = max(0, square - 1)
        elif ch == "{":
            curly += 1
        elif ch == "}":
            curly = max(0, curly - 1)

        if ch == sep and paren == 0 and square == 0 and curly == 0:
            part = "".join(cur).strip()
            if part:
                parts.append(part)
            cur = []
            continue

        cur.append(ch)

    tail = "".join(cur).strip()
    if tail:
        parts.append(tail)
    return parts


def extract_colon_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    in_single = False
    in_double = False
    escaped = False
    square_depth = 0
    curly_depth = 0

    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if escaped:
            escaped = False
            i += 1
            continue
        if ch == "\\" and (in_single or in_double):
            escaped = True
            i += 1
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue
        if in_single or in_double:
            i += 1
            continue

        if ch == "[":
            square_depth += 1
            i += 1
            continue
        if ch == "]":
            square_depth = max(0, square_depth - 1)
            i += 1
            continue
        if ch == "{":
            curly_depth += 1
            i += 1
            continue
        if ch == "}":
            curly_depth = max(0, curly_depth - 1)
            i += 1
            continue

        if ch == ":":
            if curly_depth > 0:
                i += 1
                continue
            j = i + 1
            while j < n and text[j].isspace():
                j += 1
            if j >= n:
                i += 1
                continue
            start = text[j]
            if not (start.isalpha() or start == "_" or start == "`" or start == "!"):
                i += 1
                continue
            k = j
            while k < n and text[k] not in " \t\r\n,;(){}[]<>-":
                k += 1
            expr = text[j:k]
            for token in parse_type_expr(expr):
                tokens.add(token)
            i = k
            continue

        i += 1

    return tokens


def first_top_level_map(group_text: str) -> str | None:
    in_single = False
    in_double = False
    escaped = False
    depth = 0
    start = -1

    for i, ch in enumerate(group_text):
        if escaped:
            escaped = False
            continue
        if ch == "\\" and (in_single or in_double):
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if in_single or in_double:
            continue

        if ch == "{":
            if depth == 0:
                start = i + 1
            depth += 1
            continue
        if ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                return group_text[start:i]
    return None


def split_first_top_level_colon(entry: str) -> tuple[str, str] | None:
    in_single = False
    in_double = False
    escaped = False
    paren = 0
    square = 0
    curly = 0

    for i, ch in enumerate(entry):
        if escaped:
            escaped = False
            continue
        if ch == "\\" and (in_single or in_double):
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if in_single or in_double:
            continue

        if ch == "(":
            paren += 1
        elif ch == ")":
            paren = max(0, paren - 1)
        elif ch == "[":
            square += 1
        elif ch == "]":
            square = max(0, square - 1)
        elif ch == "{":
            curly += 1
        elif ch == "}":
            curly = max(0, curly - 1)

        if ch == ":" and paren == 0 and square == 0 and curly == 0:
            return entry[:i].strip(), entry[i + 1 :].strip()

    return None


def normalize_key(raw_key: str) -> str:
    key = raw_key.strip()
    if key.startswith("`") and key.endswith("`") and len(key) >= 2:
        return key[1:-1]
    if key.startswith("'") and key.endswith("'") and len(key) >= 2:
        return key[1:-1]
    if key.startswith('"') and key.endswith('"') and len(key) >= 2:
        return key[1:-1]
    return key


def infer_literal_type(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return "Json"
    lower = value.lower()

    if value.startswith("'") or value.startswith('"'):
        return "String"
    if lower.startswith("true") or lower.startswith("false"):
        return "Bool"
    if lower.startswith("null"):
        return "Json"
    if value.startswith("[") or value.startswith("{"):
        return "Json"

    if re.match(r"^-?\d+$", value):
        return "Int64"
    if re.match(r"^-?\d+\.\d+([eE][+-]?\d+)?$", value) or re.match(
        r"^-?\d+[eE][+-]?\d+$", value
    ):
        return "Float64"

    # Temporal functions.
    if lower.startswith("datetime(") or lower.startswith("timestamp("):
        return "DateTime"
    if lower.startswith("date("):
        return "Date"
    if lower.startswith("time("):
        return "Time"
    if lower.startswith("duration("):
        return "Duration"

    # Parameters, expressions, function calls, arithmetic, etc.
    return "Json"


def merge_types(existing: str | None, new_type: str) -> str:
    if existing is None:
        return new_type
    if existing == new_type:
        return existing
    if {existing, new_type} == {"Int64", "Float64"}:
        return "Float64"
    return "Json"


def parse_map_properties(map_body: str) -> dict[str, str]:
    props: dict[str, str] = {}
    for entry in split_top_level(map_body, sep=","):
        split = split_first_top_level_colon(entry)
        if split is None:
            continue
        raw_key, raw_val = split
        key = normalize_key(raw_key)
        if not key:
            continue
        inferred = infer_literal_type(raw_val)
        props[key] = merge_types(props.get(key), inferred)
    return props


def parse_pattern_variable(group_text: str) -> str | None:
    text = group_text.strip()
    if not text:
        return None
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\b", text)
    if not m:
        return None
    return m.group(1)


def collect_variable_bindings(
    query: str, labels: set[str], edge_types: set[str]
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    node_var_labels: dict[str, set[str]] = {}
    rel_var_types: dict[str, set[str]] = {}

    for node_group in iter_groups(query, "(", ")"):
        var = parse_pattern_variable(node_group)
        if var is None:
            continue
        node_labels = extract_colon_tokens(node_group)
        node_labels = {lbl for lbl in node_labels if lbl in labels}
        if node_labels:
            node_var_labels.setdefault(var, set()).update(node_labels)

    for rel_group in iter_groups(query, "[", "]"):
        var = parse_pattern_variable(rel_group)
        if var is None:
            continue
        rel_types = extract_colon_tokens(rel_group)
        rel_types = {etype for etype in rel_types if etype in edge_types}
        if rel_types:
            rel_var_types.setdefault(var, set()).update(rel_types)

    return node_var_labels, rel_var_types


def iter_property_references(query: str) -> Iterable[tuple[str, str]]:
    in_single = False
    in_double = False
    escaped = False
    i = 0
    n = len(query)

    while i < n:
        ch = query[i]
        if escaped:
            escaped = False
            i += 1
            continue
        if ch == "\\" and (in_single or in_double):
            escaped = True
            i += 1
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue
        if in_single or in_double:
            i += 1
            continue

        if not (ch.isalpha() or ch == "_"):
            i += 1
            continue

        start = i
        i += 1
        while i < n and (query[i].isalnum() or query[i] == "_"):
            i += 1
        var = query[start:i]

        j = i
        while j < n and query[j].isspace():
            j += 1
        if j >= n or query[j] != ".":
            i = j
            continue
        j += 1
        while j < n and query[j].isspace():
            j += 1
        if j >= n:
            i = j
            continue

        if query[j] == "`":
            k = j + 1
            while k < n and query[k] != "`":
                k += 1
            if k >= n:
                i = j + 1
                continue
            prop = query[j + 1 : k]
            i = k + 1
            if prop:
                yield var, prop
            continue

        if not (query[j].isalpha() or query[j] == "_"):
            i = j
            continue

        k = j + 1
        while k < n and (query[k].isalnum() or query[k] == "_"):
            k += 1
        prop = query[j:k]
        i = k
        if prop:
            yield var, prop


def collect_assignment_map_properties(
    query: str, node_var_labels: dict[str, set[str]], rel_var_types: dict[str, set[str]]
) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    label_props: dict[str, dict[str, str]] = {}
    edge_props: dict[str, dict[str, str]] = {}

    # Capture patterns like:
    #   SET n += {k: v}
    #   SET n = {k: v}
    #   ON MATCH SET r += {k: v}
    # This intentionally ignores expression/parameter maps where literal typing is unknown.
    for m in re.finditer(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\s*(\+=|=)\s*\{([^{}]*)\}", query, flags=re.DOTALL
    ):
        var = m.group(1)
        op = m.group(2)
        map_body = m.group(3)
        if op not in ("+=", "="):
            continue
        props = parse_map_properties(map_body)
        if not props:
            continue

        if var in node_var_labels:
            for lbl in node_var_labels[var]:
                bucket = label_props.setdefault(lbl, {})
                for key, ptype in props.items():
                    bucket[key] = merge_types(bucket.get(key), ptype)

        if var in rel_var_types:
            for et in rel_var_types[var]:
                bucket = edge_props.setdefault(et, {})
                for key, ptype in props.items():
                    bucket[key] = merge_types(bucket.get(key), ptype)

    return label_props, edge_props


def scan_query_for_labels_and_edge_types(query: str) -> tuple[set[str], set[str]]:
    labels: set[str] = set()
    edge_types: set[str] = set()

    i = 0
    n = len(query)
    in_single = False
    in_double = False
    escaped = False
    square_depth = 0
    curly_depth = 0

    while i < n:
        ch = query[i]

        if escaped:
            escaped = False
            i += 1
            continue

        if ch == "\\" and (in_single or in_double):
            escaped = True
            i += 1
            continue

        if ch == "'" and not in_double:
            in_single = not in_single
            i += 1
            continue

        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue

        if in_single or in_double:
            i += 1
            continue

        if ch == "[":
            square_depth += 1
            i += 1
            continue
        if ch == "]":
            square_depth = max(0, square_depth - 1)
            i += 1
            continue
        if ch == "{":
            curly_depth += 1
            i += 1
            continue
        if ch == "}":
            curly_depth = max(0, curly_depth - 1)
            i += 1
            continue

        if ch == ":":
            # Ignore key/value separators in maps/projections.
            if curly_depth > 0:
                i += 1
                continue

            j = i + 1
            while j < n and query[j].isspace():
                j += 1

            if j >= n:
                i += 1
                continue

            # Valid label/type expressions after ':' start with one of:
            # identifier char, backtick-quoted identifier, or '!' (label negation).
            start = query[j]
            if not (start.isalpha() or start == "_" or start == "`" or start == "!"):
                i += 1
                continue

            # Read label/type expression until a hard delimiter.
            k = j
            while k < n and query[k] not in " \t\r\n,;(){}[]<>-":
                k += 1

            expr = query[j:k]
            for token in parse_type_expr(expr):
                if square_depth > 0:
                    edge_types.add(token)
                else:
                    labels.add(token)

            i = k
            continue

        i += 1

    return labels, edge_types


def collect_typed_properties(
    query: str, labels: set[str], edge_types: set[str]
) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    label_props: dict[str, dict[str, str]] = {}
    edge_props: dict[str, dict[str, str]] = {}

    # Node patterns: (...) -> labels + optional map literal.
    for node_group in iter_groups(query, "(", ")"):
        node_labels = extract_colon_tokens(node_group)
        if not node_labels:
            continue
        map_body = first_top_level_map(node_group)
        if map_body is None:
            continue
        props = parse_map_properties(map_body)
        if not props:
            continue
        for lbl in node_labels:
            if lbl not in labels:
                continue
            bucket = label_props.setdefault(lbl, {})
            for key, ptype in props.items():
                bucket[key] = merge_types(bucket.get(key), ptype)

    # Relationship patterns: [...] -> edge type(s) + optional map literal.
    for rel_group in iter_groups(query, "[", "]"):
        rel_types = extract_colon_tokens(rel_group)
        if not rel_types:
            continue
        map_body = first_top_level_map(rel_group)
        if map_body is None:
            continue
        props = parse_map_properties(map_body)
        if not props:
            continue
        for et in rel_types:
            if et not in edge_types:
                continue
            bucket = edge_props.setdefault(et, {})
            for key, ptype in props.items():
                bucket[key] = merge_types(bucket.get(key), ptype)

    return label_props, edge_props


def collect_bound_property_reference_types(
    query: str, node_var_labels: dict[str, set[str]], rel_var_types: dict[str, set[str]]
) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    label_props: dict[str, dict[str, str]] = {}
    edge_props: dict[str, dict[str, str]] = {}

    for var, prop in iter_property_references(query):
        if var in node_var_labels:
            for lbl in node_var_labels[var]:
                bucket = label_props.setdefault(lbl, {})
                # Reference-only properties default to Json when type can't be inferred.
                bucket[prop] = merge_types(bucket.get(prop), "Json")
        if var in rel_var_types:
            for et in rel_var_types[var]:
                bucket = edge_props.setdefault(et, {})
                bucket[prop] = merge_types(bucket.get(prop), "Json")

    return label_props, edge_props


def build_schema(
    labels: set[str],
    edge_types: set[str],
    label_props: dict[str, dict[str, str]],
    edge_props: dict[str, dict[str, str]],
) -> dict:
    sorted_labels = sorted(labels)
    sorted_edge_types = sorted(edge_types)

    labels_obj = {
        name: {
            "id": idx + 1,
        }
        for idx, name in enumerate(sorted_labels)
    }

    edge_types_obj = {
        name: {
            "id": idx + 1,
            "src_labels": [],
            "dst_labels": [],
        }
        for idx, name in enumerate(sorted_edge_types)
    }

    properties_obj: dict[str, dict[str, dict[str, object]]] = {}
    for lbl in sorted_labels:
        if lbl in label_props:
            properties_obj[lbl] = {
                pname: {
                    "type": ptype,
                    "nullable": True,
                }
                for pname, ptype in sorted(label_props[lbl].items())
            }
    for et in sorted_edge_types:
        if et in edge_props:
            properties_obj[et] = {
                pname: {
                    "type": ptype,
                    "nullable": True,
                }
                for pname, ptype in sorted(edge_props[et].items())
            }

    return {
        "schema_version": 1,
        "labels": labels_obj,
        "edge_types": edge_types_obj,
        "properties": properties_obj,
        "indexes": [],
    }


def generate_for_feature(feature_path: Path) -> Path:
    text = feature_path.read_text(encoding="utf-8")
    labels: set[str] = set()
    edge_types: set[str] = set()
    label_props: dict[str, dict[str, str]] = {}
    edge_props: dict[str, dict[str, str]] = {}
    ref_label_props: dict[str, set[str]] = {}
    ref_edge_props: dict[str, set[str]] = {}

    all_queries = list(extract_docstrings(text)) + list(extract_named_graph_queries(text))

    for query in all_queries:
        q_labels, q_edge_types = scan_query_for_labels_and_edge_types(query)
        labels.update(q_labels)
        edge_types.update(q_edge_types)
        q_label_props, q_edge_props = collect_typed_properties(query, labels, edge_types)
        node_var_labels, rel_var_types = collect_variable_bindings(query, labels, edge_types)
        q_ref_label_props, q_ref_edge_props = collect_bound_property_reference_types(
            query, node_var_labels, rel_var_types
        )
        q_set_label_props, q_set_edge_props = collect_assignment_map_properties(
            query, node_var_labels, rel_var_types
        )

        # First merge concrete typed evidence (map literals and SET maps).
        for source in (q_label_props, q_set_label_props):
            for lbl, props in source.items():
                bucket = label_props.setdefault(lbl, {})
                for key, ptype in props.items():
                    bucket[key] = merge_types(bucket.get(key), ptype)
        for source in (q_edge_props, q_set_edge_props):
            for et, props in source.items():
                bucket = edge_props.setdefault(et, {})
                for key, ptype in props.items():
                    bucket[key] = merge_types(bucket.get(key), ptype)

        # Track reference-only properties and materialize them after all concrete
        # evidence has been merged.
        for lbl, props in q_ref_label_props.items():
            bucket = ref_label_props.setdefault(lbl, set())
            for key in props:
                bucket.add(key)
        for et, props in q_ref_edge_props.items():
            bucket = ref_edge_props.setdefault(et, set())
            for key in props:
                bucket.add(key)

    # Materialize reference-only properties as Json where concrete typing is absent.
    for lbl, props in ref_label_props.items():
        bucket = label_props.setdefault(lbl, {})
        for key in props:
            if key not in bucket:
                bucket[key] = "Json"
    for et, props in ref_edge_props.items():
        bucket = edge_props.setdefault(et, {})
        for key in props:
            if key not in bucket:
                bucket[key] = "Json"

    schema = build_schema(labels, edge_types, label_props, edge_props)
    out_path = feature_path.with_suffix(".schema.json")
    out_path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_path


def main() -> None:
    features = sorted(FEATURES_DIR.rglob("*.feature"))
    generated = [generate_for_feature(path) for path in features]
    print(f"Generated {len(generated)} schema files.")


if __name__ == "__main__":
    main()
