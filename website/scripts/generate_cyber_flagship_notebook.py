#!/usr/bin/env python3
"""Generate the flagship Locy cyber exposure notebook."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NOTEBOOK_PATH = Path("website/docs/examples/python/locy_cyber_exposure_twin.ipynb")


def _cell_id(notebook_key: str, index: int, cell_type: str) -> str:
    raw = f"{notebook_key}:{index}:{cell_type}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def _source_lines(lines: list[str]) -> list[str]:
    return [f"{line}\n" for line in lines]


def _md_cell(notebook_key: str, index: int, lines: list[str]) -> dict[str, Any]:
    return {
        "id": _cell_id(notebook_key, index, "markdown"),
        "cell_type": "markdown",
        "metadata": {},
        "source": _source_lines(lines),
    }


def _code_cell(notebook_key: str, index: int, lines: list[str]) -> dict[str, Any]:
    return {
        "id": _cell_id(notebook_key, index, "code"),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _source_lines(lines),
    }


def _metadata() -> dict[str, Any]:
    return {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.11.0",
        },
    }


def _create_notebook(cells: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "cells": cells,
        "metadata": _metadata(),
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _render_json(obj: dict[str, Any]) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


def _build_notebook() -> dict[str, Any]:
    key = "python:locy_cyber_exposure_twin"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "# Locy Flagship #3: Cyber Exposure-to-Remediation Decision Twin",
                "",
                "This flagship is **fully integrated** (not side-by-side):",
                "",
                "1. **Hybrid retrieval** (`uni.search`) finds relevant advisory/runbook evidence.",
                "2. **Columnar analytics** computes risk rollups and ranked hotspots.",
                "3. **Locy reasoning** drives prioritized remediation with:",
                "   - `ALONG`, `FOLD`, `BEST BY`",
                "   - `DERIVE`, `ASSUME`, `ABDUCE`, `EXPLAIN RULE`",
                "",
                "It is schema-first (recommended) and designed to be readable by first-time Locy users.",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## How To Read This Notebook",
                "",
                "- Each section explains what code is doing and what output you should expect.",
                "- The dataset is deterministic for stable docs/CI execution.",
                "- Follow the flow: ingest facts -> retrieve evidence -> compute exposure -> reason and optimize.",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 1) Setup and Data Discovery",
                "",
                "What this does:",
                "Loads helpers, locates prepared data files, and creates an isolated temporary database.",
                "",
                "What to expect:",
                "Printed `DATA_DIR` and `DB_DIR` paths.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "from pathlib import Path",
                "from pprint import pprint",
                "import csv",
                "import json",
                "import os",
                "import shutil",
                "import tempfile",
                "",
                "import uni_db",
                "",
                "def _read_csv(path: Path) -> list[dict[str, str]]:",
                "    with path.open('r', encoding='utf-8', newline='') as f:",
                "        return list(csv.DictReader(f))",
                "",
                "def _esc(value: str) -> str:",
                "    return str(value).replace('\\\\', '\\\\\\\\').replace(\"'\", \"\\\\'\")",
                "",
                "def _f(value: str) -> float:",
                "    return float(value) if value not in ('', None) else 0.0",
                "",
                "def _to_int(value: str) -> int:",
                "    return int(float(value)) if value not in ('', None) else 0",
                "",
                "def _vec(value: str) -> list[float]:",
                "    return [float(x) for x in json.loads(value)]",
                "",
                "def _norm_key(key: object) -> str:",
                "    s = str(key)",
                "    if s.startswith('Variable(\"') and s.endswith('\")'):",
                "        return s[len('Variable(\"'):-2]",
                "    return s",
                "",
                "def _norm_rows(rows: list[dict[object, object]]) -> list[dict[str, object]]:",
                "    return [{_norm_key(k): v for k, v in row.items()} for row in rows]",
                "",
                "_default_candidates = [",
                "    Path('docs/examples/data/locy_cyber_exposure_twin'),",
                "    Path('website/docs/examples/data/locy_cyber_exposure_twin'),",
                "    Path('examples/data/locy_cyber_exposure_twin'),",
                "    Path('../data/locy_cyber_exposure_twin'),",
                "]",
                "if 'LOCY_DATA_DIR' in os.environ:",
                "    DATA_DIR = Path(os.environ['LOCY_DATA_DIR']).resolve()",
                "else:",
                "    DATA_DIR = next(",
                "        (p.resolve() for p in _default_candidates if (p / 'assets.csv').exists()),",
                "        _default_candidates[0].resolve(),",
                "    )",
                "if not (DATA_DIR / 'assets.csv').exists():",
                "    raise FileNotFoundError(",
                "        'Expected data under docs/examples/data/locy_cyber_exposure_twin. '",
                "        'Run from website/ (or repo root) or set LOCY_DATA_DIR.'",
                "    )",
                "DB_DIR = tempfile.mkdtemp(prefix='uni_locy_cyber_')",
                "db = uni_db.Uni.open(DB_DIR)",
                "session = db.session()",
                "print('DATA_DIR:', DATA_DIR)",
                "print('DB_DIR:', DB_DIR)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 2) Load Snapshot Data and Build Focus Cohort",
                "",
                "What this does:",
                "Loads deterministic snapshot files and picks focus assets for a fast but meaningful scenario.",
                "",
                "What to expect:",
                "Counts for assets, findings, dependencies, actions, and knowledge docs.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "assets = _read_csv(DATA_DIR / 'assets.csv')",
                "vulns = _read_csv(DATA_DIR / 'vulnerabilities.csv')",
                "kev = _read_csv(DATA_DIR / 'kev_snapshot.csv')",
                "epss = _read_csv(DATA_DIR / 'epss_snapshot.csv')",
                "findings = _read_csv(DATA_DIR / 'vuln_findings.csv')",
                "dependencies = _read_csv(DATA_DIR / 'asset_dependencies.csv')",
                "actions = _read_csv(DATA_DIR / 'remediation_actions.csv')",
                "docs = _read_csv(DATA_DIR / 'knowledge_docs.csv')",
                "notebook_cases = _read_csv(DATA_DIR / 'notebook_cases.csv')",
                "",
                "focus_asset_ids = {r['asset_id'] for r in notebook_cases}",
                "focus_asset_ids.update({r['asset_id'] for r in findings if _to_int(r.get('patch_sla_hours', '0')) <= 48})",
                "focus_assets = [r for r in assets if r['asset_id'] in focus_asset_ids]",
                "focus_asset_ids = {r['asset_id'] for r in focus_assets}",
                "focus_findings = [r for r in findings if r['asset_id'] in focus_asset_ids]",
                "focus_dependencies = [",
                "    r for r in dependencies",
                "    if r['src_asset_id'] in focus_asset_ids and r['dst_asset_id'] in focus_asset_ids",
                "]",
                "focus_cves = {r['cve_id'] for r in focus_findings}",
                "focus_vulns = [r for r in vulns if r['cve_id'] in focus_cves]",
                "focus_actions = [r for r in actions if r['cve_id'] in focus_cves]",
                "focus_docs = [r for r in docs if (not r['cve_id']) or (r['cve_id'] in focus_cves)]",
                "",
                "print('focus assets:', len(focus_assets))",
                "print('focus findings:', len(focus_findings))",
                "print('focus dependencies:', len(focus_dependencies))",
                "print('focus vulnerabilities:', len(focus_vulns))",
                "print('focus remediation actions:', len(focus_actions))",
                "print('focus knowledge docs:', len(focus_docs))",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 3) Define Schema (Recommended)",
                "",
                "What this does:",
                "Defines explicit labels, typed properties, and edge types before ingest.",
                "",
                "What to expect:",
                "A single `Schema created` confirmation.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "(",
                "    db.schema()",
                "    .label('Asset')",
                "        .property('asset_id', 'string')",
                "        .property('asset_name', 'string')",
                "        .property('owner_team', 'string')",
                "        .property('site', 'string')",
                "        .property('env', 'string')",
                "        .property('business_criticality', 'int64')",
                "        .property('internet_exposed', 'bool')",
                "        .property('current_exposure', 'float64')",
                "    .done()",
                "    .label('Vulnerability')",
                "        .property('cve_id', 'string')",
                "        .property('cwe', 'string')",
                "        .property('vendor', 'string')",
                "        .property('product_family', 'string')",
                "        .property('base_severity', 'float64')",
                "        .property('attack_surface', 'string')",
                "        .property('kev', 'bool')",
                "        .property('epss', 'float64')",
                "    .done()",
                "    .label('RemediationAction')",
                "        .property('action_id', 'string')",
                "        .property('cve_id', 'string')",
                "        .property('action_type', 'string')",
                "        .property('cost_index', 'float64')",
                "        .property('downtime_hours', 'float64')",
                "        .property('risk_reduction', 'float64')",
                "    .done()",
                "    .label('KnowledgeDoc')",
                "        .property('doc_id', 'string')",
                "        .property('doc_type', 'string')",
                "        .property('title', 'string')",
                "        .property('content', 'string')",
                "        .property('cve_id', 'string')",
                "        .vector('embedding', 4)",
                "    .done()",
                "    .edge_type('HAS_FINDING', ['Asset'], ['Vulnerability'])",
                "        .property('scan_ts', 'string')",
                "        .property('exploit_evidence', 'float64')",
                "        .property('patch_sla_hours', 'int64')",
                "        .property('base_exposure', 'float64')",
                "        .property('evidence_score', 'float64')",
                "        .property('exposure_score', 'float64')",
                "    .done()",
                "    .edge_type('DEPENDS_ON', ['Asset'], ['Asset'])",
                "        .property('propagation_risk', 'float64')",
                "    .done()",
                "    .edge_type('REMEDIATED_BY', ['Vulnerability'], ['RemediationAction'])",
                "    .done()",
                "    .edge_type('SUPPORTED_BY', ['Vulnerability'], ['KnowledgeDoc'])",
                "    .done()",
                "    .edge_type('PRIORITIZED_FOR', ['Asset'], ['RemediationAction'])",
                "    .done()",
                "    .edge_type('TEMP_CONTAINED_BY', ['Asset'], ['RemediationAction'])",
                "    .done()",
                "    .apply()",
                ")",
                "print('Schema created')",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 4) Ingest Graph Facts",
                "",
                "What this does:",
                "Ingests assets, vulnerabilities, remediation actions, knowledge docs, and links.",
                "",
                "What to expect:",
                "Graph counts for nodes and key edge types.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "kev_map = {r['cve_id']: _to_int(r['kev']) for r in kev}",
                "epss_map = {r['cve_id']: _f(r['epss']) for r in epss}",
                "",
                "tx = session.tx()",
                "for row in focus_assets:",
                "    tx.execute(",
                "        f\"CREATE (:Asset {{asset_id: '{_esc(row['asset_id'])}', asset_name: '{_esc(row['asset_name'])}', \"",
                "        f\"owner_team: '{_esc(row['owner_team'])}', site: '{_esc(row['site'])}', env: '{_esc(row['env'])}', \"",
                "        f\"business_criticality: {_to_int(row['business_criticality'])}, internet_exposed: {str(_to_int(row['internet_exposed']) > 0).lower()}, current_exposure: 0.0}})\"",
                "    )",
                "",
                "for row in focus_vulns:",
                "    cve = row['cve_id']",
                "    tx.execute(",
                "        f\"CREATE (:Vulnerability {{cve_id: '{_esc(cve)}', cwe: '{_esc(row['cwe'])}', vendor: '{_esc(row['vendor'])}', \"",
                "        f\"product_family: '{_esc(row['product_family'])}', base_severity: {_f(row['base_severity'])}, \"",
                "        f\"attack_surface: '{_esc(row['attack_surface'])}', kev: {str(kev_map.get(cve, 0) > 0).lower()}, epss: {_f(str(epss_map.get(cve, 0.0)))}}})\"",
                "    )",
                "",
                "for row in focus_actions:",
                "    tx.execute(",
                "        f\"CREATE (:RemediationAction {{action_id: '{_esc(row['action_id'])}', cve_id: '{_esc(row['cve_id'])}', \"",
                "        f\"action_type: '{_esc(row['action_type'])}', cost_index: {_f(row['cost_index'])}, \"",
                "        f\"downtime_hours: {_f(row['downtime_hours'])}, risk_reduction: {_f(row['risk_reduction'])}}})\"",
                "    )",
                "",
                "for row in focus_docs:",
                "    tx.execute(",
                "        f\"CREATE (:KnowledgeDoc {{doc_id: '{_esc(row['doc_id'])}', doc_type: '{_esc(row['doc_type'])}', \"",
                "        f\"title: '{_esc(row['title'])}', content: '{_esc(row['content'])}', cve_id: '{_esc(row['cve_id'])}', \"",
                "        f\"embedding: {_vec(row['embedding'])}}})\"",
                "    )",
                "",
                "for row in focus_findings:",
                "    cve = row['cve_id']",
                "    kev_score = 1.0 if kev_map.get(cve, 0) > 0 else 0.0",
                "    epss_score = epss_map.get(cve, 0.0)",
                "    base_exposure = min(0.99, 0.38 * kev_score + 0.42 * epss_score + 0.20 * _f(row['exploit_evidence']))",
                "    tx.execute(",
                "        f\"MATCH (a:Asset {{asset_id: '{_esc(row['asset_id'])}'}}), (v:Vulnerability {{cve_id: '{_esc(cve)}'}}) \"",
                "        f\"CREATE (a)-[:HAS_FINDING {{scan_ts: '{_esc(row['scan_ts'])}', exploit_evidence: {_f(row['exploit_evidence'])}, \"",
                "        f\"patch_sla_hours: {_to_int(row['patch_sla_hours'])}, base_exposure: {base_exposure}, evidence_score: 0.0, exposure_score: {base_exposure}}}]->(v)\"",
                "    )",
                "",
                "for row in focus_dependencies:",
                "    tx.execute(",
                "        f\"MATCH (s:Asset {{asset_id: '{_esc(row['src_asset_id'])}'}}), (d:Asset {{asset_id: '{_esc(row['dst_asset_id'])}'}}) \"",
                "        f\"CREATE (s)-[:DEPENDS_ON {{propagation_risk: {_f(row['propagation_risk'])}}}]->(d)\"",
                "    )",
                "",
                "for row in focus_actions:",
                "    tx.execute(",
                "        f\"MATCH (v:Vulnerability {{cve_id: '{_esc(row['cve_id'])}'}}), (r:RemediationAction {{action_id: '{_esc(row['action_id'])}'}}) \"",
                '        "CREATE (v)-[:REMEDIATED_BY]->(r)"',
                "    )",
                "",
                "for row in focus_docs:",
                "    if not row['cve_id']:",
                "        continue",
                "    tx.execute(",
                "        f\"MATCH (v:Vulnerability {{cve_id: '{_esc(row['cve_id'])}'}}), (d:KnowledgeDoc {{doc_id: '{_esc(row['doc_id'])}'}}) \"",
                '        "CREATE (v)-[:SUPPORTED_BY]->(d)"',
                "    )",
                "tx.commit()",
                "",
                'counts = session.query("""',
                "MATCH (a:Asset) WITH count(*) AS assets",
                "MATCH (v:Vulnerability) WITH assets, count(*) AS vulnerabilities",
                "MATCH (r:RemediationAction) WITH assets, vulnerabilities, count(*) AS actions",
                "MATCH (d:KnowledgeDoc) WITH assets, vulnerabilities, actions, count(*) AS docs",
                "MATCH ()-[f:HAS_FINDING]->() WITH assets, vulnerabilities, actions, docs, count(*) AS findings",
                "MATCH ()-[dep:DEPENDS_ON]->()",
                "RETURN assets, vulnerabilities, actions, docs, findings, count(dep) AS dependencies",
                '""")',
                "print('Graph counts:')",
                "pprint(counts[0])",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 5) Integrated Hybrid Retrieval (`uni.search`)",
                "",
                "What this does:",
                "Builds vector + full-text indexes on `KnowledgeDoc`, runs hybrid retrieval, and derives a per-CVE evidence boost.",
                "",
                "What to expect:",
                "Non-empty hybrid result rows (`doc_id`, `cve_id`, `score`, `vector_score`, `fts_score`).",
            ],
        )
    )
    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "> **FTS Auto-Build and L0 Visibility**: `CREATE FULLTEXT INDEX` automatically",
                "> builds the index — no `rebuild_indexes()` call needed. Uni also searches the",
                "> L0 in-memory write buffer, so documents ingested in the previous step are",
                "> immediately visible to FTS queries without requiring a flush to disk.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "tx = session.tx()",
                'tx.execute("CREATE FULLTEXT INDEX knowledge_doc_fts FOR (d:KnowledgeDoc) ON EACH [d.content]")',
                "tx.commit()",
                "",
                "query_text = 'internet exposed actively exploited vulnerability remediation playbook hotfix virtual patch'",
                "query_vec = [0.92, 0.20, 0.08, 0.86]",
                "",
                "hybrid_rows = []",
                "try:",
                '    hybrid_rows = session.query("""',
                "    CALL uni.search(",
                "        'KnowledgeDoc',",
                "        {vector: 'embedding', fts: 'content'},",
                "        $q,",
                "        $vec,",
                "        12",
                "    )",
                "    YIELD node, score, vector_score, fts_score",
                "    RETURN node.doc_id AS doc_id, node.cve_id AS cve_id, node.title AS title, score, vector_score, fts_score",
                "    ORDER BY score DESC",
                "    \"\"\", {'q': query_text, 'vec': query_vec})",
                "except Exception as exc:",
                "    print('uni.search fallback (manual vector+fts fusion):', exc)",
                '    vector_rows = session.query("""',
                "    CALL uni.vector.query('KnowledgeDoc', 'embedding', $vec, 12)",
                "    YIELD node, distance",
                "    RETURN node.doc_id AS doc_id, node.cve_id AS cve_id, node.title AS title, distance",
                "    ORDER BY distance ASC",
                '    """, {\'vec\': query_vec})',
                '    fts_rows = session.query("""',
                "    CALL uni.fts.query('KnowledgeDoc', 'content', $q, 12)",
                "    YIELD node, score",
                "    RETURN node.doc_id AS doc_id, node.cve_id AS cve_id, node.title AS title, score",
                "    ORDER BY score DESC",
                '    """, {\'q\': query_text})',
                "",
                "    rrf = {}",
                "    meta = {}",
                "    for rank, row in enumerate(vector_rows, start=1):",
                "        did = str(row['doc_id'])",
                "        rrf[did] = rrf.get(did, 0.0) + 1.0 / (60.0 + rank)",
                "        meta[did] = {'doc_id': did, 'cve_id': row.get('cve_id'), 'title': row.get('title'), 'vector_score': 1.0 / (1.0 + _f(str(row.get('distance', 0.0)))), 'fts_score': 0.0}",
                "    for rank, row in enumerate(fts_rows, start=1):",
                "        did = str(row['doc_id'])",
                "        rrf[did] = rrf.get(did, 0.0) + 1.0 / (60.0 + rank)",
                "        m = meta.setdefault(did, {'doc_id': did, 'cve_id': row.get('cve_id'), 'title': row.get('title'), 'vector_score': 0.0, 'fts_score': 0.0})",
                "        m['fts_score'] = _f(str(row.get('score', 0.0)))",
                "",
                "    hybrid_rows = []",
                "    for did, score in rrf.items():",
                "        m = meta[did]",
                "        hybrid_rows.append({",
                "            'doc_id': m['doc_id'],",
                "            'cve_id': m.get('cve_id', ''),",
                "            'title': m.get('title', ''),",
                "            'score': score,",
                "            'vector_score': m.get('vector_score', 0.0),",
                "            'fts_score': m.get('fts_score', 0.0),",
                "        })",
                "    hybrid_rows = sorted(hybrid_rows, key=lambda r: -_f(str(r.get('score', 0.0))))[:12]",
                "",
                "print('Hybrid rows:', len(hybrid_rows))",
                "pprint(hybrid_rows[:8])",
                "",
                "if not hybrid_rows:",
                "    raise RuntimeError('Expected non-empty hybrid retrieval rows')",
                "",
                "cve_evidence: dict[str, float] = {}",
                "for row in hybrid_rows:",
                "    cve = str(row.get('cve_id', '') or '').strip()",
                "    if not cve:",
                "        continue",
                "    cve_evidence[cve] = max(cve_evidence.get(cve, 0.0), _f(str(row.get('score', 0.0))))",
                "",
                "print('Evidence boost by CVE:')",
                "pprint(cve_evidence)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 6) Columnar Analytics: Exposure Rollups and Ranking",
                "",
                "What this does:",
                "Applies evidence boosts to finding edges, computes team-level rollups, and ranks high-exposure assets.",
                "",
                "What to expect:",
                "- Non-empty team rollups",
                "- Ranked high-exposure asset rows",
                "",
                "How to read it:",
                "This is the analytical bridge from retrieval evidence into Locy decision logic.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "tx = session.tx()",
                "for cve, evidence in cve_evidence.items():",
                "    tx.query(",
                '        """',
                "        MATCH (:Asset)-[f:HAS_FINDING]->(v:Vulnerability)",
                "        WHERE v.cve_id = $cve",
                "        SET f.evidence_score = $evidence,",
                "            f.exposure_score = f.base_exposure + (0.22 * $evidence)",
                "        RETURN count(f) AS updated",
                '        """,',
                "        {'cve': cve, 'evidence': evidence},",
                "    )",
                "",
                'tx.query("""',
                "MATCH (a:Asset)-[f:HAS_FINDING]->(:Vulnerability)",
                "WITH a, max(f.exposure_score) AS max_exposure",
                "SET a.current_exposure = max_exposure",
                "RETURN count(a) AS updated_assets",
                '""")',
                "tx.commit()",
                "",
                'team_rollup = session.query("""',
                "MATCH (a:Asset)-[f:HAS_FINDING]->(:Vulnerability)",
                "RETURN",
                "  a.owner_team AS owner_team,",
                "  count(*) AS findings,",
                "  avg(f.exposure_score) AS avg_exposure,",
                "  max(f.exposure_score) AS max_exposure,",
                "  sum(CASE WHEN f.exposure_score >= 0.72 THEN 1 ELSE 0 END) AS urgent_findings",
                "ORDER BY avg_exposure DESC, urgent_findings DESC",
                '""")',
                "print('Team exposure rollup:')",
                "pprint(team_rollup)",
                "",
                'ranked_assets = session.query("""',
                "MATCH (a:Asset)-[f:HAS_FINDING]->(v:Vulnerability)",
                "RETURN",
                "  a.owner_team AS owner_team,",
                "  a.asset_id AS asset_id,",
                "  v.cve_id AS cve_id,",
                "  f.exposure_score AS exposure_score,",
                "  ROW_NUMBER() OVER (PARTITION BY a.owner_team ORDER BY f.exposure_score DESC) AS team_rank",
                "ORDER BY owner_team, team_rank",
                '""")',
                "print('Top assets per team:')",
                "pprint(ranked_assets[:12])",
                "",
                'hot_assets = session.query("""',
                "MATCH (a:Asset)-[f:HAS_FINDING]->(v:Vulnerability)",
                "RETURN a.asset_id AS asset_id, v.cve_id AS cve_id, f.exposure_score AS exposure_score",
                "ORDER BY f.exposure_score DESC",
                "LIMIT 12",
                '""")',
                "if not hot_assets:",
                "    raise RuntimeError('Expected non-empty hot asset list')",
                "focus_source_asset = str(hot_assets[0]['asset_id'])",
                "focus_plan_asset = str(hot_assets[0]['asset_id'])",
                "print('Focus source asset:', focus_source_asset)",
                "",
                'critical_asset_count_rows = session.query("""',
                "MATCH (a:Asset)-[f:HAS_FINDING]->(:Vulnerability)",
                "WHERE f.exposure_score >= 0.72",
                "RETURN count(DISTINCT a) AS n",
                '""")',
                "total_critical_assets = _to_int(str(critical_asset_count_rows[0]['n']))",
                "print('Critical assets (threshold >= 0.72):', total_critical_assets)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 7) Integrated Locy Program (`DERIVE` + `ALONG` + `FOLD` + `BEST BY`)",
                "",
                "What this does:",
                "Uses analytics-enriched findings to propagate blast risk, derive remediation edges, and choose best actions.",
                "",
                "What to expect:",
                "- blast path rows (`source_asset`, `impacted_asset`, `path_risk`, `hops`)",
                "- derive affected count",
                "- best action rows (`asset_id`, `cve_id`, `action_type`, `residual_risk`, `action_cost`)",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_baseline = r'''",
                "CREATE RULE critical_finding AS",
                "MATCH (a:Asset)",
                "WHERE a.current_exposure >= 0.72",
                "YIELD KEY a",
                "",
                "CREATE RULE blast_path AS",
                "MATCH (src:Asset)-[d:DEPENDS_ON]->(dst:Asset)",
                "WHERE src IS critical_finding",
                "ALONG path_risk = src.current_exposure + d.propagation_risk, hops = 1",
                "BEST BY path_risk DESC, hops ASC",
                "YIELD KEY src, KEY dst, path_risk, hops",
                "",
                "CREATE RULE blast_path AS",
                "MATCH (src:Asset)-[d:DEPENDS_ON]->(mid:Asset)",
                "WHERE mid IS blast_path TO dst",
                "ALONG path_risk = prev.path_risk + d.propagation_risk, hops = prev.hops + 1",
                "BEST BY path_risk DESC, hops ASC",
                "YIELD KEY src, KEY dst, path_risk, hops",
                "",
                "CREATE RULE blast_summary AS",
                "MATCH (src:Asset)",
                "WHERE src IS blast_path TO dst",
                "FOLD impacted_assets = COUNT(dst), total_path_risk = SUM(path_risk), max_hops = MAX(hops)",
                "YIELD KEY src, impacted_assets, total_path_risk, max_hops",
                "",
                "CREATE RULE derive_priority AS",
                "MATCH (a:Asset)-[f:HAS_FINDING]->(v:Vulnerability)-[:REMEDIATED_BY]->(r:RemediationAction)",
                "WHERE a IS critical_finding",
                "DERIVE (a)-[:PRIORITIZED_FOR]->(r)",
                "",
                "CREATE RULE best_action AS",
                "MATCH (a:Asset)-[f:HAS_FINDING]->(v:Vulnerability)-[:REMEDIATED_BY]->(r:RemediationAction)",
                "WHERE a IS critical_finding",
                "ALONG residual_risk = a.current_exposure * (1.0 - r.risk_reduction), action_cost = r.cost_index, action_downtime = r.downtime_hours",
                "BEST BY residual_risk ASC, action_cost ASC, action_downtime ASC",
                "YIELD KEY a, KEY v, r, residual_risk, action_cost, action_downtime",
                "",
                "QUERY blast_path WHERE src = src RETURN src.asset_id AS source_asset, dst.asset_id AS impacted_asset, path_risk, hops",
                "DERIVE derive_priority",
                "QUERY best_action WHERE a = a RETURN a.asset_id AS asset_id, v.cve_id AS cve_id, r.action_type AS action_type, residual_risk, action_cost, action_downtime",
                "'''",
                "",
                "baseline_out = session.locy_with(program_baseline).with_config({",
                "    'max_iterations': 400, 'timeout': 60.0, 'max_abduce_candidates': 100, 'max_abduce_results': 12",
                "}).run()",
                "",
                "# Persist DERIVE edges to graph",
                "tx = session.tx()",
                "tx.apply(baseline_out.derived_fact_set)",
                "tx.commit()",
                "",
                "stats = baseline_out.stats",
                "print('Iterations:', stats.total_iterations)",
                "print('Strata:', stats.strata_evaluated)",
                "print('Queries executed:', stats.queries_executed)",
                "",
                "blast_path_rows = []",
                "best_plan_rows = []",
                "for i, cmd in enumerate(baseline_out.command_results, start=1):",
                "    print(f'\\nCommand #{i}:', cmd.command_type)",
                "    if cmd.command_type in ('query', 'cypher'):",
                "        rows = _norm_rows(cmd.rows)",
                "        print('rows:', len(rows))",
                "        pprint(rows[:5])",
                "        if rows and 'impacted_asset' in rows[0]:",
                "            blast_path_rows = rows",
                "        if rows and 'action_type' in rows[0]:",
                "            best_plan_rows = rows",
                "    elif cmd.command_type == 'derive':",
                "        print('affected:', cmd.affected)",
                "",
                "blast_rollup = {}",
                "for row in blast_path_rows:",
                "    source = str(row.get('source_asset', ''))",
                "    impacted = str(row.get('impacted_asset', ''))",
                "    info = blast_rollup.setdefault(source, {'source_asset': source, 'impacted': set(), 'total_path_risk': 0.0, 'max_hops': 0})",
                "    if impacted:",
                "        info['impacted'].add(impacted)",
                "    info['total_path_risk'] += _f(str(row.get('path_risk', '0')))",
                "    info['max_hops'] = max(int(info['max_hops']), int(_f(str(row.get('hops', '0')))))",
                "",
                "blast_rows = [",
                "    {",
                "        'source_asset': v['source_asset'],",
                "        'impacted_assets': len(v['impacted']),",
                "        'total_path_risk': v['total_path_risk'],",
                "        'max_hops': v['max_hops'],",
                "    }",
                "    for v in blast_rollup.values()",
                "]",
                "blast_rows = sorted(",
                "    blast_rows,",
                "    key=lambda r: (-int(r.get('impacted_assets', 0)), -_f(str(r.get('total_path_risk', '0'))), str(r.get('source_asset', ''))),",
                ")",
                "best_plan_rows = sorted(",
                "    best_plan_rows,",
                "    key=lambda r: (_f(str(r.get('residual_risk', '0'))), _f(str(r.get('action_cost', '0'))), str(r.get('asset_id', ''))),",
                ")",
                "",
                "if not blast_rows:",
                "    raise RuntimeError('Expected non-empty blast rows')",
                "if not best_plan_rows:",
                "    raise RuntimeError('Expected non-empty best plan rows')",
                "",
                "focus_source_asset = str(blast_rows[0]['source_asset'])",
                "focus_plan_asset = str(best_plan_rows[0]['asset_id'])",
                "print('Top blast source asset:', focus_source_asset)",
                "print('Top plan asset:', focus_plan_asset)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 8) Explain One Derivation (`EXPLAIN RULE`)",
                "",
                "What this does:",
                "Shows the derivation tree behind blast propagation for one source asset.",
                "",
                "What to expect:",
                "A tree with rule name, clause index, and child derivations.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_explain = f'''",
                "CREATE RULE critical_finding AS",
                "MATCH (a:Asset)",
                "WHERE a.current_exposure >= 0.72",
                "YIELD KEY a",
                "",
                "CREATE RULE blast_path AS",
                "MATCH (src:Asset)-[d:DEPENDS_ON]->(dst:Asset)",
                "WHERE src IS critical_finding",
                "ALONG path_risk = src.current_exposure + d.propagation_risk, hops = 1",
                "BEST BY path_risk DESC, hops ASC",
                "YIELD KEY src, KEY dst, path_risk, hops",
                "",
                "CREATE RULE blast_path AS",
                "MATCH (src:Asset)-[d:DEPENDS_ON]->(mid:Asset)",
                "WHERE mid IS blast_path TO dst",
                "ALONG path_risk = prev.path_risk + d.propagation_risk, hops = prev.hops + 1",
                "BEST BY path_risk DESC, hops ASC",
                "YIELD KEY src, KEY dst, path_risk, hops",
                "",
                "EXPLAIN RULE blast_path WHERE src.asset_id = '{focus_source_asset}' RETURN dst",
                "'''",
                "",
                "explain_out = session.locy(program_explain)",
                "explain_cmd = next(cmd for cmd in explain_out.command_results if cmd.command_type == 'explain')",
                "tree = explain_cmd.tree",
                "",
                "def _print_tree(node, depth=0, max_depth=3, max_children=3):",
                "    indent = '  ' * depth",
                "    print(f\"{indent}- rule={node.get('rule')}, clause={node.get('clause_index')}, bindings={node.get('bindings', {})}\")",
                "    if depth >= max_depth:",
                "        return",
                "    children = node.get('children', [])",
                "    for child in children[:max_children]:",
                "        _print_tree(child, depth + 1, max_depth=max_depth, max_children=max_children)",
                "    if len(children) > max_children:",
                '        print(f"{indent}  ... {len(children) - max_children} more child derivations")',
                "",
                "print('Explain tree for source asset:', focus_source_asset)",
                "_print_tree(tree)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 9) Counterfactual Containment (`ASSUME`)",
                "",
                "What this does:",
                "Temporarily applies virtual patches for high-criticality assets and measures containment impact.",
                "",
                "What to expect:",
                "Contained rows from hypothetical graph state; rollback check should be zero.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "assume_program = '''",
                "ASSUME {",
                "  MATCH (a:Asset)-[:HAS_FINDING]->(v:Vulnerability)-[:REMEDIATED_BY]->(r:RemediationAction {action_type: 'virtual_patch'})",
                "  WHERE a.business_criticality >= 4",
                "  CREATE (a)-[:TEMP_CONTAINED_BY]->(r)",
                "} THEN {",
                "  MATCH (a:Asset)-[:TEMP_CONTAINED_BY]->(r:RemediationAction)",
                "  RETURN a.asset_id AS asset_id, r.action_type AS action_type",
                "}",
                "'''",
                "",
                "assume_out = session.locy(assume_program)",
                "assume_cmd = next(cmd for cmd in assume_out.command_results if cmd.command_type == 'assume')",
                "contained_rows = assume_cmd.rows",
                "contained_asset_ids = sorted({str(r['asset_id']) for r in contained_rows})",
                "",
                "contained_critical_assets = len(contained_asset_ids)",
                "residual_critical_assets = max(0, total_critical_assets - contained_critical_assets)",
                "abduce_target_asset = contained_asset_ids[0] if contained_asset_ids else focus_plan_asset",
                "",
                "print('Critical assets total:', total_critical_assets)",
                "print('Contained critical assets:', contained_critical_assets)",
                "print('Residual critical assets:', residual_critical_assets)",
                "print('ABDUCE target asset:', abduce_target_asset)",
                "print('Contained sample:')",
                "pprint(contained_rows[:10])",
                "",
                'rollback_check = session.query("MATCH (:Asset)-[r:TEMP_CONTAINED_BY]->(:RemediationAction) RETURN count(r) AS c")',
                "print('Rollback check (should be 0):', rollback_check[0]['c'])",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 10) Minimal Change Search (`ABDUCE`)",
                "",
                "What this does:",
                "Finds minimal changes that remove urgent patch requirement for one target asset.",
                "",
                "What to expect:",
                "At least one validated modification candidate.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_abduce = f'''",
                "CREATE RULE needs_immediate_patch AS",
                "MATCH (a:Asset)-[f:HAS_FINDING]->(v:Vulnerability)-[:REMEDIATED_BY]->(r:RemediationAction)",
                "WHERE a.current_exposure >= 0.72, r.action_type = 'hotfix_patch'",
                "YIELD KEY a, KEY v",
                "",
                "ABDUCE NOT needs_immediate_patch WHERE a.asset_id = '{abduce_target_asset}' RETURN a, v",
                "'''",
                "",
                "abduce_out = session.locy_with(program_abduce).with_config({",
                "    'max_abduce_candidates': 120, 'max_abduce_results': 12, 'timeout': 60.0",
                "}).run()",
                "abduce_cmd = next(cmd for cmd in abduce_out.command_results if cmd.command_type == 'abduce')",
                "mods = abduce_cmd.modifications",
                "",
                "print('ABDUCE target asset:', abduce_target_asset)",
                "print('Abduced modifications:', len(mods))",
                "for i, item in enumerate(mods[:8], start=1):",
                "    print(f'\\nCandidate #{i}')",
                "    pprint(item)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 11) What To Expect",
                "",
                "- Hybrid retrieval should return advisory evidence rows with combined scoring.",
                "- Team rollups should highlight a concentrated exposure hotspot.",
                "- `ALONG` recursion should produce non-empty blast paths.",
                "- `BEST BY` should pick one prioritized action per urgent finding.",
                "- `ASSUME` should contain at least one critical asset in the hypothetical state.",
                "- `ABDUCE` should return at least one validated candidate.",
                "- `EXPLAIN RULE` should include child derivations.",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 12) Build-Time Assertions",
                "",
                "These checks keep notebook execution meaningful in CI/docs builds.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "assert hybrid_rows, 'Expected non-empty hybrid rows'",
                "assert team_rollup, 'Expected non-empty team rollup rows'",
                "assert blast_rows, 'Expected non-empty blast rows'",
                "assert best_plan_rows, 'Expected non-empty best plan rows'",
                "assert total_critical_assets > 0, 'Expected critical assets in focus cohort'",
                "assert contained_critical_assets > 0, 'Expected ASSUME containment to affect at least one asset'",
                "assert residual_critical_assets < total_critical_assets, 'Expected residual critical assets to decrease'",
                "assert mods, 'Expected ABDUCE to produce modifications'",
                "assert any(item.get('validated') for item in mods if isinstance(item, dict)), 'Expected at least one validated ABDUCE candidate'",
                "assert tree.get('children'), 'Expected EXPLAIN RULE tree to include child derivations'",
                "print('Notebook assertions passed.')",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 13) Cleanup",
                "",
                "Removes the temporary on-disk database created for this run.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "shutil.rmtree(DB_DIR, ignore_errors=True)",
                "print('Cleaned up', DB_DIR)",
            ],
        )
    )

    return _create_notebook(cells)


def _check(path: Path, expected: str) -> int:
    actual = path.read_text(encoding="utf-8") if path.exists() else ""
    if actual == expected:
        print("Notebook is up to date.")
        return 0

    print(f"drift detected: {path}")
    diff = list(
        difflib.unified_diff(
            actual.splitlines(),
            expected.splitlines(),
            fromfile=str(path),
            tofile=f"{path} (generated)",
            lineterm="",
            n=2,
        )
    )
    for line in diff[:120]:
        print(line)
    if len(diff) > 120:
        print("... diff truncated ...")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Check drift without writing."
    )
    args = parser.parse_args(argv)

    notebook = _build_notebook()
    rendered = _render_json(notebook)
    if args.check:
        return _check(NOTEBOOK_PATH, rendered)

    NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
    NOTEBOOK_PATH.write_text(rendered, encoding="utf-8")
    print(f"generated {NOTEBOOK_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
