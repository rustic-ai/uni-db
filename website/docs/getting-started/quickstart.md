# Quick Start

> Build structured memory, encode domain physics, simulate consequences, and get explainable decisions — all in under 5 minutes.

You have a chain of services exposed to the internet, some with known vulnerabilities. Which services are non-compliant? What if you patch one? Why is a particular service flagged? What's the minimal fix? Uni answers all four questions with a single data model.

## Prerequisites

- [Uni installed](installation.md) (CLI and Python package)
- Python 3.9+
- Terminal access

---

## Step 1: Build Structured Memory

**Cognitive Pillar: Structured Memory** — a typed property graph is your agent's world model.

Create a small service dependency graph — four nodes, three edges:

```
public ──EXPOSES──▸ api (9.1) ──DEPENDS_ON──▸ worker (4.0) ──DEPENDS_ON──▸ db (8.4)
```

```bash
# Create nodes
uni query "CREATE (:Internet {name: 'public'})" --path /tmp/security-quickstart
uni query "CREATE (:Service {name: 'api', cve_score: 9.1})" --path /tmp/security-quickstart
uni query "CREATE (:Service {name: 'worker', cve_score: 4.0})" --path /tmp/security-quickstart
uni query "CREATE (:Service {name: 'db', cve_score: 8.4})" --path /tmp/security-quickstart

# Create edges
uni query "MATCH (i:Internet {name:'public'}), (s:Service {name:'api'}) CREATE (i)-[:EXPOSES]->(s)" \
  --path /tmp/security-quickstart
uni query "MATCH (a:Service {name:'api'}), (b:Service {name:'worker'}) CREATE (a)-[:DEPENDS_ON]->(b)" \
  --path /tmp/security-quickstart
uni query "MATCH (a:Service {name:'worker'}), (b:Service {name:'db'}) CREATE (a)-[:DEPENDS_ON]->(b)" \
  --path /tmp/security-quickstart
```

Each command writes to the same on-disk graph at `/tmp/security-quickstart`. The property graph is now your agent's persistent, structured memory — typed nodes and edges that survive restarts and support formal reasoning.

---

## Step 2: Query Relationships

**Cognitive Pillar: Associative Recall** — pattern-match over the graph to retrieve related entities.

Find direct dependencies:

```bash
uni query "MATCH (a:Service)-[:DEPENDS_ON]->(b:Service) \
  RETURN a.name AS service, b.name AS depends_on" \
  --path /tmp/security-quickstart
```

```
 service | depends_on
---------+-----------
 api     | worker
 worker  | db
```

Find what's directly exposed to the internet:

```bash
uni query "MATCH (i:Internet)-[:EXPOSES]->(s:Service) \
  RETURN i.name AS source, s.name AS exposed_service" \
  --path /tmp/security-quickstart
```

```
 source | exposed_service
--------+----------------
 public | api
```

These queries find **direct** relationships — one hop at a time. But `db` is also exposed, transitively, through `api → worker → db`. To reason about transitive exposure, you need formal rules.

---

## Step 3: Encode Domain Physics

**Cognitive Pillar: Domain Physics** — declarative rules that define how your domain actually works.

Switch to Python to use Uni's [Locy](../locy/language-guide.md) reasoning engine. Open the same database you built in the CLI:

```python
import uni_db

db = uni_db.Uni.open("/tmp/security-quickstart")
session = db.session()

RULES = r'''
CREATE RULE depends_on AS
MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
YIELD KEY a, KEY b

CREATE RULE depends_on AS
MATCH (a:Service)-[:DEPENDS_ON]->(mid:Service)
WHERE mid IS depends_on TO b
YIELD KEY a, KEY b

CREATE RULE exposed AS
MATCH (i:Internet)-[:EXPOSES]->(s:Service)
YIELD KEY s

CREATE RULE exposed AS
MATCH (i:Internet)-[:EXPOSES]->(entry:Service)
WHERE entry IS depends_on TO s
YIELD KEY s

CREATE RULE vulnerable AS
MATCH (s:Service)
WHERE s.cve_score >= 7.0
YIELD KEY s

CREATE RULE non_compliant AS
MATCH (s:Service)
WHERE s IS vulnerable, s IS exposed
YIELD KEY s
'''

program = RULES + r'''
QUERY non_compliant WHERE s.name = s.name RETURN s.name AS service
'''

out = session.locy(program)

for cmd in out.command_results:
    if cmd.command_type == "query":
        for row in cmd.rows:
            print(row["service"])
```

```
api
db
```

Six rules, evaluated in a single pass:

| Rule | What it encodes |
|------|-----------------|
| `depends_on` (2 clauses) | Transitive dependency — if A→B→C, then A depends on C |
| `exposed` (2 clauses) | Transitive exposure — if the internet reaches A and A depends on B, then B is exposed |
| `vulnerable` | CVE score ≥ 7.0 |
| `non_compliant` | Conjunction: vulnerable AND exposed |

`api` and `db` are both non-compliant. `worker` is exposed but not vulnerable (CVE 4.0), so it passes. These aren't queries — they're **derived facts**, recomputed from the graph every time the rules run.

---

## Step 4: Simulate a What-If

**Cognitive Pillar: Mental Simulation** — test hypothetical changes without touching real data.

What happens if you patch `api` down to CVE 3.0?

```python
program = RULES + r'''
ASSUME {
  MATCH (s:Service {name: 'api'})
  SET s.cve_score = 3.0
} THEN {
  QUERY non_compliant WHERE s.name = s.name RETURN s.name AS service
}
'''

out = session.locy(program)
```

The `ASSUME` block creates a hypothetical world where `api.cve_score = 3.0`, evaluates the rules inside `THEN`, then **rolls back** the change. The result: `api` drops off the non-compliant list, but `db` remains — its own CVE score (8.4) still exceeds the threshold, and it's still transitively exposed.

??? note "Full script"

    ```python
    import uni_db

    db = uni_db.Uni.open("/tmp/security-quickstart")
    session = db.session()

    RULES = r'''
    CREATE RULE depends_on AS
    MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
    YIELD KEY a, KEY b

    CREATE RULE depends_on AS
    MATCH (a:Service)-[:DEPENDS_ON]->(mid:Service)
    WHERE mid IS depends_on TO b
    YIELD KEY a, KEY b

    CREATE RULE exposed AS
    MATCH (i:Internet)-[:EXPOSES]->(s:Service)
    YIELD KEY s

    CREATE RULE exposed AS
    MATCH (i:Internet)-[:EXPOSES]->(entry:Service)
    WHERE entry IS depends_on TO s
    YIELD KEY s

    CREATE RULE vulnerable AS
    MATCH (s:Service)
    WHERE s.cve_score >= 7.0
    YIELD KEY s

    CREATE RULE non_compliant AS
    MATCH (s:Service)
    WHERE s IS vulnerable, s IS exposed
    YIELD KEY s
    '''

    program = RULES + r'''
    ASSUME {
      MATCH (s:Service {name: 'api'})
      SET s.cve_score = 3.0
    } THEN {
      QUERY non_compliant WHERE s.name = s.name RETURN s.name AS service
    }
    '''

    out = session.locy(program)
    for cmd in out.command_results:
        rows = cmd.rows if cmd.command_type in ("query", "assume") else []
        print(cmd.command_type, rows)
    ```

---

## Step 5: Explain a Decision

**Cognitive Pillar: Explainable Decisions** — trace exactly why the engine reached a conclusion.

Why is `db` flagged as non-compliant?

```python
program = RULES + r'''
EXPLAIN RULE non_compliant WHERE s.name = 'db'
'''

out = session.locy(program)

explain_cmd = next(c for c in out.command_results if c.command_type == "explain")
tree = explain_cmd.tree
```

The engine returns a derivation tree showing the complete chain of reasoning:

- **non_compliant(db)** holds because:
    - **vulnerable(db)** — `db.cve_score = 8.4 ≥ 7.0`
    - **exposed(db)** — `public` EXPOSES `api`, `api` depends on `worker`, `worker` depends on `db`

This isn't a log or a confidence score — it's a formal proof trace. Every step is a rule application you can inspect and audit.

??? note "Full script"

    ```python
    import json
    import uni_db

    db = uni_db.Uni.open("/tmp/security-quickstart")
    session = db.session()

    RULES = r'''
    CREATE RULE depends_on AS
    MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
    YIELD KEY a, KEY b

    CREATE RULE depends_on AS
    MATCH (a:Service)-[:DEPENDS_ON]->(mid:Service)
    WHERE mid IS depends_on TO b
    YIELD KEY a, KEY b

    CREATE RULE exposed AS
    MATCH (i:Internet)-[:EXPOSES]->(s:Service)
    YIELD KEY s

    CREATE RULE exposed AS
    MATCH (i:Internet)-[:EXPOSES]->(entry:Service)
    WHERE entry IS depends_on TO s
    YIELD KEY s

    CREATE RULE vulnerable AS
    MATCH (s:Service)
    WHERE s.cve_score >= 7.0
    YIELD KEY s

    CREATE RULE non_compliant AS
    MATCH (s:Service)
    WHERE s IS vulnerable, s IS exposed
    YIELD KEY s
    '''

    program = RULES + r'''
    EXPLAIN RULE non_compliant WHERE s.name = 'db'
    '''

    out = session.locy(program)
    explain_cmd = next(c for c in out.command_results if c.command_type == "explain")
    print(json.dumps(explain_cmd.tree, indent=2))
    ```

---

## Step 6: Compute the Minimal Fix

**Cognitive Pillar: Explainable Decisions** — abductive reasoning searches backward from a desired outcome.

Instead of asking *why is `db` flagged?*, ask *what's the smallest change that would make `db` compliant?*

```python
program = RULES + r'''
ABDUCE NOT non_compliant WHERE s.name = 'db' RETURN s
'''

out = session.locy(program)

abduce_cmd = next(c for c in out.command_results if c.command_type == "abduce")
modifications = abduce_cmd.modifications
```

The engine searches backward from the goal (`NOT non_compliant`) and returns the minimal set of graph modifications that would achieve it — for example, lowering `db.cve_score` below 7.0 or removing the exposure path. This turns "what went wrong" into "what should we change."

??? note "Full script"

    ```python
    import uni_db

    db = uni_db.Uni.open("/tmp/security-quickstart")
    session = db.session()

    RULES = r'''
    CREATE RULE depends_on AS
    MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
    YIELD KEY a, KEY b

    CREATE RULE depends_on AS
    MATCH (a:Service)-[:DEPENDS_ON]->(mid:Service)
    WHERE mid IS depends_on TO b
    YIELD KEY a, KEY b

    CREATE RULE exposed AS
    MATCH (i:Internet)-[:EXPOSES]->(s:Service)
    YIELD KEY s

    CREATE RULE exposed AS
    MATCH (i:Internet)-[:EXPOSES]->(entry:Service)
    WHERE entry IS depends_on TO s
    YIELD KEY s

    CREATE RULE vulnerable AS
    MATCH (s:Service)
    WHERE s.cve_score >= 7.0
    YIELD KEY s

    CREATE RULE non_compliant AS
    MATCH (s:Service)
    WHERE s IS vulnerable, s IS exposed
    YIELD KEY s
    '''

    program = RULES + r'''
    ABDUCE NOT non_compliant WHERE s.name = 'db' RETURN s
    '''

    out = session.locy(program)
    abduce_cmd = next(c for c in out.command_results if c.command_type == "abduce")
    for mod in abduce_cmd.modifications:
        print(mod)
    ```

---

## What's Next

You've touched all five cognitive pillars:

| Step | Pillar | What you did |
|------|--------|--------------|
| 1 | Structured Memory | Built a typed property graph as your agent's world model |
| 2 | Associative Recall | Pattern-matched over the graph to find related entities |
| 3 | Domain Physics | Encoded transitive exposure and compliance rules in Locy |
| 4 | Mental Simulation | Tested a hypothetical patch without modifying real data |
| 5–6 | Explainable Decisions | Traced why `db` was flagged, then computed the minimal fix |

### Dive Deeper

| Topic | Link |
|-------|------|
| Rust and Python APIs | [Programming Guide](programming-guide.md) |
| Locy rule syntax | [Locy Language Guide](../locy/language-guide.md) |
| ASSUME, EXPLAIN, ABDUCE | [Advanced Reasoning](../locy/advanced/derive-assume-abduce.md) |
| CLI commands | [CLI Reference](cli-reference.md) |
| Full compliance example | [Compliance Remediation Notebook](../examples/python/locy_compliance_remediation.ipynb) |
| Full exposure twin example | [Cyber Exposure Twin Notebook](../examples/python/locy_cyber_exposure_twin.ipynb) |
| Industry use cases | [Use Cases](../locy/use-cases.md) |

---

## Troubleshooting

**"Property not found"** — Property names are case-sensitive. Use `cve_score`, not `CVE_Score`.

**"Path does not exist"** — Pass `--path /tmp/security-quickstart` to every CLI command. Without it, Uni defaults to `./storage`.

**"No module named uni_db"** — Install the Python package: `pip install uni-db`.

**"Locy returns no results"** — Ensure the graph data was created first (Step 1). Each `session.locy()` call reads from the current graph state; if the graph is empty, rules derive nothing.
