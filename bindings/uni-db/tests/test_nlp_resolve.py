"""Entity resolution with Locy, run from Python as the NLP extraction page shows.

The program is read from ``website/docs/features/nlp-extraction.md`` itself,
so this test fails if the documented program stops running from Python. No
embedding model is configured here: mentions and entities carry hand-set
vectors, and a new entity copies its mention's vector. The Rust tests in
``crates/uni/tests/common/nlp_resolve.rs`` cover the auto-embedded path.
"""

import re
from pathlib import Path

import pytest

DOCS = (
    Path(__file__).resolve().parents[3]
    / "website"
    / "docs"
    / "features"
    / "nlp-extraction.md"
)


def documented_program() -> str:
    text = DOCS.read_text(encoding="utf-8")
    match = re.search(
        r"Then run the program in a transaction:\n\n```locy\n(.*?)```", text, re.S
    )
    assert match, "resolution program not found on the NLP extraction page"
    # Without an embedder a new entity copies its mention's vector.
    return match.group(1).replace(
        "MERGE (e:Entity {name: m.text, type: m.type})\n",
        "MERGE (e:Entity {name: m.text, type: m.type})\n"
        "ON CREATE SET e.embedding = m.embedding\n",
    )


@pytest.fixture
def db(empty_db):
    (
        empty_db.schema()
        .label("Entity")
        .property("name", "string")
        .property("type", "string")
        .vector("embedding", 3)
        .done()
        .label("Mention")
        .property("text", "string")
        .property("type", "string")
        .vector("embedding", 3)
        .done()
        .apply()
    )
    with empty_db.session().tx() as tx:
        tx.execute(
            "CREATE (:Entity {name: 'Ada Lovelace', type: 'person', "
            "embedding: [1.0, 0.0, 0.0]}), "
            "(:Mention {text: 'Lovelace', type: 'person', "
            "embedding: [0.75, 0.66, 0.0]}), "
            "(:Mention {text: 'Grace Hopper', type: 'person', "
            "embedding: [0.0, 1.0, 0.0]}), "
            "(:Mention {text: 'grace hopper', type: 'person', "
            "embedding: [0.0, 1.0, 0.0]})"
        )
        tx.commit()
    return empty_db


def links(db) -> list[tuple[str, str]]:
    rows = db.session().query(
        "MATCH (m:Mention)-[:REFERS_TO]->(e:Entity) "
        "RETURN m.text AS m, e.name AS e ORDER BY m, e"
    )
    return [(r["m"], r["e"]) for r in rows]


def resolve(db) -> None:
    with db.session().tx() as tx:
        tx.locy(documented_program())
        tx.commit()


EXPECTED = [
    ("Grace Hopper", "Grace Hopper"),
    ("Lovelace", "Ada Lovelace"),
    ("grace hopper", "Grace Hopper"),
]


def test_documented_program_resolves_mentions(db):
    resolve(db)
    # The surname and similarity clauses together clear the threshold; both
    # spellings of the new name share one new entity.
    assert links(db) == EXPECTED
    count = db.session().query("MATCH (e:Entity) RETURN count(e) AS n")[0]["n"]
    assert count == 2


def test_rerunning_the_program_changes_nothing(db):
    resolve(db)
    resolve(db)
    assert links(db) == EXPECTED
