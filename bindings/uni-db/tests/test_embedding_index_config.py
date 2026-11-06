# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Parsing of a vector index's ``"embedding"`` (auto-embed) config.

The Python parser used to be an ``Option`` chain: a config with ``"source"``
(the key Cypher DDL uses) or any other mistake was dropped without a word, the
index was created without auto-embedding, and the first insert failed with
"Property 'embedding' cannot be null", which never mentions the config.

No model is downloaded. Whether auto-embedding is attached is observable
without one: with no Xervo catalog, an insert into an auto-embedded index fails
with "runtime not configured for auto-embedding", while an index whose config
was dropped fails on the NOT NULL vector instead.
"""

import pytest

import uni_db
from uni_db import DataType


def _apply(db, embedding):
    (
        db.schema()
        .label("Doc")
        .property("text", DataType.STRING())
        .vector("embedding", 4)
        .index("embedding", {"type": "vector", "embedding": embedding})
        .apply()
    )


def _insert(db):
    tx = db.session().tx()
    tx.execute("CREATE (:Doc {text: 'hello'})")
    tx.commit()


@pytest.mark.parametrize("key", ["source_properties", "source"])
def test_both_source_spellings_attach_auto_embedding(key):
    db = uni_db.Uni.temporary()
    _apply(db, {"alias": "embed/default", key: ["text"]})
    # Auto-embedding is wired: the write asks Xervo for the vector.
    with pytest.raises(
        uni_db.UniQueryError, match="runtime not configured for auto-embedding"
    ):
        _insert(db)


@pytest.mark.parametrize(
    ("embedding", "message"),
    [
        (
            {"alias": "embed/default", "sources": ["text"]},
            "unknown key 'sources'",
        ),
        (
            {
                "alias": "embed/default",
                "source": ["text"],
                "source_properties": ["text"],
            },
            "both 'source_properties' and 'source'",
        ),
        ({"source": ["text"]}, "requires 'alias'"),
        ({"alias": "", "source": ["text"]}, "requires 'alias'"),
        ({"alias": "embed/default"}, r"requires 'source_properties' \(or 'source'\)"),
        ({"alias": "embed/default", "source": []}, "at least one property"),
        ({"alias": "embed/default", "source": "text"}, "must be a list"),
        ({"alias": "embed/default", "source": ["text", 3]}, "only property names"),
        (
            {"alias": "embed/default", "source": ["text"], "batch_size": 0},
            "positive integer",
        ),
        (
            {"alias": "embed/default", "source": ["text"], "query_prefix": 1},
            "'query_prefix' must be a string",
        ),
        ("embed/default", "must be a dict"),
    ],
)
def test_malformed_config_raises_at_apply(embedding, message):
    db = uni_db.Uni.temporary()
    with pytest.raises(ValueError, match=message):
        _apply(db, embedding)


def test_optional_fields_are_accepted():
    db = uni_db.Uni.temporary()
    _apply(
        db,
        {
            "alias": "embed/default",
            "source_properties": ["text"],
            "batch_size": 8,
            "document_prefix": "passage: ",
            "query_prefix": "query: ",
        },
    )
    with pytest.raises(
        uni_db.UniQueryError, match="runtime not configured for auto-embedding"
    ):
        _insert(db)
