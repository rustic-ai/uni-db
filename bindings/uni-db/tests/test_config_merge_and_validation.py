"""`Uni.builder().config({...})` must merge, and must refuse a key it cannot honour.

Both behaviours replace a silent failure.

`config()` used to assign a value built from `UniConfig::default()`, so
`.batch_size(4096).config({"query_timeout": 11})` put `batch_size` back to its
default without saying anything. And the extractor read the keys it knew and
ignored the rest, so a misspelled `quer_timeout` -- or a real `UniConfig` field
with no Python binding, such as `ssi_enabled` -- was accepted and dropped. In
both cases the caller asked for a setting, got no error, and got the default.
"""

from __future__ import annotations

import pytest

import uni_db


def _builder(tmp_path, name="db"):
    return uni_db.UniBuilder.open(str(tmp_path / name))


# --- rejecting keys we cannot honour ---------------------------------------


@pytest.mark.parametrize(
    "key, value",
    [
        ("quer_timeout", 5.0),  # misspelling of a real key
        ("totally_not_a_real_key", 123),  # nothing like a real key
        ("ssi_enabled", False),  # a real UniConfig field with no binding
        ("commit_timeout", 5.0),  # ditto
    ],
)
def test_unknown_config_key_is_refused(tmp_path, key, value):
    with pytest.raises(ValueError) as excinfo:
        _builder(tmp_path).config({key: value})
    message = str(excinfo.value)
    assert key in message, f"the error must name the offending key: {message}"
    # Naming the accepted set is what turns a refusal into a fix.
    assert "query_timeout" in message, (
        f"the error must list the accepted keys: {message}"
    )


def test_a_valid_key_alongside_an_invalid_one_still_refuses(tmp_path):
    """Partial application would be the worst outcome: some settings taken,
    others dropped, and no error."""
    with pytest.raises(ValueError):
        _builder(tmp_path).config({"query_timeout": 5.0, "nonsense": 1})


def test_every_accepted_key_is_accepted(tmp_path):
    """The complement of the test above, and the reason it is not vacuous:
    a validator that refused everything would satisfy the refusal tests."""
    from datetime import timedelta

    accepted = {
        "query_timeout": 5.0,
        "max_query_memory": 256 * 1024 * 1024,
        "parallelism": 2,
        "cache_size": 64 * 1024 * 1024,
        "max_transaction_memory": 64 * 1024 * 1024,
        "batch_size": 2048,
        "wal_enabled": True,
        "strict_schema": False,
        "max_forks": 4,
        "fork_default_ttl": timedelta(hours=1),
        "fork_sweeper_interval": timedelta(seconds=30),
        "disable_fork_sweeper": True,
        "vector_knn_default_k": 500,
    }
    # One at a time, so a failure names the key rather than the whole dict.
    for key, value in accepted.items():
        _builder(tmp_path, f"db_{key}").config({key: value})
    # And all together.
    _builder(tmp_path, "db_all").config(accepted).build()


# --- merging ----------------------------------------------------------------


def _rejects_undeclared_label(db) -> bool:
    """Whether `strict_schema` is in force on a built database.

    `strict_schema` is the one setting reachable both as a builder method and as
    a config key whose effect is *observable* from Python, which is what makes
    the merge tests below discriminating: it defaults to `false`, so if a
    `config()` call discarded it the write below succeeds.
    """
    session = db.session()
    try:
        with session.tx() as tx:
            tx.execute("CREATE (:NeverDeclared {x: 1})")
    except Exception:
        return True
    return False


def test_config_does_not_discard_an_earlier_setter(tmp_path):
    """`.strict_schema(True)` before `.config({...})` must survive.

    This is the regression itself: `config()` assigned a freshly-defaulted
    config, so the earlier setter was silently reverted and the write below
    succeeded.
    """
    db = _builder(tmp_path).strict_schema(True).config({"query_timeout": 11.0}).build()
    assert _rejects_undeclared_label(db), (
        "strict_schema(True) was discarded by the later config() call"
    )


def test_a_setter_after_config_also_survives(tmp_path):
    """The mirror order, which the old code happened to get right."""
    db = _builder(tmp_path).config({"query_timeout": 11.0}).strict_schema(True).build()
    assert _rejects_undeclared_label(db)


def test_repeated_config_calls_accumulate(tmp_path):
    """A second `config()` must not erase the first."""
    db = (
        _builder(tmp_path)
        .config({"strict_schema": True})
        .config({"batch_size": 2048})
        .build()
    )
    assert _rejects_undeclared_label(db), "the second config() call discarded the first"


def test_the_last_write_to_one_setting_wins(tmp_path):
    """Merging must not mean the first write to a key sticks."""
    db = (
        _builder(tmp_path)
        .config({"strict_schema": True})
        .config({"strict_schema": False})
        .build()
    )
    assert not _rejects_undeclared_label(db), (
        "the later write to strict_schema did not win"
    )


# --- vector_knn_default_k -----------------------------------------------------


def _knn_db(tmp_path, name, config):
    db = _builder(tmp_path, name).config(config).build()
    db.schema().label("Doc").property("id", "int").vector("emb", 2).done().apply()
    session = db.session()
    tx = session.tx()
    for i in range(10):
        tx.execute(f"CREATE (:Doc {{id: {i}, emb: [1.0, {i / 10}]}})")
    tx.commit()
    return db, session


def test_vector_knn_default_k_sizes_an_unlimited_search(tmp_path):
    """The key is honoured, not merely accepted: an `~=` search no LIMIT sizes
    returns `vector_knn_default_k` rows and warns that it stopped there."""
    db, session = _knn_db(tmp_path, "k3", {"vector_knn_default_k": 3})
    assert db.config()["vector_knn_default_k"] == 3
    result = session.query("MATCH (d:Doc) WHERE d.emb ~= [1.0, 0.0] RETURN d.id")
    assert sorted(row["d.id"] for row in result) == [0, 1, 2]
    assert any("vector_knn_default_k" in w.message for w in result.warnings), (
        result.warnings
    )
    # A LIMIT the search feeds directly sizes it instead, with no warning.
    result = session.query(
        "MATCH (d:Doc) WHERE d.emb ~= [1.0, 0.0] RETURN d.id LIMIT 5"
    )
    assert [row["d.id"] for row in result] == [0, 1, 2, 3, 4]
    assert not any("vector_knn_default_k" in w.message for w in result.warnings)


def test_vector_knn_default_k_larger_than_the_label_does_not_warn(tmp_path):
    _db, session = _knn_db(tmp_path, "k50", {"vector_knn_default_k": 50})
    result = session.query("MATCH (d:Doc) WHERE d.emb ~= [1.0, 0.0] RETURN d.id")
    assert len(result) == 10
    assert not any("vector_knn_default_k" in w.message for w in result.warnings)
