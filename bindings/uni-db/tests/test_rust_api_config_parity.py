"""Rust configuration parity and the public adapters added by the API audit."""

import re
from datetime import timedelta
from pathlib import Path

import pytest

import uni_db


def config_values(tmp_path):
    return {
        "cache_size": 2**20,
        "parallelism": 2,
        "batch_size": 128,
        "execution_batch_size": 32,
        "max_frontier_size": 100_000,
        "auto_flush_threshold": 10_000,
        "commit_channel_capacity": 32,
        "auto_flush_interval": None,
        "auto_flush_min_mutations": 10,
        "wal_enabled": True,
        "compaction": {"enabled": False, "max_l1_runs": 16},
        "throttle": {"soft_limit": 128, "hard_limit": 256},
        "file_sandbox": {"enabled": True, "allowed_paths": [str(tmp_path)]},
        "query_timeout": 5.0,
        "commit_timeout": timedelta(seconds=7),
        "max_query_memory": 2**26,
        "regex_backtrack_limit": 20_000,
        "max_transaction_memory": 2**26,
        "max_compaction_rows": 1_000,
        "max_recursive_cte_iterations": 64,
        "vector_knn_default_k": 50,
        "lance_metadata_cache_bytes": None,
        "lance_index_cache_bytes": 2**20,
        "object_store": {"read_timeout": 3.0, "max_retries": 2},
        "index_rebuild": {"max_index_age": None, "auto_rebuild_enabled": False},
        "strict_schema": False,
        "defer_embeddings": True,
        "fork_fragment_warn_threshold": 8,
        "tx_id_reservoir_batch": 32,
        "async_flush_enabled": False,
        "max_pending_flushes": 4,
        "flush_stream_timeout": 6.0,
        "drop_fork_drain_timeout": 8.0,
        "flush_drain_timeout": 9.0,
        "max_forks": 4,
        "fork_default_ttl": None,
        "fork_sweeper_interval": 10.0,
        "disable_fork_sweeper": True,
        "fork_index_build_threshold": 100,
        "fork_index_builder_interval": timedelta(seconds=11),
        "disable_fork_index_builder": True,
        "ssi_enabled": False,
        "ec_flush_interval": 1.5,
        "ec_flush_threshold": 100,
    }


def rust_active_config_fields():
    root = Path(__file__).resolve().parents[3]
    source = (root / "crates/uni-common/src/config.rs").read_text()
    body = re.search(r"pub struct UniConfig\s*\{(.*?)\n\}", source, re.S)[1]
    return set(re.findall(r"^    pub (\w+):", body, re.M)) - {"partial_lance_writes"}


def assert_config(actual, supplied):
    assert set(actual) == rust_active_config_fields()
    for key, expected in supplied.items():
        if isinstance(expected, dict):
            for nested, value in expected.items():
                assert actual[key][nested] == value, (key, nested)
        else:
            if isinstance(expected, timedelta):
                expected = expected.total_seconds()
            assert actual[key] == expected, key


def test_every_active_rust_field_can_be_configured_and_roundtripped(tmp_path):
    supplied = config_values(tmp_path)
    assert set(supplied) == rust_active_config_fields()
    db = uni_db.UniBuilder.temporary().config(supplied).build()
    actual = db.config()
    assert_config(actual, supplied)
    # Readback must itself be acceptable input, including all nested defaults
    # and the fork duration fields that formerly accepted only timedeltas.
    other = uni_db.UniBuilder.temporary().config(actual).build()
    assert other.config() == actual
    other.shutdown()
    db.shutdown()


async def test_async_config_has_the_same_roundtrip(tmp_path):
    supplied = config_values(tmp_path)
    db = await uni_db.AsyncUniBuilder.temporary().config(supplied).build()
    actual = db.config()
    assert_config(actual, supplied)
    other = await uni_db.AsyncUniBuilder.temporary().config(actual).build()
    assert other.config() == actual
    await other.shutdown()
    await db.shutdown()


@pytest.mark.parametrize("builder", [uni_db.UniBuilder, uni_db.AsyncUniBuilder])
def test_rejected_config_keeps_preceding_settings(builder):
    obj = builder.temporary().config({"batch_size": 1234, "strict_schema": True})
    with pytest.raises(ValueError):
        obj.config({"batch_size": 99, "object_store": {"bad_field": 1}})
    # Both constructors keep a working builder after the rejection; inspect
    # its saved settings in the sync/async lifecycle tests below.
    if builder is uni_db.UniBuilder:
        db = obj.build()
        assert db.config()["batch_size"] == 1234
        assert db.config()["strict_schema"] is True
        db.shutdown()


async def test_async_rejected_config_keeps_preceding_settings():
    obj = uni_db.AsyncUniBuilder.temporary().config({"batch_size": 1234})
    with pytest.raises((TypeError, ValueError)):
        obj.config({"query_timeout": -1})
    db = await obj.build()
    assert db.config()["batch_size"] == 1234
    await db.shutdown()


@pytest.mark.parametrize("seconds", [-1.0, float("nan"), float("inf"), 1e300])
@pytest.mark.parametrize(
    "key", ["query_timeout", "commit_timeout", "fork_sweeper_interval"]
)
def test_invalid_config_durations_raise_python_errors(key, seconds):
    with pytest.raises(ValueError):
        uni_db.UniBuilder.temporary().config({key: seconds})


@pytest.mark.parametrize("nested", ["file_sandbox", "object_store", "index_rebuild"])
def test_new_nested_configs_refuse_unknown_keys(nested):
    with pytest.raises(ValueError, match="unknown"):
        uni_db.UniBuilder.temporary().config({nested: {"typo": 1}})


def test_config_aware_compilation_matches_neural_opt_out():
    db = uni_db.Uni.temporary()
    session = db.session()
    source = (
        "CREATE MODEL m AS INPUT (x) FEATURES x OUTPUT PROB p USING xervo('classify/m')"
    )
    session.compile_locy_with_config(source, uni_db.LocyConfig())
    for config in [
        uni_db.LocyConfig(neural_predicates_preview=False),
        {"neural_predicates_preview": False},
    ]:
        with pytest.raises(uni_db.UniQueryError, match="neural_predicates_preview"):
            session.compile_locy_with_config(source, config)
    db.shutdown()


@pytest.mark.parametrize(
    "spec", ["add_mult_prob", "max_min_prob", "bdd_exact", {"top_k_proofs": 4}]
)
def test_locy_semiring_and_cache_controls(spec):
    config = uni_db.LocyConfig(
        semiring=spec, classifier_cache_max=17, neural_predicates_preview=False
    )
    assert config.semiring == spec
    assert config.classifier_cache_max == 17
    assert config.neural_predicates_preview is False
    db = uni_db.Uni.temporary()
    session = db.session()
    program = "CREATE RULE r AS MATCH (n:N) YIELD KEY n QUERY r RETURN count(*) AS c"
    assert session.locy_with(program).with_config(config).run().rows()[0]["c"] == 0
    assert (
        session.locy_with(program)
        .with_config({"semiring": spec, "classifier_cache_max": 17})
        .run()
        .rows()[0]["c"]
        == 0
    )
    db.shutdown()


def test_locy_refuses_unknown_and_invalid_config():
    db = uni_db.Uni.temporary()
    with pytest.raises(ValueError, match="unknown Locy"):
        db.session().locy_with("QUERY x").with_config({"max_iteratons": 1})
    with pytest.raises(ValueError, match="semiring"):
        uni_db.LocyConfig(semiring="typo")
    with pytest.raises(ValueError):
        uni_db.LocyConfig(timeout_secs=float("inf"))
    db.shutdown()


def test_transaction_builders_bulk_params_and_session_retry():
    db = uni_db.Uni.temporary()
    session = db.session()
    session.execute_with_retry("CREATE (:N {v: 1})")
    tx = session.tx()
    tx.execute_with("CREATE (:N {v: $v})").params({"v": 2}).run()
    result = (
        tx.locy_with(
            "CREATE RULE r AS MATCH (n:N) WHERE n.v = $v YIELD KEY n QUERY r RETURN count(*) AS c"
        )
        .params({"v": 2})
        .run()
    )
    assert result.rows()[0]["c"] == 1
    tx.commit()
    assert session.query("MATCH (n:N) RETURN count(n) AS c").rows[0]["c"] == 2
    session.set_replacement_scans(False)
    assert session.replacement_scans_enabled() is False
    assert db.background_task_count() >= 0
    db.shutdown()


async def test_async_session_new_methods_and_params():
    db = await uni_db.AsyncUni.temporary()
    session = db.session()
    await session.execute_with_retry("CREATE (:N {v: 1})")
    await session.set_replacement_scans(False)
    assert await session.replacement_scans_enabled() is False
    await session.compile_locy_with_config(
        "CREATE RULE r AS MATCH (n:N) YIELD KEY n",
        uni_db.LocyConfig(classifier_cache_max=17),
    )
    tx = await session.tx()
    await tx.execute_with("CREATE (:N {v: $v})").params({"v": 2}).run()
    result = (
        await tx.locy_with(
            "CREATE RULE r AS MATCH (n:N) WHERE n.v = $v YIELD KEY n QUERY r RETURN count(*) AS c"
        )
        .params({"v": 2})
        .run()
    )
    assert result.rows()[0]["c"] == 1
    await tx.commit()
    await db.shutdown()


PLUGIN = """
@db.scalar_fn("score", args=["float"], returns="float", determinism="pure")
def score(x):
    return x * 2
"""


def test_database_plugin_is_visible_in_multiple_sessions():
    db = uni_db.Uni.temporary()
    db.load_python_plugin(PLUGIN, "ai.example.audit")
    for _ in range(2):
        assert (
            db.session().query("RETURN ai.example.audit.score(3.0) AS v").rows[0]["v"]
            == 6.0
        )
    db.shutdown()


async def test_async_database_plugin_is_visible_in_multiple_sessions():
    db = await uni_db.AsyncUni.temporary()
    await db.load_python_plugin(PLUGIN, "ai.example.audit")
    for _ in range(2):
        result = await db.session().query("RETURN ai.example.audit.score(3.0) AS v")
        assert result.rows[0]["v"] == 6.0
    await db.shutdown()


def test_fork_index_binding_delegates_to_fork_branch():
    db = (
        uni_db.UniBuilder.temporary()
        .config({"disable_fork_index_builder": True})
        .build()
    )
    primary = db.session()
    primary.execute_with_retry("CREATE (:Doc {key: 1})")
    with pytest.raises(ValueError):
        primary.build_fork_local_index("Doc", "key", "typo")
    with pytest.raises(uni_db.UniInvalidArgumentError):
        primary.build_fork_local_index("Doc", "key", "scalar_btree")
    fork = primary.fork("indexed").build()
    fork.build_fork_local_index("Doc", "key", "vid_uid")
    assert fork.query("MATCH (n:Doc) RETURN n.key AS k").rows[0]["k"] == 1
    db.shutdown()


async def test_async_fork_index_binding():
    db = (
        await uni_db.AsyncUniBuilder.temporary()
        .config({"disable_fork_index_builder": True})
        .build()
    )
    primary = db.session()
    await primary.execute_with_retry("CREATE (:Doc {key: 1})")
    fork = await primary.fork("indexed").build()
    await fork.build_fork_local_index("Doc", "key", "vid_uid")
    result = await fork.query("MATCH (n:Doc) RETURN n.key AS k")
    assert result.rows[0]["k"] == 1
    await db.shutdown()


def test_last_setter_wins_for_cache_and_parallelism():
    db = (
        uni_db.UniBuilder.temporary()
        .cache_size(1234)
        .parallelism(1)
        .config({"cache_size": 5678, "parallelism": 2})
        .build()
    )
    assert db.config()["cache_size"] == 5678
    assert db.config()["parallelism"] == 2
    db.shutdown()
    db = (
        uni_db.UniBuilder.temporary()
        .config({"cache_size": 1234, "parallelism": 1})
        .cache_size(5678)
        .parallelism(2)
        .build()
    )
    assert db.config()["cache_size"] == 5678
    assert db.config()["parallelism"] == 2
    db.shutdown()


async def test_async_last_setter_wins_for_cache_and_parallelism():
    db = await (
        uni_db.AsyncUniBuilder.temporary()
        .cache_size(1234)
        .parallelism(1)
        .config({"cache_size": 5678, "parallelism": 2})
        .build()
    )
    assert db.config()["cache_size"] == 5678
    assert db.config()["parallelism"] == 2
    await db.shutdown()


@pytest.mark.parametrize(
    "config",
    [
        {"semiring": "max_min_prob", "exact_probability": True},
        uni_db.LocyConfig(semiring="max_min_prob", exact_probability=True),
    ],
)
def test_locy_semiring_reaches_native_validation(config):
    db = uni_db.Uni.temporary()
    program = "CREATE RULE r AS MATCH (n:N) YIELD KEY n QUERY r RETURN count(*) AS c"
    with pytest.raises(uni_db.UniError):
        db.session().locy_with(program).with_config(config).run()
    db.shutdown()


@pytest.mark.parametrize(
    "config",
    [
        {
            "provider": "s3",
            "bucket": "b",
            "region": None,
            "endpoint": None,
            "access_key_id": None,
            "secret_access_key": None,
            "session_token": None,
            "virtual_hosted_style": False,
        },
        {
            "provider": "gcs",
            "bucket": "b",
            "service_account_path": None,
            "service_account_key": None,
        },
        {
            "provider": "azure",
            "container": "c",
            "account": "a",
            "access_key": None,
            "sas_token": None,
        },
    ],
)
@pytest.mark.parametrize("builder", [uni_db.UniBuilder, uni_db.AsyncUniBuilder])
def test_cloud_config_preserves_optional_fields_and_refuses_typos(config, builder):
    obj = builder.temporary().cloud_config(config)
    with pytest.raises(ValueError, match="cloud config key"):
        obj.cloud_config({**config, "typo": "not-a-credential"})
