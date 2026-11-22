# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Where a store lives: remote-only configuration, hybrid mode, layout.

The S3 cases point at a closed local port, so they run without an object-store
server: what matters is that the open goes to the configured endpoint instead of
the instance metadata service (issue #350).
"""

import gc
import json
import os
import tempfile
import time

import pytest

import uni_db

CLOSED = "http://127.0.0.1:1"
S3_CONFIG = {
    "provider": "s3",
    "bucket": "bucket",
    "region": "us-east-1",
    "endpoint": CLOSED,
    "access_key_id": "test",
    "secret_access_key": "py-secret-value",
}


def _assert_used_closed_endpoint(excinfo):
    message = str(excinfo.value)
    assert "127.0.0.1:1" in message, message
    assert "169.254.169.254" not in message, message


def test_remote_only_open_honors_cloud_config():
    builder = uni_db.UniBuilder.open("s3://bucket/p").cloud_config(S3_CONFIG)
    with pytest.raises(Exception) as excinfo:
        builder.build()
    _assert_used_closed_endpoint(excinfo)


@pytest.mark.asyncio
async def test_async_remote_only_open_honors_cloud_config():
    builder = uni_db.AsyncUniBuilder.open("s3://bucket/p").cloud_config(S3_CONFIG)
    with pytest.raises(Exception) as excinfo:
        await builder.build()
    _assert_used_closed_endpoint(excinfo)


@pytest.mark.parametrize("builder", [uni_db.UniBuilder, uni_db.AsyncUniBuilder])
def test_storage_options_return_the_builder(builder):
    b = builder.open("s3://bucket/p")
    assert b.storage_options({"aws_endpoint": CLOSED}) is not None


def test_remote_only_open_honors_storage_options():
    builder = uni_db.UniBuilder.open("s3://bucket/p").storage_options(
        {
            "aws_endpoint": CLOSED,
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
        }
    )
    with pytest.raises(Exception) as excinfo:
        builder.build()
    _assert_used_closed_endpoint(excinfo)


def test_hybrid_needs_no_cloud_config():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        builder = (
            uni_db.UniBuilder.open(tmpdir)
            .hybrid(tmpdir, "s3://bucket/p")
            .storage_options(
                {
                    "aws_endpoint": CLOSED,
                    "aws_access_key_id": "test",
                    "aws_secret_access_key": "test",
                }
            )
        )
        with pytest.raises(Exception) as excinfo:
            builder.build()
        _assert_used_closed_endpoint(excinfo)


def test_hybrid_local_path_must_be_the_open_path():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        other = os.path.join(tmpdir, "other")
        builder = (
            uni_db.UniBuilder.open(tmpdir)
            .hybrid(other, "s3://bucket/p")
            .cloud_config(S3_CONFIG)
        )
        with pytest.raises(Exception, match="must name the directory"):
            builder.build()


def test_hybrid_local_path_may_be_spelled_differently():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        builder = (
            uni_db.UniBuilder.open(tmpdir)
            .hybrid(os.path.join(tmpdir, "."), "s3://bucket/p")
            .cloud_config(S3_CONFIG)
        )
        with pytest.raises(Exception) as excinfo:
            builder.build()
        # It got as far as the object store, so the paths were accepted.
        _assert_used_closed_endpoint(excinfo)


@pytest.mark.parametrize("builder", [uni_db.UniBuilder, uni_db.AsyncUniBuilder])
def test_builder_repr_hides_secrets(builder):
    b = (
        builder.open("s3://bucket/p")
        .cloud_config(S3_CONFIG)
        .storage_options({"aws_secret_access_key": "opt-secret-value"})
    )
    assert "py-secret-value" not in repr(b)
    assert "opt-secret-value" not in repr(b)


def test_a_new_local_store_records_the_standard_layout():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        path = os.path.join(tmpdir, "db")
        db = uni_db.UniBuilder.open(path).build()
        db.shutdown()
        with open(os.path.join(path, "catalog", "layout.json")) as f:
            assert json.load(f)["layout"] == "standard"


def test_unsupported_scheme_is_refused():
    with pytest.raises(Exception, match="Unsupported"):
        uni_db.UniBuilder.open("ftp://host/x").build()


def test_a_second_writable_open_is_refused():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        path = os.path.join(tmpdir, "db")
        first = uni_db.UniBuilder.open(path).build()
        try:
            with pytest.raises(Exception, match="open for writing elsewhere"):
                uni_db.UniBuilder.open(path).build()
            reader = uni_db.UniBuilder.open(path).read_only().build()
            reader.shutdown()
        finally:
            first.shutdown()
        # Free once the first handle is shut down.
        uni_db.UniBuilder.open(path).build().shutdown()


@pytest.mark.asyncio
async def test_async_second_writable_open_is_refused():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        path = os.path.join(tmpdir, "db")
        first = await uni_db.AsyncUniBuilder.open(path).build()
        try:
            with pytest.raises(Exception, match="open for writing elsewhere"):
                await uni_db.AsyncUniBuilder.open(path).build()
        finally:
            await first.shutdown()


def test_store_locked_is_a_typed_exception_and_grace_is_configurable():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        path = os.path.join(tmpdir, "db")
        first = uni_db.UniBuilder.open(path).build()
        try:
            started = time.monotonic()
            with pytest.raises(uni_db.UniStoreLockedError) as excinfo:
                uni_db.UniBuilder.open(path).config(
                    {"writer_lease": {"acquire_grace": 0.2}}
                ).build()
            assert time.monotonic() - started < 3
            assert str(os.getpid()) in excinfo.value.holder
        finally:
            first.shutdown()


def test_unenforced_takes_no_lock():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        path = os.path.join(tmpdir, "db")
        lease = uni_db.WriteLease.UNENFORCED()
        assert repr(lease) == "WriteLease.UNENFORCED"
        a = uni_db.UniBuilder.open(path).write_lease(lease).build()
        b = uni_db.UniBuilder.open(path).write_lease(lease).build()
        b.shutdown()
        a.shutdown()


@pytest.mark.asyncio
async def test_async_read_only_coexists_with_the_writer():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        path = os.path.join(tmpdir, "db")
        w = await uni_db.AsyncUniBuilder.open(path).build()
        r = await uni_db.AsyncUniBuilder.open(path).read_only().build()
        await r.shutdown()
        await w.shutdown()


def _hybrid(builder_cls, root, host):
    bucket = os.path.join(root, "bucket")
    return (
        builder_cls.open(os.path.join(root, host))
        .hybrid(os.path.join(root, host), "file://" + os.path.join(bucket, "g"))
        .storage_options({"uni_file_bucket_root": bucket})
        .config({"auto_flush_interval": None, "auto_flush_threshold": 10**9})
    )


def _count(db):
    return db.session().query("MATCH (p:Person) RETURN count(p) AS c")[0]["c"]


def test_a_stale_hybrid_wal_is_refused_then_set_aside():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        one = _hybrid(uni_db.UniBuilder, root, "one").build()
        one.schema().label("Person").property("name", "string").apply()
        tx = one.session().tx()
        tx.execute("CREATE (:Person {name: 'Alice'})")
        tx.commit()
        one.flush()
        tx = one.session().tx()
        tx.execute("CREATE (:Person {name: 'Lost'})")
        tx.commit()
        del tx, one  # a crash: nothing flushed
        gc.collect()

        two = _hybrid(uni_db.UniBuilder, root, "two").build()
        assert _count(two) == 1
        two.shutdown()

        with pytest.raises(uni_db.UniStaleWalError) as excinfo:
            _hybrid(uni_db.UniBuilder, root, "one").build()
        assert excinfo.value.segments == 1
        assert os.path.join(root, "one") in excinfo.value.wal
        assert excinfo.value.store.endswith(os.path.join("bucket", "g"))

        one = _hybrid(uni_db.UniBuilder, root, "one").set_aside_stale_wal().build()
        assert _count(one) == 1
        one.shutdown()
        assert os.listdir(os.path.join(root, "one", "wal_set_aside"))


@pytest.mark.asyncio
async def test_async_builder_refuses_then_sets_a_stale_wal_aside():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
        one = await _hybrid(uni_db.AsyncUniBuilder, root, "one").build()
        await one.schema().label("Person").property("name", "string").apply()
        session = one.session()
        tx = await session.tx()
        await tx.execute("CREATE (:Person {name: 'Lost'})")
        await tx.commit()
        del tx, session, one  # dropped without shutdown: nothing flushed
        gc.collect()

        two = await _hybrid(uni_db.AsyncUniBuilder, root, "two").build()
        await two.shutdown()

        with pytest.raises(uni_db.UniStaleWalError):
            await _hybrid(uni_db.AsyncUniBuilder, root, "one").build()
        one = await (
            _hybrid(uni_db.AsyncUniBuilder, root, "one").set_aside_stale_wal().build()
        )
        await one.shutdown()
        assert os.listdir(os.path.join(root, "one", "wal_set_aside"))
