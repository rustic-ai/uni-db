# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Where a store lives: remote-only configuration, hybrid mode, layout.

The S3 cases point at a closed local port, so they run without an object-store
server: what matters is that the open goes to the configured endpoint instead of
the instance metadata service (issue #350).
"""

import json
import os
import tempfile

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
