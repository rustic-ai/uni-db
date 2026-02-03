# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Pytest configuration and fixtures for uni-pydantic tests."""

import tempfile

import pytest


@pytest.fixture
def temp_db_path():
    """Create a temporary directory for database storage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def db(temp_db_path):
    """Create a temporary database instance."""
    try:
        import uni_db

        return uni_db.DatabaseBuilder.open(temp_db_path).build()
    except ImportError:
        pytest.skip("uni_db not available")


@pytest.fixture
def session(db):
    """Create a UniSession with a temporary database."""
    from uni_pydantic import UniSession

    with UniSession(db) as s:
        yield s


@pytest.fixture
async def async_db():
    """Create an async temporary database instance."""
    try:
        import uni_db

        db = await uni_db.AsyncDatabase.temporary()
        yield db
    except ImportError:
        pytest.skip("uni_db not available")


@pytest.fixture
async def async_session(async_db):
    """Create an AsyncUniSession with a temporary database."""
    from uni_pydantic import AsyncUniSession

    async with AsyncUniSession(async_db) as s:
        yield s
