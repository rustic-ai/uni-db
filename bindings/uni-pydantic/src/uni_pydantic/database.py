# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Database builder wrappers for uni-pydantic."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import uni_db


class UniDatabase:
    """
    Thin wrapper around uni-db DatabaseBuilder for ergonomic database creation.

    Example:
        >>> db = UniDatabase.open("./path").cache_size(1024*1024).build()
        >>> db = UniDatabase.temporary().build()
        >>> db = UniDatabase.in_memory().build()
    """

    def __init__(self, builder: uni_db.DatabaseBuilder) -> None:
        self._builder = builder

    @classmethod
    def open(cls, path: str) -> UniDatabase:
        """Open or create a database at the given path."""
        import uni_db

        return cls(uni_db.DatabaseBuilder.open(path))

    @classmethod
    def create(cls, path: str) -> UniDatabase:
        """Create a new database at the given path."""
        import uni_db

        return cls(uni_db.DatabaseBuilder.create(path))

    @classmethod
    def open_existing(cls, path: str) -> UniDatabase:
        """Open an existing database (must already exist)."""
        import uni_db

        return cls(uni_db.DatabaseBuilder.open_existing(path))

    @classmethod
    def temporary(cls) -> UniDatabase:
        """Create an ephemeral in-memory database."""
        import uni_db

        return cls(uni_db.DatabaseBuilder.temporary())

    @classmethod
    def in_memory(cls) -> UniDatabase:
        """Create a persistent in-memory database."""
        import uni_db

        return cls(uni_db.DatabaseBuilder.in_memory())

    def cache_size(self, bytes_: int) -> UniDatabase:
        """Set the cache size in bytes."""
        self._builder = self._builder.cache_size(bytes_)
        return self

    def parallelism(self, n: int) -> UniDatabase:
        """Set the parallelism level."""
        self._builder = self._builder.parallelism(n)
        return self

    def build(self) -> uni_db.Database:
        """Build and return the database instance."""
        return self._builder.build()


class AsyncUniDatabase:
    """
    Thin wrapper around uni-db AsyncDatabaseBuilder for ergonomic async database creation.

    Example:
        >>> db = await AsyncUniDatabase.open("./path").build()
        >>> db = await AsyncUniDatabase.temporary().build()
    """

    def __init__(self, builder: uni_db.AsyncDatabaseBuilder) -> None:
        self._builder = builder

    @classmethod
    def open(cls, path: str) -> AsyncUniDatabase:
        """Open or create a database at the given path."""
        import uni_db

        return cls(uni_db.AsyncDatabaseBuilder.open(path))

    @classmethod
    def temporary(cls) -> AsyncUniDatabase:
        """Create an ephemeral in-memory database."""
        import uni_db

        return cls(uni_db.AsyncDatabaseBuilder.temporary())

    @classmethod
    def in_memory(cls) -> AsyncUniDatabase:
        """Create a persistent in-memory database."""
        import uni_db

        return cls(uni_db.AsyncDatabaseBuilder.in_memory())

    def cache_size(self, bytes_: int) -> AsyncUniDatabase:
        """Set the cache size in bytes."""
        self._builder = self._builder.cache_size(bytes_)
        return self

    def parallelism(self, n: int) -> AsyncUniDatabase:
        """Set the parallelism level."""
        self._builder = self._builder.parallelism(n)
        return self

    async def build(self) -> uni_db.AsyncDatabase:
        """Build and return the async database instance."""
        return await self._builder.build()
