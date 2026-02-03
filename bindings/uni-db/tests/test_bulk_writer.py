# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for BulkWriter API."""

import tempfile

import pytest

import uni_db


class TestBulkWriter:
    """Tests for bulk data loading functionality."""

    @pytest.fixture
    def db(self):
        """Create a database with schema for bulk loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Person")
            db.add_property("Person", "name", "string", False)
            db.add_property("Person", "age", "int", False)
            db.create_label("Company")
            db.add_property("Company", "name", "string", False)
            db.create_edge_type("WORKS_AT", ["Person"], ["Company"])
            yield db

    def test_bulk_writer_builder(self, db):
        """Test creating a bulk writer with builder."""
        writer = db.bulk_writer().batch_size(1000).build()
        assert writer is not None

    def test_bulk_insert_vertices(self, db):
        """Test bulk inserting vertices."""
        writer = db.bulk_writer().build()

        # Insert multiple vertices
        vids = writer.insert_vertices(
            "Person",
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35},
            ],
        )

        assert len(vids) == 3
        writer.commit()

        # Verify vertices were inserted
        results = db.query("MATCH (n:Person) RETURN n.name AS name ORDER BY n.name")
        assert len(results) == 3
        assert results[0]["name"] == "Alice"

    def test_bulk_insert_edges(self, db):
        """Test bulk inserting edges."""
        writer = db.bulk_writer().build()

        # First insert vertices
        person_vids = writer.insert_vertices(
            "Person",
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
        )
        company_vids = writer.insert_vertices("Company", [{"name": "TechCorp"}])

        # Insert edges - doesn't return edge IDs currently
        writer.insert_edges(
            "WORKS_AT",
            [
                (person_vids[0], company_vids[0], {}),
                (person_vids[1], company_vids[0], {}),
            ],
        )

        writer.commit()

        # Verify edges
        results = db.query(
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name AS p_name, c.name AS c_name"
        )
        assert len(results) == 2

    def test_bulk_writer_abort(self, db):
        """Test aborting a bulk write operation prevents further operations."""
        writer = db.bulk_writer().build()

        writer.insert_vertices(
            "Person",
            [{"name": "BeforeAbort", "age": 99}],
        )

        writer.abort()

        # After abort, further operations should fail
        with pytest.raises(RuntimeError):
            writer.insert_vertices("Person", [{"name": "AfterAbort", "age": 100}])

    def test_bulk_writer_deferred_indexes(self, db):
        """Test bulk writer with deferred index building."""
        writer = (
            db.bulk_writer()
            .defer_scalar_indexes(True)
            .defer_vector_indexes(True)
            .build()
        )

        writer.insert_vertices(
            "Person",
            [{"name": f"Person{i}", "age": i} for i in range(100)],
        )

        stats = writer.commit()
        assert stats.vertices_inserted == 100


class TestBulkStats:
    """Tests for BulkStats data class."""

    def test_bulk_stats_attributes(self):
        """Test BulkStats has expected attributes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.DatabaseBuilder.open(tmpdir).build()
            db.create_label("Test")

            writer = db.bulk_writer().build()
            writer.insert_vertices("Test", [{"value": 1}])
            stats = writer.commit()

            assert hasattr(stats, "vertices_inserted")
            assert hasattr(stats, "edges_inserted")
            assert hasattr(stats, "duration_secs")
            assert stats.vertices_inserted == 1
