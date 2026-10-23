# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for BulkWriter API."""

import pytest

import uni_db


class TestBulkWriter:
    """Tests for bulk data loading functionality."""

    @pytest.fixture
    def db(self):
        """Create a database with schema for bulk loading."""
        db = uni_db.UniBuilder.temporary().build()
        (
            db.schema()
            .label("Person")
            .property("name", "string")
            .property("age", "int")
            .done()
            .label("Company")
            .property("name", "string")
            .done()
            .edge_type("WORKS_AT", ["Person"], ["Company"])
            .done()
            .apply()
        )
        return db

    def test_bulk_writer_builder(self, db):
        """Test creating a bulk writer with builder."""
        session = db.session()
        tx = session.tx()
        writer = tx.bulk_writer().batch_size(1000).build()
        assert writer is not None

    def test_bulk_insert_vertices(self, db):
        """Test bulk inserting vertices."""
        session = db.session()
        tx = session.tx()
        writer = tx.bulk_writer().build()

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
        tx.commit()

        # Verify vertices were inserted
        results = session.query(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name"
        )
        assert len(results) == 3
        assert results[0]["name"] == "Alice"

    def test_bulk_insert_edges(self, db):
        """Test bulk inserting edges."""
        session = db.session()
        tx = session.tx()
        writer = tx.bulk_writer().build()

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
        tx.commit()

        # Verify edges
        results = session.query(
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name AS p_name, c.name AS c_name"
        )
        assert len(results) == 2

    def test_bulk_writer_abort(self, db):
        """Test aborting a bulk write operation prevents further operations."""
        session = db.session()
        tx = session.tx()
        writer = tx.bulk_writer().build()

        writer.insert_vertices(
            "Person",
            [{"name": "BeforeAbort", "age": 99}],
        )

        writer.abort()
        tx.rollback()

        # After abort, further operations should fail
        with pytest.raises(RuntimeError):
            writer.insert_vertices("Person", [{"name": "AfterAbort", "age": 100}])

    def test_bulk_writer_deferred_indexes(self, db):
        """Test bulk writer with deferred index building."""
        session = db.session()
        tx = session.tx()
        writer = (
            tx.bulk_writer()
            .defer_scalar_indexes(True)
            .defer_vector_indexes(True)
            .build()
        )

        writer.insert_vertices(
            "Person",
            [{"name": f"Person{i}", "age": i} for i in range(100)],
        )

        stats = writer.commit()
        tx.commit()
        assert stats.vertices_inserted == 100

    @pytest.mark.parametrize("skip", [True, False])
    def test_bulk_writer_skip_before_triggers(self, db, skip):
        """`skip_before_triggers` chains and, with no before-trigger, loads."""
        tx = db.session().tx()
        writer = tx.bulk_writer().skip_before_triggers(skip).build()
        writer.insert_vertices("Person", [{"name": "Ada", "age": 36}])
        stats = writer.commit()
        tx.commit()
        assert stats.vertices_inserted == 1

    @pytest.mark.parametrize("skip", [True, False])
    def test_appender_builder_skip_before_triggers(self, db, skip):
        """The appender builder accepts `skip_before_triggers` too."""
        tx = db.session().tx()
        builder = tx.appender_builder("Person").skip_before_triggers(skip)
        assert type(builder).__name__ == "TxAppenderBuilder"
        app = builder.build()
        app.append({"name": "Ada", "age": 36})
        stats = app.finish()
        tx.commit()
        assert stats.vertices_inserted == 1


class TestBulkStats:
    """Tests for BulkStats data class."""

    def test_bulk_stats_attributes(self):
        """Test BulkStats has expected attributes."""
        db = uni_db.UniBuilder.temporary().build()
        db.schema().label("Test").apply()

        session = db.session()
        tx = session.tx()
        writer = tx.bulk_writer().build()
        writer.insert_vertices("Test", [{"value": 1}])
        stats = writer.commit()
        tx.commit()

        assert hasattr(stats, "vertices_inserted")
        assert hasattr(stats, "edges_inserted")
        assert hasattr(stats, "duration_secs")
        assert stats.vertices_inserted == 1
