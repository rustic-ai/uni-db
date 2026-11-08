"""Tests for the Xervo NLP extraction facade (``extract_facts`` / ``extract_entities``).

No offline NLP provider is compiled into the wheels, so these tests cover the
API shape, argument validation (which runs before any model call and raises
``UniInvalidArgumentError``) and the not-configured error. Extraction itself
is covered by the Rust integration tests against a mock kniv v7 model.
"""

import pytest

import uni_db

CONVERSATION = [
    {"speaker": "user", "text": "I moved to Lisbon last year."},
    {"speaker": "assistant", "text": "Congratulations!"},
]


class TestTypes:
    def test_data_classes_are_exported(self):
        assert uni_db.ExtractedFact.__name__ == "ExtractedFact"
        assert uni_db.ExtractedEntity.__name__ == "ExtractedEntity"

    def test_methods_exist(self, empty_db):
        xervo = empty_db.xervo()
        assert callable(xervo.extract_facts)
        assert callable(xervo.extract_entities)


class TestNotConfigured:
    def test_extract_facts_without_runtime(self, empty_db):
        with pytest.raises(uni_db.UniInternalError, match="not configured"):
            empty_db.xervo().extract_facts("Ada lives in London.", relations=["lives in"])

    def test_extract_entities_without_runtime(self, empty_db):
        with pytest.raises(uni_db.UniInternalError, match="not configured"):
            empty_db.xervo().extract_entities(CONVERSATION)

    def test_accepts_every_input_shape(self, empty_db):
        xervo = empty_db.xervo()
        for value in ("one text", ["a", "b"], CONVERSATION):
            with pytest.raises(uni_db.UniInternalError):
                xervo.extract_facts(
                    value,
                    relations=[{"name": "lives in", "description": "where the head resides"}],
                    entity_types=["location"],
                    top_per_pair=2,
                    rel_offsets={"lives in": -0.5},
                )

    def test_graph_schema_is_accepted(self, empty_db):
        with pytest.raises(uni_db.UniInternalError):
            empty_db.xervo().extract_facts("text", schema="graph")
        with pytest.raises(uni_db.UniInternalError):
            empty_db.xervo().extract_facts("text", schema={"edge_types": ["LIVES_IN"]})


class TestValidation:
    def test_facts_need_relations(self, empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="give 'relations'"):
            empty_db.xervo().extract_facts("text")

    def test_schema_excludes_relations(self, empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="cannot be combined"):
            empty_db.xervo().extract_facts("text", schema="graph", relations=["r"])

    def test_bad_input_shape(self, empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="input must be"):
            empty_db.xervo().extract_facts(42, relations=["r"])

    def test_bad_turn_key(self, empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="unknown key 'who'"):
            empty_db.xervo().extract_entities([{"who": "user", "text": "hi"}])

    def test_entities_take_no_relation_options(self, empty_db):
        with pytest.raises(TypeError, match="relations"):
            empty_db.xervo().extract_entities("text", relations=["r"])

    def test_entities_reject_an_edge_type_subset(self, empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="only to uni.nlp.extract"):
            empty_db.xervo().extract_entities("text", schema={"edge_types": ["LIVES_IN"]})

    def test_duplicate_relation(self, empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="listed twice"):
            empty_db.xervo().extract_facts("text", relations=["r", "r"])

    def test_bad_schema(self, empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="option 'schema'"):
            empty_db.xervo().extract_facts("text", schema="everything")

    @pytest.mark.parametrize("value", [0, -1, 1.5, "two"])
    def test_bad_top_per_pair(self, empty_db, value):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="top_per_pair"):
            empty_db.xervo().extract_facts("text", relations=["r"], top_per_pair=value)

    def test_alias_none_is_the_default(self, empty_db):
        with pytest.raises(uni_db.UniInternalError, match="not configured"):
            empty_db.xervo().extract_entities("text", alias=None)


class TestAsync:
    async def test_extract_facts_without_runtime(self, async_empty_db):
        with pytest.raises(uni_db.UniInternalError, match="not configured"):
            await async_empty_db.xervo().extract_facts(CONVERSATION, relations=["lives in"])

    async def test_extract_entities_validation(self, async_empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="only to uni.nlp.extract"):
            await async_empty_db.xervo().extract_entities("text", schema={"edge_types": ["E"]})

    async def test_extract_facts_bad_top_per_pair(self, async_empty_db):
        with pytest.raises(uni_db.UniInvalidArgumentError, match="top_per_pair"):
            await async_empty_db.xervo().extract_facts("text", relations=["r"], top_per_pair=-1)
