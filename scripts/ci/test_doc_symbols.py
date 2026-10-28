#!/usr/bin/env python3
"""Negative controls for documentation checks that global symbol names miss."""

import unittest

import check_doc_symbols as guard


class DocumentationContracts(unittest.TestCase):
    def test_method_on_wrong_class(self):
        self.assertIn("set", guard.known_methods())
        self.assertTrue(
            guard.declared_method_errors(
                "class Session:\n    def set(self, key, value): ..."
            )
        )

    def test_factory_chain_types(self):
        self.assertTrue(
            guard.typed_call_errors(
                'from uni_db import Uni\ndb = Uni.open("db").config({}).build()'
            )
        )
        self.assertEqual(
            [],
            guard.typed_call_errors(
                'from uni_db import UniBuilder\ndb = UniBuilder.open("db").config({}).build()'
            ),
        )

    def test_locy_dictionary_keys_match_binding_inventory(self):
        setup = "from uni_db import Uni\ndb = Uni.temporary()\nsession = db.session()\n"
        self.assertTrue(
            guard.typed_call_errors(
                setup + 'session.locy_with("program").with_config({"timeout_secs": 60})'
            )
        )
        self.assertEqual(
            [],
            guard.typed_call_errors(
                setup + 'session.locy_with("program").with_config({"timeout": 60})'
            ),
        )
        self.assertEqual(
            [],
            guard.typed_call_errors(
                "from uni_db import LocyConfig\nconfig = LocyConfig(timeout_secs=60)"
            ),
        )
        self.assertTrue(
            guard.typed_call_errors(
                setup
                + 'session.locy_with("program").with_config({"invented_key": True})'
            )
        )

    def test_named_locy_config_across_notebook_cells(self):
        env, dicts = {}, {}
        guard.typed_call_errors(
            'from uni_db import Uni\ndb = Uni.temporary()\nsession = db.session()\nconfig = {"timeout_secs": 60}',
            env,
            dicts,
        )
        self.assertTrue(
            guard.typed_call_errors(
                'session.locy_with("program").with_config(config)', env, dicts
            )
        )
        guard.typed_call_errors('config = {"timeout": 60}', env, dicts)
        self.assertEqual(
            [],
            guard.typed_call_errors(
                'session.locy_with("program").with_config(config)', env, dicts
            ),
        )

    def test_qualified_module_locy_config(self):
        code = 'import uni_db as uni\ndb = uni.Uni.temporary()\nsession = db.session()\nsession.locy_with("program").with_config({"timeout_secs": 60})'
        self.assertTrue(guard.typed_call_errors(code))
        self.assertEqual(
            [], guard.typed_call_errors(code.replace('"timeout_secs"', '"timeout"'))
        )

    def test_semiconductor_notebook_rejects_original_bad_key(self):
        import json

        path = (
            guard.ROOT
            / "website/docs/examples/python/locy_semiconductor_yield_excursion.ipynb"
        )
        notebook = json.loads(path.read_text())
        env, dicts, errors = {}, {}, []
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code":
                original = "".join(cell["source"]).replace(
                    "'timeout':", "'timeout_secs':"
                )
                errors.extend(guard.typed_call_errors(original, env, dicts))
        self.assertTrue(any("timeout_secs" in error for error in errors))

    def test_other_package_is_not_inferred_as_uni(self):
        self.assertEqual(
            [],
            guard.typed_call_errors(
                "from uni_pydantic import Session\nsession = Session(db)\nsession.add(model)"
            ),
        )

    def test_config_field_removal_is_detected(self):
        source = (guard.ROOT / "website/docs/reference/configuration.md").read_text()
        self.assertEqual([], guard.config_declaration_errors(source))
        self.assertTrue(
            guard.config_declaration_errors(
                source.replace("    pub flush_stream_timeout: Duration,", "")
            )
        )
        self.assertTrue(
            guard.config_declaration_errors(
                source.replace("pub parallelism: usize,", "pub parallelism: bool,")
            )
        )

    def test_rust_locy_result_and_raw_program(self):
        self.assertTrue(
            guard.rust_locy_errors("for (name, rows) in &result.derived { }")
        )
        self.assertTrue(guard.rust_locy_errors('result.derived().get("rule")'))
        self.assertEqual(
            [], guard.rust_locy_errors('result.derived_rules().await?.get("rule")')
        )
        self.assertTrue(
            guard.rust_locy_errors(
                'let program = r#"CREATE RULE r AS\\nMATCH (n)\\nYIELD KEY n"#;'
            )
        )
        self.assertEqual(
            [],
            guard.rust_locy_errors(
                'let program = r#"CREATE RULE r AS\nMATCH (n)\nYIELD KEY n"#;'
            ),
        )

    def test_black_book_and_notebooks_are_checked(self):
        paths = guard.live_docs()
        self.assertIn(guard.ROOT / "docs/UNI_BLACK_BOOK.md", paths)
        self.assertTrue(any(p.suffix == ".ipynb" for p in paths))
        self.assertFalse(any("uni-db-workspace" in p.parts for p in paths))


if __name__ == "__main__":
    unittest.main()
