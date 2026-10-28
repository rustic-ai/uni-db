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
