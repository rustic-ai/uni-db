"""Negative controls for the Rust-to-Python API drift guard."""

import contextlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path

SOURCE = Path(__file__).with_name("check_rust_python_parity.py")


class ParityGuardTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("parity", SOURCE)
        self.guard = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.guard)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)

    def changed(self, original, before, after):
        source = original.read_text()
        self.assertIn(before, source)
        target = Path(self.temporary.name) / original.name
        target.write_text(source.replace(before, after, 1))
        return target

    def rejects(self, expected):
        output = io.StringIO()
        with (
            contextlib.redirect_stderr(output),
            contextlib.redirect_stdout(output),
            self.assertRaises(SystemExit) as error,
        ):
            self.guard.main()
        self.assertEqual(error.exception.code, 1)
        self.assertIn(expected, output.getvalue())

    def test_current_api_passes(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.guard.main(), 0)

    def test_new_session_method_is_not_silently_ignored(self):
        source = Path(self.temporary.name) / "session.rs"
        source.write_text("impl Session {\n    pub fn audit_new_method(&self) {}\n}\n")
        self.guard.RUST_BUILDER_SRC.append(source)
        self.rejects("Session.audit_new_method")

    def test_new_config_field_is_not_silently_ignored(self):
        self.guard.CONFIG_RS = self.changed(
            self.guard.CONFIG_RS,
            "pub struct UniConfig {",
            "pub struct UniConfig {\n    pub audit_new_field: usize,",
        )
        self.rejects("audit_new_field")

    def test_missing_readback_key_is_not_silently_ignored(self):
        self.guard.CONVERT_RS = self.changed(
            self.guard.CONVERT_RS,
            '    dict.set_item("cache_size", config.cache_size)?;',
            "",
        )
        self.rejects("config readback and accepted keys differ")

    def test_new_nested_index_setting_is_not_silently_ignored(self):
        self.guard.CONFIG_RS = self.changed(
            self.guard.CONFIG_RS,
            "pub struct IndexRebuildConfig {",
            "pub struct IndexRebuildConfig {\n    pub audit_new_setting: usize,",
        )
        self.rejects("audit_new_setting")

    def test_new_cloud_field_is_not_silently_ignored(self):
        self.guard.CONFIG_RS = self.changed(
            self.guard.CONFIG_RS,
            "    Azure {",
            "    Azure {\n        audit_new_credential: Option<String>,",
        )
        self.rejects("cloud config Azure")

    def test_new_cloud_provider_requires_review(self):
        self.guard.CONFIG_RS = self.changed(
            self.guard.CONFIG_RS,
            "pub enum CloudStorageConfig {",
            "pub enum CloudStorageConfig {\n    AuditProvider,",
        )
        self.rejects("cloud variants require Python review")


if __name__ == "__main__":
    unittest.main()
