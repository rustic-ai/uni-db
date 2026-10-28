#!/usr/bin/env python3
"""Exercise interrupted downloads and verify-only-complete cache publication."""

import hashlib
import io
import lzma
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import patch

import prepare_ort as ort


def archive(name=ort.LIBRARY):
    import tarfile

    raw = io.BytesIO()
    content = b"!<arch>\nfixture"
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as members:
        info = tarfile.TarInfo(name)
        info.size = len(content)
        members.addfile(info, io.BytesIO(content))
    return lzma.compress(
        raw.getvalue(),
        format=lzma.FORMAT_RAW,
        filters=[{"id": lzma.FILTER_LZMA2, "dict_size": 1 << 26}],
    )


class PrepareOrt(unittest.TestCase):
    def test_lock_version_is_pinned(self):
        ort.validate_lock(ort.ROOT / "Cargo.lock")
        with tempfile.TemporaryDirectory() as root:
            lock = Path(root) / "Cargo.lock"
            lock.write_text('[[package]]\nname="ort-sys"\nversion="new-version"\n')
            with self.assertRaisesRegex(ValueError, "lock changed"):
                ort.validate_lock(lock)

    def test_verified_cache_reuse_and_corruption_repair(self):
        data = archive()
        digest = hashlib.sha256(data).hexdigest()
        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(ort, "download") as fetch,
        ):
            fetch.side_effect = lambda url, output: output.write_bytes(data)
            destination = ort.prepare(Path(root), digest=digest)
            self.assertTrue(ort.cache_valid(destination, digest))
            self.assertEqual(1, fetch.call_count)
            ort.prepare(Path(root), digest=digest)
            self.assertEqual(1, fetch.call_count)
            (destination / ort.LIBRARY).write_bytes(b"truncated")
            ort.prepare(Path(root), digest=digest)
            self.assertEqual(2, fetch.call_count)
            self.assertTrue(ort.cache_valid(destination, digest))
            (destination / "uni-cache.json").write_text("[]")
            ort.prepare(Path(root), digest=digest)
            self.assertEqual(3, fetch.call_count)
            self.assertTrue(ort.cache_valid(destination, digest))

    def test_bad_checksum_never_publishes_cache(self):
        data = archive()
        digest = "0" * 64
        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(ort, "download") as fetch,
        ):
            fetch.side_effect = lambda url, output: output.write_bytes(data)
            with self.assertRaisesRegex(ValueError, "SHA256"):
                ort.prepare(Path(root), digest=digest)
            self.assertFalse((Path(root) / "dfbin" / ort.TARGET / digest).exists())
            self.assertFalse(list(Path(root).rglob("prepare-*")))

    def test_unexpected_archive_path_is_rejected(self):
        data = archive("../escape")
        digest = hashlib.sha256(data).hexdigest()
        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(ort, "download") as fetch,
        ):
            fetch.side_effect = lambda url, output: output.write_bytes(data)
            with self.assertRaisesRegex(ValueError, "Unexpected"):
                ort.prepare(Path(root), digest=digest)
            self.assertFalse((Path(root) / "dfbin" / ort.TARGET / digest).exists())
            self.assertFalse(list(Path(root).rglob("escape")))

    def test_interrupted_http_download_resumes(self):
        data = b"download fixture" * 100
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                value = self.headers.get("Range")
                requests.append(value)
                start = (
                    int(value.removeprefix("bytes=").removesuffix("-")) if value else 0
                )
                self.send_response(206 if start else 200)
                self.send_header("Content-Length", str(len(data) - start))
                if start:
                    self.send_header(
                        "Content-Range", f"bytes {start}-{len(data) - 1}/{len(data)}"
                    )
                self.end_headers()
                self.wfile.write(data[start:] if start else data[:100])
                self.wfile.flush()
                self.close_connection = True

        with HTTPServer(("127.0.0.1", 0), Handler) as server:
            worker = threading.Thread(target=server.serve_forever)
            worker.start()
            try:
                with tempfile.TemporaryDirectory() as root:
                    output = Path(root) / "download"
                    ort.download(
                        f"http://127.0.0.1:{server.server_port}/runtime",
                        output,
                        attempts=2,
                        retry_delay=0,
                    )
                    self.assertEqual(data, output.read_bytes())
                    self.assertEqual([None, "bytes=100-"], requests)
            finally:
                server.shutdown()
                worker.join()

    def test_download_exhaustion_fails(self):
        with (
            tempfile.TemporaryDirectory() as root,
            patch.object(ort.subprocess, "run") as run,
        ):
            run.return_value.returncode = 28
            with self.assertRaisesRegex(RuntimeError, "after 2 attempts"):
                ort.download(
                    "https://example.invalid",
                    Path(root) / "download",
                    attempts=2,
                    retry_delay=0,
                )
            self.assertEqual(2, run.call_count)


if __name__ == "__main__":
    unittest.main()
