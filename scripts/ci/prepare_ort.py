#!/usr/bin/env python3
"""Prepare the pinned Linux x64 CPU ONNX Runtime in ort-sys's native cache.

ort-sys rc.12 streams a single download through extraction, with a fixed 30m
network timeout and no retry. Separate resumable acquisition from extraction,
verify its upstream SHA256, and publish only a complete cache entry. No link
mode, feature or runtime-version changes are made.
"""

import argparse
import hashlib
import json
import lzma
import platform
import shutil
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[2]
ORT_SYS_VERSION = "2.0.0-rc.12"
TARGET = "x86_64-unknown-linux-gnu"
# From ort-sys rc.12 build/download/dist.txt: feature set `none`.
URL = f"https://cdn.pyke.io/0/pyke:ort-rs/ms@1.24.2/{TARGET}.tar.lzma2"
SHA256 = "acc1cba79c337594ead1d88ca72516147aa60054c84217b53399a31caa5ba671"
LIBRARY = "libonnxruntime.a"


def sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def validate_lock(lock: Path) -> None:
    versions = {
        p["version"]
        for p in tomllib.loads(lock.read_text())["package"]
        if p["name"] == "ort-sys"
    }
    if versions != {ORT_SYS_VERSION}:
        raise ValueError(
            f"ort-sys lock changed to {versions}; update the pinned CPU archive/checksum"
        )


def download(url: str, output: Path, attempts: int = 4, retry_delay: float = 2) -> None:
    for attempt in range(attempts):
        result = subprocess.run(
            [
                "curl",
                "--fail",
                "--location",
                "--show-error",
                "--silent",
                "--connect-timeout",
                "30",
                "--max-time",
                "300",
                "--speed-time",
                "60",
                "--speed-limit",
                "1024",
                "--continue-at",
                "-",
                "--output",
                str(output),
                url,
            ],
            check=False,
        )
        if result.returncode == 0:
            return
        if result.returncode in (33, 36):  # Server cannot resume: retry from zero.
            output.unlink(missing_ok=True)
        if attempt + 1 < attempts:
            print(
                f"ORT download attempt {attempt + 1} failed ({result.returncode}); retrying",
                flush=True,
            )
            time.sleep(retry_delay)
    raise RuntimeError(f"ORT archive download failed after {attempts} attempts")


def cache_valid(destination: Path, digest: str) -> bool:
    try:
        meta = json.loads((destination / "uni-cache.json").read_text())
        return meta["archive_sha256"] == digest and meta["library_sha256"] == sha256(
            destination / LIBRARY
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


def prepare(cache_root: Path, url: str = URL, digest: str = SHA256) -> Path:
    destination = cache_root / "dfbin" / TARGET / digest
    if cache_valid(destination, digest):
        print(f"Verified cached ONNX Runtime: {destination}", flush=True)
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="prepare-", dir=destination.parent
    ) as temporary:
        staging = Path(temporary)
        archive = staging / "runtime.tar.lzma2"
        download(url, archive)
        if sha256(archive) != digest:
            raise ValueError(
                "ONNX Runtime archive SHA256 does not match ort-sys's pinned checksum"
            )
        extracted = staging / "extracted"
        extracted.mkdir()
        with (
            lzma.LZMAFile(
                archive,
                format=lzma.FORMAT_RAW,
                filters=[{"id": lzma.FILTER_LZMA2, "dict_size": 1 << 26}],
            ) as stream,
            tarfile.open(fileobj=stream, mode="r|") as members,
        ):
            for member in members:
                if member.name != LIBRARY or not member.isfile():
                    raise ValueError(
                        f"Unexpected CPU runtime archive entry: {member.name}"
                    )
                with (
                    members.extractfile(member) as source,
                    (extracted / LIBRARY).open("xb") as output,
                ):
                    shutil.copyfileobj(source, output)
        library = extracted / LIBRARY
        metadata = {"archive_sha256": digest, "library_sha256": sha256(library)}
        (extracted / "uni-cache.json").write_text(json.dumps(metadata) + "\n")
        # This directory is dedicated to the content hash; partial entries are
        # never published and corrupt restored cache entries are replaced.
        if destination.exists():
            shutil.rmtree(destination)
        extracted.rename(destination)
    print(f"Prepared verified ONNX Runtime: {destination}", flush=True)
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--metadata", action="store_true")
    args = parser.parse_args()
    validate_lock(ROOT / "Cargo.lock")
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("This CI preparation step supports Linux x64 CPU builds only")
    cache_root = args.cache_dir.resolve()
    if args.metadata:
        print(f"cache-key=ort-cpu-v1-{ORT_SYS_VERSION}-{TARGET}-{SHA256}")
        print(f"cache-path={cache_root}")
    else:
        prepare(cache_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
