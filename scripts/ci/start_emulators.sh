#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Starts MinIO, Azurite and fake-gcs-server on their default ports and creates
# the `uni-test` bucket (or container) in each, for the `emulator_smoke` tests
# (crates/uni/tests/common/emulator_smoke.rs). Used by the `cloud` jobs in
# .github/workflows/ci.yml and nightly.yml; runs the same way locally.
#
#   scripts/ci/start_emulators.sh            # start all three
#   scripts/ci/start_emulators.sh stop       # remove the containers
set -euo pipefail

MINIO_IMAGE="minio/minio:RELEASE.2025-04-22T22-12-26Z"
AZURITE_IMAGE="mcr.microsoft.com/azure-storage/azurite:3.34.0"
FAKE_GCS_IMAGE="fsouza/fake-gcs-server:1.52.2"
BUCKET="uni-test"
# Azurite's well-known development account and key.
AZURITE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

stop() {
  docker rm -f uni-minio uni-azurite uni-fake-gcs >/dev/null 2>&1 || true
}

if [[ "${1:-}" == "stop" ]]; then
  stop
  exit 0
fi
# A rerun replaces containers left by an earlier one.
stop

wait_for() {
  local name="$1" url="$2"
  # No `timeout(1)`: macOS lacks it.
  for _ in $(seq 60); do
    curl -s -o /dev/null "$url" && return 0
    sleep 2
  done
  echo "$name did not start" >&2
  exit 1
}

docker run -d --name uni-minio -p 127.0.0.1:9000:9000 "$MINIO_IMAGE" server /data >/dev/null
docker run -d --name uni-azurite -p 127.0.0.1:10000:10000 "$AZURITE_IMAGE" \
  azurite-blob --blobHost 0.0.0.0 --loose >/dev/null
docker run -d --name uni-fake-gcs -p 127.0.0.1:4443:4443 "$FAKE_GCS_IMAGE" \
  -scheme http -port 4443 -public-host 127.0.0.1:4443 >/dev/null

wait_for MinIO http://127.0.0.1:9000/minio/health/live
# The MinIO image ships `mc`; run inside the container, where 127.0.0.1 is
# MinIO itself (host networking does not reach it on Docker Desktop).
docker exec uni-minio sh -c \
  "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null && mc mb --ignore-existing local/$BUCKET"

wait_for Azurite http://127.0.0.1:10000/devstoreaccount1
# A throwaway venv: `pip --user` fails on an externally managed Python.
VENV="$(mktemp -d)/venv"
python3 -m venv "$VENV"
"$VENV/bin/pip" install --quiet azure-storage-blob==12.24.0
"$VENV/bin/python" - "$AZURITE_CONN" "$BUCKET" <<'PY'
import sys
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
try:
    BlobServiceClient.from_connection_string(sys.argv[1]).create_container(sys.argv[2])
except ResourceExistsError:
    pass
PY

wait_for fake-gcs-server http://127.0.0.1:4443/storage/v1/b
# 200 creates the bucket, 409 means it exists; anything else fails.
status=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:4443/storage/v1/b \
  -H 'Content-Type: application/json' -d "{\"name\":\"$BUCKET\"}")
if [[ "$status" != "200" && "$status" != "409" ]]; then
  echo "fake-gcs-server: creating bucket '$BUCKET' returned HTTP $status" >&2
  exit 1
fi
rm -rf "$(dirname "$VENV")"

echo "MinIO, Azurite and fake-gcs-server are up with bucket '$BUCKET'."
