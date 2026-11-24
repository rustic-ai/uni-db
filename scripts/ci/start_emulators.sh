#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Starts Azurite on its default port and creates the `uni-test` container in
# it, and the `uni-test` bucket in the S3 emulator, for the
# `emulator_smoke` tests (crates/uni/tests/common/emulator_smoke.rs). Used by
# the `cloud` jobs in .github/workflows/ci.yml and nightly.yml; runs the same
# way locally.
#
# S3 is LocalStack, which those jobs already run as a service on port 4566;
# locally, start it first (`docker run -d -p 4566:4566 -e SERVICES=s3
# localstack/localstack:4.13.1`), or point UNI_S3_ENDPOINT at another
# S3-compatible service. MinIO used to fill this slot, but its images and
# binaries are no longer published (Docker Hub, quay.io and dl.min.io all
# refuse them).
#
# No GCS emulator: fake-gcs-server rejects the XML-API object PUT that
# object_store sends ("invalid uploadType", every release through 1.56.1), so
# the GCS smoke test runs on demand against a real bucket.
#
#   scripts/ci/start_emulators.sh            # start them
#   scripts/ci/start_emulators.sh stop       # remove the containers
set -euo pipefail

S3_ENDPOINT="${UNI_S3_ENDPOINT:-http://127.0.0.1:4566}"
AZURITE_IMAGE="mcr.microsoft.com/azure-storage/azurite:3.34.0"
BUCKET="uni-test"
# Azurite's well-known development account and key.
AZURITE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

stop() {
  docker rm -f uni-azurite >/dev/null 2>&1 || true
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

docker run -d --name uni-azurite -p 127.0.0.1:10000:10000 "$AZURITE_IMAGE" \
  azurite-blob --blobHost 0.0.0.0 --loose >/dev/null

wait_for "the S3 emulator" "$S3_ENDPOINT"
# A path-style PUT creates the bucket; 409 means it exists. LocalStack takes
# any credentials, so the request carries none.
status=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$S3_ENDPOINT/$BUCKET")
if [[ "$status" != "200" && "$status" != "409" ]]; then
  echo "S3 emulator: creating bucket '$BUCKET' returned HTTP $status" >&2
  exit 1
fi

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

rm -rf "$(dirname "$VENV")"

echo "S3 ($S3_ENDPOINT) and Azurite are up with bucket '$BUCKET'."
