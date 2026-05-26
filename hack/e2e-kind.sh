#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER_IMAGE="${SCMIGRATE_RUNNER_IMAGE:-scmigrate-rsync:e2e}"
ETCD_IMAGE="${ETCD_IMAGE:-quay.io/coreos/etcd:v3.5.18}"
E2E_PARALLEL="${E2E_PARALLEL:-4}"

require() {
	local name="$1"
	if ! command -v "$name" >/dev/null 2>&1; then
		echo "missing required command: $name" >&2
		exit 127
	fi
}

require docker
require kind
require kubectl
if [[ ! -x "${GO:-go}" ]] && ! command -v "${GO:-go}" >/dev/null 2>&1; then
	echo "missing required command: ${GO:-go}" >&2
	exit 127
fi

docker build -f "$ROOT/Dockerfile.rsync" -t "$RUNNER_IMAGE" "$ROOT"

TEST_ARGS=(./e2e -count=1 -parallel "$E2E_PARALLEL" -timeout=30m -v)
if [[ -n "${E2E_TEST_REGEX:-}" ]]; then
	TEST_ARGS+=(-run "$E2E_TEST_REGEX")
fi

SCMIGRATE_E2E=1 \
SCMIGRATE_RUNNER_IMAGE="$RUNNER_IMAGE" \
SCMIGRATE_BIN="${SCMIGRATE_BIN:-$ROOT/bin/kubectl-scmigrate}" \
ETCD_IMAGE="$ETCD_IMAGE" \
GOCACHE="${GOCACHE:-/tmp/scmigrate-go-cache}" \
"${GO:-go}" test "${TEST_ARGS[@]}"
