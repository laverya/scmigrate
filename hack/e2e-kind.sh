#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER_IMAGE="${SCMIGRATE_RUNNER_IMAGE:-scmigrate-runner:e2e}"
DEFAULT_APP_IMAGE="scmigrate-e2e-app:e2e"
APP_IMAGE="${SCMIGRATE_E2E_APP_IMAGE:-$DEFAULT_APP_IMAGE}"
ETCD_IMAGE="${ETCD_IMAGE:-quay.io/coreos/etcd:v3.5.18}"
E2E_PARALLEL="${E2E_PARALLEL:-1}"
E2E_TIMEOUT="${E2E_TIMEOUT:-45m}"
SCMIGRATE_BIN_PATH="${SCMIGRATE_BIN:-$ROOT/bin/kubectl-scmigrate}"
RUNNER_PLATFORM="${SCMIGRATE_RUNNER_PLATFORM:-linux/$(${GO:-go} env GOARCH)}"

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

RUNNER_CONTEXT="$(mktemp -d)"
trap 'rm -rf "$RUNNER_CONTEXT"' EXIT
mkdir -p "$RUNNER_CONTEXT/$RUNNER_PLATFORM"
cp "$ROOT/Dockerfile.runner" "$RUNNER_CONTEXT/Dockerfile.runner"
cp "$SCMIGRATE_BIN_PATH" "$RUNNER_CONTEXT/$RUNNER_PLATFORM/kubectl-scmigrate"
docker build --build-arg "TARGETPLATFORM=$RUNNER_PLATFORM" -f "$RUNNER_CONTEXT/Dockerfile.runner" -t "$RUNNER_IMAGE" "$RUNNER_CONTEXT"
if [[ -z "${SCMIGRATE_E2E_APP_IMAGE:-}" ]]; then
	docker build -f "$ROOT/Dockerfile.e2e-app" -t "$APP_IMAGE" "$ROOT"
else
	docker image inspect "$APP_IMAGE" >/dev/null 2>&1 || docker pull "$APP_IMAGE"
fi

TEST_ARGS=(./e2e -count=1 -parallel "$E2E_PARALLEL" -timeout="$E2E_TIMEOUT" -v)
if [[ -n "${E2E_TEST_REGEX:-}" ]]; then
	TEST_ARGS+=(-run "$E2E_TEST_REGEX")
fi

SCMIGRATE_E2E=1 \
SCMIGRATE_RUNNER_IMAGE="$RUNNER_IMAGE" \
SCMIGRATE_E2E_APP_IMAGE="$APP_IMAGE" \
SCMIGRATE_BIN="$SCMIGRATE_BIN_PATH" \
ETCD_IMAGE="$ETCD_IMAGE" \
GOCACHE="${GOCACHE:-/tmp/scmigrate-go-cache}" \
"${GO:-go}" test "${TEST_ARGS[@]}"
