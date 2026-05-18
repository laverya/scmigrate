#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${KIND_CLUSTER_NAME:-scmigrate-e2e}"
RUNNER_IMAGE="${SCMIGRATE_RUNNER_IMAGE:-scmigrate-rsync:e2e}"
ETCD_IMAGE="${ETCD_IMAGE:-quay.io/coreos/etcd:v3.5.18}"
KUBECONFIG_FILE=""

require() {
	local name="$1"
	if ! command -v "$name" >/dev/null 2>&1; then
		echo "missing required command: $name" >&2
		exit 127
	fi
}

cleanup() {
	if [[ -n "$KUBECONFIG_FILE" ]]; then
		rm -f "$KUBECONFIG_FILE"
	fi
	if [[ "${KEEP_E2E_CLUSTER:-}" != "1" ]]; then
		kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

require docker
require kind
require kubectl
if [[ ! -x "${GO:-go}" ]] && ! command -v "${GO:-go}" >/dev/null 2>&1; then
	echo "missing required command: ${GO:-go}" >&2
	exit 127
fi

if kind get clusters | grep -Fxq "$CLUSTER"; then
	kind delete cluster --name "$CLUSTER"
fi

kind create cluster --name "$CLUSTER" --wait 120s

docker build -f "$ROOT/Dockerfile.rsync" -t "$RUNNER_IMAGE" "$ROOT"
kind load docker-image "$RUNNER_IMAGE" --name "$CLUSTER"

KUBECONFIG_FILE="$(mktemp /tmp/scmigrate-e2e-kubeconfig.XXXXXX)"
kind get kubeconfig --name "$CLUSTER" >"$KUBECONFIG_FILE"

TEST_ARGS=(./e2e -count=1 -timeout=30m -v)
if [[ -n "${E2E_TEST_REGEX:-}" ]]; then
	TEST_ARGS+=(-run "$E2E_TEST_REGEX")
fi

SCMIGRATE_E2E=1 \
SCMIGRATE_RUNNER_IMAGE="$RUNNER_IMAGE" \
SCMIGRATE_BIN="${SCMIGRATE_BIN:-$ROOT/bin/kubectl-scmigrate}" \
ETCD_IMAGE="$ETCD_IMAGE" \
KUBECONFIG="$KUBECONFIG_FILE" \
GOCACHE="${GOCACHE:-/tmp/scmigrate-go-cache}" \
"${GO:-go}" test "${TEST_ARGS[@]}"
